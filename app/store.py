from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4


MAX_RETRIEVALS = 10


@dataclass(frozen=True)
class TopicInfo:
    topic_id: str
    dead_letter_topic_id: str


@dataclass(frozen=True)
class TopicRecord:
    topic_id: str
    kind: str
    parent_topic_id: str | None
    created_at: str


class TopicNotFoundError(Exception):
    pass


class InvalidTopicOperationError(Exception):
    pass


class MessageNotFoundError(Exception):
    pass


class InvalidReceiptTokenError(Exception):
    pass


class QueueStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS topics (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL CHECK (kind IN ('user', 'dlq')),
                    parent_topic_id TEXT UNIQUE,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (parent_topic_id) REFERENCES topics(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    retrieval_count INTEGER NOT NULL DEFAULT 0,
                    lease_token TEXT,
                    lease_expires_at TEXT,
                    source_message_id INTEGER,
                    FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_topics_parent_topic_id ON topics(parent_topic_id);
                CREATE INDEX IF NOT EXISTS idx_messages_topic_fifo
                    ON messages(topic_id, created_at, id);
                """
            )

    def create_topic(self) -> TopicInfo:
        topic_id = str(uuid4())
        dead_letter_topic_id = str(uuid4())
        now = self._now()

        with self._transaction() as conn:
            conn.execute(
                "INSERT INTO topics (id, kind, parent_topic_id, created_at) VALUES (?, 'user', NULL, ?)",
                (topic_id, now),
            )
            conn.execute(
                "INSERT INTO topics (id, kind, parent_topic_id, created_at) VALUES (?, 'dlq', ?, ?)",
                (dead_letter_topic_id, topic_id, now),
            )

        return TopicInfo(topic_id=topic_id, dead_letter_topic_id=dead_letter_topic_id)

    def list_topics(self) -> list[TopicRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, kind, parent_topic_id, created_at
                FROM topics
                ORDER BY created_at ASC, id ASC
                """
            ).fetchall()

        return [
            TopicRecord(
                topic_id=str(row["id"]),
                kind=str(row["kind"]),
                parent_topic_id=str(row["parent_topic_id"]) if row["parent_topic_id"] is not None else None,
                created_at=str(row["created_at"]),
            )
            for row in rows
        ]

    def delete_topic(self, topic_id: str) -> None:
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT kind FROM topics WHERE id = ?",
                (topic_id,),
            ).fetchone()
            if row is None:
                raise TopicNotFoundError
            if row["kind"] != "user":
                raise InvalidTopicOperationError
            conn.execute("DELETE FROM topics WHERE id = ?", (topic_id,))

    def clear_topic(self, topic_id: str) -> None:
        topic = self._get_user_topic(topic_id)
        with self._transaction() as conn:
            conn.execute("DELETE FROM messages WHERE topic_id = ? OR topic_id = ?", (topic.topic_id, topic.dead_letter_topic_id))

    def put_message(self, topic_id: str, payload: Any) -> int:
        topic = self._get_user_topic(topic_id)
        payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        now = self._now()

        with self._transaction() as conn:
            cursor = conn.execute(
                "INSERT INTO messages (topic_id, payload, created_at, retrieval_count) VALUES (?, ?, ?, 0)",
                (topic.topic_id, payload_json, now),
            )
            return int(cursor.lastrowid)

    def acquire_message(self, topic_id: str, visibility_timeout_seconds: int = 30) -> dict[str, Any] | None:
        topic = self._get_user_topic(topic_id)
        now = self._now()
        lease_expires_at = self._utc_datetime() + timedelta(seconds=visibility_timeout_seconds)

        with self._transaction() as conn:
            while True:
                row = conn.execute(
                    """
                    SELECT id, payload, created_at, retrieval_count
                    FROM messages
                    WHERE topic_id = ?
                    ORDER BY created_at ASC, id ASC
                    LIMIT 1
                    """,
                    (topic.topic_id,),
                ).fetchone()

                if row is None:
                    return None

                message_id = int(row["id"])
                retrieval_count = int(row["retrieval_count"]) + 1
                payload = json.loads(row["payload"])

                if retrieval_count >= MAX_RETRIEVALS:
                    self._move_to_dead_letter(conn, topic.dead_letter_topic_id, row, retrieval_count)
                    continue

                receipt_token = str(uuid4())
                conn.execute(
                    """
                    UPDATE messages
                    SET retrieval_count = ?, lease_token = ?, lease_expires_at = ?
                    WHERE id = ?
                    """,
                    (retrieval_count, receipt_token, lease_expires_at.isoformat(), message_id),
                )
                return {
                    "message_id": message_id,
                    "payload": payload,
                    "retrieval_count": retrieval_count,
                    "receipt_token": receipt_token,
                    "lease_expires_at": lease_expires_at,
                }

    def ack_message(self, topic_id: str, message_id: int, receipt_token: str) -> None:
        topic = self._get_user_topic(topic_id)

        with self._transaction() as conn:
            row = conn.execute(
                """
                SELECT id, lease_token, lease_expires_at
                FROM messages
                WHERE id = ? AND topic_id = ?
                """,
                (message_id, topic.topic_id),
            ).fetchone()

            if row is None:
                raise MessageNotFoundError

            if row["lease_token"] != receipt_token:
                raise InvalidReceiptTokenError

            conn.execute("DELETE FROM messages WHERE id = ? AND topic_id = ?", (message_id, topic.topic_id))

    def _move_to_dead_letter(
        self,
        conn: sqlite3.Connection,
        dlq_topic_id: str,
        row: sqlite3.Row,
        retrieval_count: int,
    ) -> None:
        conn.execute(
            """
            INSERT INTO messages (topic_id, payload, created_at, retrieval_count, source_message_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (dlq_topic_id, row["payload"], row["created_at"], retrieval_count, int(row["id"])),
        )
        conn.execute("DELETE FROM messages WHERE id = ?", (int(row["id"]),))

    def _get_user_topic(self, topic_id: str) -> TopicInfo:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM topics WHERE id = ? AND kind = 'user'",
                (topic_id,),
            ).fetchone()
            if row is None:
                raise TopicNotFoundError

            dlq_row = conn.execute(
                "SELECT id FROM topics WHERE parent_topic_id = ? AND kind = 'dlq'",
                (topic_id,),
            ).fetchone()
            if dlq_row is None:
                raise TopicNotFoundError

        return TopicInfo(topic_id=topic_id, dead_letter_topic_id=str(dlq_row["id"]))

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat()

    @staticmethod
    def _utc_datetime() -> datetime:
        return datetime.now(UTC)

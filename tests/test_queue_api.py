from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from app.main import create_app


def make_client(tmp_path: Path) -> TestClient:
    app = create_app(tmp_path / ".data" / "queue.db")
    return TestClient(app)


def create_topic(client: TestClient) -> tuple[str, str]:
    response = client.post("/api/v1/topics")
    assert response.status_code == 201
    body = response.json()
    return body["topic_id"], body["dead_letter_topic_id"]


def test_list_topics_returns_all_existing_topics(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    topic1_id, dlq1_id = create_topic(client)
    topic2_id, dlq2_id = create_topic(client)

    response = client.get("/api/v1/topics")
    assert response.status_code == 200

    topics = response.json()
    topic_ids = {topic["topic_id"] for topic in topics}

    assert topic1_id in topic_ids
    assert dlq1_id in topic_ids
    assert topic2_id in topic_ids
    assert dlq2_id in topic_ids


def test_create_put_acquire_ack_fifo(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    topic_id, _ = create_topic(client)

    first = client.post(f"/api/v1/topics/{topic_id}/message", content=b"first")
    second = client.post(f"/api/v1/topics/{topic_id}/message", content=b"second")
    assert first.status_code == 201
    assert second.status_code == 201

    acquired = client.get(f"/api/v1/topics/{topic_id}/message")
    assert acquired.status_code == 200
    assert acquired.content == b"first"
    assert acquired.headers["x-queue-retrieval-count"] == "1"

    ack = client.post(
        f"/api/v1/topics/{topic_id}/message/{acquired.headers['x-queue-message-id']}/ack",
        json={"receipt_token": acquired.headers["x-queue-receipt-token"]},
    )
    assert ack.status_code == 204

    acquired_again = client.get(f"/api/v1/topics/{topic_id}/message")
    assert acquired_again.status_code == 200
    assert acquired_again.content == b"second"


def test_ack_rejects_invalid_token(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    topic_id, _ = create_topic(client)
    client.post(f"/api/v1/topics/{topic_id}/message", content=b"hello")

    acquired = client.get(f"/api/v1/topics/{topic_id}/message")

    response = client.post(
        f"/api/v1/topics/{topic_id}/message/{acquired.headers['x-queue-message-id']}/ack",
        json={"receipt_token": "invalid"},
    )
    assert response.status_code == 409


def test_retrieval_count_is_persisted_across_acquires(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    topic_id, _ = create_topic(client)
    client.post(f"/api/v1/topics/{topic_id}/message", content=b"counter")

    first = client.get(f"/api/v1/topics/{topic_id}/message")
    second = client.get(f"/api/v1/topics/{topic_id}/message")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.headers["x-queue-retrieval-count"] == "1"
    assert second.headers["x-queue-retrieval-count"] == "2"


def test_clear_topic_removes_messages_from_topic_and_dlq(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    topic_id, _ = create_topic(client)
    client.post(f"/api/v1/topics/{topic_id}/message", content=b"a")
    client.post(f"/api/v1/topics/{topic_id}/clear")

    acquire = client.get(f"/api/v1/topics/{topic_id}/message")
    assert acquire.status_code == 204


def test_moves_to_dead_letter_after_ten_acquires(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    topic_id, dlq_id = create_topic(client)
    client.post(f"/api/v1/topics/{topic_id}/message", content=b"retry-me")

    latest = None
    for _ in range(9):
        acquired = client.get(f"/api/v1/topics/{topic_id}/message")
        assert acquired.status_code == 200
        latest = acquired

    assert latest is not None

    tenth = client.get(f"/api/v1/topics/{topic_id}/message")
    assert tenth.status_code == 204

    dlq_acquire = client.get(f"/api/v1/topics/{dlq_id}/message")
    assert dlq_acquire.status_code == 404


def test_delete_topic_removes_topic_pair(tmp_path: Path) -> None:
    client = make_client(tmp_path)
    topic_id, _ = create_topic(client)

    delete = client.delete(f"/api/v1/topics/{topic_id}")
    assert delete.status_code == 204

    acquire = client.get(f"/api/v1/topics/{topic_id}/message")
    assert acquire.status_code == 404

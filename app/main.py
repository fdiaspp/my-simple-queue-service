from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Response, status

from app.schemas import (
    AckMessageRequest,
    CreateTopicResponse,
    PutMessageResponse,
    TopicListItem,
)
from app.store import (
    InvalidReceiptTokenError,
    InvalidTopicOperationError,
    MessageNotFoundError,
    QueueStore,
    TopicNotFoundError,
)


def _default_db_path() -> Path:
    project_root = Path(__file__).resolve().parents[1]
    return project_root / ".data" / "queue.db"


def create_app(db_path: Path | None = None) -> FastAPI:
    store = QueueStore(db_path or _default_db_path())
    app = FastAPI(title="Simple Queue Service", version="0.1.0")

    @app.post("/api/v1/topics", response_model=CreateTopicResponse, status_code=status.HTTP_201_CREATED)
    def create_topic() -> CreateTopicResponse:
        topic = store.create_topic()
        return CreateTopicResponse(
            topic_id=topic.topic_id,
            dead_letter_topic_id=topic.dead_letter_topic_id,
        )

    @app.get("/api/v1/topics", response_model=list[TopicListItem])
    def list_topics() -> list[TopicListItem]:
        topics = store.list_topics()
        return [
            TopicListItem(
                topic_id=topic.topic_id,
                kind=topic.kind,
                parent_topic_id=topic.parent_topic_id,
                created_at=topic.created_at,
            )
            for topic in topics
        ]

    @app.delete("/api/v1/topics/{topic_id}", status_code=status.HTTP_204_NO_CONTENT)
    def delete_topic(topic_id: str) -> None:
        try:
            store.delete_topic(topic_id)
        except TopicNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topic not found") from exc
        except InvalidTopicOperationError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid topic operation") from exc
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @app.post("/api/v1/topics/{topic_id}/clear", status_code=status.HTTP_204_NO_CONTENT)
    def clear_topic(topic_id: str) -> None:
        try:
            store.clear_topic(topic_id)
        except TopicNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topic not found") from exc
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @app.post("/api/v1/topics/{topic_id}/message", response_model=PutMessageResponse, status_code=status.HTTP_201_CREATED)
    async def put_message(topic_id: str, request: Request) -> PutMessageResponse:
        try:
            message_id = store.put_message(topic_id, await request.body())
        except TopicNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topic not found") from exc
        return PutMessageResponse(message_id=message_id)

    @app.get("/api/v1/topics/{topic_id}/message")
    def acquire_message(topic_id: str) -> Response:
        try:
            message = store.acquire_message(topic_id)
        except TopicNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topic not found") from exc

        if message is None:
            return Response(status_code=status.HTTP_204_NO_CONTENT)

        return Response(
            content=message["payload"],
            media_type="application/octet-stream",
            headers={
                "X-Queue-Message-Id": str(message["message_id"]),
                "X-Queue-Receipt-Token": message["receipt_token"],
                "X-Queue-Retrieval-Count": str(message["retrieval_count"]),
                "X-Queue-Lease-Expires-At": message["lease_expires_at"].isoformat(),
            },
        )

    @app.post("/api/v1/topics/{topic_id}/message/{message_id}/ack", status_code=status.HTTP_204_NO_CONTENT)
    def ack_message(topic_id: str, message_id: int, request: AckMessageRequest) -> None:
        try:
            store.ack_message(topic_id, message_id, request.receipt_token)
        except TopicNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topic not found") from exc
        except MessageNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Message not found") from exc
        except InvalidReceiptTokenError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Invalid receipt token") from exc
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    return app


app = create_app()

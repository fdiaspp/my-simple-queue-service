from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class CreateTopicResponse(BaseModel):
    topic_id: str
    dead_letter_topic_id: str


class TopicListItem(BaseModel):
    topic_id: str
    kind: str
    parent_topic_id: str | None
    created_at: datetime


class PutMessageResponse(BaseModel):
    message_id: int


class AckMessageRequest(BaseModel):
    receipt_token: str = Field(..., min_length=1)

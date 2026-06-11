from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class CreateTopicResponse(BaseModel):
    topic_id: str
    dead_letter_topic_id: str


class TopicListItem(BaseModel):
    topic_id: str
    kind: str
    parent_topic_id: str | None
    created_at: datetime


class PutMessageRequest(BaseModel):
    payload: Any = Field(..., description="Arbitrary JSON payload")


class PutMessageResponse(BaseModel):
    message_id: int


class AcquireMessageResponse(BaseModel):
    message_id: int
    payload: Any
    retrieval_count: int
    receipt_token: str
    lease_expires_at: datetime


class AckMessageRequest(BaseModel):
    receipt_token: str = Field(..., min_length=1)

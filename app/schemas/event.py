# Pydantic schemas

from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from uuid import UUID
from typing import Any


class EventCreate(BaseModel):
    """Schema for creating a single event"""

    event_id: UUID
    occurred_at: datetime
    user_id: str = Field(..., min_length=1, max_length=255)
    event_type: str = Field(..., min_length=1, max_length=255)
    properties: dict[str, Any] = Field(default_factory=dict)

    @field_validator('user_id', 'event_type')
    @classmethod
    def validate_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError('Field cannot be empty or whitespace')
        return v.strip()


class EventBatchCreate(BaseModel):
    """Schema for batch event creation"""

    events: list[EventCreate] = Field(..., min_length=1, max_length=1000)

    @field_validator('events')
    @classmethod
    def validate_batch_size(cls, v: list[EventCreate]) -> list[EventCreate]:
        if len(v) > 1000:
            raise ValueError('Batch size cannot exceed 1000 events')
        return v


class EventResponse(BaseModel):
    """Response schema for event operations"""

    event_id: UUID
    occurred_at: datetime
    user_id: str
    event_type: str
    properties: dict[str, Any]

    model_config = {"from_attributes": True}


class BatchIngestResponse(BaseModel):
    """Response for batch ingestion"""

    total_received: int
    inserted: int
    duplicates: int
    message: str

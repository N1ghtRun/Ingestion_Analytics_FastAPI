from pydantic import BaseModel, Field
from datetime import date
from typing import List


class DAUResponse(BaseModel):
    """Daily Active Users response"""
    date: str
    unique_users: int


class TopEventResponse(BaseModel):
    """Top events response"""
    event_type: str
    count: int


class RetentionWindow(BaseModel):
    """Single retention window data"""
    week: int
    week_start: str
    retained_users: int
    retention_rate: float


class RetentionResponse(BaseModel):
    """Cohort retention response"""
    start_date: str
    cohort_size: int
    retention: List[RetentionWindow]


class DAUQueryParams(BaseModel):
    """Query parameters for DAU endpoint"""
    from_date: date = Field(..., alias="from")
    to_date: date = Field(..., alias="to")


class TopEventsQueryParams(BaseModel):
    """Query parameters for top events endpoint"""
    from_date: date = Field(..., alias="from")
    to_date: date = Field(..., alias="to")
    limit: int = Field(default=10, ge=1, le=100)


class RetentionQueryParams(BaseModel):
    """Query parameters for retention endpoint"""
    start_date: date
    windows: int = Field(default=3, ge=1, le=12)

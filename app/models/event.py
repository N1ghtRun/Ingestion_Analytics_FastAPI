# SQLAlchemy models

from sqlalchemy import Column, String, DateTime, JSON, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
import uuid

Base = declarative_base()


class Event(Base):
    __tablename__ = "events"

    event_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    occurred_at = Column(DateTime(timezone=True), nullable=False, index=True)
    user_id = Column(String, nullable=False, index=True)
    event_type = Column(String, nullable=False, index=True)
    properties = Column(JSON, nullable=False, default=dict)

    __table_args__ = (
        # Composite index for common query patterns
        Index('idx_user_occurred', 'user_id', 'occurred_at'),
        Index('idx_type_occurred', 'event_type', 'occurred_at'),
    )

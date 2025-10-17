from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.models.event import Event
from app.schemas.event import EventCreate
import structlog

logger = structlog.get_logger()


class IngestionService:
    """Service for ingesting events with idempotency"""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def ingest_events(self, events: list[EventCreate]) -> dict[str, int]:
        """
        Ingest events with idempotency using INSERT ... ON CONFLICT

        Returns:
            dict with 'inserted' and 'duplicates' counts
        """
        if not events:
            return {"inserted": 0, "duplicates": 0}

        # Get existing event_ids to calculate duplicates
        event_ids = [event.event_id for event in events]
        existing_stmt = select(Event.event_id).where(Event.event_id.in_(event_ids))
        result = await self.db.execute(existing_stmt)
        existing_ids = {row[0] for row in result.fetchall()}

        # Prepare event data
        event_data = [
            {
                "event_id": event.event_id,
                "occurred_at": event.occurred_at,
                "user_id": event.user_id,
                "event_type": event.event_type,
                "properties": event.properties
            }
            for event in events
        ]

        # Use PostgreSQL's INSERT ... ON CONFLICT DO NOTHING for idempotency
        stmt = pg_insert(Event).values(event_data)
        stmt = stmt.on_conflict_do_nothing(index_elements=['event_id'])

        await self.db.execute(stmt)
        await self.db.commit()

        # Calculate actual counts
        duplicates = len(existing_ids)
        inserted = len(events) - duplicates

        logger.info(
            "events_ingested",
            total=len(events),
            inserted=inserted,
            duplicates=duplicates
        )

        return {"inserted": inserted, "duplicates": duplicates}

# POST /events

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.schemas.event import EventBatchCreate, BatchIngestResponse
from app.services.ingestion import IngestionService
import structlog

logger = structlog.get_logger()
router = APIRouter(prefix="/events", tags=["events"])


@router.post("", response_model=BatchIngestResponse, status_code=status.HTTP_201_CREATED)
async def ingest_events(
        batch: EventBatchCreate,
        db: AsyncSession = Depends(get_db)
):
    """
    Ingest a batch of events with idempotency.

    - **events**: List of events to ingest (max 1000)
    - Duplicate event_ids are ignored automatically
    """
    try:
        service = IngestionService(db)
        result = await service.ingest_events(batch.events)

        return BatchIngestResponse(
            total_received=len(batch.events),
            inserted=result["inserted"],
            duplicates=result["duplicates"],
            message=f"Successfully processed {len(batch.events)} events"
        )

    except Exception as e:
        logger.error("ingestion_failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to ingest events"
        )

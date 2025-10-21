from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.core.config import settings
from app.schemas.event import EventBatchCreate, BatchIngestResponse
from app.services.ingestion import IngestionService
from app.services.queue import event_queue
import structlog

logger = structlog.get_logger()
router = APIRouter(prefix="/events", tags=["events"])


@router.post("", response_model=BatchIngestResponse,
             status_code=status.HTTP_202_ACCEPTED if settings.use_queue else status.HTTP_201_CREATED)
async def ingest_events(
        batch: EventBatchCreate,
        db: AsyncSession = Depends(get_db)
):
    """
    Ingest a batch of events with idempotency.

    - **events**: List of events to ingest (max 1000)
    - Duplicate event_ids are ignored automatically

    If queue is enabled, events are processed asynchronously.
    """
    try:
        if settings.use_queue and event_queue:
            # Async processing via queue
            enqueued = event_queue.enqueue(batch.events)

            return BatchIngestResponse(
                total_received=len(batch.events),
                inserted=enqueued,
                duplicates=0,
                message=f"Accepted {enqueued} events for processing"
            )
        else:
            # Sync processing
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


@router.get("/queue/status")
async def queue_status():
    """Get queue status (only available if queue is enabled)"""
    if not settings.use_queue or not event_queue:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Queue is not enabled"
        )

    return {
        "queue_size": event_queue.get_queue_size(),
        "dead_letter_queue_size": event_queue.get_dlq_size()
    }

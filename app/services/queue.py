import json
import redis
from typing import List, Optional
from app.core.config import settings
from app.schemas.event import EventCreate
import structlog

logger = structlog.get_logger()


class EventQueue:
    """Redis-based event queue"""

    def __init__(self):
        try:
            self.redis_client = redis.from_url(settings.redis_url, decode_responses=True)
            # Test connection
            self.redis_client.ping()
            self.queue_name = "event_queue"
            self.dead_letter_queue = "event_dlq"
            self.max_retries = 3
            logger.info("event_queue_initialized", redis_url=settings.redis_url)
        except Exception as e:
            logger.error("event_queue_init_failed", error=str(e), redis_url=settings.redis_url)
            raise

    def enqueue(self, events: List[EventCreate]) -> int:
        """Add events to the queue"""
        try:
            for event in events:
                event_data = {
                    "event_id": str(event.event_id),
                    "occurred_at": event.occurred_at.isoformat(),
                    "user_id": event.user_id,
                    "event_type": event.event_type,
                    "properties": event.properties,
                    "retry_count": 0
                }
                self.redis_client.rpush(self.queue_name, json.dumps(event_data))

            logger.info("events_enqueued", count=len(events))
            return len(events)

        except Exception as e:
            logger.error("enqueue_failed", error=str(e))
            raise

    def dequeue(self, batch_size: int = 100, timeout: int = 5) -> List[dict]:
        """Get events from the queue"""
        events = []

        try:
            for _ in range(batch_size):
                result = self.redis_client.blpop(self.queue_name, timeout=timeout)

                if result is None:
                    break

                _, event_json = result
                event_data = json.loads(event_json)
                events.append(event_data)

            return events

        except Exception as e:
            logger.error("dequeue_failed", error=str(e))
            return events

    def send_to_dlq(self, event: dict):
        """Send failed event to dead letter queue"""
        try:
            self.redis_client.rpush(self.dead_letter_queue, json.dumps(event))
            logger.warning("event_sent_to_dlq", event_id=event.get("event_id"))
        except Exception as e:
            logger.error("dlq_failed", error=str(e))

    def get_queue_size(self) -> int:
        """Get current queue size"""
        return self.redis_client.llen(self.queue_name)

    def get_dlq_size(self) -> int:
        """Get dead letter queue size"""
        return self.redis_client.llen(self.dead_letter_queue)


# Initialize queue if enabled
event_queue: Optional[EventQueue] = None

if settings.use_queue:
    try:
        event_queue = EventQueue()
    except Exception as e:
        logger.error("failed_to_initialize_queue", error=str(e))
        event_queue = None

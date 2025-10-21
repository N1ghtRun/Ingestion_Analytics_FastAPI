"""
Queue Worker - Processes events from Redis queue

Usage:
    python scripts/queue_worker.py
"""
import json
import sys
import time
from pathlib import Path
from datetime import datetime
from uuid import UUID

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy.orm import sessionmaker
from app.core.database import sync_engine
from app.services.queue import event_queue
from app.models.event import Event
from sqlalchemy.dialects.postgresql import insert as pg_insert
import structlog
import duckdb
import pandas as pd

logger = structlog.get_logger()


def process_events_batch(events: list) -> dict:
    """Process a batch of events from queue"""
    if not events:
        return {"inserted": 0, "failed": 0}

    Session = sessionmaker(bind=sync_engine)

    try:
        with Session() as session:
            event_data = []

            for event in events:
                try:
                    event_data.append({
                        "event_id": UUID(event["event_id"]),
                        "occurred_at": datetime.fromisoformat(event["occurred_at"]),
                        "user_id": event["user_id"],
                        "event_type": event["event_type"],
                        "properties": event["properties"]
                    })
                except Exception as e:
                    logger.error("event_parse_failed", event=event, error=str(e))

                    # Retry logic
                    event["retry_count"] = event.get("retry_count", 0) + 1
                    if event["retry_count"] >= 3:
                        event_queue.send_to_dlq(event)
                    else:
                        # Re-queue for retry
                        event_queue.redis_client.rpush(event_queue.queue_name, str(event))

            if event_data:
                # Bulk insert with idempotency
                stmt = pg_insert(Event).values(event_data)
                stmt = stmt.on_conflict_do_nothing(index_elements=['event_id'])

                result = session.execute(stmt)
                session.commit()

                inserted = result.rowcount if result.rowcount >= 0 else len(event_data)

                logger.info(
                    "batch_processed",
                    total=len(events),
                    inserted=inserted,
                    duplicates=len(event_data) - inserted
                )

                # writing to duckdb file
                try:
                    # Convert to DataFrame
                    df = pd.DataFrame(event_data)

                    # Ensure properties is always a JSON string
                    if "properties" in df.columns:
                        df["properties"] = df["properties"].apply(
                            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else str(x))

                    if not df.empty:
                        duckdb_path = Path("./app/data/analytics.duckdb")
                        duckdb_path.parent.mkdir(parents=True, exist_ok=True)

                        # Connect to DuckDB file
                        con = duckdb.connect(str(duckdb_path))

                        # Create table if not exists
                        con.execute("""
                            CREATE TABLE IF NOT EXISTS events AS 
                            SELECT * FROM df LIMIT 0;
                        """)

                        # Append new data
                        con.append("events", df)
                        con.close()
                        logger.info("duckdb_sync_success", rows=len(df))

                except Exception as e:
                    logger.error("duckdb_sync_failed", error=str(e))

                return {"inserted": inserted, "failed": 0}

        return {"inserted": 0, "failed": 0}

    except Exception as e:
        logger.error("batch_processing_failed", error=str(e))

        # Send all events to DLQ
        for event in events:
            event_queue.send_to_dlq(event)

        return {"inserted": 0, "failed": len(events)}



def main():
    """Main worker loop"""
    logger.info("worker_started", queue=event_queue.queue_name)

    print("Queue Worker started. Press Ctrl+C to stop.")

    try:
        while True:
            # Dequeue batch of events
            events = event_queue.dequeue(batch_size=100, timeout=5)

            if events:
                result = process_events_batch(events)
                print(f"Processed batch: {result['inserted']} inserted, {result['failed']} failed")
            else:
                # No events, wait a bit
                time.sleep(1)

    except KeyboardInterrupt:
        logger.info("worker_stopped")
        print("\nWorker stopped.")
    except Exception as e:
        logger.error("worker_error", error=str(e))
        raise


if __name__ == "__main__":
    main()

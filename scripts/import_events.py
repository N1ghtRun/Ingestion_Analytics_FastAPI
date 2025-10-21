"""
CSV Import Script for Events

Usage:
    python scripts/import_events.py <path-to-csv>

CSV Format:
    event_id,occurred_at,user_id,event_type,properties_json
"""

import sys
import csv
import json
from pathlib import Path
from datetime import datetime
from uuid import UUID

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.core.config import settings
from app.models.event import Event
from sqlalchemy.dialects.postgresql import insert as pg_insert


def import_csv(file_path: str, batch_size: int = 1000):
    """
    Import events from CSV file

    Args:
        file_path: Path to CSV file
        batch_size: Number of events to process per batch
    """
    file_path = Path(file_path)

    if not file_path.exists():
        print(f"Error: File not found: {file_path}")
        sys.exit(1)

    print(f"Starting import from: {file_path}")

    # Create sync engine
    engine = create_engine(settings.database_url_sync)
    Session = sessionmaker(bind=engine)

    total_processed = 0
    total_inserted = 0
    total_duplicates = 0

    with Session() as session:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # Validate headers
            required_headers = {'event_id', 'occurred_at', 'user_id', 'event_type', 'properties_json'}
            if not required_headers.issubset(reader.fieldnames):
                print(f"Error: CSV must have headers: {required_headers}")
                print(f"Found headers: {reader.fieldnames}")
                sys.exit(1)

            batch = []

            for i, row in enumerate(reader, 1):
                try:
                    # Parse properties JSON
                    properties = {}
                    if row['properties_json'] and row['properties_json'].strip():
                        properties = json.loads(row['properties_json'])

                    # Create event data
                    event_data = {
                        "event_id": UUID(row['event_id']),
                        "occurred_at": datetime.fromisoformat(row['occurred_at'].replace('Z', '+00:00')),
                        "user_id": row['user_id'],
                        "event_type": row['event_type'],
                        "properties": properties
                    }

                    batch.append(event_data)

                    # Process batch
                    if len(batch) >= batch_size:
                        # Use INSERT ... ON CONFLICT for idempotency
                        stmt = pg_insert(Event).values(batch)
                        stmt = stmt.on_conflict_do_nothing(index_elements=['event_id'])

                        result = session.execute(stmt)
                        session.commit()

                        inserted = result.rowcount if result.rowcount >= 0 else len(batch)
                        duplicates = len(batch) - inserted

                        total_inserted += inserted
                        total_duplicates += duplicates
                        total_processed += len(batch)

                        print(f"Processed {total_processed} events | "
                              f"Inserted: {total_inserted} | "
                              f"Duplicates: {total_duplicates}")

                        batch = []

                except Exception as e:
                    print(f"Error on row {i}: {e}")
                    print(f"Row data: {row}")
                    continue

            # Process remaining events
            if batch:
                stmt = pg_insert(Event).values(batch)
                stmt = stmt.on_conflict_do_nothing(index_elements=['event_id'])

                result = session.execute(stmt)
                session.commit()

                inserted = result.rowcount if result.rowcount >= 0 else len(batch)
                duplicates = len(batch) - inserted

                total_inserted += inserted
                total_duplicates += duplicates
                total_processed += len(batch)

    print("\n" + "=" * 50)
    print("Import completed!")
    print(f"Total processed: {total_processed}")
    print(f"Total inserted: {total_inserted}")
    print(f"Total duplicates: {total_duplicates}")
    print("=" * 50)


def main():
    if len(sys.argv) != 2:
        print("Usage: python scripts/import_events.py <path-to-csv>")
        sys.exit(1)

    file_path = sys.argv[1]
    import_csv(file_path)


if __name__ == "__main__":
    main()

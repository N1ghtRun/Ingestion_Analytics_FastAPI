#!/usr/bin/env python3
"""
Benchmark Script for Event Analytics API

Tests ingestion and query performance with 100k events
"""

import sys
import time
import requests
from uuid import uuid4
from datetime import datetime, timedelta
from pathlib import Path
import statistics

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def generate_events(count: int, start_date: datetime):
    """Generate test events"""
    event_types = ["page_view", "button_click", "form_submit", "purchase", "signup"]
    events = []

    for i in range(count):
        events.append({
            "event_id": str(uuid4()),
            "occurred_at": (start_date + timedelta(seconds=i)).isoformat() + "Z",
            "user_id": f"user_{i % 10000}",  # 10k unique users
            "event_type": event_types[i % len(event_types)],
            "properties": {"test": True, "index": i}
        })

    return events


def benchmark_ingestion(base_url: str, total_events: int = 100000, batch_size: int = 1000):
    """Benchmark event ingestion"""
    print(f"\n{'=' * 60}")
    print(f"BENCHMARK: Ingesting {total_events:,} events")
    print(f"{'=' * 60}")

    start_date = datetime(2024, 3, 1, 0, 0, 0)

    total_inserted = 0
    total_duplicates = 0
    batch_times = []

    start_time = time.time()

    for i in range(0, total_events, batch_size):
        batch_start = time.time()

        events = generate_events(
            min(batch_size, total_events - i),
            start_date + timedelta(seconds=i)
        )

        try:
            response = requests.post(
                f"{base_url}/events",
                json={"events": events},
                timeout=30
            )

            if response.status_code in [201, 202]:
                data = response.json()
                total_inserted += data.get("inserted", len(events))
                total_duplicates += data.get("duplicates", 0)
            else:
                print(f"Error in batch {i // batch_size}: Status {response.status_code}")

        except Exception as e:
            print(f"Error in batch {i // batch_size}: {e}")

        batch_time = time.time() - batch_start
        batch_times.append(batch_time)

        if (i // batch_size) % 10 == 0:
            print(f"Progress: {i + len(events):,} / {total_events:,} events | "
                  f"Batch time: {batch_time:.2f}s")

    total_time = time.time() - start_time

    print(f"\n{'=' * 60}")
    print(f"INGESTION RESULTS")
    print(f"{'=' * 60}")
    print(f"Total events:        {total_events:,}")
    print(f"Inserted:            {total_inserted:,}")
    print(f"Duplicates:          {total_duplicates:,}")
    print(f"Total time:          {total_time:.2f}s")
    print(f"Events/sec:          {total_events / total_time:,.0f}")
    print(f"Avg batch time:      {statistics.mean(batch_times):.2f}s")
    print(f"Min batch time:      {min(batch_times):.2f}s")
    print(f"Max batch time:      {max(batch_times):.2f}s")
    print(f"{'=' * 60}\n")

    return total_time


def main():
    base_url = "http://localhost:8000"

    print("\n" + "=" * 60)
    print("EVENT ANALYTICS API - BENCHMARK")
    print("=" * 60)
    print(f"Target: {base_url}")
    print("=" * 60)

    # Test connection
    try:
        response = requests.get(f"{base_url}/health", timeout=5)
        if response.status_code != 200:
            print("Error: API is not healthy")
            sys.exit(1)
    except Exception as e:
        print(f"Error: Cannot connect to API: {e}")
        sys.exit(1)

    # Run benchmarks
    ingestion_time = benchmark_ingestion(base_url, total_events=100000, batch_size=1000)

    print("\n" + "=" * 60)
    print("BENCHMARK COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()

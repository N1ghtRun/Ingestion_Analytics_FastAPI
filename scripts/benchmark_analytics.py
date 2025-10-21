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


def benchmark_queries(base_url: str):
    """Benchmark analytics queries"""
    print(f"\n{'=' * 60}")
    print(f"BENCHMARK: Query Performance")
    print(f"{'=' * 60}")

    queries = [
        ("DAU (7 days)", f"{base_url}/stats/dau?from=2024-03-01&to=2024-03-07"),
        ("DAU (30 days)", f"{base_url}/stats/dau?from=2024-03-01&to=2024-03-30"),
        ("Top Events (10)", f"{base_url}/stats/top-events?from=2024-03-01&to=2024-03-30&limit=10"),
        ("Top Events (100)", f"{base_url}/stats/top-events?from=2024-03-01&to=2024-03-30&limit=100"),
        ("Retention (3 weeks)", f"{base_url}/stats/retention?start_date=2024-03-01&windows=3"),
    ]

    results = []

    for name, url in queries:
        times = []

        # Run each query 5 times
        for _ in range(5):
            start = time.time()
            try:
                response = requests.get(url, timeout=30)
                elapsed = (time.time() - start) * 1000  # Convert to ms

                if response.status_code == 200:
                    times.append(elapsed)
                else:
                    print(f"Error in {name}: Status {response.status_code}")
            except Exception as e:
                print(f"Error in {name}: {e}")

        if times:
            results.append({
                "name": name,
                "p50": statistics.median(times),
                "p95": sorted(times)[int(len(times) * 0.95)] if len(times) > 1 else times[0],
                "p99": sorted(times)[int(len(times) * 0.99)] if len(times) > 1 else times[0],
                "avg": statistics.mean(times),
                "min": min(times),
                "max": max(times)
            })

    print(f"\n{'Query':<25} {'P50':>10} {'P95':>10} {'P99':>10} {'Avg':>10}")
    print(f"{'-' * 70}")
    for r in results:
        print(f"{r['name']:<25} {r['p50']:>9.0f}ms {r['p95']:>9.0f}ms "
              f"{r['p99']:>9.0f}ms {r['avg']:>9.0f}ms")

    print(f"{'=' * 60}\n")

    return results


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

    # Run benchmark

    query_results = benchmark_queries(base_url)

    print("\n" + "=" * 60)
    print("BENCHMARK COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()

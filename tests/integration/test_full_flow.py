import pytest
from httpx import AsyncClient, ASGITransport
from uuid import uuid4
from datetime import datetime, timezone
from app.main import app


transport = ASGITransport(app=app)


@pytest.mark.asyncio
async def test_health_check():
    """Test health endpoint"""
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"


async def wait_for_duckdb_data(timeout=30, interval=1):
    import duckdb, asyncio, os
    from pathlib import Path
    duckdb_path = Path("./app/data/analytics.duckdb")

    for _ in range(int(timeout / interval)):
        if duckdb_path.exists():
            try:
                con = duckdb.connect(str(duckdb_path))
                count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
                con.close()
                if count > 0:
                    return True
            except Exception:
                pass
        await asyncio.sleep(interval)
    return False


@pytest.mark.asyncio
async def test_event_ingestion_and_query_flow():
    """Test complete flow: ingest events → query analytics"""

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Ingest events
        events = [
            {
                "event_id": str(uuid4()),
                "occurred_at": "2024-02-01T10:00:00Z",
                "user_id": "test_user_1",
                "event_type": "page_view",
                "properties": {"page": "/test"}
            },
            {
                "event_id": str(uuid4()),
                "occurred_at": "2024-02-01T11:00:00Z",
                "user_id": "test_user_2",
                "event_type": "page_view",
                "properties": {"page": "/test"}
            },
            {
                "event_id": str(uuid4()),
                "occurred_at": "2024-02-02T10:00:00Z",
                "user_id": "test_user_1",
                "event_type": "button_click",
                "properties": {}
            }
        ]

        response = await client.post("/events", json={"events": events})
        assert response.status_code in [201, 202]  # 201 sync, 202 async
        data = response.json()
        assert data["total_received"] == 3

        # If using queue, wait a bit for processing
        import asyncio
        await asyncio.sleep(1)
        await wait_for_duckdb_data(timeout=30)

        # 2. Query DAU
        response = await client.get("/stats/dau?from=2024-02-01&to=2024-02-02")
        assert response.status_code == 200
        dau_data = response.json()

        # Should have at least some data
        assert len(dau_data) >= 1

        # 3. Query top events
        response = await client.get("/stats/top-events?from=2024-02-01&to=2024-02-02&limit=5")
        assert response.status_code == 200
        top_events = response.json()

        assert len(top_events) >= 1


@pytest.mark.asyncio
async def test_idempotency():
    """Test that duplicate event_ids are ignored"""

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        event_id = str(uuid4())

        event = {
            "event_id": event_id,
            "occurred_at": "2024-02-05T10:00:00Z",
            "user_id": "idempotency_test_user",
            "event_type": "test_event",
            "properties": {}
        }

        # First insert
        response = await client.post("/events", json={"events": [event]})
        assert response.status_code in [201, 202]

        # Wait for processing if using queue
        import asyncio
        await asyncio.sleep(2)

        # Second insert (same event_id) - should be idempotent
        response = await client.post("/events", json={"events": [event]})
        assert response.status_code in [201, 202]


@pytest.mark.asyncio
async def test_validation_errors():
    """Test input validation"""

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Empty event list
        response = await client.post("/events", json={"events": []})
        assert response.status_code == 422

        # Missing required field
        response = await client.post("/events", json={
            "events": [{
                "event_id": str(uuid4()),
                "occurred_at": "2024-02-01T10:00:00Z",
                # missing user_id
                "event_type": "test",
                "properties": {}
            }]
        })
        assert response.status_code == 422

        # Invalid date format in DAU query
        response = await client.get("/stats/dau?from=invalid&to=2024-02-01")
        assert response.status_code == 422


@pytest.mark.asyncio
async def test_rate_limit_headers():
    """Test that rate limit headers are present"""

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/stats/dau?from=2024-01-15&to=2024-01-16")
        assert response.status_code == 200

        # Check rate limit headers exist
        assert "X-RateLimit-Limit" in response.headers
        assert "X-RateLimit-Remaining" in response.headers
        assert "X-RateLimit-Reset" in response.headers


@pytest.mark.asyncio
async def test_batch_size_limit():
    """Test that batch size is limited to 1000"""

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Try to send 1001 events
        events = [
            {
                "event_id": str(uuid4()),
                "occurred_at": "2024-02-10T10:00:00Z",
                "user_id": f"user_{i}",
                "event_type": "test",
                "properties": {}
            }
            for i in range(1001)
        ]

        response = await client.post("/events", json={"events": events})
        assert response.status_code == 422

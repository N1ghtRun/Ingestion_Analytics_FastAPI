# Event Analytics API

A high-performance event ingestion and analytics service built with FastAPI, PostgreSQL, Redis, and DuckDB.

## 🚀 Features

- **Async Event Ingestion**: Queue-based processing with Redis
- **Idempotent Operations**: Duplicate events automatically handled
- **Analytics Queries**: Daily Active Users (DAU), Top Events, Cohort Retention
- **Dual Storage**: PostgreSQL for transactions, DuckDB for analytics
- **Rate Limiting**: Redis-backed distributed rate limiting
- **Observability**: Structured JSON logging with request metrics

## 📋 Requirements

- Python 3.11+ (developed with Python 3.14)
- Docker & Docker Compose
- PostgreSQL 16
- Redis 7

## 🏗️ Architecture

```
┌─────────────┐
│  FastAPI    │ ──> Redis Queue ──> Worker ──┬──> PostgreSQL (OLTP)
│   (API)     │                               └──> DuckDB (OLAP)
└─────────────┘
       │
       └──> Rate Limiter (Redis)
       └──> Analytics (DuckDB)
```

**Key Components:**
- **FastAPI**: Async API with Pydantic validation
- **PostgreSQL**: Primary storage with ACID guarantees
- **Redis**: Queue management and rate limiting
- **DuckDB**: Fast analytical queries (columnar storage)
- **Worker**: Async event processing with retry logic

## 🚀 Quick Start

### 1. Clone and Setup

```bash
git clone <repository-url>
cd event-analytics

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Start Services

```bash
# Start PostgreSQL and Redis
docker-compose up -d

# Run database migrations
alembic upgrade head
```

### 3. Run Application

**Option A: Local Development**

```bash
# Terminal 1: API Server
uvicorn app.main:app --reload

# Terminal 2: Queue Worker
python scripts/queue_worker.py
```

**Option B: Docker (All-in-One)**

```bash
docker-compose up
```

API available at: `http://localhost:8000`  
Interactive docs: `http://localhost:8000/docs`

## 📡 API Endpoints

### Event Ingestion

**POST /events**

Ingest events with automatic idempotency.

```bash
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {
        "event_id": "550e8400-e29b-41d4-a716-446655440000",
        "occurred_at": "2024-01-15T10:30:00Z",
        "user_id": "user_123",
        "event_type": "page_view",
        "properties": {"page": "/home", "referrer": "google"}
      }
    ]
  }'
```

**Response:**
```json
{
  "total_received": 1,
  "inserted": 1,
  "duplicates": 0,
  "message": "Accepted 1 events for processing"
}
```

### Analytics Queries

**GET /stats/dau**

Daily Active Users for date range.

```bash
curl "http://localhost:8000/stats/dau?from=2024-01-15&to=2024-01-30"
```

**Response:**
```json
[
  {"date": "2024-01-15", "unique_users": 1250},
  {"date": "2024-01-16", "unique_users": 1340}
]
```

**GET /stats/top-events**

Top event types by count.

```bash
curl "http://localhost:8000/stats/top-events?from=2024-01-15&to=2024-01-30&limit=10"
```

**GET /stats/retention**

Weekly cohort retention analysis.

```bash
curl "http://localhost:8000/stats/retention?start_date=2024-01-15&windows=3"
```

**Response:**
```json
{
  "start_date": "2024-01-15",
  "cohort_size": 1250,
  "retention": [
    {"week": 1, "week_start": "2024-01-22", "retained_users": 850, "retention_rate": 68.0},
    {"week": 2, "week_start": "2024-01-29", "retained_users": 720, "retention_rate": 57.6}
  ]
}
```

### System Endpoints

**GET /health** - Health check  
**GET /events/queue/status** - Queue status (if enabled)

## 📊 Performance Benchmarks

**Test Setup:**
- 100,000 events ingested
- 10,000 unique users
- 30-day time range
- Local Docker setup

### Ingestion Performance

| Metric | Value                        |
|--------|------------------------------|
| **Throughput** | 2,632 events/sec             |
| **Batch Time (avg)** | 0.38 seconds per 1000 events |
| **Total Time** | 38 seconds for 100k events   |
| **Capacity** | ~250M events/day             |

### Query Performance (100k events)

| Query | P50 | P95 | P99 | Avg    |
|-------|-----|-----|-----|--------|
| DAU (7 days) | 117ms | 152ms | 152ms | 123ms  |
| DAU (30 days) | 117ms | 160ms | 160ms | 122ms  |
| Top Events (10) | 66ms | 76ms | 76ms | 67ms   |
| Top Events (100) | 65ms | 74ms | 74ms | 66ms    |
| Retention (3 weeks) | 1698ms | 1756ms | 1756ms | 1697ms |

*Note:** Docker performance significantly better due to reduced network latency between services.

## 🔍 Bottleneck Analysis

### Current Bottlenecks

**1. Ingestion (~346 events/sec)**
- **Cause**: Network latency, database write throughput
- **Current Mitigation**: 
  - Redis queue for async processing
  - Batch inserts (1000 events per batch)
  - Connection pooling
- **Future**: Multiple workers, connection pooling optimization

**2. Analytics Queries (~2.1s for 100k events)**
- **Cause**: Full table scans, complex aggregations
- **Current Mitigation**:
  - Composite indexes on (occurred_at, user_id)
  - DuckDB for columnar analytics
  - Dual-write pattern
- **Future**: Materialized views, query result caching

**3. Retention Queries (slowest at ~2.4s)**
- **Cause**: Multiple IN clause queries, self-joins
- **Future**: Pre-computed cohort tables, incremental updates

### Scaling to 10M Events/Day

**Horizontal Scaling:**
- Multiple API instances behind load balancer (nginx/traefik)
- Multiple queue workers for parallel processing
- Read replicas for analytics queries

**Database Optimization:**
- Table partitioning by date (monthly partitions)
- Archive old data (>30 days) from Postgres to DuckDB/Parquet
- Separate OLTP (Postgres) and OLAP (DuckDB/ClickHouse)

**Caching Layer:**
- Redis cache for frequent queries (DAU, top events)
- TTL-based invalidation
- CDN for public dashboards

**Infrastructure:**
- Kubernetes for auto-scaling
- Managed services (AWS RDS, ElastiCache)
- Monitoring: Prometheus + Grafana + structured logs

**Expected Performance at Scale:**
- Ingestion: 10k+ events/sec (10 workers)
- Queries: <500ms (with caching and partitioning)
- Storage: 1TB/year (~30M events/day)

## 🧪 Testing

### Run Tests

```bash
# All tests
pytest tests/ -v

# With coverage report
pytest tests/ --cov=app --cov-report=html

# Integration tests only
pytest tests/integration/ -v
```

**Test Coverage:**
- ✅ Event ingestion and idempotency
- ✅ Input validation
- ✅ Analytics query flow
- ✅ Rate limiting headers
- ✅ Batch size limits

### Run Benchmark

```bash
python scripts/benchmark_ingestion.py
```

Ingests 100k events and measures query performance.

## 📥 CSV Import

Import historical data from CSV:

```bash
python scripts/import_events.py path/to/events.csv
```

**CSV Format:**
```csv
event_id,occurred_at,user_id,event_type,properties_json
550e8400-e29b-41d4-a716-446655440000,2024-01-15T10:30:00Z,user_123,page_view,"{""page"": ""/home""}"
```

## ⚙️ Configuration

Environment variables (`.env.local` for local, `.env` for Docker):

```env
# Database
DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/events
DATABASE_URL_SYNC=postgresql://postgres:postgres@localhost:5432/events

# Redis
REDIS_URL=redis://localhost:6379/0
USE_QUEUE=true

# Rate Limiting
RATE_LIMIT_REQUESTS=100
RATE_LIMIT_PERIOD=60

# Optional
API_KEY=your-secret-key-here
DEBUG=false
```

## 🏗️ Project Structure

```
.
├── app/
│   ├── api/              # API endpoints
│   │   ├── events.py     # Event ingestion
│   │   └── stats.py      # Analytics queries
│   ├── core/             # Core configuration
│   │   ├── config.py     # Settings
│   │   └── database.py   # DB connections
│   ├── middleware/       # Middleware
│   │   └── rate_limit.py # Rate limiting
│   ├── models/           # SQLAlchemy models
│   ├── schemas/          # Pydantic schemas
│   ├── services/         # Business logic
│   │   ├── ingestion.py  # Event processing
│   │   ├── analytics.py  # Query logic
│   │   └── queue.py      # Redis queue
│   └── main.py           # FastAPI app
├── scripts/
│   ├── import_events.py  # CSV import
│   ├── queue_worker.py   # Background worker
│   ├── benchmark_ingestion.py   # Ingestion performance tests
│   └── benchmark_analytics.py   # Analytics performance tests
├── tests/
│   ├── unit/
│   └── integration/
├── alembic/              # Database migrations
├── docker-compose.yml
├── requirements.txt
├── README.md
├── ADR.md               # Architecture decisions
└── LEARNED.md           # Learning outcomes
```

## 🔒 Security & Validation

- **Input Validation**: Pydantic schemas validate all inputs
- **Rate Limiting**: Token bucket algorithm (100 req/min per IP)
- **Idempotency**: UUID-based deduplication
- **Error Handling**: Proper HTTP status codes (422, 429, 500)
- **Observability**: Structured JSON logs with correlation IDs

## 🎯 Key Features Implemented

**Functional:**
- ✅ Event ingestion with idempotency
- ✅ DAU, Top Events, Retention analytics
- ✅ CSV import for historical data

**Non-Functional:**
- ✅ Docker Compose setup
- ✅ Integration tests
- ✅ Structured logging
- ✅ Performance benchmarks
- ✅ Rate limiting

**Optional Extensions (2/5):**
- ✅ Redis queue with retry & dead-letter queue
- ✅ Dual storage (Postgres + DuckDB)

## 🚧 Future Improvements

- [ ] Materialized views for common queries
- [ ] Query result caching with Redis
- [ ] API key authentication enforcement
- [ ] Batch idempotency keys
- [ ] Prometheus metrics export
- [ ] Data retention policies
- [ ] Query filters/segmentation

## 📝 License

MIT

from datetime import date, timedelta
from typing import List, Dict, Any
from sqlalchemy import text
from app.core.database import sync_engine, get_duckdb_connection
import structlog

logger = structlog.get_logger()


class AnalyticsService:
    """Service for analytics queries using DuckDB for performance"""

    def __init__(self):
        self.use_duckdb = True
        try:
            self.duckdb_conn = get_duckdb_connection()
            logger.info("analytics_using_duckdb")
        except Exception as e:
            logger.warning("duckdb_init_failed_fallback_to_postgres", error=str(e))
            self.use_duckdb = False
            self.duckdb_conn = None

    def get_dau(self, from_date: date, to_date: date) -> List[Dict[str, Any]]:
        """Get Daily Active Users (DAU) - unique users per day"""

        if self.use_duckdb and self.duckdb_conn:
            try:
                query = """
                SELECT 
                CAST(occurred_at AS DATE) AS date,
                COUNT(DISTINCT user_id) AS unique_users
            FROM events
            WHERE occurred_at >= ? AND occurred_at < DATE(?) + INTERVAL 1 DAY
            GROUP BY CAST(occurred_at AS DATE)
            ORDER BY date
                """

                result = self.duckdb_conn.execute(query, [from_date, to_date]).fetchall()

                logger.info("dau_query_duckdb", from_date=str(from_date), to_date=str(to_date))
                return [
                    {
                        "date": str(row[0]),
                        "unique_users": row[1]
                    }
                    for row in result
                ]
            except Exception as e:
                logger.error("duckdb_query_failed_fallback", error=str(e))
                # Fall back to Postgres
                return self._get_dau_postgres(from_date, to_date)
        else:
            return self._get_dau_postgres(from_date, to_date)

    def _get_dau_postgres(self, from_date: date, to_date: date) -> List[Dict[str, Any]]:
        """Fallback: Query Postgres directly"""
        query = text("""
            SELECT
                occurred_at::date as date,
                COUNT(DISTINCT user_id) as unique_users
            FROM events
            WHERE occurred_at >= :from_date::timestamp
            AND occurred_at < (:to_date::date + interval '1 day')::timestamp
            GROUP BY occurred_at::date
            ORDER BY date
        """)

        with sync_engine.connect() as conn:
            result = conn.execute(query, {"from_date": from_date, "to_date": to_date})

            logger.info("dau_query_postgres", from_date=str(from_date), to_date=str(to_date))

            return [
                {
                    "date": str(row[0]),
                    "unique_users": row[1]
                }
                for row in result
            ]

    def get_top_events(
            self,
            from_date: date,
            to_date: date,
            limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get top event types by count"""

        if self.use_duckdb and self.duckdb_conn:
            try:
                query = """
SELECT 
    event_type,
    COUNT(*) AS count
FROM events
WHERE occurred_at >= ? 
  AND occurred_at < DATE(?) + INTERVAL 1 DAY
GROUP BY event_type
ORDER BY count DESC
LIMIT ?
                """

                result = self.duckdb_conn.execute(query, [from_date, to_date, limit]).fetchall()

                logger.info("top_events_query_duckdb")

                return [
                    {
                        "event_type": row[0],
                        "count": row[1]
                    }
                    for row in result
                ]
            except Exception as e:
                logger.error("duckdb_query_failed_fallback", error=str(e))
                return self._get_top_events_postgres(from_date, to_date, limit)
        else:
            return self._get_top_events_postgres(from_date, to_date, limit)

    def _get_top_events_postgres(
            self,
            from_date: date,
            to_date: date,
            limit: int
    ) -> List[Dict[str, Any]]:
        """Fallback: Query Postgres directly"""
        query = text("""
                SELECT 
                    event_type,
                    COUNT(*) as count
                FROM events
                WHERE occurred_at >= :from_date::timestamp 
                AND occurred_at < (:to_date::date + interval '1 day')::timestamp
                GROUP BY event_type
                ORDER BY count DESC
                LIMIT :limit
            """)

        with sync_engine.connect() as conn:
            result = conn.execute(
                query,
                {"from_date": from_date, "to_date": to_date, "limit": limit}
            )

            logger.info("top_events_query_postgres")

            return [
                {
                    "event_type": row[0],
                    "count": row[1]
                }
                for row in result
            ]

    def get_retention(
            self,
            start_date: date,
            windows: int = 3
    ) -> Dict[str, Any]:
        """Calculate weekly cohort retention - always use Postgres for complex queries"""
        week_end = start_date + timedelta(days=6)

        # Get cohort users
        cohort_query = text("""
            SELECT DISTINCT user_id
            FROM events
            WHERE DATE(occurred_at) BETWEEN :start_date AND :week_end
        """)

        with sync_engine.connect() as conn:
            result = conn.execute(
                cohort_query,
                {"start_date": start_date, "week_end": week_end}
            )
            cohort_user_ids = [row[0] for row in result]

        cohort_size = len(cohort_user_ids)

        if cohort_size == 0:
            return {
                "start_date": str(start_date),
                "cohort_size": 0,
                "retention": []
            }

        # Calculate retention for each week
        retention_rates = []

        for week in range(1, windows + 1):
            week_start = start_date + timedelta(weeks=week)
            week_end_date = week_start + timedelta(days=6)

            # Count retained users
            retention_query = text("""
                SELECT COUNT(DISTINCT user_id)
                FROM events
                WHERE user_id = ANY(:user_ids)
                AND DATE(occurred_at) BETWEEN :week_start AND :week_end
            """)

            with sync_engine.connect() as conn:
                result = conn.execute(
                    retention_query,
                    {
                        "user_ids": cohort_user_ids,
                        "week_start": week_start,
                        "week_end": week_end_date
                    }
                )
                retained_users = result.scalar() or 0

            retention_rate = (retained_users / cohort_size) * 100

            retention_rates.append({
                "week": week,
                "week_start": str(week_start),
                "retained_users": retained_users,
                "retention_rate": round(retention_rate, 2)
            })

        logger.info("retention_query_postgres")

        return {
            "start_date": str(start_date),
            "cohort_size": cohort_size,
            "retention": retention_rates
        }

    def close(self):
        """Close DuckDB connection"""
        if self.duckdb_conn:
            try:
                self.duckdb_conn.close()
            except:
                pass

# GET /stats/*

from fastapi import APIRouter, HTTPException, Query
from datetime import date
from typing import List
from app.services.analytics import AnalyticsService
from app.schemas.analytics import (
    DAUResponse,
    TopEventResponse,
    RetentionResponse
)
import structlog

logger = structlog.get_logger()
router = APIRouter(prefix="/stats", tags=["analytics"])


@router.get("/dau", response_model=List[DAUResponse])
async def get_dau(
        from_date: date = Query(..., alias="from", description="Start date (YYYY-MM-DD)"),
        to_date: date = Query(..., alias="to", description="End date (YYYY-MM-DD)")
):
    """
    Get Daily Active Users (unique users per day) for the specified date range.

    - **from**: Start date (inclusive)
    - **to**: End date (inclusive)
    """
    try:
        if from_date > to_date:
            raise HTTPException(
                status_code=400,
                detail="'from' date must be before or equal to 'to' date"
            )

        service = AnalyticsService()
        result = service.get_dau(from_date, to_date)
        service.close()

        logger.info("dau_query_executed", from_date=str(from_date), to_date=str(to_date))
        return result

    except Exception as e:
        logger.error("dau_query_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to fetch DAU data")


@router.get("/top-events", response_model=List[TopEventResponse])
async def get_top_events(
        from_date: date = Query(..., alias="from", description="Start date (YYYY-MM-DD)"),
        to_date: date = Query(..., alias="to", description="End date (YYYY-MM-DD)"),
        limit: int = Query(default=10, ge=1, le=100, description="Number of top events")
):
    """
    Get top event types by count for the specified date range.

    - **from**: Start date (inclusive)
    - **to**: End date (inclusive)
    - **limit**: Number of top events to return (max 100)
    """
    try:
        if from_date > to_date:
            raise HTTPException(
                status_code=400,
                detail="'from' date must be before or equal to 'to' date"
            )

        service = AnalyticsService()
        result = service.get_top_events(from_date, to_date, limit)
        service.close()

        logger.info(
            "top_events_query_executed",
            from_date=str(from_date),
            to_date=str(to_date),
            limit=limit
        )
        return result

    except Exception as e:
        logger.error("top_events_query_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to fetch top events")


@router.get("/retention", response_model=RetentionResponse)
async def get_retention(
        start_date: date = Query(..., description="Cohort start date (YYYY-MM-DD)"),
        windows: int = Query(default=3, ge=1, le=12, description="Number of weeks to track")
):
    """
    Calculate weekly cohort retention.

    Returns retention rates for users who had events during the start week.

    - **start_date**: Week start date for the cohort
    - **windows**: Number of weeks to track retention (max 12)
    """
    try:
        service = AnalyticsService()
        result = service.get_retention(start_date, windows)
        service.close()

        logger.info(
            "retention_query_executed",
            start_date=str(start_date),
            windows=windows
        )
        return result

    except Exception as e:
        logger.error("retention_query_failed", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to calculate retention")

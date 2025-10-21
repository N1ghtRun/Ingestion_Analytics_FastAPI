from fastapi import Request, HTTPException, status
from fastapi.responses import JSONResponse
import time
import redis
import structlog
from app.core.config import settings

logger = structlog.get_logger()


class RedisTokenBucket:
    """Redis-backed token bucket rate limiter"""

    def __init__(self, rate: int, period: int):
        """
        Args:
            rate: Number of requests allowed
            period: Time period in seconds
        """
        self.rate = rate
        self.period = period
        try:
            self.redis_client = redis.from_url(settings.redis_url, decode_responses=False)
            self.redis_client.ping()
            self.use_redis = True
            logger.info("rate_limiter_using_redis")
        except Exception as e:
            logger.warning("rate_limiter_redis_failed_using_memory", error=str(e))
            self.use_redis = False
            self.buckets = {}

    def is_allowed(self, key: str) -> bool:
        """
        Check if request is allowed for given key using Redis

        Args:
            key: Identifier (e.g., IP address or API key)

        Returns:
            True if allowed, False if rate limit exceeded
        """
        if self.use_redis:
            return self._is_allowed_redis(key)
        else:
            return self._is_allowed_memory(key)

    def _is_allowed_redis(self, key: str) -> bool:
        """Redis-based rate limiting using sliding window"""
        redis_key = f"rate_limit:{key}"
        now = time.time()
        window_start = now - self.period

        pipe = self.redis_client.pipeline()

        # Remove old entries outside the window
        pipe.zremrangebyscore(redis_key, 0, window_start)

        # Count requests in current window
        pipe.zcard(redis_key)

        # Add current request
        pipe.zadd(redis_key, {str(now): now})

        # Set expiry
        pipe.expire(redis_key, self.period)

        results = pipe.execute()

        # results[1] is the count before adding current request
        request_count = results[1]

        return request_count < self.rate

    def _is_allowed_memory(self, key: str) -> bool:
        """Fallback: in-memory token bucket"""
        if key not in self.buckets:
            self.buckets[key] = {
                "tokens": self.rate,
                "last_update": time.time()
            }

        bucket = self.buckets[key]
        now = time.time()

        # Calculate time passed and refill tokens
        time_passed = now - bucket["last_update"]
        bucket["last_update"] = now

        # Refill tokens based on time passed
        refill_amount = (time_passed / self.period) * self.rate
        bucket["tokens"] = min(self.rate, bucket["tokens"] + refill_amount)

        # Check if we have tokens available
        if bucket["tokens"] >= 1:
            bucket["tokens"] -= 1
            return True

        return False

    def get_remaining(self, key: str) -> int:
        """Get remaining requests for a key"""
        if self.use_redis:
            redis_key = f"rate_limit:{key}"
            now = time.time()
            window_start = now - self.period

            # Count requests in current window
            count = self.redis_client.zcount(redis_key, window_start, now)
            return max(0, self.rate - count)
        else:
            bucket = self.buckets.get(key)
            if not bucket:
                return self.rate
            return int(bucket["tokens"])


# Global rate limiter instance
rate_limiter = RedisTokenBucket(
    rate=settings.rate_limit_requests,
    period=settings.rate_limit_period
)


async def rate_limit_middleware(request: Request, call_next):
    """
    Rate limiting middleware

    Limits requests per IP address or API key
    """
    # Skip rate limiting for health check
    if request.url.path == "/health":
        return await call_next(request)

    # Determine rate limit key (API key or IP address)
    api_key = request.headers.get("X-API-Key")
    if api_key and settings.api_key and api_key == settings.api_key:
        # Valid API key gets higher rate limit or bypass
        rate_limit_key = f"api_key:{api_key}"
    else:
        # Use IP address for rate limiting
        client_ip = request.client.host if request.client else "unknown"
        rate_limit_key = f"ip:{client_ip}"

    # Check rate limit
    if not rate_limiter.is_allowed(rate_limit_key):
        remaining = rate_limiter.get_remaining(rate_limit_key)

        logger.warning(
            "rate_limit_exceeded",
            key=rate_limit_key,
            path=request.url.path,
            remaining=remaining
        )

        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={
                "detail": "Rate limit exceeded. Please try again later.",
                "retry_after": settings.rate_limit_period
            },
            headers={
                "X-RateLimit-Limit": str(settings.rate_limit_requests),
                "X-RateLimit-Remaining": str(max(0, remaining)),
                "X-RateLimit-Reset": str(settings.rate_limit_period),
                "Retry-After": str(settings.rate_limit_period)
            }
        )

    # Add rate limit headers to response
    response = await call_next(request)

    remaining = rate_limiter.get_remaining(rate_limit_key)
    response.headers["X-RateLimit-Limit"] = str(settings.rate_limit_requests)
    response.headers["X-RateLimit-Remaining"] = str(max(0, remaining))
    response.headers["X-RateLimit-Reset"] = str(settings.rate_limit_period)

    return response

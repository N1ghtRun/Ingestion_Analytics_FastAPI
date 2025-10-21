# Pydantic settings

from pydantic_settings import BaseSettings, SettingsConfigDict
import os


class Settings(BaseSettings):
    """Application settings"""

    # App
    app_name: str = "Event Analytics API"
    debug: bool = False

    # Database
    database_url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/events"
    database_url_sync: str = "postgresql://postgres:postgres@localhost:5432/events"

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    use_queue: bool = True

    # Rate limiting
    rate_limit_requests: int = 100
    rate_limit_period: int = 60  # seconds

    # API Key (optional)
    api_key: str | None = None

    model_config = SettingsConfigDict(
        # Use .env.local if it exists (for local dev), otherwise .env (for Docker)
        env_file=".env.local" if os.path.exists(".env.local") else ".env",
        case_sensitive=False
    )


settings = Settings()

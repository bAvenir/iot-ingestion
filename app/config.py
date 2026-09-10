"""Application configuration loaded from environment variables."""

from functools import lru_cache
from pathlib import Path
from typing import ClassVar

from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        # Anchored to the repo root so pytest and `python -m` work from any cwd.
        # Absent inside the container, where compose injects real env vars instead.
        env_file=REPO_ROOT / ".env",
        case_sensitive=False,
        extra="ignore",
    )

    LOG_LEVEL: str = "INFO"

    DATABASE_URL: str
    VALKEY_URL: str

    S3_ACCESS_KEY: str
    S3_SECRET_KEY: str
    S3_ENDPOINT: str
    S3_PORT: int = 443
    S3_USE_SSL: bool = True

    S3_PATH_STYLE: bool = True
    S3_REGION: str = "us-east-1"

    S3_BUCKET_LANDING: str
    S3_BUCKET_WAREHOUSE: str
    S3_BUCKET_PUBLISHED: str

    INLINE_PAYLOAD_MAX_BYTES: int = 65536
    BRONZE_BATCH_SIZE: int = 100
    BRONZE_BATCH_WINDOW_SECONDS: int = 10

    @property
    def s3_endpoint_url(self) -> str:
        """Full S3 endpoint URL, e.g. http://localhost:9000."""
        scheme = "https" if self.S3_USE_SSL else "http"
        return f"{scheme}://{self.S3_ENDPOINT}:{self.S3_PORT}"

    @property
    def warehouse_uri(self) -> str:
        """Iceberg warehouse location, e.g. s3://warehouse/."""
        return f"s3://{self.S3_BUCKET_WAREHOUSE}/"

    @property
    def catalog_properties(self) -> dict[str, str]:
        """Property dict for pyiceberg's SqlCatalog."""
        return {
            "type": "sql",
            "uri": self.DATABASE_URL,
            "warehouse": self.warehouse_uri,
            "s3.endpoint": self.s3_endpoint_url,
            "s3.access-key-id": self.S3_ACCESS_KEY,
            "s3.secret-access-key": self.S3_SECRET_KEY,
            "s3.region": self.S3_REGION,
            "s3.path-style-access": str(self.S3_PATH_STYLE).lower(),
        }


@lru_cache
def get_settings() -> Settings:
    """Cached settings. Lazy so a missing var fails at use, not at import."""
    return Settings()  # pyright: ignore[reportCallIssue]

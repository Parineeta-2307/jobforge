"""Settings read from environment variables (see .env.example)."""

import os


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://jobforge:jobforge@localhost:5432/jobforge")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

LEASE_SECONDS = _int("LEASE_SECONDS", 30)
HEARTBEAT_SECONDS = _int("HEARTBEAT_SECONDS", 10)
POLL_SECONDS = _int("POLL_SECONDS", 1)
BACKOFF_BASE_SECONDS = _int("BACKOFF_BASE_SECONDS", 2)
BACKOFF_MAX_SECONDS = _int("BACKOFF_MAX_SECONDS", 60)

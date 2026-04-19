"""
Central configuration for the earthquake pipeline.
All tunables live here — no magic numbers scattered through the codebase.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Project root so paths work regardless of where the script is invoked from
PROJECT_ROOT = Path(__file__).parent.parent.parent

@dataclass
class Config:
    # --- API ---
    base_url: str = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    lookback_days: int = 30
    page_size: int = 1000          # USGS max is 20,000 but 1000 is safe and debuggable
    request_timeout_seconds: int = 30
    max_retries: int = 3

    # --- Storage ---
    db_path: Path = PROJECT_ROOT / "earthquake.db"

    # --- Logging ---
    log_level: str = "INFO"
    log_file: Path = PROJECT_ROOT / "pipeline.log"

    # --- Magnitude buckets ---
    magnitude_buckets: list[tuple[float, float, str]] = field(default_factory=lambda: [
        (float("-inf"), 2.0, "<2"),
        (2.0,           4.0, "2-4"),
        (4.0,           6.0, "4-6"),
        (6.0,  float("inf"), "6+"),
    ])

    @property
    def start_time(self) -> datetime:
        return datetime.now(timezone.utc) - timedelta(days=self.lookback_days)

    @property
    def end_time(self) -> datetime:
        return datetime.now(timezone.utc)


# Singleton used everywhere — override fields in tests by instantiating a fresh Config()
config = Config()
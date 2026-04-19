"""
Pipeline orchestrator for the earthquake pipeline.

Responsibilities:
- Wire together fetch → transform → store in the correct sequence
- Own the pipeline_runs audit trail — every execution is recorded
- Guarantee fail_run is called if anything goes wrong, anywhere
- Surface errors to the caller after recording them — never swallow

Design decisions:
- Pipeline accepts Config and instantiates its own Fetcher and Storage.
  This keeps run.py dead simple and makes the Pipeline independently testable
  by patching USGSFetcher and StorageManager at the module level.
- try/except/raise pattern guarantees the audit trail is always written
  even if the failure occurs mid-run. The exception is re-raised so the
  caller (run.py / scheduler) sees the failure and can alert accordingly.
- pages_fetched is tracked on the fetcher instance so complete_run can
  log it without the pipeline needing to know pagination internals.
"""

import logging
import time
from datetime import datetime, timezone

from earthquake.config import Config
from earthquake.fetcher import USGSFetcher
from earthquake.models import EarthquakeEvent, DailyAggregate
from earthquake.storage import StorageManager
from earthquake.transform import transform

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, config: Config):
        self.config  = config
        self.fetcher = USGSFetcher(config=config)
        self.storage = StorageManager(config=config)

    def run(self) -> None:
        """
        Execute the full pipeline:
          1. Initialize schema (idempotent)
          2. Record run start
          3. Fetch all events from USGS
          4. Transform into raw events + daily aggregates
          5. Store both
          6. Record run success

        On any failure:
          - Record run as failed with error message
          - Re-raise so the caller knows the run failed
        """
        logger.info(
            "pipeline_starting",
            extra={
                "lookback_days": self.config.lookback_days,
                "db_path":       str(self.config.db_path),
                "page_size":     self.config.page_size,
            },
        )

        self.storage.initialize_schema()
        run_id = self.storage.start_run()
        start  = time.monotonic()

        try:
            # --- Fetch ---
            logger.info("stage_starting", extra={"stage": "fetch"})
            events = self.fetcher.fetch_all()
            logger.info(
                "stage_complete",
                extra={"stage": "fetch", "events": len(events)},
            )

            # --- Transform ---
            logger.info("stage_starting", extra={"stage": "transform"})
            raw_events, aggregates = transform(events, self.config)
            logger.info(
                "stage_complete",
                extra={
                    "stage":      "transform",
                    "events":     len(raw_events),
                    "aggregates": len(aggregates),
                },
            )

            # --- Store ---
            logger.info("stage_starting", extra={"stage": "store"})
            self.storage.upsert_events(raw_events)
            self.storage.upsert_aggregates(aggregates)
            logger.info("stage_complete", extra={"stage": "store"})

            # --- Complete ---
            duration = time.monotonic() - start
            pages    = getattr(self.fetcher, "pages_fetched", 0)

            self.storage.complete_run(
                run_id,
                events_fetched=len(events),
                pages_fetched=pages,
            )

            logger.info(
                "pipeline_complete",
                extra={
                    "run_id":        run_id,
                    "events":        len(events),
                    "aggregates":    len(aggregates),
                    "duration_secs": round(duration, 2),
                    "pages":         pages,
                },
            )

        except Exception as exc:
            duration = time.monotonic() - start
            error    = f"{type(exc).__name__}: {exc}"

            logger.error(
                "pipeline_failed",
                extra={
                    "run_id":        run_id,
                    "error":         error,
                    "duration_secs": round(duration, 2),
                },
                exc_info=True,
            )

            self.storage.fail_run(run_id, error=error)
            raise
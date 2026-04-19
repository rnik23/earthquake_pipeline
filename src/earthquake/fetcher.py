"""
USGS Earthquake API client.

Responsibilities:
- Hit the /count endpoint first to log expected volume
- Walk pages using offset-based pagination
- Retry transient failures with exponential backoff via tenacity
- Validate each raw feature through Pydantic models at the boundary
- Deduplicate by event_id across pages
- Return a clean List[EarthquakeEvent] to the pipeline
"""

import logging
import requests
from datetime import datetime, timezone
from typing import Any

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
    retry_if_exception_type,
)

from earthquake.config import Config
from earthquake.models import EarthquakeEvent

logger = logging.getLogger(__name__)

# Retry on any network-level failure — not on 4xx (those are our bugs)
RETRYABLE = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


class USGSFetcher:
    def __init__(self, config: Config):
        self.config = config
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch_all(self) -> list[EarthquakeEvent]:
        """
        Fetch all earthquake events for the configured lookback window.
        Paginates automatically. Deduplicates by event_id.
        Returns a flat list of validated EarthquakeEvent objects.
        """
        total = self.fetch_count()
        estimated_pages = -(-total // self.config.page_size)  # ceiling division
        logger.info(
            "starting_fetch",
            extra={
                "total_events": total,
                "estimated_pages": estimated_pages,
                "page_size": self.config.page_size,
                "start_time": self.config.start_time.isoformat(),
                "end_time": self.config.end_time.isoformat(),
            },
        )

        seen_ids: set[str] = set()
        all_events: list[EarthquakeEvent] = []
        offset = 1
        page = 0
        self.pages_fetched = 0

        while True:
            page += 1
            self.pages_fetched = page
            events = self.fetch_page(offset=offset)

            # Deduplicate across pages
            new_events = [e for e in events if e.event_id not in seen_ids]
            dupes = len(events) - len(new_events)

            for e in new_events:
                seen_ids.add(e.event_id)
            all_events.extend(new_events)

            logger.info(
                "page_fetched",
                extra={
                    "page": page,
                    "offset": offset,
                    "events_on_page": len(events),
                    "new_events": len(new_events),
                    "duplicates_skipped": dupes,
                    "running_total": len(all_events),
                },
            )

            # Terminal condition — short page means we've exhausted results
            if len(events) < self.config.page_size:
                logger.info(
                    "pagination_complete",
                    extra={"total_fetched": len(all_events), "pages": page},
                )
                break

            offset += self.config.page_size

        return all_events

    def fetch_count(self) -> int:
        """
        Hit the USGS /count endpoint to get total events for the window.
        Used for logging and progress estimation only — not for loop control.
        """
        params = self._base_params()
        params["format"] = "geojson"

        response = self._get(
            url=self.config.base_url.replace("/query", "/count").rstrip("/").rsplit("/", 1)[0] + "/count",
            params=params,
        )
        count = response.json().get("count", 0)
        logger.debug("count_fetched", extra={"count": count})
        return count

    def fetch_page(self, offset: int) -> list[EarthquakeEvent]:
        """
        Fetch a single page of results at the given offset.
        Invalid features are logged and skipped — never crash the pipeline.
        """
        params = self._base_params()
        params.update({
            "format": "geojson",
            "limit": self.config.page_size,
            "offset": offset,
            "orderby": "time-asc",
        })

        response = self._get(url=self.config.base_url, params=params)
        features = response.json().get("features", [])

        events = []
        for feature in features:
            try:
                events.append(EarthquakeEvent.from_usgs_feature(feature))
            except Exception as exc:
                logger.warning(
                    "feature_parse_failed",
                    extra={
                        "event_id": feature.get("id", "unknown"),
                        "error": str(exc),
                    },
                )

        return events

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _base_params(self) -> dict[str, Any]:
        return {
            "starttime": self.config.start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "endtime": self.config.end_time.strftime("%Y-%m-%dT%H:%M:%S"),
        }

    @retry(
        retry=retry_if_exception_type(RETRYABLE),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _get(self, url: str, params: dict) -> requests.Response:
        """
        Thin wrapper around requests.get with retry logic baked in.
        Only retries on network errors — 4xx/5xx are raised immediately
        via raise_for_status so they surface as bugs, not retried silently.
        """
        logger.debug("http_get", extra={"url": url, "params": params})
        response = requests.get(
            url,
            params=params,
            timeout=self.config.request_timeout_seconds,
        )
        response.raise_for_status()
        return response
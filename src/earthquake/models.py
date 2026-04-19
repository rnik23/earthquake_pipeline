"""
Pydantic models that define the shape of data moving through the pipeline.
Validates at the boundary (API response) so the rest of the code can trust its inputs.
"""

from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field, field_validator


class EarthquakeEvent(BaseModel):
    """Represents a single raw earthquake event from the USGS API."""

    event_id: str
    magnitude: Optional[float]
    place: Optional[str]
    occurred_at: datetime          # UTC
    latitude: Optional[float]
    longitude: Optional[float]
    depth_km: Optional[float]
    event_type: Optional[str]
    raw_status: Optional[str]

    @field_validator("occurred_at", mode="before")
    @classmethod
    def parse_epoch_ms(cls, v: int | float | datetime) -> datetime:
        """USGS returns epoch milliseconds — convert to UTC datetime."""
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v / 1000, tz=timezone.utc)
        return v

    @classmethod
    def from_usgs_feature(cls, feature: dict) -> "EarthquakeEvent":
        """Parse a single GeoJSON feature from the USGS response."""
        props = feature.get("properties", {})
        geometry = feature.get("geometry", {})
        coords = geometry.get("coordinates") or []

        return cls(
            event_id=feature["id"],
            magnitude=props.get("mag"),
            place=props.get("place"),
            occurred_at=props["time"],
            latitude=coords[1] if len(coords) > 1 else None,
            longitude=coords[0] if len(coords) > 0 else None,
            depth_km=coords[2] if len(coords) > 2 else None,
            event_type=props.get("type"),
            raw_status=props.get("status"),
        )


class DailyAggregate(BaseModel):
    """Daily count of earthquakes per magnitude bucket."""

    date: str                      # ISO format: YYYY-MM-DD
    bucket: str                    # e.g. "0-2", "2-4", "4-6", "6+"
    count: int = Field(ge=0)
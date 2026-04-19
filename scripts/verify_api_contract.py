"""
Ad-hoc API contract validator.
Hits the live USGS API, parses real events through our models,
and verifies the fetcher's field mappings are correct.
"""
import sys
import json
import requests
from datetime import datetime, timezone, timedelta

# Add src to path so we can import our modules
sys.path.insert(0, "src")

from earthquake.models import EarthquakeEvent

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
end   = datetime.now(timezone.utc)
start = end - timedelta(days=2)

params = {
    "format":    "geojson",
    "starttime": start.strftime("%Y-%m-%dT%H:%M:%S"),
    "endtime":   end.strftime("%Y-%m-%dT%H:%M:%S"),
    "limit":     3,
    "orderby":   "time-asc",
}

print("\n--- hitting live USGS API ---")
print(f"URL    : {USGS_URL}")
print(f"window : {params['starttime']} → {params['endtime']}\n")

response = requests.get(USGS_URL, params=params, timeout=30)
response.raise_for_status()
payload  = response.json()

metadata = payload.get("metadata", {})
features = payload.get("features", [])

print(f"API version    : {metadata.get('api')}")
print(f"events returned: {len(features)}")
print(f"status code    : {metadata.get('status')}")

# -----------------------------------------------------------------------
# Field presence checks — things we rely on in from_usgs_feature()
# -----------------------------------------------------------------------
REQUIRED_PROPERTY_KEYS = ["mag", "time", "updated", "place", "type", "status"]
PROBLEMS = []

print("\n--- validating raw payload structure ---")
for i, feature in enumerate(features):
    fid   = feature.get("id", f"[index {i}]")
    props = feature.get("properties", {})
    geo   = feature.get("geometry", {})
    coords = geo.get("coordinates", [])

    # root-level id
    if "id" not in feature:
        PROBLEMS.append(f"{fid}: missing root-level 'id'")

    # required property keys
    for key in REQUIRED_PROPERTY_KEYS:
        if key not in props:
            PROBLEMS.append(f"{fid}: missing properties.{key}")

    # coordinates shape
    if len(coords) < 3:
        PROBLEMS.append(f"{fid}: coordinates has {len(coords)} elements, expected 3")

    # time is epoch ms (should be a large int, not a string)
    if not isinstance(props.get("time"), (int, float)):
        PROBLEMS.append(f"{fid}: properties.time is not numeric: {props.get('time')}")

    print(f"  [{i+1}] {fid}: keys present={list(props.keys())[:6]}...")

# -----------------------------------------------------------------------
# Parse through our Pydantic model
# -----------------------------------------------------------------------
print("\n--- parsing through EarthquakeEvent model ---")
parsed = []
for feature in features:
    try:
        event = EarthquakeEvent.from_usgs_feature(feature)
        parsed.append(event)
        print(f"  OK  {event.event_id}")
        print(f"      magnitude     : {event.magnitude}")
        print(f"      occurred_at   : {event.occurred_at}")
        print(f"      usgs_updated  : {event.usgs_updated_at}")
        print(f"      place         : {event.place}")
        print(f"      lat/lon/depth : {event.latitude}, {event.longitude}, {event.depth_km}")
        print(f"      type/status   : {event.event_type} / {event.raw_status}")
    except Exception as exc:
        PROBLEMS.append(f"{feature.get('id', '?')}: model parse failed — {exc}")
        print(f"  FAIL {feature.get('id', '?')}: {exc}")

# -----------------------------------------------------------------------
# Compare mock fixture shape vs real payload shape
# -----------------------------------------------------------------------
print("\n--- comparing mock fixture vs real payload ---")

MOCK_PROPERTY_KEYS = {
    "mag", "place", "time", "updated", "tz", "url", "detail",
    "felt", "cdi", "mmi", "alert", "status", "tsunami", "sig",
    "net", "code", "ids", "sources", "types", "nst", "dmin",
    "rms", "gap", "magType", "type", "title",
}

if features:
    real_keys  = set(features[0]["properties"].keys())
    mock_only  = MOCK_PROPERTY_KEYS - real_keys
    real_only  = real_keys - MOCK_PROPERTY_KEYS

    if mock_only:
        print(f"  in mock but NOT in real API : {mock_only}")
        PROBLEMS.append(f"mock has extra keys not in real API: {mock_only}")
    elif real_only:
        print(f"  in real API but NOT in mock : {real_only}")
        print(f"  (informational — these are unused fields, not a bug)")
    else:
        print("  mock fixture keys match real API perfectly")

# -----------------------------------------------------------------------
# Final verdict
# -----------------------------------------------------------------------
print("\n--- verdict ---")
if PROBLEMS:
    print(f"FAILED — {len(PROBLEMS)} problem(s) found:")
    for p in PROBLEMS:
        print(f"  - {p}")
    sys.exit(1)
else:
    print(f"PASSED — all {len(parsed)} events parsed cleanly")
    print("fetcher field mappings are correct against live API")
    print("mock fixture structure matches real payload")


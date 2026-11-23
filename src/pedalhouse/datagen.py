"""Synthetic raw data for Pedalhouse, a bike-share company in three cities.

Each call to ``generate_batch`` simulates one day of operations and writes:

    <landing>/stations/stations.csv                 station reference data (written once)
    <landing>/trips/lisbon/batch_0001.csv           Lisbon trips, CSV
    <landing>/trips/montreal/batch_0001.csv         Montreal trips, CSV
    <landing>/trips/seoul/batch_0001.json           Seoul trips, nested JSON lines
    <landing>/members_cdc/batch_0001.json           member change events (INSERT/UPDATE/DELETE)
    <landing>/_state/state.json                     what was generated, used by the grader

The data is deliberately realistic, which means messy:

* about ``bad_rate`` of the trips break a quality rule (end before start,
  missing station, unknown bike type, unparseable end time)
* a few trips are suspiciously long (8 hours), valid but worth a warning
* from batch 3 onward, Lisbon adds a new column ``helmet_used`` (schema drift)
* the members feed contains plan changes, deletes and out-of-order events
* one station has no coordinates

Everything is pure Python (no Spark), so it runs in a notebook, in a job
task, or on your laptop. On Databricks, Unity Catalog volumes are plain
folders under /Volumes, so ``open()`` works on them.
"""

from __future__ import annotations

import csv
import json
import os
import random
from datetime import datetime, timedelta

FIRST_DAY = datetime(2026, 11, 2)
TS_FORMAT = "%Y-%m-%d %H:%M:%S"

CITIES = {
    "lisbon": {
        "code": "LIS", "format": "csv", "center": (38.7223, -9.1393),
        "stations": ["Alfama", "Baixa", "Belem", "Chiado", "Graca", "Oriente",
                     "Principe Real", "Santos"],
    },
    "montreal": {
        "code": "MTL", "format": "csv", "center": (45.5017, -73.5673),
        "stations": ["Plateau", "Old Port", "Mile End", "Griffintown", "Westmount",
                     "Verdun", "Outremont", "Rosemont"],
    },
    "seoul": {
        "code": "SEL", "format": "json", "center": (37.5665, 126.9780),
        "stations": ["Gangnam", "Hongdae", "Itaewon", "Jongno", "Mapo", "Seongsu",
                     "Yeouido", "Jamsil"],
    },
}

PLANS = ("basic", "plus", "pro")
BAD_KINDS = ("ends_before_start", "missing_station", "unknown_bike_type", "unparseable_end")
TRIP_CSV_COLUMNS = ["trip_id", "member_id", "bike_id", "bike_type", "start_station_id",
                    "end_station_id", "started_at", "ended_at"]
SCHEMA_DRIFT_FROM_BATCH = 3

FIRST_NAMES = ["Ana", "Joao", "Ines", "Liam", "Chloe", "Noah", "Min-jun", "Seo-yeon",
               "Ji-ho", "Sofia", "Lucas", "Emma", "Rafael", "Yuna", "Hugo", "Mei"]
LAST_NAMES = ["Silva", "Costa", "Tremblay", "Roy", "Gagnon", "Kim", "Lee", "Park",
              "Santos", "Martin", "Choi", "Pereira", "Bouchard", "Jung"]


# --------------------------------------------------------------------------- helpers

def _write_csv(path: str, rows: list[dict], columns: list[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: ("" if row.get(k) is None else row.get(k)) for k in columns})


def _write_jsonl(path: str, rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def _state_path(landing_dir: str) -> str:
    return os.path.join(landing_dir, "_state", "state.json")


def load_state(landing_dir: str) -> dict:
    """Return what has been generated so far (empty state if nothing yet)."""
    path = _state_path(landing_dir)
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {"next_batch": 1, "next_member_seq": 1, "members": {}, "batches": {},
            "out_of_order_members": {}, "poison_files": []}


def _save_state(landing_dir: str, state: dict) -> None:
    path = _state_path(landing_dir)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)


def _ts(dt: datetime) -> str:
    return dt.strftime(TS_FORMAT)


# --------------------------------------------------------------------------- stations

def station_rows() -> list[dict]:
    """24 valid stations (8 per city) plus one broken station with no coordinates."""
    rng = random.Random(7)
    rows = []
    for city, info in CITIES.items():
        lat0, lon0 = info["center"]
        for i, name in enumerate(info["stations"], start=1):
            rows.append({
                "station_id": f"{info['code']}-{i:02d}",
                "station_name": name,
                "city": city,
                "lat": round(lat0 + rng.uniform(-0.04, 0.04), 6),
                "lon": round(lon0 + rng.uniform(-0.04, 0.04), 6),
                "capacity": rng.choice([15, 20, 25, 30, 40]),
            })
    # A station someone added in the back office without coordinates.
    rows.append({"station_id": "SEL-99", "station_name": "Pop-up (no GPS)", "city": "seoul",
                 "lat": None, "lon": None, "capacity": 10})
    return rows


STATION_COLUMNS = ["station_id", "station_name", "city", "lat", "lon", "capacity"]
VALID_STATION_COUNT = sum(len(c["stations"]) for c in CITIES.values())


# --------------------------------------------------------------------------- members

def _member_events(rng: random.Random, state: dict, batch_no: int, day: datetime) -> list[dict]:
    """Change events for the members feed, applied to ``state['members']`` as they happen."""
    events: list[dict] = []
    members = state["members"]

    def new_member(at: datetime) -> None:
        seq = state["next_member_seq"]
        state["next_member_seq"] += 1
        member_id = f"M{seq:05d}"
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        record = {
            "member_id": member_id,
            "full_name": f"{first} {last}",
            "email": f"{first.lower().replace('-', '')}.{last.lower()}{seq}@example.com",
            "home_city": rng.choice(list(CITIES)),
            "plan": rng.choice(PLANS),
        }
        members[member_id] = {**record, "active": True}
        events.append({**record, "operation": "INSERT", "event_ts": _ts(at)})

    if batch_no == 1:
        for _ in range(120):
            new_member(day + timedelta(minutes=rng.randint(0, 180)))
        return events

    # New sign-ups happen early in the morning, before anyone rides.
    for _ in range(8):
        new_member(day + timedelta(minutes=rng.randint(0, 300)))

    active = [m for m, r in members.items() if r["active"]]
    rng.shuffle(active)
    changing, email_only, leaving, disordered = active[:10], active[10:14], active[14:16], active[16]

    # Plan changes during the day: trips before and after the change pay differently.
    for member_id in changing:
        rec = members[member_id]
        rec["plan"] = rng.choice([p for p in PLANS if p != rec["plan"]])
        at = day + timedelta(hours=rng.randint(7, 20), minutes=rng.randint(0, 59))
        events.append({**{k: rec[k] for k in ("member_id", "full_name", "email", "home_city", "plan")},
                       "operation": "UPDATE", "event_ts": _ts(at)})

    # Email changes: not interesting history, a good candidate for SCD1-style tracking.
    for member_id in email_only:
        rec = members[member_id]
        rec["email"] = rec["email"].replace("@example.com", f".b{batch_no}@example.com")
        at = day + timedelta(hours=rng.randint(7, 20), minutes=rng.randint(0, 59))
        events.append({**{k: rec[k] for k in ("member_id", "full_name", "email", "home_city", "plan")},
                       "operation": "UPDATE", "event_ts": _ts(at)})

    # Out-of-order delivery: the 18:00 change is written BEFORE the 09:00 change.
    rec = members[disordered]
    first_plan = rng.choice([p for p in PLANS if p != rec["plan"]])
    final_plan = rng.choice([p for p in PLANS if p != first_plan])
    base = {k: rec[k] for k in ("member_id", "full_name", "email", "home_city")}
    late = {**base, "plan": final_plan, "operation": "UPDATE",
            "event_ts": _ts(day + timedelta(hours=18))}
    early = {**base, "plan": first_plan, "operation": "UPDATE",
             "event_ts": _ts(day + timedelta(hours=9))}
    events.extend([late, early])  # file order: newest first
    rec["plan"] = final_plan
    state["out_of_order_members"][disordered] = {"batch": batch_no, "expected_plan": final_plan}

    # Members leaving late at night, after their last ride of the day.
    for member_id in leaving:
        rec = members[member_id]
        rec["active"] = False
        events.append({**{k: rec[k] for k in ("member_id", "full_name", "email", "home_city", "plan")},
                       "operation": "DELETE", "event_ts": _ts(day + timedelta(hours=23, minutes=30))})
    return events


# --------------------------------------------------------------------------- trips

def _trip(rng, city, info, batch_no, seq, day, members_in_city) -> dict:
    stations = [f"{info['code']}-{i:02d}" for i in range(1, len(info["stations"]) + 1)]
    start = day + timedelta(hours=6, minutes=rng.randint(0, 16 * 60 - 1), seconds=rng.randint(0, 59))
    electric = rng.random() < 0.35
    minutes = rng.uniform(3, 25) if electric else rng.uniform(4, 45)
    member_id = rng.choice(members_in_city) if members_in_city and rng.random() < 0.7 else None
    return {
        "trip_id": f"{info['code']}-{batch_no:04d}-{seq:05d}",
        "member_id": member_id,
        "bike_id": f"{info['code']}-B{rng.randint(100, 499)}",
        "bike_type": "electric" if electric else "classic",
        "start_station_id": rng.choice(stations),
        "end_station_id": rng.choice(stations),
        "started_at": start,
        "ended_at": start + timedelta(minutes=minutes),
    }


def _break(rng, trip: dict, kind: str) -> dict:
    if kind == "ends_before_start":
        trip["ended_at"] = trip["started_at"] - timedelta(minutes=rng.randint(5, 30))
    elif kind == "missing_station":
        trip[rng.choice(["start_station_id", "end_station_id"])] = None
    elif kind == "unknown_bike_type":
        trip["bike_type"] = rng.choice(["unicycle", "scooter", "ELECTRIC?"])
    elif kind == "unparseable_end":
        trip["ended_at"] = "not-a-time"
    return trip


def _to_seoul_json(t: dict) -> dict:
    """Seoul's system sends nested JSON with different field names."""
    def fmt(v):
        return _ts(v) if isinstance(v, datetime) else v
    return {
        "trip_id": t["trip_id"],
        "rider": {"member_id": t["member_id"],
                  "rider_type": "member" if t["member_id"] else "casual"},
        "bike": {"bike_id": t["bike_id"], "type": t["bike_type"]},
        "route": {"from_station": t["start_station_id"], "to_station": t["end_station_id"]},
        "times": {"start": fmt(t["started_at"]), "end": fmt(t["ended_at"])},
    }


# --------------------------------------------------------------------------- public API

def generate_batch(landing_dir: str, trips_per_city: int = 250, bad_rate: float = 0.04,
                   seed: int = 2026) -> dict:
    """Simulate the next day of operations and write its files. Returns a summary."""
    state = load_state(landing_dir)
    batch_no = state["next_batch"]
    day = FIRST_DAY + timedelta(days=batch_no - 1)
    rng = random.Random(seed * 1000 + batch_no)
    files: list[str] = []

    stations_file = os.path.join(landing_dir, "stations", "stations.csv")
    if not os.path.exists(stations_file):
        _write_csv(stations_file, station_rows(), STATION_COLUMNS)
        files.append(stations_file)

    events = _member_events(rng, state, batch_no, day)
    members_file = os.path.join(landing_dir, "members_cdc", f"batch_{batch_no:04d}.json")
    _write_jsonl(members_file, events)
    files.append(members_file)

    city_summary = {}
    for city, info in CITIES.items():
        members_in_city = [m for m, r in state["members"].items()
                           if r["active"] and r["home_city"] == city]
        trips, bad, outliers = [], 0, 0
        for seq in range(1, trips_per_city + 1):
            trip = _trip(rng, city, info, batch_no, seq, day, members_in_city)
            roll = rng.random()
            if roll < bad_rate:
                trip = _break(rng, trip, BAD_KINDS[seq % len(BAD_KINDS)])
                bad += 1
            elif roll < bad_rate + 0.01:
                trip["ended_at"] = trip["started_at"] + timedelta(hours=8, minutes=rng.randint(0, 59))
                outliers += 1
            trips.append(trip)

        if info["format"] == "csv":
            columns = list(TRIP_CSV_COLUMNS)
            rows = []
            for t in trips:
                row = {k: (_ts(v) if isinstance(v, datetime) else v) for k, v in t.items()}
                if city == "lisbon" and batch_no >= SCHEMA_DRIFT_FROM_BATCH:
                    row["helmet_used"] = str(rng.random() < 0.6).lower()
                rows.append(row)
            if city == "lisbon" and batch_no >= SCHEMA_DRIFT_FROM_BATCH:
                columns.append("helmet_used")
            path = os.path.join(landing_dir, "trips", city, f"batch_{batch_no:04d}.csv")
            _write_csv(path, rows, columns)
        else:
            path = os.path.join(landing_dir, "trips", city, f"batch_{batch_no:04d}.json")
            _write_jsonl(path, [_to_seoul_json(t) for t in trips])
        files.append(path)
        city_summary[city] = {"rows": len(trips), "bad": bad, "outliers": outliers,
                              "file": os.path.basename(path)}

    summary = {
        "batch_no": batch_no,
        "day": day.strftime("%Y-%m-%d"),
        "cities": city_summary,
        "member_events": len(events),
        "active_members": sum(1 for r in state["members"].values() if r["active"]),
        "schema_drift": batch_no >= SCHEMA_DRIFT_FROM_BATCH,
        "files": [os.path.relpath(f, landing_dir) for f in files],
    }
    state["batches"][str(batch_no)] = summary
    state["next_batch"] = batch_no + 1
    _save_state(landing_dir, state)
    return summary


def write_poison_file(landing_dir: str, rows: int = 3) -> str:
    """Write Lisbon trips with NO trip_id. The pipeline is told to FAIL on these.

    Used in lab 03 to see ``ON VIOLATION FAIL UPDATE`` stop a pipeline, and to
    practice recovering from it.
    """
    state = load_state(landing_dir)
    n = len(state["poison_files"]) + 1
    day = FIRST_DAY + timedelta(days=max(state["next_batch"] - 2, 0))
    trips = []
    for i in range(rows):
        start = day + timedelta(hours=12, minutes=i * 7)
        trips.append({"trip_id": None, "member_id": None, "bike_id": "LIS-B999",
                      "bike_type": "classic", "start_station_id": "LIS-01",
                      "end_station_id": "LIS-02", "started_at": _ts(start),
                      "ended_at": _ts(start + timedelta(minutes=12))})
    path = os.path.join(landing_dir, "trips", "lisbon", f"poison_{n:02d}.csv")
    _write_csv(path, trips, TRIP_CSV_COLUMNS)
    state["poison_files"].append(os.path.relpath(path, landing_dir))
    _save_state(landing_dir, state)
    return path


def fix_poison_files(landing_dir: str) -> list[str]:
    """The provider re-sends the bad files, corrected, under the same name.

    The failed micro-batch is replayed from the same file paths, so after
    this fix the next pipeline update succeeds without a full refresh.
    """
    state = load_state(landing_dir)
    fixed = []
    for rel in state["poison_files"]:
        path = os.path.join(landing_dir, rel)
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        tag = os.path.splitext(os.path.basename(path))[0].upper().replace("_", "")
        for i, row in enumerate(rows, start=1):
            if not row["trip_id"]:
                row["trip_id"] = f"LIS-{tag}-{i:05d}"
        _write_csv(path, rows, TRIP_CSV_COLUMNS)
        fixed.append(path)
    return fixed


def expected_totals(landing_dir: str) -> dict:
    """Totals across every generated batch, for checking pipeline results."""
    state = load_state(landing_dir)
    totals = {"batches": 0, "rows": 0, "bad": 0, "outliers": 0, "per_city": {}}
    for summary in state["batches"].values():
        totals["batches"] += 1
        for city, c in summary["cities"].items():
            pc = totals["per_city"].setdefault(city, {"rows": 0, "bad": 0, "outliers": 0})
            for key in ("rows", "bad", "outliers"):
                pc[key] += c[key]
                totals[key] += c[key]
    totals["active_members"] = sum(1 for r in state["members"].values() if r["active"])
    totals["total_members_ever"] = len(state["members"])
    return totals

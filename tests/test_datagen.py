"""Tests for the synthetic data generator (pure Python, no Spark)."""

import csv
import json
import os

from pedalhouse import datagen


def _read_csv(path):
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _read_jsonl(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def test_first_batch_writes_every_file(tmp_path):
    landing = str(tmp_path)
    summary = datagen.generate_batch(landing, trips_per_city=50)

    assert summary["batch_no"] == 1
    for rel in ["stations/stations.csv", "trips/lisbon/batch_0001.csv",
                "trips/montreal/batch_0001.csv", "trips/seoul/batch_0001.json",
                "members_cdc/batch_0001.json", "_state/state.json"]:
        assert os.path.exists(os.path.join(landing, rel)), rel

    stations = _read_csv(os.path.join(landing, "stations/stations.csv"))
    assert len(stations) == datagen.VALID_STATION_COUNT + 1
    assert [s["station_id"] for s in stations if s["lat"] == ""] == ["SEL-99"]

    events = _read_jsonl(os.path.join(landing, "members_cdc/batch_0001.json"))
    assert len(events) == 120 and {e["operation"] for e in events} == {"INSERT"}


def test_batches_are_deterministic(tmp_path):
    a, b = str(tmp_path / "a"), str(tmp_path / "b")
    for landing in (a, b):
        datagen.generate_batch(landing, trips_per_city=40)
        datagen.generate_batch(landing, trips_per_city=40)
    for rel in ["trips/lisbon/batch_0002.csv", "members_cdc/batch_0002.json"]:
        with open(os.path.join(a, rel)) as fa, open(os.path.join(b, rel)) as fb:
            assert fa.read() == fb.read(), rel


def test_seoul_is_nested_json(tmp_path):
    landing = str(tmp_path)
    datagen.generate_batch(landing, trips_per_city=20)
    row = _read_jsonl(os.path.join(landing, "trips/seoul/batch_0001.json"))[0]
    assert set(row) == {"trip_id", "rider", "bike", "route", "times"}
    assert set(row["times"]) == {"start", "end"}


def test_schema_drift_starts_at_batch_3(tmp_path):
    landing = str(tmp_path)
    for _ in range(3):
        datagen.generate_batch(landing, trips_per_city=20)
    header_1 = list(_read_csv(os.path.join(landing, "trips/lisbon/batch_0001.csv"))[0])
    header_3 = list(_read_csv(os.path.join(landing, "trips/lisbon/batch_0003.csv"))[0])
    montreal_3 = list(_read_csv(os.path.join(landing, "trips/montreal/batch_0003.csv"))[0])
    assert "helmet_used" not in header_1
    assert header_3[-1] == "helmet_used"
    assert "helmet_used" not in montreal_3


def test_out_of_order_events_and_member_bookkeeping(tmp_path):
    landing = str(tmp_path)
    datagen.generate_batch(landing, trips_per_city=20)
    datagen.generate_batch(landing, trips_per_city=20)
    state = datagen.load_state(landing)

    [(member_id, info)] = state["out_of_order_members"].items()
    events = [e for e in _read_jsonl(os.path.join(landing, "members_cdc/batch_0002.json"))
              if e["member_id"] == member_id]
    assert len(events) == 2
    assert events[0]["event_ts"] > events[1]["event_ts"], "newest event must be written first"
    assert events[0]["plan"] == info["expected_plan"]
    assert state["members"][member_id]["plan"] == info["expected_plan"]

    ops = [e["operation"] for e in _read_jsonl(os.path.join(landing, "members_cdc/batch_0002.json"))]
    assert ops.count("DELETE") == 2 and ops.count("INSERT") == 8
    assert datagen.expected_totals(landing)["active_members"] == 120 + 8 - 2


def test_bad_rows_are_counted(tmp_path):
    landing = str(tmp_path)
    summary = datagen.generate_batch(landing, trips_per_city=400, bad_rate=0.10)
    for city, c in summary["cities"].items():
        assert 20 <= c["bad"] <= 70, (city, c)
    rows = _read_csv(os.path.join(landing, "trips/lisbon/batch_0001.csv"))
    assert any(r["ended_at"] == "not-a-time" for r in rows)


def test_poison_file_and_fix(tmp_path):
    landing = str(tmp_path)
    datagen.generate_batch(landing, trips_per_city=10)
    path = datagen.write_poison_file(landing)
    assert all(r["trip_id"] == "" for r in _read_csv(path))
    assert datagen.fix_poison_files(landing) == [path]
    fixed_ids = [r["trip_id"] for r in _read_csv(path)]
    assert all(fixed_ids) and len(set(fixed_ids)) == len(fixed_ids)

"""Tests for the Parquet batch store: merge semantics, locking, persistence."""
from __future__ import annotations

import threading

import pyarrow.parquet as pq
import pytest

from app.storage import (
    BatchValidationError,
    DayNotFoundError,
    ManifestConflictError,
    ScanStore,
    StorageError,
    utc_day_from_epoch,
)

DAY = "2026-11-11"
BATCH_A = [
    {"waybill_id": "WB1001", "station": "BJ-01", "scanned_at": 1794355200},
    {"waybill_id": "WB1002", "station": "BJ-01", "scanned_at": 1794358800},
    {"waybill_id": "WB1003", "station": "BJ-02", "scanned_at": 1794362400},
]
BATCH_B = [
    {"waybill_id": "WB1002", "station": "SH-09", "scanned_at": 1794398400},
    {"waybill_id": "WB1004", "station": "SH-09", "scanned_at": 1794402000},
]
BATCH_C = [
    {"waybill_id": "WB1005", "station": "GZ-03", "scanned_at": 1794370000},
]


@pytest.fixture()
def store(tmp_path) -> ScanStore:
    s = ScanStore(tmp_path / "data")
    s.ensure_layout()
    return s


def test_utc_day_helper():
    assert utc_day_from_epoch(1794355200) == DAY
    assert utc_day_from_epoch(0) == "1970-01-01"


def test_ingest_writes_parquet_and_appends_manifest(store):
    entry = store.ingest_batch(BATCH_A)
    assert entry["kind"] == "batch"
    assert entry["day"] == DAY
    assert entry["row_count"] == 3
    table = pq.read_table(store.resolve(entry["path"]))
    assert table.num_rows == 3
    assert table.column_names == ["waybill_id", "station", "scanned_at"]
    manifest = store.load_manifest()
    assert [e["id"] for e in manifest["days"][DAY]] == [entry["id"]]


def test_batch_must_be_single_utc_day(store):
    with pytest.raises(BatchValidationError):
        store.ingest_batch([
            {"waybill_id": "W1", "station": "S", "scanned_at": 1794355200},
            {"waybill_id": "W2", "station": "S", "scanned_at": 1794441600},  # next UTC day
        ])
    with pytest.raises(BatchValidationError):
        store.ingest_batch([])
    with pytest.raises(BatchValidationError):
        store.ingest_batch(BATCH_A, declared_day="2020-01-01")


def test_merge_keeps_all_rows_including_duplicates_and_never_deletes(store):
    e1 = store.ingest_batch(BATCH_A)
    e2 = store.ingest_batch(BATCH_B)
    result = store.merge_day(DAY)

    assert result["row_count"] == 5  # 3 + 2, WB1002 duplicated and retained
    merged = result["merged_entry"]
    assert merged["kind"] == "merged"
    assert merged["source_ids"] == [e1["id"], e2["id"]]

    table = pq.read_table(store.resolve(merged["path"]))
    assert table.num_rows == 5
    assert table.column("waybill_id").to_pylist().count("WB1002") == 2

    manifest = store.load_manifest()
    day_entries = manifest["days"][DAY]
    assert [e["id"] for e in day_entries] == [merged["id"]]
    # input files stay on the shared volume for holders of the old manifest
    assert store.resolve(e1["path"]).exists()
    assert store.resolve(e2["path"]).exists()


def test_new_batch_arriving_during_merge_is_preserved(store):
    store.ingest_batch(BATCH_A)
    store.ingest_batch(BATCH_B)

    started = threading.Event()
    release = threading.Event()
    errors = []

    def merge_worker():
        # Make the merge wait while the third batch tries to enter; the ingest
        # blocks on the manifest flock until the merge releases it.
        orig_read = store._read_parquet

        def slow_read(path):
            started.set()
            release.wait(timeout=5)
            return orig_read(path)

        store._read_parquet = slow_read
        try:
            store.merge_day(DAY)
        except Exception as exc:  # pragma: no cover - surfaced below
            errors.append(exc)
        finally:
            store._read_parquet = orig_read

    t = threading.Thread(target=merge_worker)
    t.start()
    assert started.wait(timeout=5)
    # merge is in progress; this ingest waits on the lock
    e3 = store.ingest_batch(BATCH_C)
    release.set()
    t.join()
    assert not errors

    manifest = store.load_manifest()
    entries = manifest["days"][DAY]
    assert len(entries) == 2
    assert entries[0]["kind"] == "merged"
    assert entries[1]["id"] == e3["id"]  # mid-merge batch kept, appended after
    assert pq.read_table(store.resolve(entries[0]["path"])).num_rows == 5


def test_read_failure_aborts_without_touching_manifest(store):
    e1 = store.ingest_batch(BATCH_A)
    store.ingest_batch(BATCH_B)
    store.resolve(e1["path"]).unlink()  # simulate unreadable/missing input
    with pytest.raises(StorageError):
        store.merge_day(DAY)
    manifest = store.load_manifest()
    assert [e["kind"] for e in manifest["days"][DAY]] == ["batch", "batch"]


def test_stale_snapshot_conflict_aborts_and_keeps_manifest(store, monkeypatch):
    e1 = store.ingest_batch(BATCH_A)
    original_read = store._read_manifest_unlocked
    calls = {"n": 0}

    def tampered_read():
        calls["n"] += 1
        manifest = original_read()
        if calls["n"] == 2:  # the pre-commit re-read: another round swapped ids
            manifest["days"][DAY][0] = dict(
                manifest["days"][DAY][0], id="replaced-by-other-round")
        return manifest

    monkeypatch.setattr(store, "_read_manifest_unlocked", tampered_read)
    with pytest.raises(ManifestConflictError):
        store.merge_day(DAY)

    manifest = original_read()
    assert [e["id"] for e in manifest["days"][DAY]] == [e1["id"]]
    # no merged output left behind
    assert not list((store.merged_root / DAY).glob("*.parquet"))


def test_merge_unknown_day(store):
    with pytest.raises(DayNotFoundError):
        store.merge_day("2026-11-12")


def test_manifest_survives_store_restart(tmp_path):
    data_dir = tmp_path / "data"
    s1 = ScanStore(data_dir)
    s1.ensure_layout()
    entry = s1.ingest_batch(BATCH_A)
    s1.merge_day(DAY)

    s2 = ScanStore(data_dir)  # fresh instance, like a service restart
    manifest = s2.load_manifest()
    assert manifest["days"][DAY][0]["kind"] == "merged"
    assert manifest["days"][DAY][0]["source_ids"] == [entry["id"]]

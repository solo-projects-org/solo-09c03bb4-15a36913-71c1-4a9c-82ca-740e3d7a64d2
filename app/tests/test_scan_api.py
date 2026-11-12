"""API-level tests for /batches, /merges, /manifest and the preserved /analyze."""
from __future__ import annotations

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.storage import ScanStore

DAY = "2026-11-11"
BATCH_A = {
    "day": DAY,
    "records": [
        {"waybill_id": "WB1001", "station": "BJ-01", "scanned_at": 1794355200},
        {"waybill_id": "WB1002", "station": "BJ-01", "scanned_at": 1794358800},
        {"waybill_id": "WB1003", "station": "BJ-02", "scanned_at": 1794362400},
    ],
}
BATCH_B = {
    "day": DAY,
    "records": [
        {"waybill_id": "WB1002", "station": "SH-09", "scanned_at": 1794398400},
        {"waybill_id": "WB1004", "station": "SH-09", "scanned_at": 1794402000},
    ],
}


@pytest.fixture()
def client(tmp_path):
    store = ScanStore(tmp_path / "data")
    store.ensure_layout()
    app.state.scan_store = store
    with TestClient(app) as c:
        yield c
    app.state.scan_store = None


def _ingest(client, payload):
    resp = client.post("/batches", json=payload)
    assert resp.status_code == 201, resp.text
    return resp.json()


def test_ingest_two_batches_then_merge_and_read_manifest(client):
    r1 = _ingest(client, BATCH_A)
    r2 = _ingest(client, BATCH_B)
    assert r1["manifest_entries"] == 1
    assert r2["manifest_entries"] == 2

    merge = client.post("/merges", json={"day": DAY})
    assert merge.status_code == 200, merge.text
    body = merge.json()
    assert body["replaced_count"] == 2
    assert body["kept_new_count"] == 0
    assert body["row_count"] == 5
    merged = body["merged_entry"]
    assert merged["kind"] == "merged"
    assert merged["source_ids"] == [
        r1["entry"]["id"], r2["entry"]["id"]]

    manifest = client.get(f"/manifest?day={DAY}").json()
    assert manifest["total_files"] == 1
    assert manifest["entries"][0]["id"] == merged["id"]
    assert manifest["total_row_count"] == 5

    # merged parquet readable via the shared-volume relative path
    store: ScanStore = app.state.scan_store
    table = pq.read_table(store.resolve(merged["path"]))
    assert table.num_rows == 5
    assert table.column("waybill_id").to_pylist().count("WB1002") == 2


def test_batch_arriving_after_merge_waits_for_next_round(client):
    _ingest(client, BATCH_A)
    merged = client.post("/merges", json={"day": DAY}).json()
    late = _ingest(client, {"day": DAY, "records": [
        {"waybill_id": "WB9999", "station": "GZ-03", "scanned_at": 1794370000}]})

    manifest = client.get(f"/manifest?day={DAY}").json()
    assert [e["kind"] for e in manifest["entries"]] == ["merged", "batch"]
    assert manifest["entries"][1]["id"] == late["entry"]["id"]

    # a second merge folds the late batch in, old merged file stays on disk
    store: ScanStore = app.state.scan_store
    old_merged_path = store.resolve(merged["merged_entry"]["path"])
    second = client.post("/merges", json={"day": DAY})
    assert second.status_code == 200
    assert second.json()["replaced_count"] == 2
    assert old_merged_path.exists()


def test_merge_unknown_day_returns_404(client):
    resp = client.post("/merges", json={"day": "2026-11-12"})
    assert resp.status_code == 404
    assert client.get("/manifest?day=2026-11-12").status_code == 404


def test_batch_spanning_two_days_returns_400(client):
    resp = client.post("/batches", json={"records": [
        {"waybill_id": "W1", "station": "S", "scanned_at": 1794355200},
        {"waybill_id": "W2", "station": "S", "scanned_at": 1794441600},
    ]})
    assert resp.status_code == 400


def test_bad_day_format_returns_400_or_422(client):
    assert client.get("/manifest?day=11-11-2026").status_code == 400
    # pydantic pattern rejection on the merge body
    resp = client.post("/merges", json={"day": "11/11/2026"})
    assert resp.status_code == 422


def test_full_manifest_lists_all_days_sorted(client):
    _ingest(client, BATCH_A)
    other = client.post("/batches", json={"records": [
        {"waybill_id": "W5", "station": "X", "scanned_at": 1794441600},  # 2026-11-12
    ]})
    assert other.status_code == 201
    body = client.get("/manifest").json()
    days = {e["day"] for e in body["entries"]}
    assert days == {"2026-11-11", "2026-11-12"}
    assert body["entries"][0]["day"] <= body["entries"][-1]["day"]


def test_analyze_endpoint_still_available(client):
    payload = {
        "sources": [{"name": "waybills",
                     "columns": ["waybill_id", "receiver_zip_old"]}],
        "models": [{"name": "stg",
                    "sql": "SELECT waybill_id, receiver_zip_old AS zip FROM waybills"}],
        "targets": [{"table": "waybills", "column": "receiver_zip_old"}],
    }
    resp = client.post("/analyze", json=payload)
    assert resp.status_code == 200, resp.text
    assert resp.json()["models"]["stg"]["dependent_output_columns"][0][
        "output_column"] == "zip"


def test_swagger_documents_new_endpoints_and_batch_examples(client):
    schema = client.get("/openapi.json").json()
    for path in ("/batches", "/merges", "/manifest", "/analyze"):
        assert path in schema["paths"]
    examples = (schema["components"]["schemas"]["BatchIngestRequest"]
                ["examples"])
    assert len(examples) == 2  # two sample batches for the Swagger demo
    assert examples[0]["records"][0]["station"] == "BJ-01"
    assert examples[1]["records"][0]["station"] == "SH-09"


def test_health(client):
    assert client.get("/healthz").json() == {"status": "ok"}

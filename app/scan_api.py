"""HTTP API for daily waybill-scan batch ingestion and per-day merging."""
from __future__ import annotations

import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from .storage import (
    BatchValidationError,
    DayNotFoundError,
    ManifestConflictError,
    ScanStore,
    StorageError,
)

router = APIRouter(tags=["scan-files"])

DAY_PATTERN = r"^\d{4}-\d{2}-\d{2}$"


# ----------------------------------------------------------------- models
class ScanRecord(BaseModel):
    waybill_id: str = Field(..., min_length=1, description="运单号")
    station: str = Field(..., min_length=1, description="站点标识")
    scanned_at: int = Field(..., description="扫描时刻，UTC 秒（Unix epoch seconds）")


class BatchIngestRequest(BaseModel):
    day: Optional[str] = Field(
        default=None,
        pattern=DAY_PATTERN,
        description="可选，UTC 日 YYYY-MM-DD；给出时会与记录扫描时刻核对",
    )
    records: list[ScanRecord] = Field(..., min_length=1, description="本批扫描记录，同批只含一个 UTC 日")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "day": "2026-11-11",
                    "records": [
                        {"waybill_id": "WB1001", "station": "BJ-01", "scanned_at": 1794355200},
                        {"waybill_id": "WB1002", "station": "BJ-01", "scanned_at": 1794358800},
                        {"waybill_id": "WB1003", "station": "BJ-02", "scanned_at": 1794362400},
                    ],
                },
                {
                    "day": "2026-11-11",
                    "records": [
                        # WB1002 在两批中重复上报，合并时保留全部行（不去重）
                        {"waybill_id": "WB1002", "station": "SH-09", "scanned_at": 1794398400},
                        {"waybill_id": "WB1004", "station": "SH-09", "scanned_at": 1794402000},
                    ],
                },
            ]
        }
    }


class ManifestEntry(BaseModel):
    id: str
    path: str = Field(..., description="相对共享数据卷根目录的 Parquet 路径")
    day: str
    kind: str = Field(..., description="batch=站点小文件，merged=合并文件")
    row_count: int
    source_ids: Optional[list[str]] = Field(
        default=None, description="合并文件包含的输入条目 id（仅 merged）")


class BatchIngestResponse(BaseModel):
    day: str
    entry: ManifestEntry
    manifest_entries: int = Field(..., description="写入后该日清单条目总数")


class MergeRequest(BaseModel):
    day: str = Field(..., pattern=DAY_PATTERN, description="要合并的 UTC 日 YYYY-MM-DD")


class MergeResponse(BaseModel):
    day: str
    merged_entry: ManifestEntry
    replaced_count: int = Field(..., description="本轮换下的输入文件条目数")
    kept_new_count: int = Field(..., description="合并期间新入库、被保留的条目数")
    row_count: int = Field(..., description="合并文件总行数（含重复行）")


class ManifestResponse(BaseModel):
    day: Optional[str] = None
    entries: list[ManifestEntry]
    total_row_count: int
    total_files: int


# --------------------------------------------------------------- accessors
def get_store(request: Request) -> ScanStore:
    store: Optional[ScanStore] = getattr(request.app.state, "scan_store", None)
    if store is None:  # pragma: no cover - defensive, lifespan always sets it
        store = ScanStore(os.environ.get("SCAN_DATA_DIR", "/data"))
        store.ensure_layout()
        request.app.state.scan_store = store
    return store


def _entry_out(entry: dict) -> dict:
    out = dict(entry)
    out.setdefault("source_ids", None)
    return out


# ----------------------------------------------------------------- routes
@router.post(
    "/batches",
    response_model=BatchIngestResponse,
    status_code=201,
    summary="接收站点一批运单扫描，落一份 Parquet 并追加到当日清单",
)
def ingest_batch(payload: BatchIngestRequest, request: Request) -> BatchIngestResponse:
    store = get_store(request)
    rows = [r.model_dump() for r in payload.records]
    try:
        entry = store.ingest_batch(rows, declared_day=payload.day)
    except BatchValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except StorageError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    manifest = store.load_manifest()
    day_entries = manifest["days"].get(entry["day"], [])
    return BatchIngestResponse(
        day=entry["day"],
        entry=ManifestEntry(**_entry_out(entry)),
        manifest_entries=len(day_entries),
    )


@router.post(
    "/merges",
    response_model=MergeResponse,
    summary="按 UTC 日把清单中的 Parquet 合并成一份（快照语义，保留重复行）",
)
def merge_day(payload: MergeRequest, request: Request) -> MergeResponse:
    store = get_store(request)
    try:
        result = store.merge_day(payload.day)
    except DayNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ManifestConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except StorageError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return MergeResponse(
        day=result["day"],
        merged_entry=ManifestEntry(**_entry_out(result["merged_entry"])),
        replaced_count=len(result["replaced_entries"]),
        kept_new_count=len(result["kept_new_entries"]),
        row_count=result["row_count"],
    )


@router.get(
    "/manifest",
    response_model=ManifestResponse,
    summary="下游取数：按日（或全部）读取当前文件清单",
)
def get_manifest(request: Request, day: Optional[str] = None) -> ManifestResponse:
    if day is not None:
        # validate format ourselves so a bad pattern yields 400 not 422 docs
        import re
        if not re.match(DAY_PATTERN, day):
            raise HTTPException(status_code=400, detail=f"日期格式应为 YYYY-MM-DD: {day}")
    store = get_store(request)
    manifest = store.load_manifest()
    if day is not None:
        entries = manifest.get("days", {}).get(day)
        if entries is None:
            raise HTTPException(
                status_code=404, detail=f"日期 {day} 在清单中不存在")
        selected = entries
    else:
        selected = [
            entry
            for day_key in sorted(manifest.get("days", {}))
            for entry in manifest["days"][day_key]
        ]
    return ManifestResponse(
        day=day,
        entries=[ManifestEntry(**_entry_out(e)) for e in selected],
        total_row_count=sum(int(e.get("row_count", 0)) for e in selected),
        total_files=len(selected),
    )

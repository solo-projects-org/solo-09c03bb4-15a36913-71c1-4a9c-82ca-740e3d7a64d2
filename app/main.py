"""FastAPI service: column-level dependency analysis and scan-file merging."""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .lineage import AggregatedAnalysisError, AnalysisError, Analyzer
from .scan_api import router as scan_router
from .storage import ScanStore


@asynccontextmanager
async def lifespan(app: FastAPI):
    store: Optional[ScanStore] = getattr(app.state, "scan_store", None)
    if store is None:
        data_dir = os.environ.get("SCAN_DATA_DIR", "/data")
        store = ScanStore(data_dir)
        store.ensure_layout()
        app.state.scan_store = store
    yield


app = FastAPI(
    title="配送数仓服务：列依赖分析 + 运单扫描按日合并",
    description=(
        "两部分能力：\n\n"
        "1. `POST /analyze`：基于 SQLGlot 解析（不执行 SQL），沿别名、普通星号与"
        "非递归 CTE 跨模型追踪待下线源列的引用。\n"
        "2. `/batches`、`/merges`、`/manifest`：站点把每批运单扫描落成一份 "
        "Parquet 并追加到当日清单；值班员按 UTC 日把合并开始时清单里的文件并成"
        "一份，一次替换清单条目，合并期间新入库的批次保留；旧文件不删除，拿到"
        "旧清单的下游作业仍可从共享数据卷继续读取。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


class SourceTable(BaseModel):
    name: str = Field(..., description="源表名（大小写不敏感）")
    columns: list[str] = Field(..., description="源表当前全部列名")


class ModelSpec(BaseModel):
    name: str = Field(..., description="模型名称")
    sql: str = Field(..., description="该模型的 DuckDB SELECT 语句（可含非递归 CTE）")


class TargetColumn(BaseModel):
    table: str = Field(..., description="待下线列所在源表")
    column: str = Field(..., description="待下线的源列名（旧邮编列）")


class AnalyzeRequest(BaseModel):
    sources: list[SourceTable] = Field(..., description="源表及其列名")
    models: list[ModelSpec] = Field(..., description="各模型的名称和 DuckDB SELECT")
    target_columns: list[TargetColumn] = Field(
        ..., alias="targets", description="待下线的源列列表")

    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "examples": [
                {
                    "sources": [
                        {
                            "name": "waybills",
                            "columns": [
                                "waybill_id", "receiver_zip_old", "city_id", "weight_kg",
                            ],
                        },
                        {
                            "name": "dim_delivery_zone",
                            "columns": ["zone_id", "zip_code", "zone_name"],
                        },
                    ],
                    "models": [
                        {
                            "name": "stg_waybill",
                            "sql": (
                                "WITH cleaned AS (\n"
                                "  SELECT waybill_id, receiver_zip_old AS zip, city_id\n"
                                "  FROM waybills\n"
                                "  WHERE receiver_zip_old IS NOT NULL\n"
                                ")\n"
                                "SELECT waybill_id, zip, city_id\n"
                                "FROM cleaned"
                            ),
                        },
                        {
                            "name": "dwd_delivery_fact",
                            "sql": (
                                "SELECT s.waybill_id,\n"
                                "       s.zip AS receiver_zip,\n"
                                "       z.zone_name,\n"
                                "       s.weight_kg * 1000 AS weight_g\n"
                                "FROM stg_waybill s\n"
                                "JOIN dim_delivery_zone z ON s.zip = z.zip_code\n"
                                "WHERE s.zip LIKE '10%'"
                            ),
                        },
                    ],
                    "targets": [
                        {"table": "waybills", "column": "receiver_zip_old"}
                    ],
                }
            ]
        },
    }


class OutputDependency(BaseModel):
    output_column: str
    reference_chains: list[list[str]] = Field(
        ..., description="每条链自当前模型输出列起，到源表列止")


class ConditionDependency(BaseModel):
    type: str = Field(..., description="WHERE / JOIN ON / JOIN USING / QUALIFY")
    scope: str = Field(..., description="条件所在的模型或 CTE 作用域")
    expression: str
    column: str
    reference_chain: list[str]


class ModelResult(BaseModel):
    dependent_output_columns: list[OutputDependency]
    filter_conditions: list[ConditionDependency]


class AnalyzeResponse(BaseModel):
    targets: list[TargetColumn]
    models: dict[str, ModelResult]


class ErrorItem(BaseModel):
    model: Optional[str] = None
    location: Optional[str] = None
    message: str


class ErrorResponse(BaseModel):
    detail: list[ErrorItem]


@app.post(
    "/analyze",
    response_model=AnalyzeResponse,
    responses={400: {"model": ErrorResponse, "description": "解析、引用或不支持构造错误"}},
    summary="分析待下线源列在各模型中的依赖",
)
def analyze(request: AnalyzeRequest) -> AnalyzeResponse:
    source_names = [s.name for s in request.sources]
    dup_sources = {n for n in source_names if source_names.count(n) > 1}
    if dup_sources:
        raise HTTPException(400, [{"message": f"源表名重复: {sorted(dup_sources)}"}])
    model_names = [m.name for m in request.models]
    dup_models = {n for n in model_names if model_names.count(n) > 1}
    if dup_models:
        raise HTTPException(400, [{"message": f"模型名重复: {sorted(dup_models)}"}])
    if not request.target_columns:
        raise HTTPException(400, [{"message": "targets 不能为空"}])

    sources = {s.name: s.columns for s in request.sources}
    source_lookup = {
        name.lower(): {col.lower(): col for col in columns}
        for name, columns in sources.items()
    }
    target_tuples = [(t.table, t.column) for t in request.target_columns]
    unknown = [
        {"message": f"待下线列 {t.table}.{t.column} 在源表定义中不存在"}
        for t in request.target_columns
        if t.column.lower() not in source_lookup.get(t.table.lower(), {})
    ]
    if unknown:
        raise HTTPException(400, unknown)

    analyzer = Analyzer(
        sources=sources,
        models={m.name: m.sql for m in request.models},
        targets=target_tuples,
    )
    try:
        per_model = analyzer.run()
    except AggregatedAnalysisError as exc:
        raise HTTPException(400, [
            {"model": e.model, "location": e.location, "message": e.message}
            for e in exc.errors
        ])
    except AnalysisError as exc:
        raise HTTPException(400, [{
            "model": exc.model, "location": exc.location, "message": exc.message}])

    return AnalyzeResponse(
        targets=request.target_columns,
        models=per_model,
    )


app.include_router(scan_router)


@app.get("/healthz", include_in_schema=False)
def healthz() -> dict:
    return {"status": "ok"}

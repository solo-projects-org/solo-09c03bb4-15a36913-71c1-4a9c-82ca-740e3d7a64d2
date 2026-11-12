# 配送数仓服务：列依赖分析 + 运单扫描按日合并

服务包含两部分能力：

1. **列依赖分析 `POST /analyze`**：接收源表定义、各模型的 DuckDB SELECT 与待下线
   源列，**只用 SQLGlot 解析、不执行任何 SQL**，沿别名、普通星号与非递归 CTE
   跨模型追踪列引用（规则见下文）。
2. **运单扫描按日合并 `/batches`、`/merges`、`/manifest`**：站点把每批运单扫描
   落成一份 Parquet 小文件并追加到当日清单；值班员按 UTC 日把合并开始时清单里的
   文件并成一份，一次替换清单条目，供下游按日取数。

---

# 一、运单扫描按日合并

## 数据模型

每条扫描记录三列，所有 Parquet 文件 schema 一致：

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `waybill_id` | string | 运单号 |
| `station` | string | 站点标识 |
| `scanned_at` | int64 | 扫描时刻，UTC 秒（Unix epoch seconds） |

站点每批上报若干记录，**同批只允许含一个 UTC 日**，服务把整批写成一份 Parquet
（`scans/<UTC日>/<uuid>.parquet`），并在清单 `manifest.json` 中追加一个条目。

## 接口

### `POST /batches` — 接收一批扫描

请求体见 `examples/batch_beijing.json`、`examples/batch_shanghai.json`，Swagger
内置了同 UTC 日（2026-11-11）的两批样例，可直接 Try it out。可选字段 `day`
（`YYYY-MM-DD`）给出时会与记录扫描时刻所属的 UTC 日核对，不一致返回 400。

### `POST /merges` — 按日合并

请求体 `{"day": "2026-11-11"}`。合并语义：

- **快照语义**：只合并"合并开始时"清单里该日的文件，合并期间照常接收的新批不参与
  本轮，新条目在清单中原样保留（响应里的 `kept_new_count`）。
- **保留全部行，包括重复行**：直接拼接 Parquet，不去重、不改写列。
- **一次替换**：新合并文件（`merged/<UTC日>/<uuid>.parquet`）写完后，在清单中
  一次性把本轮输入条目换成合并条目；中途入库的新批条目保留。
- **旧文件不删除**：拿到旧清单的下游作业仍可从共享数据卷继续读小文件。
- **失败不改清单**：任一输入 Parquet 读失败、合并文件写失败（HTTP 500），或本轮
  输入已被另一轮合并换下（HTTP 409），本轮报错且清单保持不变，已写出的半成品
  合并文件会被清理。该日无文件时返回 404。

### `GET /manifest?day=YYYY-MM-DD` — 下游按日取清单

返回该日当前清单的全部条目（`path` 为相对共享数据卷根目录的 Parquet 路径）、
文件数与总行数；不带 `day` 则返回所有日期的条目（按日期排序）。

## 并发、持久化与重启

- 清单通过同目录侧车锁文件上的 `fcntl.flock` 保护，多个 uvicorn worker/进程安全；
  每次更新都在锁内重读清单并比对合并开始时的快照 id，以发现并发合并。
- 清单与 Parquet 都先写临时文件再 `os.replace` 原子替换，并对目录 `fsync`。
- 所有状态都在共享数据卷（环境变量 `SCAN_DATA_DIR`，容器内 `/data`），**服务重启
  后直接读取当前清单继续服务**。

---

# 二、列依赖分析（POST /analyze）

运单源表即将停供旧邮编列。本服务接收源表定义、各模型的 DuckDB SELECT 与待下线源列，
**只用 SQLGlot 解析、不执行任何 SQL**，沿别名、普通星号与非递归 CTE 跨模型追踪列引用，
按模型输出：

- 依赖该源列的**输出列**，每项附完整引用链；
- 依赖该源列的 **WHERE / JOIN ON（含 USING）/ QUALIFY 条件**，每项附引用链。
  条件依赖**不计入**输出列。

## 追踪规则

- 别名：`receiver_zip_old AS zip` 会把依赖从 `zip` 继续追到源列。
- 普通星号：`SELECT *`、`SELECT t.*` 按关系实际输出列展开；支持
  `* EXCLUDE (...)`（DuckDB 里 `EXCEPT` 同义）与 `* REPLACE (...)`。
- 非递归 CTE：CTE 可引用同模型内更早的 CTE，作用域链以 `模型.CTE.列` 呈现。
- 跨模型：模型可以引用其他模型的输出列，分析按依赖拓扑顺序进行，链上保留模型跳。
- 名称匹配大小写不敏感，源表可带 catalog/schema 前缀（按表名匹配）。
- 引用链自当前模型的输出列/条件列起，到 `源表.源列` 止。

## 明确不支持（提交即 400 并指出所在模型）

- 聚合函数、`GROUP BY`、`HAVING`；
- 集合运算 `UNION / INTERSECT / EXCEPT`；
- CTE 之外的子查询（FROM 子查询、标量子查询、`EXISTS` 等）；
- 递归 CTE（`WITH RECURSIVE`）、`LATERAL`、`UNNEST`/表函数、`NATURAL JOIN`。

## 找不到或有歧义的引用（400，指出模型）

- 列在当前查询所有关系中都不存在；
- 未限定的列名可由多个关系提供（歧义，要求加表别名）；
- 表别名不存在、表/CTE 未定义、输出列重名等。

## 启动（Docker Compose）

```bash
docker compose up --build
# 服务: http://localhost:8000
# Swagger: http://localhost:8000/docs
# OpenAPI: http://localhost:8000/openapi.json
```

Compose 把命名卷 `scan-data` 挂到容器 `/data`（`SCAN_DATA_DIR`），Parquet 与
`manifest.json` 都落在该卷；下游作业可挂载同一卷按清单路径取数，容器重建不丢数据。

### Swagger 走一遍合并流程

1. 打开 http://localhost:8000/docs，在 `POST /batches` 用内置样例提交**两批**
   （北京批 3 行、上海批 2 行，同属 UTC 日 2026-11-11，运单 WB1002 两批重复）。
2. `POST /merges` 提交 `{"day": "2026-11-11"}`，返回 `replaced_count=2`、
   `row_count=5`。
3. `GET /manifest?day=2026-11-11` 查看清单：只剩一个 `kind=merged` 条目，
   `source_ids` 记录被换下的两个小文件 id。

也可用 curl：

```bash
curl -X POST http://localhost:8000/batches -H 'Content-Type: application/json' \
  --data @examples/batch_beijing.json
curl -X POST http://localhost:8000/batches -H 'Content-Type: application/json' \
  --data @examples/batch_shanghai.json
curl -X POST http://localhost:8000/merges -H 'Content-Type: application/json' \
  -d '{"day": "2026-11-11"}'
curl 'http://localhost:8000/manifest?day=2026-11-11'
```

## 列依赖分析请求示例

`examples/request_zip_rename.json` 是完整样例（也可用 Swagger 内置样例）：

```bash
curl -X POST http://localhost:8000/analyze \
  -H 'Content-Type: application/json' \
  --data @examples/request_zip_rename.json
```

请求结构：

```json
{
  "sources": [
    {"name": "waybills", "columns": ["waybill_id", "receiver_zip_old", "city_id"]}
  ],
  "models": [
    {"name": "stg_waybill", "sql": "WITH cleaned AS (SELECT waybill_id, receiver_zip_old AS zip FROM waybills WHERE receiver_zip_old IS NOT NULL) SELECT * FROM cleaned"}
  ],
  "targets": [
    {"table": "waybills", "column": "receiver_zip_old"}
  ]
}
```

响应结构（按模型分组）：

```json
{
  "targets": [{"table": "waybills", "column": "receiver_zip_old"}],
  "models": {
    "stg_waybill": {
      "dependent_output_columns": [
        {
          "output_column": "zip",
          "reference_chains": [
            ["stg_waybill.zip", "stg_waybill.cleaned.zip", "waybills.receiver_zip_old"]
          ]
        }
      ],
      "filter_conditions": [
        {
          "type": "WHERE",
          "scope": "stg_waybill.cleaned",
          "expression": "receiver_zip_old IS NOT NULL",
          "column": "receiver_zip_old",
          "reference_chain": ["stg_waybill.cleaned.zip", "waybills.receiver_zip_old"]
        }
      ]
    }
  }
}
```

错误响应统一为 `400`，`detail` 是数组，每项含 `model`、`location`、`message`，
一次返回收集到的全部问题。

## 本地开发（不用 Docker）

```bash
python -m venv --without-pip .venv   # 若系统带 ensurepip 可直接 python -m venv .venv
.venv/bin/pip install -r requirements.txt
SCAN_DATA_DIR=./data PYTHONPATH=. .venv/bin/uvicorn app.main:app --reload
.venv/bin/pytest                       # 55 个测试
```

## 目录

```
app/
  main.py        FastAPI 应用、生命周期、/analyze 路由与请求模型
  lineage.py     SQLGlot 解析与列级血缘分析（不执行 SQL）
  scan_api.py    /batches、/merges、/manifest 路由、请求/响应模型与 Swagger 样例
  storage.py     Parquet 读写、清单持久化、flock 并发控制与按日合并
  tests/         分析器、存储层与 API 测试
examples/        邮编改名请求样例与两批扫描入库样例
Dockerfile, docker-compose.yml, requirements.txt
```

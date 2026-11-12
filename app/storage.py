"""Daily waybill-scan batch storage and merge coordination.

Layout under a shared data directory (``DATA_DIR``):

::

    data/
      manifest.json                 # one manifest, rewritten atomically
      scans/2026-11-11/<uuid>.parquet
      merged/2026-11-11/<uuid>.parquet

A *manifest* maps each UTC day to an ordered list of immutable file entries.
Batches append one entry; a merge reads a point-in-time snapshot of the day's
entries, concatenates their Parquet rows into one new file, and then swaps the
snapshot entries for the merged entry in a single manifest rewrite. Entries
that arrive during the merge are not part of the snapshot and stay untouched.

Concurrency model: the manifest file is protected by an ``fcntl.flock`` on a
sidecar lock file, so multiple uvicorn workers / processes coordinate safely.
Every manifest update re-reads the current manifest inside the lock and
compares it against the expected snapshot before writing, which detects the
case where another merge already replaced one of this round's inputs.

Files themselves are never deleted: downstream jobs holding an older manifest
can keep reading the small Parquet files even after they leave the current
manifest.
"""
from __future__ import annotations

import fcntl
import json
import os
import tempfile
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

import pyarrow as pa
import pyarrow.parquet as pq

#: Columns (and order) of every stored Parquet file.
SCAN_SCHEMA = pa.schema([
    pa.field("waybill_id", pa.string(), nullable=False),
    pa.field("station", pa.string(), nullable=False),
    pa.field("scanned_at", pa.int64(), nullable=False),
])

MANIFEST_NAME = "manifest.json"
LOCK_NAME = ".manifest.lock"
SCANS_DIR = "scans"
MERGED_DIR = "merged"


class StorageError(Exception):
    """Raised when Parquet read/write or manifest persistence fails."""


class BatchValidationError(ValueError):
    """Raised when an ingested batch is empty or spans multiple UTC days."""


class DayNotFoundError(ValueError):
    """Raised when a merge/listing targets a UTC day absent from the manifest."""


class ManifestConflictError(Exception):
    """Raised when a merge snapshot is no longer the current manifest state."""


def utc_day_from_epoch(epoch_seconds: int) -> str:
    """Return the UTC calendar day (``YYYY-MM-DD``) of a Unix-timestamp second."""
    return datetime.fromtimestamp(int(epoch_seconds), tz=timezone.utc).strftime("%Y-%m-%d")


class ScanStore:
    """File-backed batch storage with a JSON manifest and flock coordination."""

    def __init__(self, data_dir: str | os.PathLike):
        self.data_dir = Path(data_dir)
        self.scans_root = self.data_dir / SCANS_DIR
        self.merged_root = self.data_dir / MERGED_DIR
        self.manifest_path = self.data_dir / MANIFEST_NAME
        self.lock_path = self.data_dir / LOCK_NAME

    # ------------------------------------------------------------ bootstrap
    def ensure_layout(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.scans_root.mkdir(parents=True, exist_ok=True)
        self.merged_root.mkdir(parents=True, exist_ok=True)
        self.lock_path.touch(exist_ok=True)
        if not self.manifest_path.exists():
            self._write_manifest_unlocked({"days": {}})

    # ----------------------------------------------------------------- locks
    @contextmanager
    def _manifest_lock(self) -> Iterator[None]:
        self.ensure_layout()
        with open(self.lock_path, "w") as lock_fh:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)

    # ------------------------------------------------------------ manifest io
    def _read_manifest_unlocked(self) -> dict:
        try:
            with open(self.manifest_path, "r", encoding="utf-8") as fh:
                manifest = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            raise StorageError(f"清单读取失败: {exc}") from exc
        manifest.setdefault("days", {})
        return manifest

    def _write_manifest_unlocked(self, manifest: dict) -> None:
        """Atomically replace the manifest via a temp file in the same dir."""
        fd, tmp_name = tempfile.mkstemp(
            prefix=".manifest.", suffix=".tmp", dir=str(self.data_dir))
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(manifest, fh, ensure_ascii=False, indent=2)
                fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, self.manifest_path)
        except OSError as exc:
            tmp_path.unlink(missing_ok=True)
            raise StorageError(f"清单写入失败: {exc}") from exc

    def load_manifest(self) -> dict:
        """Return the current manifest (snapshot read under the lock)."""
        with self._manifest_lock():
            return self._read_manifest_unlocked()

    # ----------------------------------------------------------- parquet io
    @staticmethod
    def _table_from_rows(rows: list[dict]) -> pa.Table:
        return pa.table({
            "waybill_id": [r["waybill_id"] for r in rows],
            "station": [r["station"] for r in rows],
            "scanned_at": [int(r["scanned_at"]) for r in rows],
        }, schema=SCAN_SCHEMA)

    def _write_parquet_atomic(self, target: Path, table: pa.Table) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=".tmp-", suffix=".parquet", dir=str(target.parent))
        os.close(fd)
        tmp_path = Path(tmp_name)
        try:
            pq.write_table(table, tmp_path, compression="snappy")
            os.replace(tmp_path, target)
        except (OSError, pa.ArrowException) as exc:
            tmp_path.unlink(missing_ok=True)
            raise StorageError(f"Parquet 写入失败 {target.name}: {exc}") from exc

    def _read_parquet(self, path: Path) -> pa.Table:
        try:
            return pq.read_table(path)
        except (OSError, pa.ArrowException) as exc:
            raise StorageError(f"Parquet 读取失败 {path.name}: {exc}") from exc

    # ------------------------------------------------------------- ingestion
    def ingest_batch(self, rows: list[dict], declared_day: Optional[str] = None) -> dict:
        """Persist one batch as a single Parquet file and append its entry.

        All rows must belong to the same UTC day. Returns the new manifest
        entry. The file is written and fsynced before the manifest mentions it.
        """
        if not rows:
            raise BatchValidationError("批次不能为空")
        days = {utc_day_from_epoch(r["scanned_at"]) for r in rows}
        if len(days) != 1:
            raise BatchValidationError(
                f"同一批次只能包含一个 UTC 日，发现 {sorted(days)}")
        day = days.pop()
        if declared_day is not None and declared_day != day:
            raise BatchValidationError(
                f"批次扫描时刻属于 UTC 日 {day}，与指定日期 {declared_day} 不一致")

        entry_id = str(uuid.uuid4())
        rel_path = f"{SCANS_DIR}/{day}/{entry_id}.parquet"
        target = self.data_dir / rel_path
        table = self._table_from_rows(rows)
        # Write the immutable data file first; a crash here leaves an orphan
        # temp/file but never a manifest entry pointing at missing data.
        self._write_parquet_atomic(target, table)
        self._fsync_dir(target.parent)

        entry = {
            "id": entry_id,
            "path": rel_path,
            "day": day,
            "kind": "batch",
            "row_count": len(rows),
        }
        with self._manifest_lock():
            manifest = self._read_manifest_unlocked()
            manifest.setdefault("days", {}).setdefault(day, []).append(entry)
            self._write_manifest_unlocked(manifest)
        return entry

    # ---------------------------------------------------------------- merge
    def merge_day(self, day: str) -> dict:
        """Merge all currently-listed files of ``day`` into one Parquet file.

        Snapshot semantics: only entries present at merge start are merged.
        Entries appended while the merge runs remain in the manifest and are
        returned alongside the merged entry. On read/write failure or a stale
        snapshot (another merge already swapped an input), the manifest is not
        modified and an error is raised.
        """
        with self._manifest_lock():
            manifest = self._read_manifest_unlocked()
            entries = list(manifest.get("days", {}).get(day, []))
            if not entries:
                raise DayNotFoundError(
                    f"日期 {day} 在清单中没有可合并的文件")
            snapshot = [dict(e) for e in entries]
            snapshot_ids = [e["id"] for e in snapshot]

            # Read + concatenate while holding the lock. New batches block on
            # the lock and therefore cannot alter the manifest underneath us;
            # the snapshot comparison below still guards cross-process history.
            tables = []
            total_rows = 0
            for entry in snapshot:
                table = self._read_parquet(self.data_dir / entry["path"])
                tables.append(table)
                total_rows += table.num_rows
            merged_table = pa.concat_tables(tables, promote_options="default")

            merged_id = str(uuid.uuid4())
            merged_rel = f"{MERGED_DIR}/{day}/{merged_id}.parquet"
            merged_path = self.data_dir / merged_rel
            self._write_parquet_atomic(merged_path, merged_table)
            self._fsync_dir(merged_path.parent)

            # Re-read before committing: another round may already have
            # replaced some snapshot entries.
            current = self._read_manifest_unlocked()
            current_entries = current.get("days", {}).get(day, [])
            current_ids = [e["id"] for e in current_entries]
            prefix = current_ids[: len(snapshot_ids)]
            if prefix != snapshot_ids:
                # Inputs no longer match the merge-start snapshot. Do not
                # touch the manifest; drop the merged file we just produced.
                merged_path.unlink(missing_ok=True)
                raise ManifestConflictError(
                    f"日期 {day} 的输入文件已被另一轮合并换下，本轮终止且不改清单")

            merged_entry = {
                "id": merged_id,
                "path": merged_rel,
                "day": day,
                "kind": "merged",
                "row_count": total_rows,
                "source_ids": snapshot_ids,
            }
            # One single swap: snapshot entries -> merged entry, preserving
            # any entries appended after the snapshot.
            current["days"][day] = [merged_entry, *current_entries[len(snapshot_ids):]]
            self._write_manifest_unlocked(current)

            return {
                "day": day,
                "merged_entry": merged_entry,
                "replaced_entries": snapshot,
                "kept_new_entries": current["days"][day][1:],
                "row_count": total_rows,
            }

    # ------------------------------------------------------------- utilities
    @staticmethod
    def _fsync_dir(path: Path) -> None:
        try:
            fd = os.open(str(path), os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            os.close(fd)

    def resolve(self, rel_path: str) -> Path:
        return self.data_dir / rel_path

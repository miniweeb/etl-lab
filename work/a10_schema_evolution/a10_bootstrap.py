"""A10 bootstrap — paths, connection, hằng số, và ledger helper cho ops.etl_runs.

Import: from work.a10_schema_evolution.a10_bootstrap import boot, section, RunLog
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
import json

import duckdb

from lib.labpaths import data_root

# ── hằng số của bài ──────────────────────────────────────────────────────────
DAY_V1, DAY_V2, DAY_V3 = "2026-06-02", "2026-07-16", "2026-08-01"
MONTH2_START, MONTH2_END = "2026-07-16", "2026-08-08"
BAD_DAY = "2026-07-22"            # Task 7c — KHÔNG sửa hôm nay, để dành A11
LAKE_VER = "orders_v=20260814"    # thư mục lake mới, versioned (pattern A09)
STEP = "a10_lake_rebuild"         # giá trị cột step trong ops.etl_runs


def section(title: str) -> None:
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")


def boot(scale: str = "small"):
    """Trả (con, ROOT, RAW, MANIFEST)."""
    root = data_root(scale)
    (root / "tmp").mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(root / "warehouse" / "warehouse.duckdb"))
    con.execute("SET memory_limit='8GB'; SET threads=8; "
                f"SET temp_directory='{(root / 'tmp').as_posix()}';")
    for sch in ("staging", "core", "ops"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {sch}")
    return con, root, root / "raw" / "orders", root / "raw" / "manifest"


def manifest(manifest_dir, day: str) -> dict:
    return json.loads((manifest_dir / f"orders_{day}.json").read_text())


def daterange(start: str, end: str):
    d, e = date.fromisoformat(start), date.fromisoformat(end)
    while d <= e:
        yield d.isoformat()
        d += timedelta(days=1)


# ── ops.etl_runs ─────────────────────────────────────────────────────────────
# Schema thật (A07): run_id BIGINT PK · run_date DATE · step VARCHAR ·
# status VARCHAR · started_at TIMESTAMP · finished_at TIMESTAMP · rows BIGINT ·
# source_file VARCHAR · error VARCHAR
#
# KHÔNG có cột cho số dòng bị bỏ. Đó không phải thiếu sót của A07: ledger ghi
# lại LẦN CHẠY, còn số dòng bị bỏ là một phép đo về DỮ LIỆU. Nên RunLog cộng
# dồn dropped ở phía Python — Task 7d cần tổng đó để đóng phép cộng.
class RunLog:
    def __init__(self, con, step: str = STEP):
        self.con, self.step = con, step
        self.dropped_total = 0
        self.failed: list[str] = []

    def _next_id(self) -> int:
        return self.con.execute(
            "SELECT coalesce(max(run_id), 0) + 1 FROM ops.etl_runs").fetchone()[0]

    def start(self, day: str, source_file: str) -> int:
        rid = self._next_id()
        self.con.execute(
            "INSERT INTO ops.etl_runs (run_id, run_date, step, status, started_at, "
            "finished_at, rows, source_file, error) VALUES (?,?,?,?,?,NULL,NULL,?,NULL)",
            [rid, day, self.step, "running", datetime.now(), source_file])
        return rid

    def ok(self, rid: int, rows: int, dropped: int = 0) -> None:
        self.dropped_total += dropped
        self.con.execute(
            "UPDATE ops.etl_runs SET status='success', finished_at=?, rows=? "
            "WHERE run_id=?", [datetime.now(), rows, rid])

    def fail(self, rid: int, day: str, err: str) -> None:
        self.failed.append(day)
        self.con.execute(
            "UPDATE ops.etl_runs SET status='failed', finished_at=?, error=? "
            "WHERE run_id=?", [datetime.now(), err[:500], rid])


if __name__ == "__main__":
    con, root, raw, man = boot()
    section("A10 bootstrap")
    print("root     :", root)
    print("raw csv  :", len(list(raw.glob("orders_*.csv"))), "(Expect 69 sau Task 1)")
    print("manifests:", len(list(man.glob("orders_*.json"))))
    con.sql("SELECT step, status, count(*) AS n FROM ops.etl_runs GROUP BY 1,2").show()
    con.close()
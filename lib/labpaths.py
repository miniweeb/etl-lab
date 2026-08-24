"""Where does the data live? One answer for the whole lab.

Resolution order:
  1. env var ETL_LAB_DATA (recommended: a fast local, non-synced disk,
     e.g. F:\\etl_lab). Never a OneDrive/Dropbox folder.
  2. fallback: <repo>/data (fine only for the small scale).

Usage:
    from lib.labpaths import data_root, lake_dir, warehouse_path
    root = data_root("small")        # <root>/small
    root = data_root("full")         # <root>
"""
from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def base_root() -> Path:
    env = os.environ.get("ETL_LAB_DATA")
    return Path(env) if env else REPO_ROOT / "data"


def data_root(scale: str = "full") -> Path:
    if scale not in ("full", "small"):
        raise ValueError(f"scale must be 'full' or 'small', got {scale!r}")
    root = base_root()
    return root / "small" if scale == "small" else root


def raw_orders_dir(scale: str = "full") -> Path:
    return data_root(scale) / "raw" / "orders"


def raw_dims_dir(scale: str = "full") -> Path:
    return data_root(scale) / "raw" / "dims"


def manifest_dir(scale: str = "full") -> Path:
    return data_root(scale) / "raw" / "manifest"


def lake_dir(scale: str = "full") -> Path:
    return data_root(scale) / "lake"


def warehouse_path(scale: str = "full") -> Path:
    return data_root(scale) / "warehouse" / "warehouse.duckdb"


def quarantine_dir(scale: str = "full") -> Path:
    return data_root(scale) / "quarantine"


def tmp_dir(scale: str = "full") -> Path:
    return data_root(scale) / "tmp"


def ensure_dirs(scale: str = "full") -> None:
    for p in (raw_orders_dir(scale), raw_dims_dir(scale), manifest_dir(scale),
              lake_dir(scale), warehouse_path(scale).parent,
              quarantine_dir(scale), tmp_dir(scale)):
        p.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    print(f"repo:       {REPO_ROOT}")
    print(f"data root:  {base_root()}"
          + ("   (from ETL_LAB_DATA)" if os.environ.get("ETL_LAB_DATA")
             else "   (fallback <repo>/data — set ETL_LAB_DATA!)"))
    for scale in ("small", "full"):
        print(f"  {scale:5}: {data_root(scale)}")

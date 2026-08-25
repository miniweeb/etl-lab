# Lab Journal
## A00 — Environment Setup & Data Generation

### System & Environment Specs
* **Operating System / Shell**: Windows 11 (PowerShell)[cite: 1]
* **CPU / RAM**: 8 logical cores | 15.6 GB RAM[cite: 1]
* **Storage Allocation**: Drive `D:` (186.5 GB free)[cite: 1]
* **Core Tools**: Python 3.13.13, Git 2.50.1, DuckDB 1.5.5[cite: 1]
* **Data Root Directory**: `$env:ETL_LAB_DATA = "D:\etl_lab"`[cite: 1]

---

### Execution Performance Metrics

| Metric | Small Scale (`--scale small`) | Full Scale (`--scale full`) |
| :--- | :--- | :--- |
| **Time Period** | 2026-06-01 to 2026-07-15 (45 files)[cite: 1] | 2026-06-01 to 2026-07-15 (45 files)[cite: 1] |
| **Worker Threads** | 4 parallel workers[cite: 1] | 4 parallel workers[cite: 1] |
| **Dimension Stats** | Standard testing dims[cite: 1] | 1,200,000 customers | 40,000 products | 400 stores[cite: 1] |
| **Total Order Rows** | 2,773,566 rows[cite: 1] | 55,471,709 rows[cite: 1] |
| **Execution Time** | 0.4 min (24 sec)[cite: 1] | 9.3 min (558 sec)[cite: 1] |
| **Throughput** | ~123,654 rows/sec[cite: 1] | ~99,700 rows/sec[cite: 1] |
| **Verification** | Passed (34/34 checks)[cite: 1] | Passed (34/34 checks)[cite: 1] |

---

### Key Takeaways & Surprises

* **Linear Throughput Scaling**: Generating 55.4M rows (`full`) maintained a steady ~100k rows/sec output across 4 workers without degrading CPU efficiency[cite: 1].
* **Double-Quote Escaping in CSV (RFC 4180)**: Inspecting `orders_2026-06-02.csv` revealed how nested JSON columns (`items`, `meta`) are safely serialized[cite: 1]:
  * Internal JSON double quotes are doubled (`""sku""`)[cite: 1].
  * The entire JSON string is enclosed in outer quotes `"{...}"` to prevent commas from breaking column delimiters[cite: 1].
* **Data Lake Auditability**: The manifest JSON files track metadata per partition (e.g., `late_rows`, `dup_rows`, `bytes`), serving as the single source of truth for downstream reconciliation[cite: 1].

### Advanced Key Takeaways & Technical Surprises

* **Windows Environment Variable Gotcha (`setx`)**: 
  `setx ETL_LAB_DATA` persists paths in the Registry[cite: 1], but fails to update the active PowerShell environment session dynamically. Explicitly setting `$env:ETL_LAB_DATA`[cite: 1] or restarting the shell is strictly required for `labpaths.py`[cite: 1] to resolve paths.

* **Cross-Platform Path Standardization (`as_posix()`)**: 
  Windows backslashes (`\`) break DuckDB glob pattern matching (`*/*.parquet`). Path strings must be converted using Python's `.as_posix()` method before being passed to SQL queries.



# A01 — First Contact: Raw Feed Exploration & Parquet Benchmark

### Data Lake Specs & Audit
* **Raw Files Scope**: 45 CSV files totaling 15.7 GB (`$root\raw\orders`)
* **Single File Sample (`orders_2026-06-02.csv`)**: ~359 MB on disk
* **Manifest Reconciliation**:
  * **Small Scale**: Counted 59,024 vs Manifest 59,024 (`Match=True`)
  * **Full Scale**: Counted 1,180,490 vs Manifest 1,180,490 (`Match=True`)
* **Data Quality Anomalies**:
  * Inconsistent `order_ts` format: 249 dirty rows (0.42%) in `small`, 4,899 dirty rows (0.41%) in `full`
* **Memory Footprint**: Pandas memory expansion ratio is ~2.7x (343 MB file expands to 910 MB RAM)

---

### Execution Performance Metrics

| Metric / Query | CSV (Row-oriented Text) | Parquet (Columnar Storage) | Speedup / Efficiency |
| :--- | :--- | :--- | :--- |
| **`count(*)`** | 1,380 ms (1,180,490 rows) | 3 ms (1,180,490 rows) | **~460x faster** |
| **`filter store 42`** | 1,382 ms (589 rows) | 11 ms (589 rows) | **~125x faster** |
| **`group by status`** | 1,419 ms ('delivered', 474,553) | 19 ms ('delivered', 474,553) | **~74x faster** |
| **Row Groups Metadata** | Full scan required | 10 row groups (9x ~123k rows, 1x 70,235) | Metadata pruning |
| **`order_id` Min / Max** | Full file parsing | Min: `1510000204` \| Max: `1521162182` | Instant lookup |

---

### Key Takeaways & Data Quality Pitfalls

* **Schema Auto-Detect Pitfalls**: DuckDB's CSV auto-detection misinterprets `customer_id` as `DOUBLE` due to Excel floats (e.g., `1601.0`), `order_ts` as `VARCHAR` due to date string variations, and `order_total` as `VARCHAR` due to `$` symbols.
* **Pandas In-Memory Bottleneck**: Loading 15.7 GB of CSVs in Pandas requires ~42 GB RAM (2.7x factor). On a 16 GB RAM machine, this triggers OOM crashes. DuckDB processes out-of-core in 1.4s staying under 0.72 GB RSS.
* **Explicit Casting with `TRY_CAST`**: Using `TRY_CAST(order_ts AS TIMESTAMP)` safely isolates dirty timestamp strings (returning `NULL`) without crashing execution pipelines.

---

### Advanced Key Takeaways & Columnar Storage Advantages

* **Column Projection & Metadata Pruning**: Parquet reads only the target columns requested by the query and uses footer metadata (Row Group min/max bounds) to skip irrelevant data blocks entirely without reading them from disk.
* **VS Code Kernel vs PowerShell Venv Mismatch (`ModuleNotFoundError`)**: 
  Installing packages via `pip install duckdb` in the active terminal `.venv` does not automatically update VS Code's Interactive Window / Jupyter extension if it points to the global Python interpreter (e.g., Python 3.13.13). The kernel must be explicitly switched to `.venv\Scripts\python.exe` or installed using `%pip install` directly within the cell.
* **Pandas Memory Explosion (2.7x Expansion Factor)**: 
  A 343 MB raw CSV expands to 910 MB in RAM when loaded as a Pandas DataFrame due to Python object overhead. Processing the full 15.7 GB monthly feed via Pandas would demand ~42 GB RAM, triggering Out-Of-Memory (OOM) crashes on 16 GB development machines, highlighting the necessity of streaming/out-of-core engines like DuckDB.
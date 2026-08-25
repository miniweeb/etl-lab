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


# A02 — Hive Partitioning, Late-Arriving Data & Lake Building

### Data Lake Specs & Audit
* **Raw Files Scope**: 45 CSV files in daily batch feed (`$env:ETL_LAB_DATA\small\raw\orders`)[cite: 5]
* **Lake Storage Layout**: 
  * **Directories**: 46 directories (45 date partitions `order_date=YYYY-MM-DD` + 1 `__HIVE_DEFAULT_PARTITION__` for NULL timestamps)[cite: 5]
  * **File Count & Size**: 377 total Parquet files totaling ~214 MB on disk (small scale)[cite: 5]
* **Manifest Reconciliation & Build Performance**:
  * **Small Scale Build**: 26.5 seconds[cite: 5] | Lake count: 2,773,566 vs Manifest: 2,773,566 (`MATCH=True`)[cite: 5]
  * **Full Scale Build**: 138.5 seconds[cite: 5] | Lake count: 55,471,709 vs Manifest: 55,471,709 (`MATCH=True`)[cite: 5]

---

### Task 1 Analysis: Raw Timestamp Heterogeneity & Late-Arriving Data

Audit of single batch file (`orders_2026-06-10.csv`, total 60,782 rows)[cite: 5]:

| Data Category | Row Count | Description |
| :--- | :--- | :--- |
| **Own-Day Events** | 59,857 | Orders placed on `2026-06-10`[cite: 5] |
| **Late-Arriving Updates** | 911 | Backdated updates spanning prior 7 days (`06-03` to `06-09`)[cite: 5] |
| **Corrupted Timestamps** | 14 | Unparseable timestamp values evaluating to `NULL`[cite: 5] |

**Format Probe Breakdown (`try_strptime`)**:
* **3 Formats (`null_3fmt`)**: 14 invalid rows (actual corrupted records)[cite: 5]
* **2 Formats (`null_2fmt`)**: 222 invalid rows[cite: 5]
* **Data Loss Delta**: Omitting the ISO-T format (`%Y-%m-%dT%H:%M:%S`) mistakenly drops **208 valid orders** into `NULL`[cite: 5]

---

### Partition Pruning Benchmark (Task 5)

| Metric / Filter Type | Query A: Partition Filter (`order_date = '2026-06-19'`) | Query B: File Content Filter (`CAST(order_ts_clean AS DATE) = '2026-06-19'`) | Performance Efficiency |
| :--- | :--- | :--- | :--- |
| **Execution Time (Median)** | **10.5 ms**[cite: 5] | **31.6 ms**[cite: 5] | **~3x faster**[cite: 5] |
| **Scanned Parquet Files** | **8 / 377 files**[cite: 5] | **377 / 377 files**[cite: 5] | **369 files pruned from I/O**[cite: 5] |
| **Result Verification** | 180,183 rows (avg store: 267.17)[cite: 5] | 180,183 rows (avg store: 267.17)[cite: 5] | Identical results[cite: 5] |

---

### Key Takeaways & Data Quality Pitfalls

* **Implicit `TRY_CAST` Blindspots**: DuckDB's native `TRY_CAST` silently accepts ISO-T timestamps (`2026-06-02T11:41:17`), masking format variations[cite: 5]. Explicit validation via `try_strptime` with a complete format array is required to prevent accidental data dropping during ingestion[cite: 5].
* **Hive Default Partitioning**: Non-standard or invalid timestamps (e.g., `2026-06-31`) resolve to `NULL` dates and are isolated in `__HIVE_DEFAULT_PARTITION__`, preserving overall lake integrity without crashing ingestion[cite: 5].

---

### Real-World Surprises & Technical Gotchas

* **The `OVERWRITE_OR_IGNORE` Incident (Data Loss Crisis)**: 
  When processing daily batch files in `OVERWRITE_OR_IGNORE` mode, incoming batch files containing late corrections for previous days overwrite static filenames (e.g., `data_0.parquet`) in target partition directories[cite: 5]. In testing, processing the June 2nd file erased 60,686 original June 1st rows, leaving only 885 late-arriving records[cite: 5].
* **UUID-based `APPEND` Resolution**: 
  Switching the lake ingestion mode to `APPEND` forces DuckDB/Parquet writers to assign unique UUID file names per write batch[cite: 5]. This allows new Parquet files to be added alongside existing files in partition directories without overwriting prior records[cite: 5].
* **File Fan-out in Early Partition Directories**: 
  Because daily batch files carry late updates for up to 7 prior days, earlier partition folders end up containing multiple smaller Parquet files (e.g., 8 files per folder for early dates)[cite: 5].

# A03 — Data Transformation, Cleaning & Enrichment Pipeline

### Data Audit & Environment Specs
* **Target Date & Scope**: `2026-06-02` batch processing (`$root\raw\orders\orders_2026-06-02.csv`)[cite: 1]
* **Schema Layout**: 4 isolated schemas (`core`, `marts`, `ops`, `staging`)[cite: 1]
* **Raw Staging Volume**: 59,024 raw CSV rows ingested (Small Scale)[cite: 1]
* **Data Quality Anomalies Identified**:
  * **Excel Float Strings**: 120 rows with trailing `.0` (e.g., `123456.0`)[cite: 1]
  * **Missing Identifiers**: 94 rows with `NULL` `customer_id`[cite: 1]
  * **Corrupted Timestamps**: 18 unparseable date strings (e.g., `2026-06-31`)[cite: 1]
  * **Late-Arriving Records**: 883 rows belonging to `2026-06-01`[cite: 1]
* **Test Suite Verification**: 7/7 unit tests passed (`pytest` in 1.43s)[cite: 1]

---

### Execution Performance Metrics

| Metric / Process | Small Scale (`--scale small`) | Full Scale (`--scale full`) | Performance Efficiency |
| :--- | :--- | :--- | :--- |
| **Raw Ingestion Volume** | 59,024 rows[cite: 1] | 1,180,490 rows[cite: 1] | **20x volume scaling** |
| **Manifest Audit Match** | 59,024 vs 59,024 (`Match=True`)[cite: 1] | 1,180,490 vs 1,180,490 (`Match=True`)[cite: 1] | **100% reconciliation**[cite: 1] |
| **Cleaned & Staged Rows** | 58,993 rows[cite: 1] | 1,179,758 rows[cite: 1] | **~99.93% retention**[cite: 1] |
| **Enriched Orders Volume** | 58,993 rows[cite: 1] | 1,179,758 rows[cite: 1] | **100% staged match**[cite: 1] |
| **Pipeline Execution Time** | **1.9 seconds**[cite: 1] | **24.8 seconds**[cite: 1] | **~47,600 rows/sec**[cite: 1] |

---

### Task Breakdown & Data Cleaning Audit (`ops.cleaning_report`)

Audit of transformation rules applied across 59,024 raw records[cite: 1]:

| Cleaning Rule (`rule`) | Affected Rows | Technical Resolution & Impact |
| :--- | :--- | :--- |
| **`ts_ddmmyyyy_fixed`** | 231[cite: 1] | Parsed `DD/MM/YYYY HH:MM:SS` string format into native `TIMESTAMP`[cite: 1] |
| **`payment_recased`** | 204[cite: 1] | Standardized text casing and stripped padding (e.g., `" COD "` $\rightarrow$ `"cod"`)[cite: 1] |
| **`total_decimal_comma_fixed`** | 187[cite: 1] | Replaced European comma decimal separators with standard dots (`1.234,56` $\rightarrow$ `1234.56`)[cite: 1] |
| **`ts_isot_fixed`** | 164[cite: 1] | Standardized ISO-T format (`YYYY-MM-DDTHH:MM:SS`) into native `TIMESTAMP`[cite: 1] |
| **`customer_id_float_fixed`** | 120[cite: 1] | Cast Excel float strings (`123456.0`) to integer `BIGINT`[cite: 1] |
| **`total_na_or_empty_to_null`** | 117[cite: 1] | Evaluated `N/A` and empty string total values to `NULL`[cite: 1] |
| **`total_dollar_stripped`** | 112[cite: 1] | Stripped leading `$` currency symbols from numeric order totals[cite: 1] |
| **`customer_id_missing`** | 94[cite: 1] | Isolated records missing customer identifiers[cite: 1] |
| **`status_recased`** | 74[cite: 1] | Trimmed and lower-cased status string variants[cite: 1] |
| **`status_junk_to_null`** | 71[cite: 1] | Replaced invalid status strings outside whitelist (e.g., `"unknown"`) with `NULL`[cite: 1] |
| **`total_negative_kept`** | 52[cite: 1] | Preserved negative monetary values post-cleaning for downstream audit (A05)[cite: 1] |
| **`dup_order_id_removed`** | 31[cite: 1] | Deduplicated primary key records across the ingested batch[cite: 1] |
| **`ts_unparseable_to_null`** | 18[cite: 1] | Replaced impossible date strings (`2026-06-31`) with `NULL`[cite: 1] |

---

### Key Takeaways & Data Quality Pitfalls

* **Manifest vs. SQL Deduplication Reconciliation Delta**:
  Manifest reported 29 duplicate rows (`dup_rows: 29`), whereas SQL `GROUP BY order_id` deduplication removed **31 rows**[cite: 1]. The 2-row delta originates from late-arriving data[cite: 1]:
  * Manifest evaluates duplicates strictly within same-day event time records (29 duplicates for `2026-06-02`)[cite: 1].
  * The June 2nd file contains 883 late-arriving updates belonging to `2026-06-01`, which contained **2 internal duplicate rows**[cite: 1].
  
* $\text{Total Duplicates Removed (31)} = \text{Same-Day Duplicates (29)} + \text{Late-Arriving Duplicates (2)}$$[cite: 1]

* **Rule as Data vs. Hardcoded SQL Logic**:
  Initial mapping of customer countries yielded 27,322 `NULL` values due to unmapped variants[cite: 1]. Storing country mappings inside `staging.shopcore_country_map` allowed adding missing variants via `INSERT` statements[cite: 1]. This reduced `null_country` down to **548 rows** without altering query logic or redeploying code[cite: 1].

* **Daily Sales Rollup Multiplicity (723 Rows vs. 400 Stores)**:
  `core.daily_store_sales` aggregated 723 rows for 400 stores because the aggregation grain is `(store_id, order_date)`[cite: 1]. Since daily batch files contain late-arriving records across multiple dates (averaging ~1.8 dates per store per batch) plus unmapped/missing date buckets, total rollup rows exceed the physical store count[cite: 1].

---

### Advanced Key Takeaways & Technical Surprises

* **Timezone Safety in Epoch Decoding (`SET TimeZone='UTC'`)**:
  `signup_ts` contains mixed formats including ISO strings, `DD/MM/YYYY` dates, and Unix epoch seconds[cite: 1]. Parsing epoch values without explicitly executing `SET TimeZone='UTC'` causes DuckDB to apply local system offsets, shifting timestamp values by several hours without throwing errors[cite: 1].

* **Compound Latching in Rule Auditing (`status_recased`)**:
  Auditing `status_recased` required three joint conditions (`status IS NOT NULL`, `status <> lower(TRIM(status))`, and `clean_enum() IS NOT NULL`)[cite: 1]. Omitting the clean enum validation causes invalid uppercase values (e.g., `"UNKNOWN"`) to be miscounted as casing fixes instead of junk records, resulting in double-counting across `status_junk_to_null`[cite: 1].

* **Regression Prevention via Pytest Fixtures**:
  In-memory DuckDB connections (`duckdb.connect()`) inside `pytest` fixtures isolated test runs, verifying string cleaners against synthetic `VALUES` tables[cite: 1]. Intentionally breaking a test case (asserting `"Paid"` instead of `"paid"`) immediately flagged `1 failed, 6 passed`, verifying that pipeline regressions are caught prior to merging into main branches[cite: 1].
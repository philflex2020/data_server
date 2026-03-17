# TODO

## Scripts

- **Extract `date_glob_paths()` into a shared utility** (`scripts/parquet_utils.py` or similar).
  Currently duplicated in `duckdb_queries_x86.py` and `migration_compare.py`.
  Both scripts are self-contained by design for now — extract when a third caller appears
  or when the function needs to change.

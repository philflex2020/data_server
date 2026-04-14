#!/usr/bin/env python3
"""
torture_validate.py — post-run validation for the Docker torture test

Usage:
    python3 docker/torture/scripts/torture_validate.py [DATA_DIRS...]

    DATA_DIRS: one or more paths to writer output directories.
               Defaults to all docker volume mounts under /var/lib/docker/volumes/torture_data-*/

Exit codes:
    0  all checks pass
    1  at least one FAIL
"""
import sys, os, argparse, subprocess, json
import duckdb

CHECKS_PASS = []
CHECKS_FAIL = []

def check(name, passed, detail=""):
    if passed:
        CHECKS_PASS.append(name)
        print(f"  ✓  {name}{(' — ' + detail) if detail else ''}")
    else:
        CHECKS_FAIL.append(name)
        print(f"  ✗  {name}{(' — ' + detail) if detail else ''}")

def find_volumes():
    """Find docker volume mount points for torture_data-* volumes."""
    try:
        result = subprocess.run(
            ["docker", "volume", "inspect",
             "torture_data-a", "torture_data-b", "torture_data-c", "torture_data-d"],
            capture_output=True, text=True
        )
        vols = json.loads(result.stdout)
        return [v["Mountpoint"] for v in vols if "Mountpoint" in v]
    except Exception:
        return []

def validate_dir(con, d):
    glob = f"{d}/**/*.parquet"
    label = os.path.basename(d.rstrip("/"))

    # ── C1: no corrupt files ─────────────────────────────────────────────────
    try:
        n = con.execute(f"SELECT COUNT(DISTINCT file_name) FROM parquet_metadata('{glob}')").fetchone()[0]
        check(f"{label}: readable parquet files", n > 0, f"{n} files")
    except Exception as e:
        check(f"{label}: readable parquet files", False, str(e))
        return  # can't run further checks

    # ── C2: no orphan .tmp files ─────────────────────────────────────────────
    tmps = [f for f in os.popen(f"find {d} -name '*.tmp' 2>/dev/null").read().splitlines() if f]
    check(f"{label}: no orphan .tmp files", len(tmps) == 0,
          f"{len(tmps)} found: {tmps[:3]}" if tmps else "")

    # ── C3: zero null counts ────────────────────────────────────────────────
    try:
        rows = con.execute(f"""
            SELECT path_in_schema, SUM(stats_null_count) AS nulls
            FROM parquet_metadata('{glob}')
            GROUP BY path_in_schema
            HAVING SUM(stats_null_count) > 0
        """).fetchall()
        check(f"{label}: zero nulls", len(rows) == 0,
              f"columns with nulls: {[r[0] for r in rows]}" if rows else "")
    except Exception as e:
        check(f"{label}: zero nulls", False, str(e))

    # ── C4: no duplicate rows ────────────────────────────────────────────────
    try:
        total, distinct = con.execute(f"""
            SELECT COUNT(*),
                   COUNT(DISTINCT (timestamp, cell_id, site_id, rack_id, module_id))
            FROM read_parquet('{glob}', union_by_name=true)
        """).fetchone()
        check(f"{label}: no duplicate rows", total == distinct,
              f"total={total:,} distinct={distinct:,} dupes={total-distinct:,}")
    except Exception as e:
        check(f"{label}: no duplicate rows", False, str(e))

    # ── C5: WAL date correctness (partition date matches timestamp date) ─────
    try:
        mismatches = con.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT
                    regexp_extract(file_name, '/(\d{{4}}/\d{{2}}/\d{{2}})/', 1) AS part_date,
                    strftime(to_timestamp(MIN(stats_min_value)), '%Y/%m/%d')      AS data_date
                FROM parquet_metadata('{glob}')
                WHERE path_in_schema = 'timestamp'
                GROUP BY file_name
                HAVING part_date <> data_date AND part_date <> ''
            )
        """).fetchone()[0]
        check(f"{label}: WAL date correctness", mismatches == 0,
              f"{mismatches} files have timestamp date != partition date")
    except Exception as e:
        check(f"{label}: WAL date correctness", False, str(e))

    # ── C6: row count (basic sanity — at least some data) ────────────────────
    try:
        n = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{glob}', union_by_name=true)"
        ).fetchone()[0]
        check(f"{label}: has rows", n > 0, f"{n:,} rows")
    except Exception as e:
        check(f"{label}: has rows", False, str(e))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dirs", nargs="*", help="parquet output directories to validate")
    args = ap.parse_args()

    dirs = args.dirs
    if not dirs:
        dirs = find_volumes()
    if not dirs:
        # fallback: look for local test dirs
        for candidate in ["/data/writer-a", "/data/writer-b", "/data/writer-c", "/data/writer-d",
                          "/tmp/parquet-kpi", "/tmp/diff_old", "/tmp/diff_new"]:
            if os.path.isdir(candidate):
                dirs.append(candidate)
    if not dirs:
        print("No directories found. Pass paths as arguments or run inside the Docker environment.")
        sys.exit(1)

    print(f"\nValidating {len(dirs)} director{'y' if len(dirs)==1 else 'ies'}…\n")
    con = duckdb.connect()

    for d in dirs:
        if not os.path.isdir(d):
            print(f"  ! skipping {d} — not a directory")
            continue
        validate_dir(con, d)
        print()

    # ── cross-writer row-count consistency ───────────────────────────────────
    if len(dirs) > 1:
        counts = []
        for d in dirs:
            try:
                n = con.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{d}/**/*.parquet', union_by_name=true)"
                ).fetchone()[0]
                counts.append((os.path.basename(d.rstrip("/")), n))
            except Exception:
                pass
        if len(counts) > 1:
            max_n = max(c[1] for c in counts)
            min_n = min(c[1] for c in counts)
            pct_diff = (max_n - min_n) / max(max_n, 1) * 100
            detail = "  ".join(f"{n}={c:,}" for n, c in counts)
            check("cross-writer row counts within 1%", pct_diff < 1.0,
                  f"spread={pct_diff:.2f}%  {detail}")
        print()

    # ── summary ──────────────────────────────────────────────────────────────
    total = len(CHECKS_PASS) + len(CHECKS_FAIL)
    if CHECKS_FAIL:
        print(f"✗  {len(CHECKS_FAIL)}/{total} checks FAILED: {', '.join(CHECKS_FAIL)}")
        sys.exit(1)
    else:
        print(f"✓  all {total} checks passed")
        sys.exit(0)


if __name__ == "__main__":
    main()

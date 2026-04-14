#!/usr/bin/env python3
"""Convert Evelyn InfluxDB annotated CSV exports to Parquet."""

import io
import os
import sys
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

INPUT_DIR = "/tmp/evelyn_sample"
OUTPUT_DIR = "/home/phil/parquet"

FILES = [
    "Evelyn unit data sample.csv",
    "Evelyn site data sample.csv",
    "Evelyn pcs data sample.csv",
    "Evelyn rack data sample.csv",
    "Evelyn meter data sample.csv",
    "Evelyn rtac data sample.csv",
]


def read_influx_csv(path: str) -> pd.DataFrame:
    """
    InfluxDB annotated CSV has multiple 'tables' separated by blank lines,
    each preceded by #group/#datatype/#default annotation lines and a header row.
    We collect all data rows, inferring the column schema from the first header.
    """
    chunks = []
    header = None
    buf_lines = []

    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if line.startswith("#"):
                # flush any buffered data rows before new annotation block
                if buf_lines and header:
                    chunks.append(pd.read_csv(
                        io.StringIO("\n".join([header] + buf_lines)),
                        low_memory=False,
                    ))
                    buf_lines = []
                continue
            if line == "":
                # blank separator — flush
                if buf_lines and header:
                    chunks.append(pd.read_csv(
                        io.StringIO("\n".join([header] + buf_lines)),
                        low_memory=False,
                    ))
                    buf_lines = []
                header = None
                continue
            if line.startswith(",result,table"):
                # This is a header row; flush existing buffer first
                if buf_lines and header:
                    chunks.append(pd.read_csv(
                        io.StringIO("\n".join([header] + buf_lines)),
                        low_memory=False,
                    ))
                    buf_lines = []
                header = line
                continue
            if header is not None:
                buf_lines.append(line)

    # final flush
    if buf_lines and header:
        chunks.append(pd.read_csv(
            io.StringIO("\n".join([header] + buf_lines)),
            low_memory=False,
        ))

    if not chunks:
        return pd.DataFrame()

    df = pd.concat(chunks, ignore_index=True)

    # Drop InfluxDB bookkeeping columns (leading comma produces "Unnamed: 0")
    drop_cols = ["", "Unnamed: 0", "result", "table", "_start", "_stop"]
    df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True)

    # Rename InfluxDB columns to cleaner names
    df.rename(columns={
        "_time":        "ts",
        "_value":       "_raw_value",
        "_field":       "dtype",
        "_measurement": "measurement",
    }, inplace=True)

    # Parse timestamp
    df["ts"] = pd.to_datetime(df["ts"], utc=True, format="ISO8601")

    # Split _raw_value into value_f / value_i based on dtype
    mask_int = df["dtype"].isin(["integer", "boolean_integer"])
    df["value_f"] = pd.to_numeric(df.loc[~mask_int, "_raw_value"], errors="coerce")
    df["value_i"] = pd.to_numeric(df.loc[mask_int,  "_raw_value"], errors="coerce") \
                      .astype("Int64")
    df.drop(columns=["_raw_value"], inplace=True)

    return df


def convert(csv_name: str, out_dir: str):
    path = os.path.join(INPUT_DIR, csv_name)
    # derive a clean output name from the measurement keyword in the filename
    # e.g. "Evelyn pcs data sample.csv" → "pcs"
    words = csv_name.lower().replace(".csv", "").split()
    # second word is the measurement type
    measurement = words[1] if len(words) > 1 else "data"
    out_path = os.path.join(out_dir, f"{measurement}.parquet")

    print(f"\n{'='*60}")
    print(f"  {csv_name}")
    print(f"  → {out_path}")

    t0 = time.time()
    df = read_influx_csv(path)
    t1 = time.time()
    print(f"  read:  {len(df):,} rows  ({t1-t0:.1f}s)")
    if df.empty:
        print("  (empty — skipping)")
        return

    print(f"  cols:  {list(df.columns)}")
    print(f"  dtypes hint breakdown:\n{df['dtype'].value_counts().to_string()}")

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(
        table, out_path,
        compression="snappy",
        row_group_size=250_000,
    )
    t2 = time.time()
    size_mb = os.path.getsize(out_path) / 1e6
    print(f"  write: {size_mb:.1f} MB  ({t2-t1:.1f}s)")
    ratio = os.path.getsize(path) / os.path.getsize(out_path)
    print(f"  compression ratio: {ratio:.1f}×")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    total_t0 = time.time()
    for f in FILES:
        convert(f, OUTPUT_DIR)
    elapsed = time.time() - total_t0
    print(f"\nDone in {elapsed:.1f}s")
    print("\nOutput files:")
    for f in sorted(os.listdir(OUTPUT_DIR)):
        p = os.path.join(OUTPUT_DIR, f)
        print(f"  {f:30s}  {os.path.getsize(p)/1e6:.1f} MB")


if __name__ == "__main__":
    main()

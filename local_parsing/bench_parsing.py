#!/usr/bin/env python
"""Benchmark column-pruned parsing (``python_parsing_optimized.py``).

    python local_parsing/bench_parsing.py account                        # 250,150,50 x 3 runs
    python local_parsing/bench_parsing.py account --columns all,200,100 --repeats 5
    python local_parsing/bench_parsing.py account --columns 50 --repeats 1     # smoke test

One Spark session.  Untimed setup: build session -> Oracle read (history job,
full table unless --start-date/--end-date) -> land ALL the raw XML once as
Parquet (--raw-path) -> one warm-up pass.

Timed, per run and per column count N (each run times every N once, so
slow drift on the machine hits every N equally):
    read raw Parquet -> apply_xml_parsing(columns=N) -> normalize_arrays
    -> createOrReplace iceberg.bronze.<dataset>_<N>_columns
The clock stops when the Iceberg commit returns.  Results are printed as one
row per run (run | N1 time | N2 time | ...) and written in the same layout to
local_parsing/parsing_results.csv (overwritten on every invocation).
"""

from __future__ import annotations

import argparse
import csv
import os
import statistics
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
from python_parsing_optimized import apply_xml_parsing, normalize_arrays
from run_parsing import (
    _date_arg,
    build_spark_session,
    load_lookup,
    logger,
    read_jdbc,
    read_raw_parquet,
    write_raw_parquet,
)

RESULTS_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "parsing_results.csv")


def write_iceberg(df, target: str) -> None:
    """Same create path / table properties as run_parsing.write_result (history = replace)."""
    (df.writeTo(target).using("iceberg")
       .tableProperty("format-version", "2")
       .tableProperty("write.merge.mode", "merge-on-read")
       .tableProperty("write.update.mode", "merge-on-read")
       .tableProperty("write.metadata.previous-versions-max", "5")
       .tableProperty("write.metadata.delete-after-commit.enabled", "true")
       .createOrReplace())


def parse_and_write(spark, schema_df, dataset, raw_path: str, columns, target: str) -> float:
    """Timed section: raw Parquet read -> parse selected columns -> Iceberg commit."""
    from pyspark.sql import functions as F

    t = time.perf_counter()
    raw_df = read_raw_parquet(spark, raw_path)
    parsed = apply_xml_parsing(spark, raw_df, schema_df,
                               metadata_cols=[F.col("recid")], columns=columns)
    if dataset.normalize_arrays:
        parsed = normalize_arrays(parsed)
    write_iceberg(parsed, target)
    return time.perf_counter() - t


def _columns_arg(s: str) -> list:
    out = []
    for tok in s.split(","):
        tok = tok.strip().lower()
        if tok == "all":
            out.append(None)
        elif tok.isdigit() and int(tok) > 0:
            out.append(int(tok))
        else:
            raise argparse.ArgumentTypeError(f"{tok!r} is not 'all' or a positive integer")
    return out


def write_results(labels: list[str], runs: list[dict]) -> None:
    """One row per run: run, <N> columns (s), ... - same layout as the terminal table."""
    with open(RESULTS_CSV, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["run"] + labels)
        for i, run in enumerate(runs, 1):
            w.writerow([i] + [f"{run[lbl]:.2f}" for lbl in labels])


def print_results(title: str, labels: list[str], runs: list[dict]) -> None:
    width = max(len(lbl) for lbl in labels) + 2
    print(f"\n==== {title} ====")
    print(f"{'run':<8}" + "".join(f"{lbl:>{width}}" for lbl in labels))
    for i, run in enumerate(runs, 1):
        print(f"{i:<8}" + "".join(f"{run[lbl]:>{width}.2f}" for lbl in labels))
    for name, fn in (("min", min), ("median", statistics.median)):
        print(f"{name:<8}" + "".join(
            f"{fn([run[lbl] for run in runs]):>{width}.2f}" for lbl in labels))


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="bench_parsing.py", description=__doc__.split("\n")[0])
    p.add_argument("dataset", help=f"one of: {', '.join(config.DATASETS)}")
    p.add_argument("--columns", type=_columns_arg, default=_columns_arg("250,150,50"),
                   help="comma-separated column counts, 'all' = every lookup column")
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--raw-path", default=None,
                   help="raw XML Parquet folder (default <RAW_PARQUET_ROOT>/<dataset>_raw/load_id=bench)")
    p.add_argument("--start-date", type=_date_arg, metavar="YYYYMMDD")
    p.add_argument("--end-date", type=_date_arg, metavar="YYYYMMDD")
    args = p.parse_args(argv)

    if args.dataset not in config.DATASETS:
        p.error(f"unknown dataset {args.dataset!r}; known: {', '.join(config.DATASETS)}")
    if bool(args.start_date) != bool(args.end_date):
        p.error("--start-date and --end-date must be given together")
    if args.repeats < 1:
        p.error("--repeats must be >= 1")
    window = (args.start_date, args.end_date) if args.start_date else None
    dataset = config.DATASETS[args.dataset]
    job = config.JOBS["history"]
    raw_path = args.raw_path or config.raw_parquet_path(dataset, "bench")

    # ---- untimed setup ---------------------------------------------------- #
    spark = build_spark_session(job)
    try:
        schema_df = load_lookup(spark, dataset)
        total_cols = schema_df.count()

        # the bench re-lands into one fixed folder, so overwrite is intended here
        write_raw_parquet(read_jdbc(spark, dataset, job, window), raw_path, mode="overwrite")
        source_rows = read_raw_parquet(spark, raw_path).count()
        logger.info("raw parquet %s: %d rows, lookup columns: %d", raw_path, source_rows, total_cols)
        if source_rows == 0:
            logger.error("no source rows - nothing to benchmark")
            return 1

        def target_for(n):
            label = "all" if n is None else str(n)
            return config._qualify(f"{config.NAMESPACE}.{dataset.name}_{label}_columns",
                                   config.NAMESPACE)

        logger.info("warm-up pass (untimed)")
        parse_and_write(spark, schema_df, dataset, raw_path, None, target_for(None))

        # ---- timed passes: one run = every column count once --------------- #
        labels = [f"{total_cols if n is None else min(n, total_cols)} columns (s)"
                  for n in args.columns]
        runs: list[dict] = []
        for r in range(1, args.repeats + 1):
            run: dict = {}
            for n, label in zip(args.columns, labels):
                target = target_for(n)
                secs = parse_and_write(spark, schema_df, dataset, raw_path, n, target)
                target_rows = spark.table(target).count()   # sanity check, untimed
                logger.info("run=%d  %s  %.2fs  -> %s (%d rows)",
                            r, label, secs, target, target_rows)
                run[label] = secs
            runs.append(run)
    finally:
        spark.stop()

    write_results(labels, runs)
    print_results(f"{dataset.name}: parse + Iceberg write, {source_rows} rows "
                  f"({datetime.now():%Y-%m-%d %H:%M})", labels, runs)
    print(f"\nwritten to {RESULTS_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

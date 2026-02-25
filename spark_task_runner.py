#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import importlib.util
import os
from pathlib import Path

from pyspark import SparkFiles
from pyspark.sql import SparkSession



def resolve_module_path(module_path: str) -> str:
    resolved = Path(module_path).expanduser()
    if resolved.is_file() and os.access(resolved, os.R_OK):
        return str(resolved.resolve())

    candidate = None
    try:
        candidate = Path(SparkFiles.get(resolved.name))
    except Exception:
        candidate = None

    if candidate and candidate.is_file():
        return str(candidate.resolve())

    cwd_candidate = Path.cwd() / resolved.name
    if cwd_candidate.is_file():
        return str(cwd_candidate.resolve())

    return str(resolved)


def load_module_from_path(module_path: str):
    module_path = resolve_module_path(module_path)
    spec = importlib.util.spec_from_file_location("dag_module", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--module", required=True, help="Path to DAG python file")
    parser.add_argument("--task", required=True, help="Task function name to execute")
    parser.add_argument("--app-name", default=None)
    args = parser.parse_args()

    os.environ["SPARK_TASK_RUNNER"] = "1"

    app_name = args.app_name or args.task
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    sc = spark.sparkContext

    module = load_module_from_path(args.module)
    func = getattr(module, args.task, None)
    if func is None:
        raise RuntimeError(f"Task {args.task} not found in {args.module}")

    func(spark, sc)

    spark.stop()


if __name__ == "__main__":
    main()

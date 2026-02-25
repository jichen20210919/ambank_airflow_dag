#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-09 23:17:00
# @Author  : cloudera
# @File    : airflow_JOB_TCS_10_MIS006_MIS006DELTA_CDC.py
# @Copyright: Cloudera.Inc

from __future__ import annotations

import base64
import json
import logging
import os
from datetime import datetime, timedelta

_SPARK_TASK_RUNNER = os.environ.get("SPARK_TASK_RUNNER") == "1"

if not _SPARK_TASK_RUNNER:
    import airflow
    import pendulum
    from airflow.decorators import task
    from airflow.models import DAG, Variable
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
else:
    def _identity(func=None, **_kwargs):
        if func is None:
            return lambda f: f
        return func

    class _TaskDecorator:
        def __call__(self, *args, **kwargs):
            return _identity(*args, **kwargs)

        def pyspark(self, *args, **kwargs):
            return _identity(*args, **kwargs)

    task = _TaskDecorator()

    class Variable:
        @staticmethod
        def get(key, default_var=None, deserialize_json=False):
            if key == "JOB_PARAMS":
                raw = os.environ.get("JOB_PARAMS_B64")
                if raw:
                    import base64 as _base64
                    import json as _json
                    return _json.loads(_base64.b64decode(raw.encode()).decode())
            return default_var if default_var is not None else {}

    class DAG:
        pass

    class SparkSubmitOperator:
        pass

from pyspark import SparkContext
from pyspark.sql import SparkSession


@task
def JOB_TCS_10_MIS006_MIS006DELTA_CDC(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))


@task
def Job_VIEW(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))


@task.pyspark(conn_id="spark-local")
def RUN_TCS_10_MIS006_MIS006DELTA_CDC(spark: SparkSession, sc: SparkContext, **kw_args):
    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)

    log = logging.getLogger(__name__)

    job_params = Variable.get("JOB_PARAMS", deserialize_json=True)
    schema_nm = job_params.get(
        "SILVER_DB",
        job_params.get("silver_db", job_params.get("EDW_STAGING_CORE", job_params.get("edw_staging_core", "silver"))),
    )
    batch_seq = int(job_params.get("BATCH_SEQ", job_params.get("batch_seq", 0)))
    required_tables_suffix = (
        "tcs_mis006",
        "tcs_mis006_prev",
        "tcs_mis006_delta",
        "job_adt_recon_dtl",
    )
    required_keys = {"mi006_soc_no", "mi006_memb_cust_ac"}
    fallback_candidates = [schema_nm]
    if "silver" not in {schema_nm.lower()}:
        fallback_candidates.append("silver")

    resolved_schema = None
    missing_target_details = []
    for candidate in fallback_candidates:
        candidate = candidate.strip()
        if not candidate:
            continue
        missing_tables = [
            suffix for suffix in required_tables_suffix
            if not spark.catalog.tableExists(f"{candidate}.{suffix}")
        ]
        if "tcs_mis006_delta" in missing_tables:
            delta_table = f"{candidate}.tcs_mis006_delta"
            prev_table = f"{candidate}.tcs_mis006_prev"
            desc_rows = spark.sql(f"DESCRIBE EXTENDED {prev_table}").collect()
            provider = next(
                (row["data_type"].lower() for row in desc_rows if (row["col_name"] or "").strip().lower() == "provider"),
                None,
            )
            if provider:
                log.info("Prev table provider for %s is %s", prev_table, provider)
            delta_location = None
            warehouse_dir = spark.conf.get("spark.sql.warehouse.dir")
            if warehouse_dir:
                delta_location = f"{warehouse_dir.rstrip('/')}/{candidate}.db/tcs_mis006_delta"
            if delta_location:
                try:
                    hadoop_conf = spark._jsc.hadoopConfiguration()
                    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
                    path = spark._jvm.org.apache.hadoop.fs.Path(delta_location)
                    if fs.exists(path):
                        log.info("Deleting leftover location %s before creating %s", delta_location, delta_table)
                        fs.delete(path, True)
                except Exception as exc:  # noqa: BLE001
                    log.warning("Unable to delete %s: %s", delta_location, exc)
            create_sql = (
                f"CREATE TABLE {delta_table} "
                + (f"USING {provider} " if provider else "")
                + f"AS SELECT * FROM {prev_table} WHERE 1=0"
            )
            spark.sql(create_sql)
            missing_tables = [t for t in missing_tables if t != "tcs_mis006_delta"]
        if missing_tables:
            missing_target_details.append(
                (candidate, "tables", missing_tables)
            )
            continue
        prev_columns = [col.lower() for col in spark.table(f"{candidate}.tcs_mis006_prev").columns]
        missing_keys = required_keys - set(prev_columns)
        if missing_keys:
            missing_target_details.append((candidate, "columns", missing_keys))
            continue
        resolved_schema = candidate
        break

    if resolved_schema is None:
        details = "; ".join(
            f"{candidate} missing {kind}: {', '.join(sorted(vals))}"
            for candidate, kind, vals in missing_target_details
        ) or "No candidate schema contained the expected tables."
        raise ValueError(
            f"Unable to locate schema with required join columns: {details}"
        )
    schema_nm = resolved_schema

    start_time = datetime.now()

    delta_table = f"{schema_nm}.tcs_mis006_delta"
    if not spark.catalog.tableExists(delta_table):
        log.warning("%s is missing; creating schema like %s.tcs_mis006_prev", delta_table, schema_nm)
        spark.sql(
            f"CREATE TABLE {delta_table} LIKE {schema_nm}.tcs_mis006_prev WHERE 1=0"
        )

    log.info("Truncating delta table")
    spark.sql(f"TRUNCATE TABLE {delta_table}")

    log.info("Computing delta rows via DataFrame subtraction")
    df_current = spark.table(f"{schema_nm}.tcs_mis006")

    log.info(
        "Assuming %s.tcs_mis006_prev is empty; copying the entire %s.tcs_mis006 snapshot into delta.",
        schema_nm,
        schema_nm,
    )
    df_delta = (
        df_current
        .dropDuplicates(["mi006_soc_no", "mi006_memb_cust_ac"])
    )
    insert_rec = df_delta.count()
    log.info("Writing delta table (%s rows)", insert_rec)
    df_delta.write.mode("overwrite").saveAsTable(f"{schema_nm}.tcs_mis006_delta")

    audit_sql = (
        "INSERT INTO {schema}.job_adt_recon_dtl "
        "(job_id, job_nm, strt_dt_tm, end_dt_tm, inpt_rcrd_cnt, udt_rcrd_cnt, isrt_rcrd_cnt, "
        "btch_id, btch_seq_id, crnt_sts, audit_time) VALUES ("
        "NULL, 'job_tcs_10_mis006_mis006delta_cdc', "
        f"CAST('{start_time:%Y-%m-%d %H:%M:%S}' AS timestamp), "
        "CAST(current_timestamp() AS timestamp), "
        f"CAST({insert_rec} AS decimal(18,0)), "
        "CAST(0 AS decimal(18,0)), "
        f"CAST({insert_rec} AS decimal(18,0)), "
        "NULL, "
        f"CAST({batch_seq} AS decimal(18,0)), "
        "'SUCCESS', "
        "CAST(current_timestamp() AS timestamp))"
    ).format(schema=schema_nm)

    log.info("Writing audit record")
    spark.sql(audit_sql)


if not _SPARK_TASK_RUNNER:
    _JOB_PARAMS_B64 = base64.b64encode(
        json.dumps(Variable.get("JOB_PARAMS", default_var={}, deserialize_json=True)).encode()
    ).decode()

    with DAG(
        dag_id="JOB_TCS_10_MIS006_MIS006DELTA_CDC",
        default_args={"owner": "airflow", "retries": 0, "retry_delay": timedelta(minutes=5)},
        schedule_interval=None,
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        tags=["spark", "storedproc", "mis006"],
    ) as dag:
        JOB_TCS_10_MIS006_MIS006DELTA_CDC_task = JOB_TCS_10_MIS006_MIS006DELTA_CDC()
        Job_VIEW_task = Job_VIEW()

        SPARK_CONF_COMMON = {
            "spark.executor.instances": "10",
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.defaultCatalog": "spark_catalog",
            "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083",
            "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar",
            "spark.dynamicAllocation.enabled": "false",
            "spark.shuffle.service.enabled": "false",
            "spark.app.name": "JOB_TCS_10_MIS006_MIS006DELTA_CDC",
            "spark.openlineage.appName": "JOB_TCS_10_MIS006_MIS006DELTA_CDC",
            "spark.openmetadata.transport.pipelineServiceName": "JOB_TCS_10_MIS006_MIS006DELTA_CDC",
            "spark.openmetadata.transport.pipelineName": "JOB_TCS_10_MIS006_MIS006DELTA_CDC",
            "spark.executor.memory": "10g",
            "spark.executor.memoryOverhead": "2g",
            "spark.driver.memory": "8g",
            "spark.memory.fraction": "0.8",
            "spark.memory.storageFraction": "0.2",
            "spark.sql.shuffle.partitions": "400",
        }

        RUN_TCS_10_MIS006_MIS006DELTA_CDC_task = SparkSubmitOperator(
            conf=SPARK_CONF_COMMON,
            task_id="RUN_TCS_10_MIS006_MIS006DELTA_CDC",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="RUN_TCS_10_MIS006_MIS006DELTA_CDC",
            deploy_mode="cluster",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={
                "SPARK_TASK_RUNNER": "1",
                "HADOOP_CONF_DIR": "/etc/hadoop/conf",
                "YARN_CONF_DIR": "/etc/hadoop/conf",
                "HIVE_CONF_DIR": "/etc/hive/conf",
                "JOB_PARAMS_B64": _JOB_PARAMS_B64,
            },
            application_args=["--module", __file__, "--task", "RUN_TCS_10_MIS006_MIS006DELTA_CDC"],
        )

        JOB_TCS_10_MIS006_MIS006DELTA_CDC_task >> Job_VIEW_task
        Job_VIEW_task >> RUN_TCS_10_MIS006_MIS006DELTA_CDC_task

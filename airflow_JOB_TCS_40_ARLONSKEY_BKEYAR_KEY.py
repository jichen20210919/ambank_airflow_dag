#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-10 00:32:47
# @Author  : cloudera
# @File    : airflow_JOB_TCS_40_ARLONSKEY_BKEYAR_KEY.py
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
def JOB_TCS_40_ARLONSKEY_BKEYAR_KEY(**kw_args) -> str:
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))


@task
def Job_VIEW(**kw_args) -> str:
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))


@task.pyspark(conn_id="spark-local")
def RUN_TCS_40_ARLONSKEY_BKEYAR_KEY(spark: SparkSession, sc: SparkContext, **kw_args):
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, TimestampType
    from ds_functions import spark_register_ds_common_functions

    spark_register_ds_common_functions(spark)

    log = logging.getLogger(__name__)

    job_params = Variable.get("JOB_PARAMS", deserialize_json=True)
    schema_staging = job_params.get(
        "SILVER_DB",
        job_params.get("silver_db", job_params.get("EDW_STAGING_CORE", job_params.get("edw_staging_core", "silver"))),
    )
    schema_work = job_params.get(
        "SILVER_DB",
        job_params.get("silver_db", job_params.get("EDW_WORK_CORE", job_params.get("edw_work_core", "silver"))),
    )
    schema_sor = job_params.get(
        "SILVER_DB",
        job_params.get("silver_db", job_params.get("EDW_SOR", job_params.get("edw_sor", "silver"))),
    )
    batch_id = int(job_params.get("BATCH_ID", job_params.get("batch_id", 0)))
    batch_seq = int(job_params.get("BATCH_SEQ", job_params.get("batch_seq", 0)))

    required_tables = (
        f"{schema_staging}.job_adt_recon_dtl",
        f"{schema_work}.tcs_ar_lon_skey",
        f"{schema_sor}.bkey_ar",
    )
    missing = [t for t in required_tables if not spark.catalog.tableExists(t)]
    if missing:
        raise ValueError("Missing required tables in expected schemas: {}".format(", ".join(missing)))

    start_time = datetime.now()

    log.info("Loading source tables")
    df_skey = spark.table(f"{schema_work}.tcs_ar_lon_skey")
    df_bkey = spark.table(f"{schema_sor}.bkey_ar")

    log.info("Filtering NEW keys and removing existing ar_id")
    df_new = df_skey.filter(F.col("keyflag") == "NEW")
    df_to_insert = df_new.join(df_bkey.select("ar_id"), on="ar_id", how="left_anti")

    df_final = df_to_insert.select(
        F.col("ar_id"),
        F.col("unq_id_src_stm"),
        F.lit(start_time).cast(TimestampType()).alias("ppn_dt"),
        F.lit("TCS").alias("src_stm_id"),
        F.lit(batch_id).cast(IntegerType()).alias("edw_btch_id"),
    )

    log.info("Appending to bkey_ar via SQL insert")
    temp_view = "tmp_bkey_ar_insert"
    df_final.createOrReplaceTempView(temp_view)
    spark.sql(f"INSERT INTO {schema_sor}.bkey_ar SELECT * FROM {temp_view}")

    insert_rec = df_final.count()

    audit_sql = (
        "INSERT INTO {schema}.job_adt_recon_dtl "
        "(job_id, job_nm, strt_dt_tm, end_dt_tm, inpt_rcrd_cnt, udt_rcrd_cnt, isrt_rcrd_cnt, "
        "btch_id, btch_seq_id, crnt_sts, audit_time) VALUES ("
        "NULL, 'job_tcs_40_arlonskey_bkeyar_key', "
        f"CAST('{start_time:%Y-%m-%d %H:%M:%S}' AS timestamp), "
        "CAST(current_timestamp() AS timestamp), "
        f"CAST({insert_rec} AS decimal(18,0)), "
        "CAST(0 AS decimal(18,0)), "
        f"CAST({insert_rec} AS decimal(18,0)), "
        f"CAST({batch_id} AS decimal(18,0)), "
        f"CAST({batch_seq} AS decimal(18,0)), "
        "'SUCCESS', "
        "CAST(current_timestamp() AS timestamp))"
    ).format(schema=schema_staging)

    log.info("Writing audit record")
    spark.sql(audit_sql)


if not _SPARK_TASK_RUNNER:
    _JOB_PARAMS_B64 = base64.b64encode(
        json.dumps(Variable.get("JOB_PARAMS", default_var={}, deserialize_json=True)).encode()
    ).decode()

    with DAG(
        dag_id="JOB_TCS_40_ARLONSKEY_BKEYAR_KEY",
        default_args={"owner": "airflow", "retries": 0, "retry_delay": timedelta(minutes=5)},
        schedule_interval=None,
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=False,
        tags=["spark", "storedproc", "mis006"],
    ) as dag:
        JOB_TCS_40_ARLONSKEY_BKEYAR_KEY_task = JOB_TCS_40_ARLONSKEY_BKEYAR_KEY()
        Job_VIEW_task = Job_VIEW()

        SPARK_CONF_COMMON = {
            "spark.executor.instances": "10",
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.defaultCatalog": "spark_catalog",
            "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083",
            "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar",
            "spark.dynamicAllocation.enabled": "false",
            "spark.shuffle.service.enabled": "false",
            "spark.app.name": "JOB_TCS_40_ARLONSKEY_BKEYAR_KEY",
            "spark.openlineage.appName": "JOB_TCS_40_ARLONSKEY_BKEYAR_KEY",
            "spark.openmetadata.transport.pipelineServiceName": "JOB_TCS_40_ARLONSKEY_BKEYAR_KEY",
            "spark.openmetadata.transport.pipelineName": "JOB_TCS_40_ARLONSKEY_BKEYAR_KEY",
        }

        RUN_TCS_40_ARLONSKEY_BKEYAR_KEY_task = SparkSubmitOperator(
            conf=SPARK_CONF_COMMON,
            task_id="RUN_TCS_40_ARLONSKEY_BKEYAR_KEY",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="RUN_TCS_40_ARLONSKEY_BKEYAR_KEY",
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
            application_args=["--module", __file__, "--task", "RUN_TCS_40_ARLONSKEY_BKEYAR_KEY"],
        )

        JOB_TCS_40_ARLONSKEY_BKEYAR_KEY_task >> Job_VIEW_task
        Job_VIEW_task >> RUN_TCS_40_ARLONSKEY_BKEYAR_KEY_task

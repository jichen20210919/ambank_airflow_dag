#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-10 00:33:25
# @Author  : cloudera
# @File    : airflow_JOB_TCS_40_MIS006_AR_UPD.py
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
def JOB_TCS_40_MIS006_AR_UPD(**kw_args) -> str:
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))


@task
def Job_VIEW(**kw_args) -> str:
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))


@task.pyspark(conn_id="spark-local")
def RUN_TCS_40_MIS006_AR_UPD(spark: SparkSession, sc: SparkContext, **kw_args):
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
    batch_seq = int(job_params.get("BATCH_SEQ", job_params.get("batch_seq", 0)))

    required_tables = (
        f"{schema_staging}.job_adt_recon_dtl",
        f"{schema_staging}.tcs_mis006",
        f"{schema_sor}.bkey_ar",
        f"{schema_sor}.ar",
    )
    missing = [t for t in required_tables if not spark.catalog.tableExists(t)]
    if missing:
        raise ValueError("Missing required tables in expected schemas: {}".format(", ".join(missing)))

    start_time = datetime.now()

    log.info("Loading source tables")
    df_mis006 = spark.table(f"{schema_staging}.tcs_mis006").alias("c")
    df_bkey = spark.table(f"{schema_sor}.bkey_ar").alias("b")
    df_ar = spark.table(f"{schema_sor}.ar").alias("a")

    log.info("Preparing updates")
    df_joined = (
        df_ar.join(df_bkey, F.col("a.ar_id") == F.col("b.ar_id"), "inner")
        .join(
            df_mis006,
            F.concat(F.lit("TCS#"), F.col("c.mi006_soc_no"), F.lit("#"), F.col("c.mi006_memb_cust_ac"))
            == F.col("b.unq_id_src_stm"),
            "inner",
        )
        .filter(F.col("a.end_dt") == 99991231)
    )

    df_updates = df_joined.select(
        F.col("a.ar_id").alias("upd_ar_id"),
        F.col("c.mi006_loan_age").alias("upd_am_ac_age"),
        F.col("c.mi006_lst_mnt_date").alias("upd_am_src_rcrd_udt_dt"),
        F.col("c.mi006_lst_fin_date").alias("upd_am_last_fnc_dt"),
        F.col("c.mi006_bois_payment_counter").alias("upd_am_pymt_cnter"),
    )

    df_ar_updated = (
        df_ar.join(df_updates, df_ar.ar_id == df_updates.upd_ar_id, "left")
        .withColumn(
            "am_ac_age",
            F.when(F.col("upd_am_ac_age").isNotNull(), F.col("upd_am_ac_age")).otherwise(F.col("am_ac_age")),
        )
        .withColumn(
            "am_src_rcrd_udt_dt",
            F.when(
                F.col("upd_am_src_rcrd_udt_dt").isNotNull(),
                F.col("upd_am_src_rcrd_udt_dt"),
            ).otherwise(F.col("am_src_rcrd_udt_dt")),
        )
        .withColumn(
            "am_last_fnc_dt",
            F.when(F.col("upd_am_last_fnc_dt").isNotNull(), F.col("upd_am_last_fnc_dt")).otherwise(
                F.col("am_last_fnc_dt")
            ),
        )
        .withColumn(
            "am_pymt_cnter",
            F.when(F.col("upd_am_pymt_cnter").isNotNull(), F.col("upd_am_pymt_cnter")).otherwise(
                F.col("am_pymt_cnter")
            ),
        )
        .drop("upd_ar_id", "upd_am_ac_age", "upd_am_src_rcrd_udt_dt", "upd_am_last_fnc_dt", "upd_am_pymt_cnter")
    )

    log.info("Writing updated SOR table")
    df_ar_updated.write.mode("overwrite").format("parquet").saveAsTable(f"{schema_sor}.ar")

    update_rec = df_updates.count()

    audit_sql = (
        "INSERT INTO {schema}.job_adt_recon_dtl "
        "(job_id, job_nm, strt_dt_tm, end_dt_tm, inpt_rcrd_cnt, udt_rcrd_cnt, isrt_rcrd_cnt, "
        "btch_id, btch_seq_id, crnt_sts, audit_time) VALUES ("
        "NULL, 'job_tcs_40_mis006_ar_upd', "
        f"CAST('{start_time:%Y-%m-%d %H:%M:%S}' AS timestamp), "
        "CAST(current_timestamp() AS timestamp), "
        "CAST(0 AS decimal(18,0)), "
        f"CAST({update_rec} AS decimal(18,0)), "
        "CAST(0 AS decimal(18,0)), "
        "NULL, "
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
        dag_id="JOB_TCS_40_MIS006_AR_UPD",
        default_args={"owner": "airflow", "retries": 0, "retry_delay": timedelta(minutes=5)},
        schedule_interval=None,
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=False,
        tags=["spark", "storedproc", "mis006"],
    ) as dag:
        JOB_TCS_40_MIS006_AR_UPD_task = JOB_TCS_40_MIS006_AR_UPD()
        Job_VIEW_task = Job_VIEW()

        SPARK_CONF_COMMON = {
            "spark.executor.instances": "10",
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.defaultCatalog": "spark_catalog",
            "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083",
            "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar",
            "spark.dynamicAllocation.enabled": "false",
            "spark.shuffle.service.enabled": "false",
            "spark.app.name": "JOB_TCS_40_MIS006_AR_UPD",
            "spark.openlineage.appName": "JOB_TCS_40_MIS006_AR_UPD",
            "spark.openmetadata.transport.pipelineServiceName": "JOB_TCS_40_MIS006_AR_UPD",
            "spark.openmetadata.transport.pipelineName": "JOB_TCS_40_MIS006_AR_UPD",
        }

        RUN_TCS_40_MIS006_AR_UPD_task = SparkSubmitOperator(
            conf=SPARK_CONF_COMMON,
            task_id="RUN_TCS_40_MIS006_AR_UPD",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="RUN_TCS_40_MIS006_AR_UPD",
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
            application_args=["--module", __file__, "--task", "RUN_TCS_40_MIS006_AR_UPD"],
        )

        JOB_TCS_40_MIS006_AR_UPD_task >> Job_VIEW_task
        Job_VIEW_task >> RUN_TCS_40_MIS006_AR_UPD_task

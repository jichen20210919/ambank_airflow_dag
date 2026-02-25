#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-10 20:00:45
# @Author  : cloudera
# @File    : spark_bronze_crmsxml_trdm_stg_crms_1.py
# @Copyright: Cloudera.Inc

from __future__ import annotations

import base64

import decimal
import glob
import json
import logging
import os
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple
import xml.etree.ElementTree as ET

import os
_SPARK_TASK_RUNNER = os.environ.get("SPARK_TASK_RUNNER") == "1"

if not _SPARK_TASK_RUNNER:
    import airflow
    from airflow.decorators import task, task_group
    from airflow.models import DAG, Variable
    from airflow.models.dag import DAG
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

    def task_group(*args, **kwargs):
        return _identity

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
from jinja2 import Template
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType


ACCOUNT_FIELD_MAP: Dict[str, str] = {
    "140000": "EBIT",
    "140001": "EBITDA",
    "140002": "LESS_TAX",
    "140003": "ACCT_PAYBL",
}

ACCOUNT_SCALE_MAP: Dict[str, int] = {
    "140000": 2,
    "140001": 2,
    "140002": 2,
    "140003": 2,
}


def resolve_input_paths(path: str) -> List[str]:
    if "://" in path and not path.startswith("file://"):
        return [path]
    if os.path.isdir(path):
        return sorted(glob.glob(os.path.join(path, "*.xml")))
    matches = glob.glob(path)
    return matches if matches else [path]


def build_schema() -> StructType:
    return StructType([
        StructField("REFERENCE_NO", StringType(), True),
        StructField("BNM_CORE_SECTOR", StringType(), True),
        StructField("BNM_INDS_CLASFN", StringType(), True),
        StructField("BNM_SECTOR_CD", StringType(), True),
        StructField("BUSINESS", StringType(), True),
        StructField("CRM_INDS_CLASFN", StringType(), True),
        StructField("FIN_YEAR_END_T", TimestampType(), True),
        StructField("EBIT", DecimalType(18, 2), True),
        StructField("EBITDA", DecimalType(18, 2), True),
        StructField("LESS_TAX", DecimalType(18, 2), True),
        StructField("ACCT_PAYBL", DecimalType(16, 2), True),
    ])


@task
def spark_bronze_crmsxml_trdm_stg_crms_1(**kw_args) -> str:
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))


@task
def Job_VIEW(**kw_args) -> str:
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))
def CRMSXML_TO_BRONZE(spark: SparkSession, sc: SparkContext, **kw_args):
    log = logging.getLogger(__name__)

    job_params = Variable.get("JOB_PARAMS", default_var={}, deserialize_json=True)
    input_template = Variable.get(
        "CRMSXML_INPUT_PATH",
        default_var="/data/bronze/crmsxml/mCRMS_Sample_data_volume_test.xml",
    )
    output_template = Variable.get(
        "CRMSXML_OUTPUT_PATH",
        default_var="/data/bronze/crmsxml_trdm_stg_crms_1",
    )

    try:
        input_path = Template(input_template).render(job_params)
        output_path = Template(output_template).render(job_params)
    except Exception:
        input_path = input_template
        output_path = output_template

    schema = build_schema()
    input_paths = resolve_input_paths(input_path)

    # Use Spark XML datasource to parse CustomerRating rows.
    base_df = (
        spark.read.format("xml")
        .option("attributePrefix", "@")
        .option("valueTag", "value")
        .option("ignoreNamespace", "true")
        .option("inferSchema", "false")
        .option("rowTag", "CustomerRating")
        .load(input_paths)
    )

    cust = F.col("MEPOnlineReq.Customer")
    base = base_df.select(
        cust.ReferenceNo.alias("REFERENCE_NO"),
        cust.BNMCoreSector.alias("BNM_CORE_SECTOR"),
        cust.BNMIndClass.alias("BNM_INDS_CLASFN"),
        cust.BNMSectorCd.alias("BNM_SECTOR_CD"),
        cust.Business.alias("BUSINESS"),
        F.coalesce(
        cust.CRMIndClass.getField("@Value").cast("string"),
        cust.CRMIndClass.getField("value").cast("string"),
        ).alias("CRM_INDS_CLASFN"),
        F.col("MEPOnlineReq.Financials.Statement").alias("STATEMENTS"),
    )

    exploded = base.withColumn(
        "STATEMENT",
        F.explode_outer("STATEMENTS"),
    )

    exploded = exploded.filter(
        F.col("STATEMENT.`@StmtName`").rlike("^[0-9]{8}$")
    )

    kv_expr = "map_from_entries(transform(STATEMENT.Values.Value, x -> struct(x.`@Account`, coalesce(x.`@Value`, x.value))))"
    df = exploded.select(
        "REFERENCE_NO",
        "BNM_CORE_SECTOR",
        "BNM_INDS_CLASFN",
        "BNM_SECTOR_CD",
        "BUSINESS",
        "CRM_INDS_CLASFN",
        F.from_unixtime(F.unix_timestamp(F.col("STATEMENT.`@StmtName`").cast("string"), "yyyyMMdd")).cast("timestamp").alias("FIN_YEAR_END_T"),
        F.expr(kv_expr).alias("RAW_MAP"),
    )

    # Rebuild with explicit columns using map lookups and scale.
    df = df.select(
    F.coalesce(F.col("REFERENCE_NO").getField("value").cast("string"),
               F.col("REFERENCE_NO").getField("@Value").cast("string")).alias("REFERENCE_NO"),
    F.coalesce(F.col("BNM_CORE_SECTOR").getField("value").cast("string"),
               F.col("BNM_CORE_SECTOR").getField("@Value").cast("string")).alias("BNM_CORE_SECTOR"),
    F.coalesce(F.col("BNM_INDS_CLASFN").getField("value").cast("string"),
               F.col("BNM_INDS_CLASFN").getField("@Value").cast("string")).alias("BNM_INDS_CLASFN"),
    F.coalesce(F.col("BNM_SECTOR_CD").getField("value").cast("string"),
               F.col("BNM_SECTOR_CD").getField("@Value").cast("string")).alias("BNM_SECTOR_CD"),
    F.coalesce(F.col("BUSINESS").getField("value").cast("string"),
               F.col("BUSINESS").getField("@Value").cast("string")).alias("BUSINESS"),

    # keep your CRM fix (already string)
    F.col("CRM_INDS_CLASFN").alias("CRM_INDS_CLASFN"),

    F.col("FIN_YEAR_END_T"),
    F.col("RAW_MAP")["140000"].cast("decimal(18,2)").alias("EBIT"),
    F.col("RAW_MAP")["140001"].cast("decimal(18,2)").alias("EBITDA"),
    F.col("RAW_MAP")["140002"].cast("decimal(18,2)").alias("LESS_TAX"),
    F.col("RAW_MAP")["140003"].cast("decimal(16,2)").alias("ACCT_PAYBL"),
    )

    # df = df.select(
    #     "REFERENCE_NO",
    #     "BNM_CORE_SECTOR",
    #     "BNM_INDS_CLASFN",
    #     "BNM_SECTOR_CD",
    #     "BUSINESS",
    #     "CRM_INDS_CLASFN",
    #     "FIN_YEAR_END_T",
    #     F.col("RAW_MAP")["140000"].cast("decimal(18,2)").alias("EBIT"),
    #     F.col("RAW_MAP")["140001"].cast("decimal(18,2)").alias("EBITDA"),
    #     F.col("RAW_MAP")["140002"].cast("decimal(18,2)").alias("LESS_TAX"),
    #     F.col("RAW_MAP")["140003"].cast("decimal(16,2)").alias("ACCT_PAYBL"),
    # )

    row_count = df.count()
    log.info("Parsed %s rows from %s", row_count, input_path)
    # df.write.mode("overwrite").parquet(output_path)
    # make sure the database exists
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("DROP TABLE IF EXISTS bronze.crmsxml_trdm_stg_crms_1")
    # write the dataframe as a table
    df.write.mode("overwrite").format("parquet").saveAsTable("bronze.crmsxml_trdm_stg_crms_1")

    # log.info("Wrote parquet to %s", output_path)


####################################[Main]###################################
if not _SPARK_TASK_RUNNER:
    _JOB_PARAMS_B64 = base64.b64encode(json.dumps(Variable.get("JOB_PARAMS", default_var={}, deserialize_json=True)).encode()).decode()
    with DAG(
        dag_id="spark_bronze_crmsxml_trdm_stg_crms_1",
        start_date=airflow.utils.dates.days_ago(1),
        schedule_interval=None,
        tags=['datastage'],
    ) as dag:
    
        spark_bronze_crmsxml_trdm_stg_crms_1_task = spark_bronze_crmsxml_trdm_stg_crms_1()
        Job_VIEW_task = Job_VIEW()
        CRMSXML_TO_BRONZE_task = SparkSubmitOperator(
            conf={"spark.executor.instances": "10", "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar,/home/ec2-user/airflow/jars/spark-xml_2.12-0.18.0.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false", "spark.openmetadata.transport.pipelineServiceName": "spark_bronze_crmsxml_trdm_stg_crms_1", "spark.openmetadata.transport.pipelineName": "CRMSXML_TO_BRONZE", "spark.openlineage.facets.disabled": "", "spark.openlineage.facets.spark.logicalPlan.disabled": "false", "spark.openlineage.facets.debug.disabled": "false", "spark.openlineage.appName": "CRMSXML_TO_BRONZE"},
            task_id="CRMSXML_TO_BRONZE",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="CRMSXML_TO_BRONZE",
            deploy_mode="cluster",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "CRMSXML_TO_BRONZE"],
        )
    
        spark_bronze_crmsxml_trdm_stg_crms_1_task >> Job_VIEW_task
        Job_VIEW_task >> CRMSXML_TO_BRONZE_task

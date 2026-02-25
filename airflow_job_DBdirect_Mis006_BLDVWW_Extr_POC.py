
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-02 20:17:07
# @Author  : cloudera
# @File    : job_DBdirect_Mis006_BLDVWW_Extr_POC.py
# @Copyright: Cloudera.Inc




from __future__ import annotations

import base64
from abc import abstractmethod
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
from datetime import datetime, timedelta
from jinja2 import Template
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col,expr,lit
from pyspark.sql.functions import lit, col, input_file_name
from pyspark.sql.window import Window
from pyspark.sql import functions as F

from pyspark.sql.types import *
import json
import logging
if not _SPARK_TASK_RUNNER:
    import pendulum
import textwrap

@task
def job_DBdirect_Mis006_BLDVWW_Extr_POC(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def Job_VIEW(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))
def NETZ_SRC_TBL_NM(spark: SparkSession, sc: SparkContext, **kw_args):
        
    print("[Cloudera PS] Started")    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    catalog=Variable.get("ICEBERG_CATALOG_NAME", default_var="iceberg")
    
    #spark.sql(f"use {catalog}.default").show()
    
    
    
    
    
    sql=Template("""WITH max_recno_cte AS (
    
        SELECT 
    
            BLDVWW.MEMB_NO,
    
            MAX(BLDVWW.RECNO) AS MAX_RECNO
    
        FROM {{dbdir.pODS_SCHM}}.BORM BORM
    
        INNER JOIN {{dbdir.pODS_SCHM}}.BLDVWW BLDVWW
    
            ON BLDVWW.SOCIETY = SUBSTRING(BORM.KEY_1, 1, 3)
    
            AND BLDVWW.MEMB_NO = SUBSTRING(BORM.KEY_1, 4, 16)
    
        GROUP BY BLDVWW.MEMB_NO
    
    )
    
    SELECT
    
        BORM.KEY_1 AS BORM_KEY_1,
    
        BLDVWW.MEMB_NO,
    
        BLDVWW.CAFM_HIGH_RTE,
    
        BLDVWW.CAFM_START_RTE_DTE,
    
        BLDVWW.CAFM_RESET_RTE_DTE,
    
        BLDVWW.CAFM_TYPE,
    
        BORM.REPAY_FREQ
    
    FROM {{dbdir.pODS_SCHM}}.BORM BORM
    
    LEFT JOIN {{dbdir.pODS_SCHM}}.BLDVWW BLDVWW
    
        ON BLDVWW.SOCIETY = SUBSTRING(BORM.KEY_1, 1, 3)
    
        AND BLDVWW.MEMB_NO = SUBSTRING(BORM.KEY_1, 4, 16)
    
    LEFT JOIN max_recno_cte mrc
    
        ON BLDVWW.MEMB_NO = mrc.MEMB_NO
    
        AND BLDVWW.RECNO = mrc.MAX_RECNO""").render(job_params)
    
    # Edited by Zuling Kang
    # WHERE mrc.MAX_RECNO IS NOT NULL""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    NETZ_SRC_TBL_NM_v = spark.sql(sql)

    # print(f"SQL: {sql}")
    # print(f"NETZ_SRC_TBL_NM_v size: {NETZ_SRC_TBL_NM_v.count()}")
    # NETZ_SRC_TBL_NM_v.show()
    
    #spark.sql(f"use spark_catalog.default").show()
    
    NETZ_SRC_TBL_NM_lnk_Source__v=NETZ_SRC_TBL_NM_v.select(NETZ_SRC_TBL_NM_v[0].cast('string').alias('BORM_KEY_1'),NETZ_SRC_TBL_NM_v[1].cast('string').alias('MEMB_NO'),NETZ_SRC_TBL_NM_v[2].cast('decimal(7,4)').alias('CAFM_HIGH_RTE'),NETZ_SRC_TBL_NM_v[3].cast('integer').alias('CAFM_START_RTE_DTE'),NETZ_SRC_TBL_NM_v[4].cast('integer').alias('CAFM_RESET_RTE_DTE'),NETZ_SRC_TBL_NM_v[5].cast('string').alias('CAFM_TYPE'),NETZ_SRC_TBL_NM_v[6].cast('string').alias('REPAY_FREQ'))
    
    NETZ_SRC_TBL_NM_lnk_Source__v = NETZ_SRC_TBL_NM_lnk_Source__v.selectExpr("RTRIM(BORM_KEY_1) AS BORM_KEY_1","RTRIM(MEMB_NO) AS MEMB_NO","CAFM_HIGH_RTE","CAFM_START_RTE_DTE","CAFM_RESET_RTE_DTE","RTRIM(CAFM_TYPE) AS CAFM_TYPE","RTRIM(REPAY_FREQ) AS REPAY_FREQ").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'BORM_KEY_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(19)'}}, {'name': 'MEMB_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(16)'}}, {'name': 'CAFM_HIGH_RTE', 'type': 'decimal(7,4)', 'nullable': True, 'metadata': {}}, {'name': 'CAFM_START_RTE_DTE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'CAFM_RESET_RTE_DTE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'CAFM_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'REPAY_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_BLDVWW_Extr_POC__NETZ_SRC_TBL_NM_lnk_Source__v PURGE").show()
    
    print("NETZ_SRC_TBL_NM_lnk_Source__v")
    
    print(NETZ_SRC_TBL_NM_lnk_Source__v.schema.json())
    
    print("count:{}".format(NETZ_SRC_TBL_NM_lnk_Source__v.count()))
    
    NETZ_SRC_TBL_NM_lnk_Source__v.show(1000,False)
    
    NETZ_SRC_TBL_NM_lnk_Source__v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_BLDVWW_Extr_POC__NETZ_SRC_TBL_NM_lnk_Source__v")
    

@task
def V0A59(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))
def Sort_56_lnk_Source__Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    NETZ_SRC_TBL_NM_lnk_Source__v=spark.table('datastage_temp_job_DBdirect_Mis006_BLDVWW_Extr_POC__NETZ_SRC_TBL_NM_lnk_Source__v')
    
    Sort_56_lnk_Source__Part_v=NETZ_SRC_TBL_NM_lnk_Source__v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_BLDVWW_Extr_POC__Sort_56_lnk_Source__Part_v PURGE").show()
    
    print("Sort_56_lnk_Source__Part_v")
    
    print(Sort_56_lnk_Source__Part_v.schema.json())
    
    print("count:{}".format(Sort_56_lnk_Source__Part_v.count()))
    
    Sort_56_lnk_Source__Part_v.show(1000,False)
    
    Sort_56_lnk_Source__Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_BLDVWW_Extr_POC__Sort_56_lnk_Source__Part_v")
def Sort_56(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Sort_56_lnk_Source__Part_v=spark.table('datastage_temp_job_DBdirect_Mis006_BLDVWW_Extr_POC__Sort_56_lnk_Source__Part_v')
    
    Sort_56_v = Sort_56_lnk_Source__Part_v
    print(Sort_56_v.schema)
    Sort_56_lnk_Source_v_0 = Sort_56_v.orderBy(col('BORM_KEY_1').asc())
    window_spec = Window.orderBy("BORM_KEY_1")
    df = Sort_56_lnk_Source_v_0.withColumn("_PREV_BORM_KEY_1", F.lag("BORM_KEY_1").over(window_spec))
    print(df.schema)
    # KeyChange() logic: 1 if changed (or first row), 0 if same
    df = df.withColumn("KeyChange", 
        F.when(F.col("_PREV_BORM_KEY_1").isNull() | (F.col("_PREV_BORM_KEY_1") != F.col("BORM_KEY_1")), 1)
        .otherwise(0)
    )
    Sort_56_lnk_Source_v = df.select(col('BORM_KEY_1').cast('string').alias('BORM_KEY_1'),col('MEMB_NO').cast('string').alias('MEMB_NO'),col('CAFM_HIGH_RTE').cast('decimal(7,4)').alias('CAFM_HIGH_RTE'),col('CAFM_START_RTE_DTE').cast('integer').alias('CAFM_START_RTE_DTE'),col('CAFM_RESET_RTE_DTE').cast('integer').alias('CAFM_RESET_RTE_DTE'),col('CAFM_TYPE').cast('string').alias('CAFM_TYPE'),col('REPAY_FREQ').cast('string').alias('REPAY_FREQ'),expr("""KeyChange""").cast('integer').alias('keyChange'))
    
    Sort_56_lnk_Source_v = Sort_56_lnk_Source_v.selectExpr("RTRIM(BORM_KEY_1) AS BORM_KEY_1","RTRIM(MEMB_NO) AS MEMB_NO","CAFM_HIGH_RTE","CAFM_START_RTE_DTE","CAFM_RESET_RTE_DTE","RTRIM(CAFM_TYPE) AS CAFM_TYPE","RTRIM(REPAY_FREQ) AS REPAY_FREQ","keyChange").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'BORM_KEY_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(19)'}}, {'name': 'MEMB_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(16)'}}, {'name': 'CAFM_HIGH_RTE', 'type': 'decimal(7,4)', 'nullable': True, 'metadata': {}}, {'name': 'CAFM_START_RTE_DTE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'CAFM_RESET_RTE_DTE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'CAFM_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'REPAY_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'keyChange', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_BLDVWW_Extr_POC__Sort_56_lnk_Source_v PURGE").show()
    
    print("Sort_56_lnk_Source_v")
    
    print(Sort_56_lnk_Source_v.schema.json())
    
    print("count:{}".format(Sort_56_lnk_Source_v.count()))
    
    Sort_56_lnk_Source_v.show(1000,False)
    
    Sort_56_lnk_Source_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_BLDVWW_Extr_POC__Sort_56_lnk_Source_v")
def Transformer_52_lnk_Source_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Sort_56_lnk_Source_v=spark.table('datastage_temp_job_DBdirect_Mis006_BLDVWW_Extr_POC__Sort_56_lnk_Source_v')
    
    Transformer_52_lnk_Source_Part_v=Sort_56_lnk_Source_v.filter(col('keyChange') == 1)
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_BLDVWW_Extr_POC__Transformer_52_lnk_Source_Part_v PURGE").show()
    
    print("Transformer_52_lnk_Source_Part_v")
    
    print(Transformer_52_lnk_Source_Part_v.schema.json())
    
    print("count:{}".format(Transformer_52_lnk_Source_Part_v.count()))
    
    Transformer_52_lnk_Source_Part_v.show(1000,False)
    
    Transformer_52_lnk_Source_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_BLDVWW_Extr_POC__Transformer_52_lnk_Source_Part_v")
def Transformer_52(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_52_lnk_Source_Part_v=spark.table('datastage_temp_job_DBdirect_Mis006_BLDVWW_Extr_POC__Transformer_52_lnk_Source_Part_v')
    
    Transformer_52_v = Transformer_52_lnk_Source_Part_v
    
    Transformer_52_Lnk_BLDVWW_Tgt_v = Transformer_52_v.select(expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM BORM_KEY_1))""").cast('string').alias('B_KEY'),expr("""IF(REPAY_FREQ = '97' AND ISNOTNULL(MEMB_NO), CAFM_HIGH_RTE, 0)""").cast('decimal(8,4)').alias('MI006_CAFM_HIGH_RTE'),expr("""IF(REPAY_FREQ = '97' AND ISNOTNULL(MEMB_NO), CAFM_RESET_RTE_DTE, 0)""").cast('integer').alias('MI006_CAFM_RESET_RTE_DTE'),expr("""IF(REPAY_FREQ = '97' AND ISNOTNULL(MEMB_NO), CAFM_TYPE, '')""").cast('string').alias('MI006_CAFM_TYPE'))
    
    Transformer_52_Lnk_BLDVWW_Tgt_v = Transformer_52_Lnk_BLDVWW_Tgt_v.selectExpr("B_KEY","MI006_CAFM_HIGH_RTE","MI006_CAFM_RESET_RTE_DTE","RTRIM(MI006_CAFM_TYPE) AS MI006_CAFM_TYPE").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAFM_HIGH_RTE', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAFM_RESET_RTE_DTE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAFM_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_BLDVWW_Extr_POC__Transformer_52_Lnk_BLDVWW_Tgt_v PURGE").show()
    
    print("Transformer_52_Lnk_BLDVWW_Tgt_v")
    
    print(Transformer_52_Lnk_BLDVWW_Tgt_v.schema.json())
    
    print("count:{}".format(Transformer_52_Lnk_BLDVWW_Tgt_v.count()))
    
    Transformer_52_Lnk_BLDVWW_Tgt_v.show(1000,False)
    
    Transformer_52_Lnk_BLDVWW_Tgt_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_BLDVWW_Extr_POC__Transformer_52_Lnk_BLDVWW_Tgt_v")
def TGT_BLDVWW_Lnk_BLDVWW_Tgt_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_52_Lnk_BLDVWW_Tgt_v=spark.table('datastage_temp_job_DBdirect_Mis006_BLDVWW_Extr_POC__Transformer_52_Lnk_BLDVWW_Tgt_v')
    
    TGT_BLDVWW_Lnk_BLDVWW_Tgt_Part_v=Transformer_52_Lnk_BLDVWW_Tgt_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_BLDVWW_Extr_POC__TGT_BLDVWW_Lnk_BLDVWW_Tgt_Part_v PURGE").show()
    
    print("TGT_BLDVWW_Lnk_BLDVWW_Tgt_Part_v")
    
    print(TGT_BLDVWW_Lnk_BLDVWW_Tgt_Part_v.schema.json())
    
    print("count:{}".format(TGT_BLDVWW_Lnk_BLDVWW_Tgt_Part_v.count()))
    
    TGT_BLDVWW_Lnk_BLDVWW_Tgt_Part_v.show(1000,False)
    
    TGT_BLDVWW_Lnk_BLDVWW_Tgt_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_BLDVWW_Extr_POC__TGT_BLDVWW_Lnk_BLDVWW_Tgt_Part_v")
def TGT_BLDVWW(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    TGT_BLDVWW_Lnk_BLDVWW_Tgt_Part_v=spark.table('datastage_temp_job_DBdirect_Mis006_BLDVWW_Extr_POC__TGT_BLDVWW_Lnk_BLDVWW_Tgt_Part_v')
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    locations = Template('{{dbdir.pPROCESSING_DIR}}MIS006_BLDVWW.ds').render(job_params)
    
    log.info("write dataset files to "+locations)
    
    TGT_BLDVWW_Lnk_BLDVWW_Tgt_Part_v.write.mode("overwrite").format("parquet").save(locations)
    

####################################[Main]###################################
if not _SPARK_TASK_RUNNER:
    _JOB_PARAMS_B64 = base64.b64encode(json.dumps(Variable.get("JOB_PARAMS", default_var={}, deserialize_json=True)).encode()).decode()
    with DAG(
        dag_id="job_DBdirect_Mis006_BLDVWW_Extr_POC",
        start_date=airflow.utils.dates.days_ago(1),
        schedule_interval=None,
        tags=['datastage'],
    ) as dag:
        
        job_DBdirect_Mis006_BLDVWW_Extr_POC_task = job_DBdirect_Mis006_BLDVWW_Extr_POC()
        
        Job_VIEW_task = Job_VIEW()
        
        NETZ_SRC_TBL_NM_task = SparkSubmitOperator(
            conf={"spark.executor.instances": "10", "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false"},
            task_id="NETZ_SRC_TBL_NM",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="NETZ_SRC_TBL_NM",
            deploy_mode="cluster",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "NETZ_SRC_TBL_NM"],
        )
        
        V0A59_task = V0A59()
        
        Sort_56_lnk_Source__Part_task = SparkSubmitOperator(
            conf={"spark.executor.instances": "10", "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false"},
            task_id="Sort_56_lnk_Source__Part",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="Sort_56_lnk_Source__Part",
            deploy_mode="cluster",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "Sort_56_lnk_Source__Part"],
        )
        
        Sort_56_task = SparkSubmitOperator(
            conf={"spark.executor.instances": "10", "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false"},
            task_id="Sort_56",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="Sort_56",
            deploy_mode="cluster",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "Sort_56"],
        )
        
        Transformer_52_lnk_Source_Part_task = SparkSubmitOperator(
            conf={"spark.executor.instances": "10", "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false"},
            task_id="Transformer_52_lnk_Source_Part",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="Transformer_52_lnk_Source_Part",
            deploy_mode="cluster",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "Transformer_52_lnk_Source_Part"],
        )
        
        Transformer_52_task = SparkSubmitOperator(
            conf={"spark.executor.instances": "10", "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false"},
            task_id="Transformer_52",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="Transformer_52",
            deploy_mode="cluster",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "Transformer_52"],
        )
        
        TGT_BLDVWW_Lnk_BLDVWW_Tgt_Part_task = SparkSubmitOperator(
            conf={"spark.executor.instances": "10", "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false"},
            task_id="TGT_BLDVWW_Lnk_BLDVWW_Tgt_Part",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="TGT_BLDVWW_Lnk_BLDVWW_Tgt_Part",
            deploy_mode="cluster",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "TGT_BLDVWW_Lnk_BLDVWW_Tgt_Part"],
        )
        
        TGT_BLDVWW_task = SparkSubmitOperator(
            conf={"spark.executor.instances": "10", "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false"},
            task_id="TGT_BLDVWW",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="TGT_BLDVWW",
            deploy_mode="cluster",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "TGT_BLDVWW"],
        )
        
        
        job_DBdirect_Mis006_BLDVWW_Extr_POC_task >> Job_VIEW_task
        
        Job_VIEW_task >> NETZ_SRC_TBL_NM_task
        
        Job_VIEW_task >> V0A59_task
        
        NETZ_SRC_TBL_NM_task >> Sort_56_lnk_Source__Part_task
        
        Sort_56_lnk_Source__Part_task >> Sort_56_task
        
        Sort_56_task >> Transformer_52_lnk_Source_Part_task
        
        Transformer_52_lnk_Source_Part_task >> Transformer_52_task
        
        Transformer_52_task >> TGT_BLDVWW_Lnk_BLDVWW_Tgt_Part_task
        
        TGT_BLDVWW_Lnk_BLDVWW_Tgt_Part_task >> TGT_BLDVWW_task
        
    
    

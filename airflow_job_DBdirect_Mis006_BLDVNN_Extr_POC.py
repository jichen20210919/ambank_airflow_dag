
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-02 19:35:10
# @Author  : cloudera
# @File    : job_DBdirect_Mis006_BLDVNN_Extr_POC.py
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
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import json
import logging
if not _SPARK_TASK_RUNNER:
    import pendulum
import textwrap

@task
def job_DBdirect_Mis006_BLDVNN_Extr_POC(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def Job_VIEW(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))
def BORM_X_BLDVNN(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    catalog=Variable.get("ICEBERG_CATALOG_NAME", default_var="iceberg")
    
    #spark.sql(f"use {catalog}.default").show()
    
    
    
    
    
    sql=Template("""SELECT 
    
        COALESCE(b.CURR_DATE, 0) as CURR_DATE,
    
        a.B_KEY,
    
        COALESCE(b.START_DATE_X, '0') as START_DATE_X,
    
        COALESCE(b.REPAYMENT_X, '0') as REPAYMENT_X,
    
        COALESCE(b.HEX_KEY, '0') as HEX_KEY,
    
        COALESCE(b.RNK, 0) as RNK,
    
        COALESCE(b.START_DATE_01, 0) as START_DATE_01,
    
        COALESCE(b.REPAYMENT_01, 0) as REPAYMENT_01,
    
        COALESCE(b.REPAYMENT_TYPE_01, '') as REPAYMENT_TYPE_01,
    
        COALESCE(b.PRINC_DUE_01, 0) as PRINC_DUE_01,
    
        COALESCE(b.INSUR_AMT_DUE_01, 0) as INSUR_AMT_DUE_01,
    
        COALESCE(b.POST_DATE, 0) as POST_DATE,
    
        COALESCE(b.BALANCE_01, 0) as BALANCE_01,
    
        a.CREDIT_ARREARS,
    
        a.CIV_ACT_COD_DAT,
    
        a.LEGAL_MARKER_FLAG,
    
        a.CIV_ACT_CODE,
    
        a.LGL_TAG,
    
        a.LGL_TAG_DATE
    
    FROM 
    
        (SELECT 
    
            CAST(BORM.KEY_1 AS STRING) as B_KEY,
    
            BLDVLL.CIV_ACT_COD_DAT,
    
            CASE WHEN TRIM(BLDVLL.CIV_ACT_CODE) NOT IN ('0', '') 
    
                THEN 'Y' ELSE 'N' END AS LEGAL_MARKER_FLAG,
    
            BLDVLL.CIV_ACT_CODE,
    
            BLDVLL.LGL_TAG,
    
            BLDVLL.LGL_TAG_DATE,
    
            CAST(((BORM.THEO_UNPD_PRIN_BAL + BORM.CAP_THEO_UNPD_INT) - 
    
                  (BORM.UNPD_PRIN_BAL + BORM.CAP_UNPD_INT)) AS DECIMAL(17,5)) as CREDIT_ARREARS
    
        FROM {{dbdir.pODS_SCHM}}.BORM BORM
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.BLDVLL BLDVLL 
    
            ON SUBSTRING(BLDVLL.KEY_1, 1, 12) = BORM.KEY_1
    
        ) a
    
    LEFT JOIN 
    
        (SELECT 
    
            DATE_DIFF({{Curr_Date}},'1900-01-01') + 1 AS CURR_DATE,
            --has space, use 12 instead of 19, because it has space
            -- since data type is string instead of char(23)    
            SUBSTRING(BLDVNN.KEY_1, 1, 12) as B_KEY,
    
            CASE
                WHEN BLDVNN.START_DATE_01 IS NULL AND BLDVNN.START_DATE_02 IS NULL AND BLDVNN.START_DATE_03 IS NULL
                  AND BLDVNN.START_DATE_04 IS NULL AND BLDVNN.START_DATE_05 IS NULL AND BLDVNN.START_DATE_06 IS NULL
                  AND BLDVNN.START_DATE_07 IS NULL AND BLDVNN.START_DATE_08 IS NULL AND BLDVNN.START_DATE_09 IS NULL
                  AND BLDVNN.START_DATE_10 IS NULL AND BLDVNN.START_DATE_11 IS NULL AND BLDVNN.START_DATE_12 IS NULL
                  AND BLDVNN.START_DATE_13 IS NULL AND BLDVNN.START_DATE_14 IS NULL AND BLDVNN.START_DATE_15 IS NULL
                THEN NULL
                ELSE CONCAT_WS(',',
                    BLDVNN.START_DATE_01, BLDVNN.START_DATE_02, BLDVNN.START_DATE_03,
                    BLDVNN.START_DATE_04, BLDVNN.START_DATE_05, BLDVNN.START_DATE_06,
                    BLDVNN.START_DATE_07, BLDVNN.START_DATE_08, BLDVNN.START_DATE_09,
                    BLDVNN.START_DATE_10, BLDVNN.START_DATE_11, BLDVNN.START_DATE_12,
                    BLDVNN.START_DATE_13, BLDVNN.START_DATE_14, BLDVNN.START_DATE_15
                )
            END as START_DATE_X,
    
            CASE
                WHEN BLDVNN.REPAYMENT_01 IS NULL AND BLDVNN.REPAYMENT_02 IS NULL AND BLDVNN.REPAYMENT_03 IS NULL
                  AND BLDVNN.REPAYMENT_04 IS NULL AND BLDVNN.REPAYMENT_05 IS NULL AND BLDVNN.REPAYMENT_06 IS NULL
                  AND BLDVNN.REPAYMENT_07 IS NULL AND BLDVNN.REPAYMENT_08 IS NULL AND BLDVNN.REPAYMENT_09 IS NULL
                  AND BLDVNN.REPAYMENT_10 IS NULL AND BLDVNN.REPAYMENT_11 IS NULL AND BLDVNN.REPAYMENT_12 IS NULL
                  AND BLDVNN.REPAYMENT_13 IS NULL AND BLDVNN.REPAYMENT_14 IS NULL AND BLDVNN.REPAYMENT_15 IS NULL
                THEN NULL
                ELSE CONCAT_WS(',',
                    BLDVNN.REPAYMENT_01, BLDVNN.REPAYMENT_02, BLDVNN.REPAYMENT_03,
                    BLDVNN.REPAYMENT_04, BLDVNN.REPAYMENT_05, BLDVNN.REPAYMENT_06,
                    BLDVNN.REPAYMENT_07, BLDVNN.REPAYMENT_08, BLDVNN.REPAYMENT_09,
                    BLDVNN.REPAYMENT_10, BLDVNN.REPAYMENT_11, BLDVNN.REPAYMENT_12,
                    BLDVNN.REPAYMENT_13, BLDVNN.REPAYMENT_14, BLDVNN.REPAYMENT_15
                )
            END as REPAYMENT_X,
    
            HEX(SUBSTRING(BLDVNN.KEY_1, 20, 4)) as HEX_KEY,
    
            CAST(RANK() OVER (PARTITION BY SUBSTRING(BLDVNN.KEY_1, 1, 19) 
    
                 ORDER BY HEX(SUBSTRING(BLDVNN.KEY_1, 20, 4))) AS INT) as RNK,
    
            BLDVNN.START_DATE_01,
    
            BLDVNN.REPAYMENT_01,
    
            BLDVNN.REPAYMENT_TYPE_01,
    
            BLDVNN.PRINC_DUE_01,
    
            BLDVNN.INSUR_AMT_DUE_01,
    
            BLDVNN.POST_DATE,
    
            BLDVNN.BALANCE_01
    
        FROM {{dbdir.pODS_SCHM}}.BLDVNN BLDVNN
    
        ) b
    
    ON a.B_KEY = b.B_KEY
    
    ORDER BY a.B_KEY, b.RNK""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    BORM_X_BLDVNN_v = spark.sql(sql)
    
    
    
    
    
    #spark.sql(f"use spark_catalog.default").show()
    
    BORM_X_BLDVNN_lnk_Source_v=BORM_X_BLDVNN_v
    #.select(BORM_X_BLDVNN_v[0].cast('integer').alias('CURR_DATE'),BORM_X_BLDVNN_v[1].cast('string').alias('B_KEY'),BORM_X_BLDVNN_v[2].cast('string').alias('START_DATE_X'),BORM_X_BLDVNN_v[3].cast('string').alias('REPAYMENT_X'),BORM_X_BLDVNN_v[4].cast('string').alias('HEX_KEY'),BORM_X_BLDVNN_v[5].cast('integer').alias('RNK'),BORM_X_BLDVNN_v[6].cast('integer').alias('START_DATE_01'),BORM_X_BLDVNN_v[7].cast('decimal(17,3)').alias('REPAYMENT_01'),BORM_X_BLDVNN_v[8].cast('string').alias('REPAYMENT_TYPE_01'),BORM_X_BLDVNN_v[9].cast('decimal(17,3)').alias('PRINC_DUE_01'),BORM_X_BLDVNN_v[10].cast('decimal(17,3)').alias('INSUR_AMT_DUE_01'),BORM_X_BLDVNN_v[11].cast('integer').alias('POST_DATE'),BORM_X_BLDVNN_v[12].cast('decimal(17,3)').alias('BALANCE_01'),BORM_X_BLDVNN_v[13].cast('decimal(17,5)').alias('CREDIT_ARREARS'),BORM_X_BLDVNN_v[14].cast('integer').alias('CIV_ACT_COD_DAT'),BORM_X_BLDVNN_v[15].cast('string').alias('LEGAL_MARKER_FLAG'),BORM_X_BLDVNN_v[16].cast('string').alias('CIV_ACT_CODE'),BORM_X_BLDVNN_v[17].cast('string').alias('LGL_TAG'),BORM_X_BLDVNN_v[18].cast('integer').alias('LGL_TAG_DATE'))
    
    BORM_X_BLDVNN_lnk_Source_v = BORM_X_BLDVNN_lnk_Source_v.selectExpr("CURR_DATE","B_KEY","START_DATE_X","REPAYMENT_X","HEX_KEY","RNK","START_DATE_01","REPAYMENT_01","RTRIM(REPAYMENT_TYPE_01) AS REPAYMENT_TYPE_01","PRINC_DUE_01","INSUR_AMT_DUE_01","POST_DATE","BALANCE_01","CREDIT_ARREARS","CIV_ACT_COD_DAT","LEGAL_MARKER_FLAG","RTRIM(CIV_ACT_CODE) AS CIV_ACT_CODE","RTRIM(LGL_TAG) AS LGL_TAG","LGL_TAG_DATE").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'CURR_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'START_DATE_X', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_X', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'HEX_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'RNK', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'START_DATE_01', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_TYPE_01', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'PRINC_DUE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'INSUR_AMT_DUE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'POST_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'BALANCE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'CREDIT_ARREARS', 'type': 'decimal(17,5)', 'nullable': True, 'metadata': {}}, {'name': 'CIV_ACT_COD_DAT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'LEGAL_MARKER_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'CIV_ACT_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'LGL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'LGL_TAG_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__BORM_X_BLDVNN_lnk_Source_v PURGE").show()
    
    print("BORM_X_BLDVNN_lnk_Source_v")
    
    print(BORM_X_BLDVNN_lnk_Source_v.schema.json())
    
    print("count:{}".format(BORM_X_BLDVNN_lnk_Source_v.count()))
    
    BORM_X_BLDVNN_lnk_Source_v.show(100,False)
    
    BORM_X_BLDVNN_lnk_Source_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__BORM_X_BLDVNN_lnk_Source_v")
def Sort_142_lnk_Source_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    BORM_X_BLDVNN_lnk_Source_v=spark.table('datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__BORM_X_BLDVNN_lnk_Source_v')
    
    Sort_142_lnk_Source_Part_v=BORM_X_BLDVNN_lnk_Source_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Sort_142_lnk_Source_Part_v PURGE").show()
    
    print("Sort_142_lnk_Source_Part_v")
    
    print(Sort_142_lnk_Source_Part_v.schema.json())
    
    print("count:{}".format(Sort_142_lnk_Source_Part_v.count()))
    
    Sort_142_lnk_Source_Part_v.show(100,False)
    
    Sort_142_lnk_Source_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Sort_142_lnk_Source_Part_v")
def Sort_142(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Sort_142_lnk_Source_Part_v=spark.table('datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Sort_142_lnk_Source_Part_v')
    
    Sort_142_v = Sort_142_lnk_Source_Part_v
    
    Sort_142_DSLink148X_v_0 = Sort_142_v.orderBy(col('B_KEY').asc(),col('RNK').asc())
    
    Sort_142_DSLink148X_v = Sort_142_DSLink148X_v_0.select(col('CURR_DATE').cast('integer').alias('CURR_DATE'),col('B_KEY').cast('string').alias('B_KEY'),col('START_DATE_X').cast('string').alias('START_DATE_X'),col('REPAYMENT_X').cast('string').alias('REPAYMENT_X'),col('HEX_KEY').cast('string').alias('HEX_KEY'),col('RNK').cast('integer').alias('RNK'),col('START_DATE_01').cast('integer').alias('START_DATE_01'),col('REPAYMENT_01').cast('decimal(17,3)').alias('REPAYMENT_01'),col('REPAYMENT_TYPE_01').cast('string').alias('REPAYMENT_TYPE_01'),col('PRINC_DUE_01').cast('decimal(17,3)').alias('PRINC_DUE_01'),col('INSUR_AMT_DUE_01').cast('decimal(17,3)').alias('INSUR_AMT_DUE_01'),col('POST_DATE').cast('integer').alias('POST_DATE'),col('BALANCE_01').cast('decimal(17,3)').alias('BALANCE_01'),col('CREDIT_ARREARS').cast('decimal(17,5)').alias('CREDIT_ARREARS'),col('CIV_ACT_COD_DAT').cast('integer').alias('CIV_ACT_COD_DAT'),col('LEGAL_MARKER_FLAG').cast('string').alias('LEGAL_MARKER_FLAG'),col('CIV_ACT_CODE').cast('string').alias('CIV_ACT_CODE'),col('LGL_TAG').cast('string').alias('LGL_TAG'),col('LGL_TAG_DATE').cast('integer').alias('LGL_TAG_DATE'))
    
    Sort_142_DSLink148X_v = Sort_142_DSLink148X_v.selectExpr("CURR_DATE","B_KEY","START_DATE_X","REPAYMENT_X","HEX_KEY","RNK","START_DATE_01","REPAYMENT_01","RTRIM(REPAYMENT_TYPE_01) AS REPAYMENT_TYPE_01","PRINC_DUE_01","INSUR_AMT_DUE_01","POST_DATE","BALANCE_01","CREDIT_ARREARS","CIV_ACT_COD_DAT","LEGAL_MARKER_FLAG","RTRIM(CIV_ACT_CODE) AS CIV_ACT_CODE","RTRIM(LGL_TAG) AS LGL_TAG","LGL_TAG_DATE").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'CURR_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'START_DATE_X', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_X', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'HEX_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'RNK', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'START_DATE_01', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_TYPE_01', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'PRINC_DUE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'INSUR_AMT_DUE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'POST_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'BALANCE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'CREDIT_ARREARS', 'type': 'decimal(17,5)', 'nullable': True, 'metadata': {}}, {'name': 'CIV_ACT_COD_DAT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'LEGAL_MARKER_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'CIV_ACT_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'LGL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'LGL_TAG_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Sort_142_DSLink148X_v PURGE").show()
    
    print("Sort_142_DSLink148X_v")
    
    print(Sort_142_DSLink148X_v.schema.json())
    
    print("count:{}".format(Sort_142_DSLink148X_v.count()))
    
    Sort_142_DSLink148X_v.show(100,False)
    
    Sort_142_DSLink148X_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Sort_142_DSLink148X_v")
def Copy_227_DSLink148X_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Sort_142_DSLink148X_v=spark.table('datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Sort_142_DSLink148X_v')
    
    Copy_227_DSLink148X_Part_v=Sort_142_DSLink148X_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Copy_227_DSLink148X_Part_v PURGE").show()
    
    print("Copy_227_DSLink148X_Part_v")
    
    print(Copy_227_DSLink148X_Part_v.schema.json())
    
    print("count:{}".format(Copy_227_DSLink148X_Part_v.count()))
    
    Copy_227_DSLink148X_Part_v.show(100,False)
    
    Copy_227_DSLink148X_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Copy_227_DSLink148X_Part_v")
def Copy_227(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Copy_227_DSLink148X_Part_v=spark.table('datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Copy_227_DSLink148X_Part_v')
    
    Copy_227_v=Copy_227_DSLink148X_Part_v
    
    Copy_227_DSLink148_v = Copy_227_v.select(col('CURR_DATE').cast('integer').alias('CURR_DATE'),col('B_KEY').cast('string').alias('B_KEY'),col('START_DATE_X').cast('string').alias('START_DATE_X'),col('REPAYMENT_X').cast('string').alias('REPAYMENT_X'),col('HEX_KEY').cast('string').alias('HEX_KEY'),col('RNK').cast('integer').alias('RNK'),col('START_DATE_01').cast('integer').alias('START_DATE_01'),col('REPAYMENT_01').cast('decimal(17,3)').alias('REPAYMENT_01'),col('REPAYMENT_TYPE_01').cast('string').alias('REPAYMENT_TYPE_01'),col('PRINC_DUE_01').cast('decimal(17,3)').alias('PRINC_DUE_01'),col('INSUR_AMT_DUE_01').cast('decimal(17,3)').alias('INSUR_AMT_DUE_01'),col('POST_DATE').cast('integer').alias('POST_DATE'),col('BALANCE_01').cast('decimal(17,3)').alias('BALANCE_01'),col('CREDIT_ARREARS').cast('decimal(17,5)').alias('CREDIT_ARREARS'),col('CIV_ACT_COD_DAT').cast('integer').alias('CIV_ACT_COD_DAT'),col('LEGAL_MARKER_FLAG').cast('string').alias('LEGAL_MARKER_FLAG'),col('CIV_ACT_CODE').cast('string').alias('CIV_ACT_CODE'),col('LGL_TAG').cast('string').alias('LGL_TAG'),col('LGL_TAG_DATE').cast('integer').alias('LGL_TAG_DATE'))
    
    Copy_227_DSLink148_v = Copy_227_DSLink148_v.selectExpr("CURR_DATE","B_KEY","START_DATE_X","REPAYMENT_X","HEX_KEY","RNK","START_DATE_01","REPAYMENT_01","RTRIM(REPAYMENT_TYPE_01) AS REPAYMENT_TYPE_01","PRINC_DUE_01","INSUR_AMT_DUE_01","POST_DATE","BALANCE_01","CREDIT_ARREARS","CIV_ACT_COD_DAT","LEGAL_MARKER_FLAG","RTRIM(CIV_ACT_CODE) AS CIV_ACT_CODE","RTRIM(LGL_TAG) AS LGL_TAG","LGL_TAG_DATE").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'CURR_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'START_DATE_X', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_X', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'HEX_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'RNK', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'START_DATE_01', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_TYPE_01', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'PRINC_DUE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'INSUR_AMT_DUE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'POST_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'BALANCE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'CREDIT_ARREARS', 'type': 'decimal(17,5)', 'nullable': True, 'metadata': {}}, {'name': 'CIV_ACT_COD_DAT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'LEGAL_MARKER_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'CIV_ACT_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'LGL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'LGL_TAG_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Copy_227_DSLink148_v PURGE").show()
    
    print("Copy_227_DSLink148_v")
    
    print(Copy_227_DSLink148_v.schema.json())
    
    print("count:{}".format(Copy_227_DSLink148_v.count()))
    
    Copy_227_DSLink148_v.show(100,False)
    
    Copy_227_DSLink148_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Copy_227_DSLink148_v")
def Copy_of_Transformer_149_DSLink148_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Copy_227_DSLink148_v=spark.table('datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Copy_227_DSLink148_v')
    
    Copy_of_Transformer_149_DSLink148_Part_v=Copy_227_DSLink148_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Copy_of_Transformer_149_DSLink148_Part_v PURGE").show()
    
    print("Copy_of_Transformer_149_DSLink148_Part_v")
    
    print(Copy_of_Transformer_149_DSLink148_Part_v.schema.json())
    
    print("count:{}".format(Copy_of_Transformer_149_DSLink148_Part_v.count()))
    
    Copy_of_Transformer_149_DSLink148_Part_v.show(100,False)
    
    Copy_of_Transformer_149_DSLink148_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Copy_of_Transformer_149_DSLink148_Part_v")
def Copy_of_Transformer_149(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Copy_of_Transformer_149_DSLink148_Part_v=spark.table('datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Copy_of_Transformer_149_DSLink148_Part_v')

    # 1. Setup the Grouping and History Collection
    # Replicates StageVars: C, F, COL, SD, RT, REP, BAL
    df_grouped = Copy_of_Transformer_149_DSLink148_Part_v.groupBy("B_KEY").agg(
        # Order arrays by RNK to match DSX Sort/@ITERATION behavior
        F.expr("""
            transform(sort_array(collect_list(named_struct('rnk', RNK, 'v', START_DATE_X))), x -> x.v)
        """).alias("C"),
        F.expr("""
            transform(sort_array(collect_list(named_struct('rnk', RNK, 'v', REPAYMENT_X))), x -> x.v)
        """).alias("F"),
        F.expr("""
            transform(sort_array(collect_list(named_struct('rnk', RNK, 'v', POST_DATE))), x -> x.v)
        """).alias("COL"),
        F.expr("""
            transform(sort_array(collect_list(named_struct('rnk', RNK, 'v', START_DATE_01))), x -> x.v)
        """).alias("SD"),
        F.expr("""
            transform(sort_array(collect_list(named_struct('rnk', RNK, 'v', REPAYMENT_01))), x -> x.v)
        """).alias("REP"),
        F.expr("""
            transform(sort_array(collect_list(named_struct('rnk', RNK, 'v', BALANCE_01))), x -> x.v)
        """).alias("BAL"),
        # RT Logic: String concatenation with '~' delimiter, ordered by RNK
        F.expr("""
            transform(
                sort_array(collect_list(named_struct('rnk', RNK, 'v',
                    concat(coalesce(REPAYMENT_TYPE_01, ''), '~', PRINC_DUE_01)
                ))),
                x -> x.v
            )
        """).alias("RT"),
        F.first("CURR_DATE").alias("CURR_DATE"),
        F.first("CREDIT_ARREARS").alias("CREDIT_ARREARS"),
        F.first("REPAYMENT_01").alias("REP_ORIG"),    # For the iteration == 0 fallback
        F.first("BALANCE_01").alias("BAL_ORIG"),       # For the iteration == 0 fallback
        F.first("INSUR_AMT_DUE_01").alias("INSUR_AMT_DUE_01"),
        F.first("CIV_ACT_COD_DAT").alias("CIV_ACT_COD_DAT"),
        F.first("LEGAL_MARKER_FLAG").alias("LEGAL_MARKER_FLAG"),
        F.first("CIV_ACT_CODE").alias("CIV_ACT_CODE"),
        F.first("LGL_TAG").alias("LGL_TAG")
        ,F.first("LGL_TAG_DATE").alias("LGL_TAG_DATE")


        
    )

    # 2. Calculate Loop Variables (Z, P, R1) using Array Operations
    df_processed = df_grouped.withColumn(
        "E", F.size(F.col("C"))
    ).withColumn(
        # Z: First index (1-based) where START_DATE_X > CURR_DATE
        "Z", F.expr("""
            element_at(filter(transform(C, (x, i) -> i + 1), 
            (x, i) -> CAST(C[i] AS DOUBLE) > CURR_DATE), 1)
        """)
    ).withColumn(
        # P: First index (1-based) where START_DATE_X == 0
        "P", F.expr("""
            element_at(filter(transform(C, (x, i) -> i + 1), 
            (x, i) -> CAST(C[i] AS DOUBLE) == 0), 1)
        """)
    ).withColumn(
        # R1: The iteration where credit arrears (CA) are first cleared
        # Using aggregate to maintain state (rex) across the array scan
        "R1", F.expr("""
            aggregate(
                transform(C, (x, i) -> i),
                CAST(STRUCT(0.0 as rex, 0 as r1_idx) AS STRUCT<rex:DOUBLE, r1_idx:INT>),
                (acc, i) -> struct(
                    IF(CAST(C[i] AS DOUBLE) > CURR_DATE, acc.rex - CAST(F[i] AS DOUBLE), acc.rex),
                    IF(acc.r1_idx == 0 AND (CREDIT_ARREARS + IF(CAST(C[i] AS DOUBLE) > CURR_DATE, acc.rex - CAST(F[i] AS DOUBLE), acc.rex)) <= 0 
                    AND IF(CAST(C[i] AS DOUBLE) > CURR_DATE, acc.rex - CAST(F[i] AS DOUBLE), acc.rex) != 0, 
                    i + 1, acc.r1_idx)
                )
            ).r1_idx
        """)
    )

    # 3. Final Output Logic and Variable Mapping
    # Replicates the / 15 indexing and column assignments
    df_final = df_processed.select(
        "*",
        F.expr("""
            CASE 
                WHEN coalesce(P, 0) == 0 THEN floor(E / 15)
                ELSE ceil(P / 15)
            END
        """).alias("idx_ref")
    ).select(
        "*",
        # START_DATE_X & REPAYMENT_X (based on Z)
        F.expr("IF(Z IS NULL OR Z == 0, 0, C[int(Z - 1)])").alias("START_DATE_X"),
        F.expr("IF(Z IS NULL OR Z == 0, 0, F[int(Z - 1)])").alias("REPAYMENT_X"),
        
        # Partitioned Data (SD, REP, BAL, RT based on idx_ref)
        F.expr("IF(idx_ref < 1, SD[0], SD[int(idx_ref - 1)])").alias("START_DATE_01"),
        F.expr("IF(coalesce(P, 0) == 0, REP_ORIG, REP[int(idx_ref - 1)])").alias("REPAYMENT_01"),
        F.expr("split(RT[int(IF(idx_ref < 1, 0, idx_ref - 1))], '~')[0]").alias("REPAYMENT_TYPE_01"),
        F.expr("split(RT[int(IF(idx_ref < 1, 0, idx_ref - 1))], '~')[1]").alias("PRINC_DUE_01"),
        F.expr("COL[int(IF(idx_ref < 1, 0, idx_ref - 1))]").alias("POST_DATE"),
        F.expr("IF(coalesce(P, 0) == 0, BAL_ORIG, BAL[int(idx_ref - 1)])").alias("BALANCE_01"),
        
        # Financial Date Logic (LAST_PAY_DATE_X & NEXT_DUE_DATE_X)
        F.expr("IF(coalesce(P, 0) <= 1, 0, C[int(P - 2)])").alias("LAST_PAY_DATE_X"),
        F.expr("IF(R1 IS NULL OR R1 == 0, 0, C[int(R1 - 1)])").alias("NEXT_DUE_DATE_X"),
        
        # Original Q logic
        F.expr("""
            IF(E > 400, 27, IF(E > 300, 21, IF(E > 200, 14, IF(E > 100, 7, 1))))
        """).alias("Q")
    )
    Copy_of_Transformer_149_v = df_final
    print(Copy_of_Transformer_149_v.schema)
    Copy_of_Transformer_149_DSLink151_v = Copy_of_Transformer_149_v.select(
        col('B_KEY').cast('string').alias('B_KEY'),
        expr("""IF(Z = 0, 0, DS_STRINGTODECIMAL(DS_FIELD(CONCAT_WS(',',C), ',', Z)))""").cast('integer').alias('START_DATE_X'),
        expr("""IF(Z = 0, 0, DS_STRINGTODECIMAL(DS_FIELD(CONCAT_WS(',',F), ',', Z)))""").cast('decimal(17,3)').alias('REPAYMENT_X'),
        expr("""DS_FIELD(CONCAT_WS(',',SD), ',', (IF(P = 0, FLOOR(E / 15), (IF(P % 15 = 0, FLOOR(P / 15), (FLOOR(P / 15) + 1))))))""").cast('integer').alias('START_DATE_01'),
        expr("""IF(P = 0, REPAYMENT_01, (DS_FIELD(CONCAT_WS(',',REP), ',', (IF(P = 0, FLOOR(E / 15), (IF(P % 15 = 0, FLOOR(P / 15), (FLOOR(P / 15) + 1))))))))""").cast('decimal(17,3)').alias('REPAYMENT_01'),
        expr("""IF(DS_FIELD(DS_FIELD(CONCAT_WS(',',RT), ',', (IF(P > 600, 41, (IF(P > 500, 34, (IF(P > 400, 27, (IF(P > 300, 21, (IF(P > 200, 14, (IF(P > 100, 7, (IF(P = 0, (IF(E / 15 > 40, 41, (IF(E / 15 > 33, 34, (IF(E / 15 > 26, 27, IF(E / 15 > 20, 21, (IF(E / 15 > 13, 14, (IF(E / 15 > 6, 7, 1))))))))))), 1))))))))))))))), '~', 1) = '', NULL, DS_FIELD(DS_FIELD(CONCAT_WS(',',RT), ',', (IF(P > 600, 41, (IF(P > 500, 34, (IF(P > 400, 27, (IF(P > 300, 21, (IF(P > 200, 14, (IF(P > 100, 7, (IF(P = 0, (IF(E / 15 > 40, 41, (IF(E / 15 > 33, 34, (IF(E / 15 > 26, 27, IF(E / 15 > 20, 21, (IF(E / 15 > 13, 14, (IF(E / 15 > 6, 7, 1))))))))))), 1))))))))))))))), '~', 1))""").cast('string').alias('REPAYMENT_TYPE_01'),
        expr("""IF(DS_FIELD(DS_FIELD(CONCAT_WS(',',RT), ',', (IF(P > 600, 41, (IF(P > 500, 34, (IF(P > 400, 27, (IF(P > 300, 21, (IF(P > 200, 14, (IF(P > 100, 7, (IF(P = 0, (IF(E / 15 > 40, 41, (IF(E / 15 > 33, 34, (IF(E / 15 > 26, 27, IF(E / 15 > 20, 21, (IF(E / 15 > 13, 14, (IF(E / 15 > 6, 7, 1))))))))))), 1))))))))))))))), '~', 2) = '', NULL, DS_FIELD(DS_FIELD(CONCAT_WS(',',RT), ',', (IF(P > 600, 41, (IF(P > 500, 34, (IF(P > 400, 27, (IF(P > 300, 21, (IF(P > 200, 14, (IF(P > 100, 7, (IF(P = 0, (IF(E / 15 > 40, 41, (IF(E / 15 > 33, 34, (IF(E / 15 > 26, 27, IF(E / 15 > 20, 21, (IF(E / 15 > 13, 14, (IF(E / 15 > 6, 7, 1))))))))))), 1))))))))))))))), '~', 2))""").cast('decimal(17,3)').alias('PRINC_DUE_01'),
        col('INSUR_AMT_DUE_01').cast('decimal(17,3)').alias('INSUR_AMT_DUE_01'),
        expr("""DS_FIELD(CONCAT_WS(',',COL), ',', (IF(P = 0, FLOOR(E / 15), (IF(P % 15 = 0, FLOOR(P / 15), (FLOOR(P / 15) + 1))))))""").cast('integer').alias('POST_DATE'),
        expr("""IF(P = 0, BALANCE_01, (DS_FIELD(CONCAT_WS(',',BAL), ',', (IF(P = 0, FLOOR(E / 15), (IF(P % 15 = 0, FLOOR(P / 15), (FLOOR(P / 15) + 1))))))))""").cast('decimal(17,3)').alias('BALANCE_01'),
        expr("""IF(P = 0, 0, DS_STRINGTODECIMAL(DS_FIELD(CONCAT_WS(',',C), ',', P - 1)))""").cast('integer').alias('LAST_PAY_DATE_X'),
        expr("""IF(R1 = 0, 0, DS_STRINGTODECIMAL(DS_FIELD(CONCAT_WS(',',C), ',', CAST(R1 as INT))))""").cast('integer').alias('NEXT_DUE_DATE_X'),
        col('CIV_ACT_COD_DAT').cast('integer').alias('CIV_ACT_COD_DAT'),
        col('LEGAL_MARKER_FLAG').cast('string').alias('LEGAL_MARKER_FLAG'),
        col('CIV_ACT_CODE').cast('string').alias('CIV_ACT_CODE'),
        col('LGL_TAG').cast('string').alias('LGL_TAG'),
        col('LGL_TAG_DATE').cast('integer').alias('LGL_TAG_DATE'))
        
    print(Copy_of_Transformer_149_DSLink151_v.schema)

    Copy_of_Transformer_149_DSLink151_v = Copy_of_Transformer_149_DSLink151_v.selectExpr("B_KEY","START_DATE_X","REPAYMENT_X","START_DATE_01","REPAYMENT_01","RTRIM(REPAYMENT_TYPE_01) AS REPAYMENT_TYPE_01","PRINC_DUE_01","INSUR_AMT_DUE_01","POST_DATE","BALANCE_01","LAST_PAY_DATE_X","NEXT_DUE_DATE_X","CIV_ACT_COD_DAT","LEGAL_MARKER_FLAG","RTRIM(CIV_ACT_CODE) AS CIV_ACT_CODE","RTRIM(LGL_TAG) AS LGL_TAG","LGL_TAG_DATE").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'START_DATE_X', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_X', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'START_DATE_01', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_TYPE_01', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'PRINC_DUE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'INSUR_AMT_DUE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'POST_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'BALANCE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'LAST_PAY_DATE_X', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'NEXT_DUE_DATE_X', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'CIV_ACT_COD_DAT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'LEGAL_MARKER_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'CIV_ACT_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'LGL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'LGL_TAG_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Copy_of_Transformer_149_DSLink151_v PURGE").show()
    
    print("Copy_of_Transformer_149_DSLink151_v")
    
    print(Copy_of_Transformer_149_DSLink151_v.schema.json())
    
    print("count:{}".format(Copy_of_Transformer_149_DSLink151_v.count()))
    
    Copy_of_Transformer_149_DSLink151_v.show(100,False)
    
    Copy_of_Transformer_149_DSLink151_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Copy_of_Transformer_149_DSLink151_v")
def Transformer_241_DSLink151_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Copy_of_Transformer_149_DSLink151_v=spark.table('datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Copy_of_Transformer_149_DSLink151_v')
    
    Transformer_241_DSLink151_Part_v=Copy_of_Transformer_149_DSLink151_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Transformer_241_DSLink151_Part_v PURGE").show()
    
    print("Transformer_241_DSLink151_Part_v")
    
    print(Transformer_241_DSLink151_Part_v.schema.json())
    
    print("count:{}".format(Transformer_241_DSLink151_Part_v.count()))
    
    Transformer_241_DSLink151_Part_v.show(100,False)
    
    Transformer_241_DSLink151_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Transformer_241_DSLink151_Part_v")
def Transformer_241(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_241_DSLink151_Part_v=spark.table('datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Transformer_241_DSLink151_Part_v')
    
    Transformer_241_v = Transformer_241_DSLink151_Part_v
    
    Transformer_241_DSLink238_v = Transformer_241_v.select(expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM B_KEY))""").cast('string').alias('B_KEY'),col('START_DATE_X').cast('integer').alias('MI006_NEXT_REPAY_DATE'),col('REPAYMENT_X').cast('decimal(17,3)').alias('MI006_NEXT_REPAY_AMT'),col('START_DATE_01').cast('integer').alias('MI006_BLDVNN_START_DT_1'),col('REPAYMENT_01').cast('decimal(17,3)').alias('MI006_BLDVNN_REPAYMNT_1'),col('REPAYMENT_TYPE_01').cast('string').alias('MI006_REPAYMENT_TYPE_1'),col('INSUR_AMT_DUE_01').cast('decimal(17,3)').alias('MI006_BLDVNN_INSUR_AMT_DUE_1'),col('POST_DATE').cast('integer').alias('MI006_BLDVNN_POST_DATE'),col('BALANCE_01').cast('decimal(17,3)').alias('MI006_BLDVNN_BALANCE_01'),col('LAST_PAY_DATE_X').cast('integer').alias('MI006_LAST_REPAY_DT'),col('NEXT_DUE_DATE_X').cast('integer').alias('MI006_NEXT_DUE_DATE'),col('LAST_PAY_DATE_X').cast('integer').alias('MI006_REPAY_END_DATE'),col('CIV_ACT_COD_DAT').cast('integer').alias('MI006_LEGAL_MARKER_DATE'),col('LEGAL_MARKER_FLAG').cast('string').alias('MI006_LEGAL_MARKER_FLAG'),col('CIV_ACT_CODE').cast('string').alias('MI006_LEGAL_MARKER_STAT'),col('PRINC_DUE_01').cast('decimal(18,3)').alias('MI006_BLDVNN_PRIN_DUE_01'),col('LGL_TAG').cast('string').alias('MI006_LEGAL_FLAG'),col('CIV_ACT_COD_DAT').cast('integer').alias('MI006_BLDVLL_CIV_COD_DAT'),col('START_DATE_X').cast('integer').alias('MI006_PSSO_NEXT_PAY_DATE'),col('LGL_TAG_DATE').cast('integer').alias('MI006_LEGAL_DATE'),expr("""IF(CIV_ACT_CODE = '12', CIV_ACT_COD_DAT, 0)""").cast('integer').alias('MI006_BANKRUPTCY_DATE'),expr("""RIGHT(CONCAT(LPAD('0', 2, '0'), TRIM(CASE WHEN CIV_ACT_CODE = '' OR CIV_ACT_CODE IS NULL THEN '0' WHEN NOT CAST(CIV_ACT_CODE AS INT) IS NULL THEN CIV_ACT_CODE ELSE '0' END)), 2)""").cast('string').alias('MI006_LEGAL_STAT'),expr("""RIGHT(CONCAT(LPAD('0', 2, '0'), TRIM(CASE WHEN CIV_ACT_CODE = '' OR CIV_ACT_CODE IS NULL THEN '0' WHEN NOT CAST(CIV_ACT_CODE AS INT) IS NULL THEN CIV_ACT_CODE ELSE '0' END)), 2)""").cast('string').alias('MI006_BLDVLL_CIV_ACT_CODE'),col('START_DATE_X').cast('integer').alias('MI006_BLDVNN_NXT_REPAY_DT'))
    
    Transformer_241_DSLink238_v = Transformer_241_DSLink238_v.selectExpr("B_KEY","MI006_NEXT_REPAY_DATE","MI006_NEXT_REPAY_AMT","MI006_BLDVNN_START_DT_1","MI006_BLDVNN_REPAYMNT_1","RTRIM(MI006_REPAYMENT_TYPE_1) AS MI006_REPAYMENT_TYPE_1","MI006_BLDVNN_INSUR_AMT_DUE_1","MI006_BLDVNN_POST_DATE","MI006_BLDVNN_BALANCE_01","MI006_LAST_REPAY_DT","MI006_NEXT_DUE_DATE","MI006_REPAY_END_DATE","MI006_LEGAL_MARKER_DATE","RTRIM(MI006_LEGAL_MARKER_FLAG) AS MI006_LEGAL_MARKER_FLAG","MI006_LEGAL_MARKER_STAT","MI006_BLDVNN_PRIN_DUE_01","RTRIM(MI006_LEGAL_FLAG) AS MI006_LEGAL_FLAG","MI006_BLDVLL_CIV_COD_DAT","MI006_PSSO_NEXT_PAY_DATE","MI006_LEGAL_DATE","MI006_BANKRUPTCY_DATE","MI006_LEGAL_STAT","MI006_BLDVLL_CIV_ACT_CODE","MI006_BLDVNN_NXT_REPAY_DT").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NEXT_REPAY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NEXT_REPAY_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_START_DT_1', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_REPAYMNT_1', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REPAYMENT_TYPE_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BLDVNN_INSUR_AMT_DUE_1', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_POST_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_BALANCE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LAST_REPAY_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NEXT_DUE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REPAY_END_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LEGAL_MARKER_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LEGAL_MARKER_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_LEGAL_MARKER_STAT', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_PRIN_DUE_01', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LEGAL_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BLDVLL_CIV_COD_DAT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PSSO_NEXT_PAY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LEGAL_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BANKRUPTCY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LEGAL_STAT', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVLL_CIV_ACT_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_NXT_REPAY_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Transformer_241_DSLink238_v PURGE").show()
    
    print("Transformer_241_DSLink238_v")
    
    print(Transformer_241_DSLink238_v.schema.json())
    
    print("count:{}".format(Transformer_241_DSLink238_v.count()))
    
    Transformer_241_DSLink238_v.show(100,False)
    
    Transformer_241_DSLink238_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Transformer_241_DSLink238_v")
def TGT_BLDVNN_DSLink238_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_241_DSLink238_v=spark.table('datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__Transformer_241_DSLink238_v')
    
    TGT_BLDVNN_DSLink238_Part_v=Transformer_241_DSLink238_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__TGT_BLDVNN_DSLink238_Part_v PURGE").show()
    
    print("TGT_BLDVNN_DSLink238_Part_v")
    
    print(TGT_BLDVNN_DSLink238_Part_v.schema.json())
    
    print("count:{}".format(TGT_BLDVNN_DSLink238_Part_v.count()))
    
    TGT_BLDVNN_DSLink238_Part_v.show(100,False)
    
    TGT_BLDVNN_DSLink238_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__TGT_BLDVNN_DSLink238_Part_v")
def TGT_BLDVNN(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    TGT_BLDVNN_DSLink238_Part_v=spark.table('datastage_temp_job_DBdirect_Mis006_BLDVNN_Extr_POC__TGT_BLDVNN_DSLink238_Part_v')
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    locations = Template('{{dbdir.pPROCESSING_DIR}}MIS006_BLDVNN.ds').render(job_params)
    
    log.info("write dataset files to "+locations)
    
    TGT_BLDVNN_DSLink238_Part_v.write.mode("overwrite").format("parquet").save(locations)
    

####################################[Main]###################################
if not _SPARK_TASK_RUNNER:
    _JOB_PARAMS_B64 = base64.b64encode(json.dumps(Variable.get("JOB_PARAMS", default_var={}, deserialize_json=True)).encode()).decode()
    with DAG(
        dag_id="job_DBdirect_Mis006_BLDVNN_Extr_POC",
        start_date=airflow.utils.dates.days_ago(1),
        schedule_interval=None,
        tags=['datastage'],
    ) as dag:
        
        job_DBdirect_Mis006_BLDVNN_Extr_POC_task = job_DBdirect_Mis006_BLDVNN_Extr_POC()
        
        Job_VIEW_task = Job_VIEW()
        spark_params = Variable.get("SPARK_PARAMS",deserialize_json=True)
        
        BORM_X_BLDVNN_task = SparkSubmitOperator(
            conf={"spark.executor.instances": spark_params['spark.executor.instances'], "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.skewJoin.enabled": "true", "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5", "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB", "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false"},
            task_id="BORM_X_BLDVNN",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="BORM_X_BLDVNN",
            deploy_mode="client",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "BORM_X_BLDVNN"],
        )
        
        Sort_142_lnk_Source_Part_task = SparkSubmitOperator(
            conf={"spark.executor.instances": spark_params['spark.executor.instances'], "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false"},
            task_id="Sort_142_lnk_Source_Part",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="Sort_142_lnk_Source_Part",
            deploy_mode="client",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "Sort_142_lnk_Source_Part"],
        )
        
        Sort_142_task = SparkSubmitOperator(
            conf={"spark.executor.instances": spark_params['spark.executor.instances'], "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false"},
            task_id="Sort_142",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="Sort_142",
            deploy_mode="client",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "Sort_142"],
        )
        
        Copy_227_DSLink148X_Part_task = SparkSubmitOperator(
            conf={"spark.executor.instances": spark_params['spark.executor.instances'], "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false"},
            task_id="Copy_227_DSLink148X_Part",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="Copy_227_DSLink148X_Part",
            deploy_mode="client",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "Copy_227_DSLink148X_Part"],
        )
        
        Copy_227_task = SparkSubmitOperator(
            conf={"spark.executor.instances": spark_params['spark.executor.instances'], "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false"},
            task_id="Copy_227",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="Copy_227",
            deploy_mode="client",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "Copy_227"],
        )
        
        Copy_of_Transformer_149_DSLink148_Part_task = SparkSubmitOperator(
            conf={"spark.executor.instances": spark_params['spark.executor.instances'], "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false"},
            task_id="Copy_of_Transformer_149_DSLink148_Part",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="Copy_of_Transformer_149_DSLink148_Part",
            deploy_mode="client",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "Copy_of_Transformer_149_DSLink148_Part"],
        )
        
        Copy_of_Transformer_149_task = SparkSubmitOperator(
            conf={"spark.executor.instances": spark_params['spark.executor.instances'], "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false"},
            task_id="Copy_of_Transformer_149",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="Copy_of_Transformer_149",
            deploy_mode="client",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "Copy_of_Transformer_149"],
        )
        
        Transformer_241_DSLink151_Part_task = SparkSubmitOperator(
            conf={"spark.executor.instances": spark_params['spark.executor.instances'], "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false"},
            task_id="Transformer_241_DSLink151_Part",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="Transformer_241_DSLink151_Part",
            deploy_mode="client",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "Transformer_241_DSLink151_Part"],
        )
        
        Transformer_241_task = SparkSubmitOperator(
            conf={"spark.executor.instances": spark_params['spark.executor.instances'], "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false"},
            task_id="Transformer_241",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="Transformer_241",
            deploy_mode="client",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "Transformer_241"],
        )
        
        TGT_BLDVNN_DSLink238_Part_task = SparkSubmitOperator(
            conf={"spark.executor.instances": spark_params['spark.executor.instances'], "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false"},
            task_id="TGT_BLDVNN_DSLink238_Part",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="TGT_BLDVNN_DSLink238_Part",
            deploy_mode="client",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "TGT_BLDVNN_DSLink238_Part"],
        )
        
        TGT_BLDVNN_task = SparkSubmitOperator(
            conf={"spark.executor.instances": spark_params['spark.executor.instances'], "spark.sql.catalogImplementation": "hive", "spark.sql.defaultCatalog": "spark_catalog", "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083", "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar", "spark.dynamicAllocation.enabled": "false", "spark.shuffle.service.enabled": "false"},
            task_id="TGT_BLDVNN",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="TGT_BLDVNN",
            deploy_mode="client",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={"SPARK_TASK_RUNNER": "1", "HADOOP_CONF_DIR": "/etc/hadoop/conf", "YARN_CONF_DIR": "/etc/hadoop/conf", "HIVE_CONF_DIR": "/etc/hive/conf", "JOB_PARAMS_B64": _JOB_PARAMS_B64},
            application_args=["--module", __file__, "--task", "TGT_BLDVNN"],
        )
        
        
        job_DBdirect_Mis006_BLDVNN_Extr_POC_task >> Job_VIEW_task
        
        Job_VIEW_task >> BORM_X_BLDVNN_task
        
        BORM_X_BLDVNN_task >> Sort_142_lnk_Source_Part_task
        
        Sort_142_lnk_Source_Part_task >> Sort_142_task
        
        Sort_142_task >> Copy_227_DSLink148X_Part_task
        
        Copy_227_DSLink148X_Part_task >> Copy_227_task
        
        Copy_227_task >> Copy_of_Transformer_149_DSLink148_Part_task
        
        Copy_of_Transformer_149_DSLink148_Part_task >> Copy_of_Transformer_149_task
        
        Copy_of_Transformer_149_task >> Transformer_241_DSLink151_Part_task
        
        Transformer_241_DSLink151_Part_task >> Transformer_241_task
        
        Transformer_241_task >> TGT_BLDVNN_DSLink238_Part_task
        
        TGT_BLDVNN_DSLink238_Part_task >> TGT_BLDVNN_task
        
    

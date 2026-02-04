
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-02 19:35:10
# @Author  : cloudera
# @File    : job_DBdirect_Mis006_BLDVNN_Extr_POC.py
# @Copyright: Cloudera.Inc




from __future__ import annotations
from abc import abstractmethod
from airflow.decorators import task, task_group
from airflow.models import DAG
from airflow.models import Variable
from airflow.models.dag import DAG
from datetime import datetime, timedelta
from jinja2 import Template
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col,expr,lit
from pyspark.sql.functions import lit, col, input_file_name
from pyspark.sql.types import *
import json
import logging
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

@task.pyspark(conn_id="spark-local")
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
    
            ON SUBSTRING(BLDVLL.KEY_1, 1, 19) = BORM.KEY_1
    
        ) a
    
    LEFT JOIN 
    
        (SELECT 
    
            {{Curr_Date}} AS CURR_DATE,
    
            SUBSTRING(BLDVNN.KEY_1, 1, 19) as B_KEY,
    
            CONCAT_WS(',', 
    
                BLDVNN.START_DATE_01, BLDVNN.START_DATE_02, BLDVNN.START_DATE_03,
    
                BLDVNN.START_DATE_04, BLDVNN.START_DATE_05, BLDVNN.START_DATE_06,
    
                BLDVNN.START_DATE_07, BLDVNN.START_DATE_08, BLDVNN.START_DATE_09,
    
                BLDVNN.START_DATE_10, BLDVNN.START_DATE_11, BLDVNN.START_DATE_12,
    
                BLDVNN.START_DATE_13, BLDVNN.START_DATE_14, BLDVNN.START_DATE_15
    
            ) as START_DATE_X,
    
            CONCAT_WS(',', 
    
                BLDVNN.REPAYMENT_01, BLDVNN.REPAYMENT_02, BLDVNN.REPAYMENT_03,
    
                BLDVNN.REPAYMENT_04, BLDVNN.REPAYMENT_05, BLDVNN.REPAYMENT_06,
    
                BLDVNN.REPAYMENT_07, BLDVNN.REPAYMENT_08, BLDVNN.REPAYMENT_09,
    
                BLDVNN.REPAYMENT_10, BLDVNN.REPAYMENT_11, BLDVNN.REPAYMENT_12,
    
                BLDVNN.REPAYMENT_13, BLDVNN.REPAYMENT_14, BLDVNN.REPAYMENT_15
    
            ) as REPAYMENT_X,
    
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
    
    BORM_X_BLDVNN_lnk_Source_v=BORM_X_BLDVNN_v.select(BORM_X_BLDVNN_v[0].cast('integer').alias('CURR_DATE'),BORM_X_BLDVNN_v[1].cast('string').alias('B_KEY'),BORM_X_BLDVNN_v[2].cast('string').alias('START_DATE_X'),BORM_X_BLDVNN_v[3].cast('string').alias('REPAYMENT_X'),BORM_X_BLDVNN_v[4].cast('string').alias('HEX_KEY'),BORM_X_BLDVNN_v[5].cast('integer').alias('RNK'),BORM_X_BLDVNN_v[6].cast('integer').alias('START_DATE_01'),BORM_X_BLDVNN_v[7].cast('decimal(17,3)').alias('REPAYMENT_01'),BORM_X_BLDVNN_v[8].cast('string').alias('REPAYMENT_TYPE_01'),BORM_X_BLDVNN_v[9].cast('decimal(17,3)').alias('PRINC_DUE_01'),BORM_X_BLDVNN_v[10].cast('decimal(17,3)').alias('INSUR_AMT_DUE_01'),BORM_X_BLDVNN_v[11].cast('integer').alias('POST_DATE'),BORM_X_BLDVNN_v[12].cast('decimal(17,3)').alias('BALANCE_01'),BORM_X_BLDVNN_v[13].cast('decimal(17,5)').alias('CREDIT_ARREARS'),BORM_X_BLDVNN_v[14].cast('integer').alias('CIV_ACT_COD_DAT'),BORM_X_BLDVNN_v[15].cast('string').alias('LEGAL_MARKER_FLAG'),BORM_X_BLDVNN_v[16].cast('string').alias('CIV_ACT_CODE'),BORM_X_BLDVNN_v[17].cast('string').alias('LGL_TAG'),BORM_X_BLDVNN_v[18].cast('integer').alias('LGL_TAG_DATE'))
    
    BORM_X_BLDVNN_lnk_Source_v = BORM_X_BLDVNN_lnk_Source_v.selectExpr("CURR_DATE","B_KEY","START_DATE_X","REPAYMENT_X","HEX_KEY","RNK","START_DATE_01","REPAYMENT_01","RTRIM(REPAYMENT_TYPE_01) AS REPAYMENT_TYPE_01","PRINC_DUE_01","INSUR_AMT_DUE_01","POST_DATE","BALANCE_01","CREDIT_ARREARS","CIV_ACT_COD_DAT","LEGAL_MARKER_FLAG","RTRIM(CIV_ACT_CODE) AS CIV_ACT_CODE","RTRIM(LGL_TAG) AS LGL_TAG","LGL_TAG_DATE").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'CURR_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'START_DATE_X', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_X', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'HEX_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'RNK', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'START_DATE_01', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_TYPE_01', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'PRINC_DUE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'INSUR_AMT_DUE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'POST_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'BALANCE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'CREDIT_ARREARS', 'type': 'decimal(17,5)', 'nullable': True, 'metadata': {}}, {'name': 'CIV_ACT_COD_DAT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'LEGAL_MARKER_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'CIV_ACT_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'LGL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'LGL_TAG_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS BORM_X_BLDVNN_lnk_Source_v").show()
    
    print("BORM_X_BLDVNN_lnk_Source_v")
    
    print(BORM_X_BLDVNN_lnk_Source_v.schema.json())
    
    print("count:{}".format(BORM_X_BLDVNN_lnk_Source_v.count()))
    
    BORM_X_BLDVNN_lnk_Source_v.show(1000,False)
    
    BORM_X_BLDVNN_lnk_Source_v.write.mode("overwrite").saveAsTable("BORM_X_BLDVNN_lnk_Source_v")
    

@task.pyspark(conn_id="spark-local")
def Sort_142_lnk_Source_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    BORM_X_BLDVNN_lnk_Source_v=spark.table('BORM_X_BLDVNN_lnk_Source_v')
    
    Sort_142_lnk_Source_Part_v=BORM_X_BLDVNN_lnk_Source_v
    
    spark.sql("DROP TABLE IF EXISTS Sort_142_lnk_Source_Part_v").show()
    
    print("Sort_142_lnk_Source_Part_v")
    
    print(Sort_142_lnk_Source_Part_v.schema.json())
    
    print("count:{}".format(Sort_142_lnk_Source_Part_v.count()))
    
    Sort_142_lnk_Source_Part_v.show(1000,False)
    
    Sort_142_lnk_Source_Part_v.write.mode("overwrite").saveAsTable("Sort_142_lnk_Source_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Sort_142(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Sort_142_lnk_Source_Part_v=spark.table('Sort_142_lnk_Source_Part_v')
    
    Sort_142_v = Sort_142_lnk_Source_Part_v
    
    Sort_142_DSLink148X_v_0 = Sort_142_v.orderBy(col('B_KEY').asc(),col('RNK').asc())
    
    Sort_142_DSLink148X_v = Sort_142_DSLink148X_v_0.select(col('CURR_DATE').cast('integer').alias('CURR_DATE'),col('B_KEY').cast('string').alias('B_KEY'),col('START_DATE_X').cast('string').alias('START_DATE_X'),col('REPAYMENT_X').cast('string').alias('REPAYMENT_X'),col('HEX_KEY').cast('string').alias('HEX_KEY'),col('RNK').cast('integer').alias('RNK'),col('START_DATE_01').cast('integer').alias('START_DATE_01'),col('REPAYMENT_01').cast('decimal(17,3)').alias('REPAYMENT_01'),col('REPAYMENT_TYPE_01').cast('string').alias('REPAYMENT_TYPE_01'),col('PRINC_DUE_01').cast('decimal(17,3)').alias('PRINC_DUE_01'),col('INSUR_AMT_DUE_01').cast('decimal(17,3)').alias('INSUR_AMT_DUE_01'),col('POST_DATE').cast('integer').alias('POST_DATE'),col('BALANCE_01').cast('decimal(17,3)').alias('BALANCE_01'),col('CREDIT_ARREARS').cast('decimal(17,5)').alias('CREDIT_ARREARS'),col('CIV_ACT_COD_DAT').cast('integer').alias('CIV_ACT_COD_DAT'),col('LEGAL_MARKER_FLAG').cast('string').alias('LEGAL_MARKER_FLAG'),col('CIV_ACT_CODE').cast('string').alias('CIV_ACT_CODE'),col('LGL_TAG').cast('string').alias('LGL_TAG'),col('LGL_TAG_DATE').cast('integer').alias('LGL_TAG_DATE'))
    
    Sort_142_DSLink148X_v = Sort_142_DSLink148X_v.selectExpr("CURR_DATE","B_KEY","START_DATE_X","REPAYMENT_X","HEX_KEY","RNK","START_DATE_01","REPAYMENT_01","RTRIM(REPAYMENT_TYPE_01) AS REPAYMENT_TYPE_01","PRINC_DUE_01","INSUR_AMT_DUE_01","POST_DATE","BALANCE_01","CREDIT_ARREARS","CIV_ACT_COD_DAT","LEGAL_MARKER_FLAG","RTRIM(CIV_ACT_CODE) AS CIV_ACT_CODE","RTRIM(LGL_TAG) AS LGL_TAG","LGL_TAG_DATE").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'CURR_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'START_DATE_X', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_X', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'HEX_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'RNK', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'START_DATE_01', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_TYPE_01', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'PRINC_DUE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'INSUR_AMT_DUE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'POST_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'BALANCE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'CREDIT_ARREARS', 'type': 'decimal(17,5)', 'nullable': True, 'metadata': {}}, {'name': 'CIV_ACT_COD_DAT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'LEGAL_MARKER_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'CIV_ACT_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'LGL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'LGL_TAG_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS Sort_142_DSLink148X_v").show()
    
    print("Sort_142_DSLink148X_v")
    
    print(Sort_142_DSLink148X_v.schema.json())
    
    print("count:{}".format(Sort_142_DSLink148X_v.count()))
    
    Sort_142_DSLink148X_v.show(1000,False)
    
    Sort_142_DSLink148X_v.write.mode("overwrite").saveAsTable("Sort_142_DSLink148X_v")
    

@task.pyspark(conn_id="spark-local")
def Copy_227_DSLink148X_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Sort_142_DSLink148X_v=spark.table('Sort_142_DSLink148X_v')
    
    Copy_227_DSLink148X_Part_v=Sort_142_DSLink148X_v
    
    spark.sql("DROP TABLE IF EXISTS Copy_227_DSLink148X_Part_v").show()
    
    print("Copy_227_DSLink148X_Part_v")
    
    print(Copy_227_DSLink148X_Part_v.schema.json())
    
    print("count:{}".format(Copy_227_DSLink148X_Part_v.count()))
    
    Copy_227_DSLink148X_Part_v.show(1000,False)
    
    Copy_227_DSLink148X_Part_v.write.mode("overwrite").saveAsTable("Copy_227_DSLink148X_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Copy_227(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Copy_227_DSLink148X_Part_v=spark.table('Copy_227_DSLink148X_Part_v')
    
    Copy_227_v=Copy_227_DSLink148X_Part_v
    
    Copy_227_DSLink148_v = Copy_227_v.select(col('CURR_DATE').cast('integer').alias('CURR_DATE'),col('B_KEY').cast('string').alias('B_KEY'),col('START_DATE_X').cast('string').alias('START_DATE_X'),col('REPAYMENT_X').cast('string').alias('REPAYMENT_X'),col('HEX_KEY').cast('string').alias('HEX_KEY'),col('RNK').cast('integer').alias('RNK'),col('START_DATE_01').cast('integer').alias('START_DATE_01'),col('REPAYMENT_01').cast('decimal(17,3)').alias('REPAYMENT_01'),col('REPAYMENT_TYPE_01').cast('string').alias('REPAYMENT_TYPE_01'),col('PRINC_DUE_01').cast('decimal(17,3)').alias('PRINC_DUE_01'),col('INSUR_AMT_DUE_01').cast('decimal(17,3)').alias('INSUR_AMT_DUE_01'),col('POST_DATE').cast('integer').alias('POST_DATE'),col('BALANCE_01').cast('decimal(17,3)').alias('BALANCE_01'),col('CREDIT_ARREARS').cast('decimal(17,5)').alias('CREDIT_ARREARS'),col('CIV_ACT_COD_DAT').cast('integer').alias('CIV_ACT_COD_DAT'),col('LEGAL_MARKER_FLAG').cast('string').alias('LEGAL_MARKER_FLAG'),col('CIV_ACT_CODE').cast('string').alias('CIV_ACT_CODE'),col('LGL_TAG').cast('string').alias('LGL_TAG'),col('LGL_TAG_DATE').cast('integer').alias('LGL_TAG_DATE'))
    
    Copy_227_DSLink148_v = Copy_227_DSLink148_v.selectExpr("CURR_DATE","B_KEY","START_DATE_X","REPAYMENT_X","HEX_KEY","RNK","START_DATE_01","REPAYMENT_01","RTRIM(REPAYMENT_TYPE_01) AS REPAYMENT_TYPE_01","PRINC_DUE_01","INSUR_AMT_DUE_01","POST_DATE","BALANCE_01","CREDIT_ARREARS","CIV_ACT_COD_DAT","LEGAL_MARKER_FLAG","RTRIM(CIV_ACT_CODE) AS CIV_ACT_CODE","RTRIM(LGL_TAG) AS LGL_TAG","LGL_TAG_DATE").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'CURR_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'START_DATE_X', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_X', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'HEX_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'RNK', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'START_DATE_01', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_TYPE_01', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'PRINC_DUE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'INSUR_AMT_DUE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'POST_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'BALANCE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'CREDIT_ARREARS', 'type': 'decimal(17,5)', 'nullable': True, 'metadata': {}}, {'name': 'CIV_ACT_COD_DAT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'LEGAL_MARKER_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'CIV_ACT_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'LGL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'LGL_TAG_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS Copy_227_DSLink148_v").show()
    
    print("Copy_227_DSLink148_v")
    
    print(Copy_227_DSLink148_v.schema.json())
    
    print("count:{}".format(Copy_227_DSLink148_v.count()))
    
    Copy_227_DSLink148_v.show(1000,False)
    
    Copy_227_DSLink148_v.write.mode("overwrite").saveAsTable("Copy_227_DSLink148_v")
    

@task.pyspark(conn_id="spark-local")
def Copy_of_Transformer_149_DSLink148_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Copy_227_DSLink148_v=spark.table('Copy_227_DSLink148_v')
    
    Copy_of_Transformer_149_DSLink148_Part_v=Copy_227_DSLink148_v
    
    spark.sql("DROP TABLE IF EXISTS Copy_of_Transformer_149_DSLink148_Part_v").show()
    
    print("Copy_of_Transformer_149_DSLink148_Part_v")
    
    print(Copy_of_Transformer_149_DSLink148_Part_v.schema.json())
    
    print("count:{}".format(Copy_of_Transformer_149_DSLink148_Part_v.count()))
    
    Copy_of_Transformer_149_DSLink148_Part_v.show(1000,False)
    
    Copy_of_Transformer_149_DSLink148_Part_v.write.mode("overwrite").saveAsTable("Copy_of_Transformer_149_DSLink148_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Copy_of_Transformer_149(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Copy_of_Transformer_149_DSLink148_Part_v=spark.table('Copy_of_Transformer_149_DSLink148_Part_v')
    
    Copy_of_Transformer_149_v = Copy_of_Transformer_149_DSLink148_Part_v.withColumn('Last', expr("""CASE WHEN LEAD(B_KEY) OVER (PARTITION BY B_KEY ORDER BY B_KEY ASC NULLS LAST) IS NULL THEN TRUE ELSE FALSE END""").cast('integer').alias('Last')).withColumn('C', expr("""IF(D = 0, START_DATE_X, CONCAT_WS('', CONCAT_WS('', B, ','), START_DATE_X))""").cast('string').alias('C')).withColumn('B', col('C').cast('string').alias('B')).withColumn('E', expr("""COUNT(C, ',') + 1""").cast('integer').alias('E')).withColumn('F', expr("""IF(D = 0, REPAYMENT_X, CONCAT_WS('', CONCAT_WS('', G, ','), REPAYMENT_X))""").cast('string').alias('F')).withColumn('G', col('F').cast('string').alias('G')).withColumn('COL', expr("""IF(D = 0, POST_DATE, CONCAT_WS('', CONCAT_WS('', COL, ','), POST_DATE))""").cast('string').alias('COL')).withColumn('SD', expr("""IF(D = 0, START_DATE_01, CONCAT_WS('', CONCAT_WS('', SD, ','), START_DATE_01))""").cast('string').alias('SD')).withColumn('RT', expr("""IF(D = 0, CONCAT_WS('', CONCAT_WS('', (IF(ISNOTNULL((REPAYMENT_TYPE_01)), (REPAYMENT_TYPE_01), (''))), '~'), PRINC_DUE_01), CONCAT_WS('', CONCAT_WS('', CONCAT_WS('', CONCAT_WS('', RT, ','), (IF(ISNOTNULL((REPAYMENT_TYPE_01)), (REPAYMENT_TYPE_01), ('')))), '~'), PRINC_DUE_01))""").cast('string').alias('RT')).withColumn('REP', expr("""IF(D = 0, (IF(ISNOTNULL((REPAYMENT_01)), (REPAYMENT_01), '')), CONCAT_WS('', CONCAT_WS('', REP, ','), (IF(ISNOTNULL((REPAYMENT_01)), (REPAYMENT_01), ''))))""").cast('string').alias('REP')).withColumn('BAL', expr("""IF(D = 0, BALANCE_01, CONCAT_WS('', CONCAT_WS('', BAL, ','), BALANCE_01))""").cast('string').alias('BAL')).withColumn('Q', expr("""IF(E > 400, 27, (IF(E > 300, 21, (IF(E > 200, 14, (IF(E > 100, 7, 1)))))))""").cast('integer').alias('Q')).withColumn('D', expr("""IF(Last, 0, 1)""").cast('integer').alias('D'))
    
    Copy_of_Transformer_149_DSLink151_v = Copy_of_Transformer_149_v.select(col('B_KEY').cast('string').alias('B_KEY'),expr("""IF(Z = 0, 0, DS_STRINGTODECIMAL(DS_FIELD(C, ',', Z)))""").cast('integer').alias('START_DATE_X'),expr("""IF(Z = 0, 0, DS_STRINGTODECIMAL(DS_FIELD(F, ',', Z)))""").cast('decimal(17,3)').alias('REPAYMENT_X'),expr("""DS_FIELD(SD, ',', (IF(P = 0, FLOOR(${ITERATION} / 15), (IF(P % 15 = 0, (P / 15), (FLOOR(P / 15) + 1))))))""").cast('integer').alias('START_DATE_01'),expr("""IF(P = 0, REPAYMENT_01, (DS_FIELD(REP, ',', (IF(P = 0, FLOOR(${ITERATION} / 15), (IF(P % 15 = 0, (P / 15), (FLOOR(P / 15) + 1))))))))""").cast('decimal(17,3)').alias('REPAYMENT_01'),expr("""IF(DS_FIELD(DS_FIELD(RT, ',', (IF(P > 600, 41, (IF(P > 500, 34, (IF(P > 400, 27, (IF(P > 300, 21, (IF(P > 200, 14, (IF(P > 100, 7, (IF(P = 0, (IF(E / 15 > 40, 41, (IF(E / 15 > 33, 34, (IF(E / 15 > 26, 27, IF(E / 15 > 20, 21, (IF(E / 15 > 13, 14, (IF(E / 15 > 6, 7, 1))))))))))), 1))))))))))))))), '~', 1) = '', NULL, DS_FIELD(DS_FIELD(RT, ',', (IF(P > 600, 41, (IF(P > 500, 34, (IF(P > 400, 27, (IF(P > 300, 21, (IF(P > 200, 14, (IF(P > 100, 7, (IF(P = 0, (IF(E / 15 > 40, 41, (IF(E / 15 > 33, 34, (IF(E / 15 > 26, 27, IF(E / 15 > 20, 21, (IF(E / 15 > 13, 14, (IF(E / 15 > 6, 7, 1))))))))))), 1))))))))))))))), '~', 1))""").cast('string').alias('REPAYMENT_TYPE_01'),expr("""IF(DS_FIELD(DS_FIELD(RT, ',', (IF(P > 600, 41, (IF(P > 500, 34, (IF(P > 400, 27, (IF(P > 300, 21, (IF(P > 200, 14, (IF(P > 100, 7, (IF(P = 0, (IF(E / 15 > 40, 41, (IF(E / 15 > 33, 34, (IF(E / 15 > 26, 27, IF(E / 15 > 20, 21, (IF(E / 15 > 13, 14, (IF(E / 15 > 6, 7, 1))))))))))), 1))))))))))))))), '~', 2) = '', NULL, DS_FIELD(DS_FIELD(RT, ',', (IF(P > 600, 41, (IF(P > 500, 34, (IF(P > 400, 27, (IF(P > 300, 21, (IF(P > 200, 14, (IF(P > 100, 7, (IF(P = 0, (IF(E / 15 > 40, 41, (IF(E / 15 > 33, 34, (IF(E / 15 > 26, 27, IF(E / 15 > 20, 21, (IF(E / 15 > 13, 14, (IF(E / 15 > 6, 7, 1))))))))))), 1))))))))))))))), '~', 2))""").cast('decimal(17,3)').alias('PRINC_DUE_01'),col('INSUR_AMT_DUE_01').cast('decimal(17,3)').alias('INSUR_AMT_DUE_01'),expr("""DS_FIELD(COL, ',', (IF(P = 0, FLOOR(${ITERATION} / 15), (IF(P % 15 = 0, (P / 15), (FLOOR(P / 15) + 1))))))""").cast('integer').alias('POST_DATE'),expr("""IF(P = 0, BALANCE_01, (DS_FIELD(BAL, ',', (IF(P = 0, FLOOR(${ITERATION} / 15), (IF(P % 15 = 0, (P / 15), (FLOOR(P / 15) + 1))))))))""").cast('decimal(17,3)').alias('BALANCE_01'),expr("""IF(P = 0, 0, DS_STRINGTODECIMAL(DS_FIELD(C, ',', P - 1)))""").cast('integer').alias('LAST_PAY_DATE_X'),expr("""IF(R1 = 0, 0, DS_STRINGTODECIMAL(DS_FIELD(C, ',', R1)))""").cast('integer').alias('NEXT_DUE_DATE_X'),col('CIV_ACT_COD_DAT').cast('integer').alias('CIV_ACT_COD_DAT'),col('LEGAL_MARKER_FLAG').cast('string').alias('LEGAL_MARKER_FLAG'),col('CIV_ACT_CODE').cast('string').alias('CIV_ACT_CODE'),col('LGL_TAG').cast('string').alias('LGL_TAG'),col('LGL_TAG_DATE').cast('integer').alias('LGL_TAG_DATE'))
    
    Copy_of_Transformer_149_DSLink151_v = Copy_of_Transformer_149_DSLink151_v.selectExpr("B_KEY","START_DATE_X","REPAYMENT_X","START_DATE_01","REPAYMENT_01","RTRIM(REPAYMENT_TYPE_01) AS REPAYMENT_TYPE_01","PRINC_DUE_01","INSUR_AMT_DUE_01","POST_DATE","BALANCE_01","LAST_PAY_DATE_X","NEXT_DUE_DATE_X","CIV_ACT_COD_DAT","LEGAL_MARKER_FLAG","RTRIM(CIV_ACT_CODE) AS CIV_ACT_CODE","RTRIM(LGL_TAG) AS LGL_TAG","LGL_TAG_DATE").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'START_DATE_X', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_X', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'START_DATE_01', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_TYPE_01', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'PRINC_DUE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'INSUR_AMT_DUE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'POST_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'BALANCE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'LAST_PAY_DATE_X', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'NEXT_DUE_DATE_X', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'CIV_ACT_COD_DAT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'LEGAL_MARKER_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'CIV_ACT_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'LGL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'LGL_TAG_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS Copy_of_Transformer_149_DSLink151_v").show()
    
    print("Copy_of_Transformer_149_DSLink151_v")
    
    print(Copy_of_Transformer_149_DSLink151_v.schema.json())
    
    print("count:{}".format(Copy_of_Transformer_149_DSLink151_v.count()))
    
    Copy_of_Transformer_149_DSLink151_v.show(1000,False)
    
    Copy_of_Transformer_149_DSLink151_v.write.mode("overwrite").saveAsTable("Copy_of_Transformer_149_DSLink151_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_241_DSLink151_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Copy_of_Transformer_149_DSLink151_v=spark.table('Copy_of_Transformer_149_DSLink151_v')
    
    Transformer_241_DSLink151_Part_v=Copy_of_Transformer_149_DSLink151_v
    
    spark.sql("DROP TABLE IF EXISTS Transformer_241_DSLink151_Part_v").show()
    
    print("Transformer_241_DSLink151_Part_v")
    
    print(Transformer_241_DSLink151_Part_v.schema.json())
    
    print("count:{}".format(Transformer_241_DSLink151_Part_v.count()))
    
    Transformer_241_DSLink151_Part_v.show(1000,False)
    
    Transformer_241_DSLink151_Part_v.write.mode("overwrite").saveAsTable("Transformer_241_DSLink151_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_241(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_241_DSLink151_Part_v=spark.table('Transformer_241_DSLink151_Part_v')
    
    Transformer_241_v = Transformer_241_DSLink151_Part_v
    
    Transformer_241_DSLink238_v = Transformer_241_v.select(expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM B_KEY))""").cast('string').alias('B_KEY'),col('START_DATE_X').cast('integer').alias('MI006_NEXT_REPAY_DATE'),col('REPAYMENT_X').cast('decimal(17,3)').alias('MI006_NEXT_REPAY_AMT'),col('START_DATE_01').cast('integer').alias('MI006_BLDVNN_START_DT_1'),col('REPAYMENT_01').cast('decimal(17,3)').alias('MI006_BLDVNN_REPAYMNT_1'),col('REPAYMENT_TYPE_01').cast('string').alias('MI006_REPAYMENT_TYPE_1'),col('INSUR_AMT_DUE_01').cast('decimal(17,3)').alias('MI006_BLDVNN_INSUR_AMT_DUE_1'),col('POST_DATE').cast('integer').alias('MI006_BLDVNN_POST_DATE'),col('BALANCE_01').cast('decimal(17,3)').alias('MI006_BLDVNN_BALANCE_01'),col('LAST_PAY_DATE_X').cast('integer').alias('MI006_LAST_REPAY_DT'),col('NEXT_DUE_DATE_X').cast('integer').alias('MI006_NEXT_DUE_DATE'),col('LAST_PAY_DATE_X').cast('integer').alias('MI006_REPAY_END_DATE'),col('CIV_ACT_COD_DAT').cast('integer').alias('MI006_LEGAL_MARKER_DATE'),col('LEGAL_MARKER_FLAG').cast('string').alias('MI006_LEGAL_MARKER_FLAG'),col('CIV_ACT_CODE').cast('string').alias('MI006_LEGAL_MARKER_STAT'),col('PRINC_DUE_01').cast('decimal(18,3)').alias('MI006_BLDVNN_PRIN_DUE_01'),col('LGL_TAG').cast('string').alias('MI006_LEGAL_FLAG'),col('CIV_ACT_COD_DAT').cast('integer').alias('MI006_BLDVLL_CIV_COD_DAT'),col('START_DATE_X').cast('integer').alias('MI006_PSSO_NEXT_PAY_DATE'),col('LGL_TAG_DATE').cast('integer').alias('MI006_LEGAL_DATE'),expr("""IF(CIV_ACT_CODE = '12', CIV_ACT_COD_DAT, 0)""").cast('integer').alias('MI006_BANKRUPTCY_DATE'),expr("""RIGHT(CONCAT(LPAD('0', 2, '0'), TRIM(CASE WHEN CIV_ACT_CODE = '' OR CIV_ACT_CODE IS NULL THEN '0' WHEN NOT CAST(CIV_ACT_CODE AS INT) IS NULL THEN CIV_ACT_CODE ELSE '0' END)), 2)""").cast('string').alias('MI006_LEGAL_STAT'),expr("""RIGHT(CONCAT(LPAD('0', 2, '0'), TRIM(CASE WHEN CIV_ACT_CODE = '' OR CIV_ACT_CODE IS NULL THEN '0' WHEN NOT CAST(CIV_ACT_CODE AS INT) IS NULL THEN CIV_ACT_CODE ELSE '0' END)), 2)""").cast('string').alias('MI006_BLDVLL_CIV_ACT_CODE'),col('START_DATE_X').cast('integer').alias('MI006_BLDVNN_NXT_REPAY_DT'))
    
    Transformer_241_DSLink238_v = Transformer_241_DSLink238_v.selectExpr("B_KEY","MI006_NEXT_REPAY_DATE","MI006_NEXT_REPAY_AMT","MI006_BLDVNN_START_DT_1","MI006_BLDVNN_REPAYMNT_1","RTRIM(MI006_REPAYMENT_TYPE_1) AS MI006_REPAYMENT_TYPE_1","MI006_BLDVNN_INSUR_AMT_DUE_1","MI006_BLDVNN_POST_DATE","MI006_BLDVNN_BALANCE_01","MI006_LAST_REPAY_DT","MI006_NEXT_DUE_DATE","MI006_REPAY_END_DATE","MI006_LEGAL_MARKER_DATE","RTRIM(MI006_LEGAL_MARKER_FLAG) AS MI006_LEGAL_MARKER_FLAG","MI006_LEGAL_MARKER_STAT","MI006_BLDVNN_PRIN_DUE_01","RTRIM(MI006_LEGAL_FLAG) AS MI006_LEGAL_FLAG","MI006_BLDVLL_CIV_COD_DAT","MI006_PSSO_NEXT_PAY_DATE","MI006_LEGAL_DATE","MI006_BANKRUPTCY_DATE","MI006_LEGAL_STAT","MI006_BLDVLL_CIV_ACT_CODE","MI006_BLDVNN_NXT_REPAY_DT").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NEXT_REPAY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NEXT_REPAY_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_START_DT_1', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_REPAYMNT_1', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REPAYMENT_TYPE_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BLDVNN_INSUR_AMT_DUE_1', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_POST_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_BALANCE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LAST_REPAY_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NEXT_DUE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REPAY_END_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LEGAL_MARKER_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LEGAL_MARKER_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_LEGAL_MARKER_STAT', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_PRIN_DUE_01', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LEGAL_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BLDVLL_CIV_COD_DAT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PSSO_NEXT_PAY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LEGAL_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BANKRUPTCY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LEGAL_STAT', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVLL_CIV_ACT_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_NXT_REPAY_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS Transformer_241_DSLink238_v").show()
    
    print("Transformer_241_DSLink238_v")
    
    print(Transformer_241_DSLink238_v.schema.json())
    
    print("count:{}".format(Transformer_241_DSLink238_v.count()))
    
    Transformer_241_DSLink238_v.show(1000,False)
    
    Transformer_241_DSLink238_v.write.mode("overwrite").saveAsTable("Transformer_241_DSLink238_v")
    

@task.pyspark(conn_id="spark-local")
def TGT_BLDVNN_DSLink238_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_241_DSLink238_v=spark.table('Transformer_241_DSLink238_v')
    
    TGT_BLDVNN_DSLink238_Part_v=Transformer_241_DSLink238_v
    
    spark.sql("DROP TABLE IF EXISTS TGT_BLDVNN_DSLink238_Part_v").show()
    
    print("TGT_BLDVNN_DSLink238_Part_v")
    
    print(TGT_BLDVNN_DSLink238_Part_v.schema.json())
    
    print("count:{}".format(TGT_BLDVNN_DSLink238_Part_v.count()))
    
    TGT_BLDVNN_DSLink238_Part_v.show(1000,False)
    
    TGT_BLDVNN_DSLink238_Part_v.write.mode("overwrite").saveAsTable("TGT_BLDVNN_DSLink238_Part_v")
    

@task.pyspark(conn_id="spark-local")
def TGT_BLDVNN(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    TGT_BLDVNN_DSLink238_Part_v=spark.table('TGT_BLDVNN_DSLink238_Part_v')
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    locations = Template('{{dbdir.pPROCESSING_DIR}}MIS006_BLDVNN.ds').render(job_params)
    
    log.info("write dataset files to "+locations)
    
    spark.table("TGT_BLDVNN_DSLink238_Part_v").write.mode("overwrite").format("parquet").save(locations)
    

####################################[Main]###################################
import airflow
with DAG(
    dag_id="job_DBdirect_Mis006_BLDVNN_Extr_POC",
    start_date=airflow.utils.dates.days_ago(1),
    tags=['datastage'],
) as dag:
    
    job_DBdirect_Mis006_BLDVNN_Extr_POC_task = job_DBdirect_Mis006_BLDVNN_Extr_POC()
    
    Job_VIEW_task = Job_VIEW()
    
    BORM_X_BLDVNN_task = BORM_X_BLDVNN()
    
    Sort_142_lnk_Source_Part_task = Sort_142_lnk_Source_Part()
    
    Sort_142_task = Sort_142()
    
    Copy_227_DSLink148X_Part_task = Copy_227_DSLink148X_Part()
    
    Copy_227_task = Copy_227()
    
    Copy_of_Transformer_149_DSLink148_Part_task = Copy_of_Transformer_149_DSLink148_Part()
    
    Copy_of_Transformer_149_task = Copy_of_Transformer_149()
    
    Transformer_241_DSLink151_Part_task = Transformer_241_DSLink151_Part()
    
    Transformer_241_task = Transformer_241()
    
    TGT_BLDVNN_DSLink238_Part_task = TGT_BLDVNN_DSLink238_Part()
    
    TGT_BLDVNN_task = TGT_BLDVNN()
    
    
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
    



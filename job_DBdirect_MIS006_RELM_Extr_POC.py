
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-08 14:39:53
# @Author  : cloudera
# @File    : job_DBdirect_MIS006_RELM_Extr_POC.py
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
def job_DBdirect_MIS006_RELM_Extr_POC(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def Job_VIEW(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task.pyspark(conn_id="spark-local")
def NETZ_SRC_RELM_DLCD(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    catalog=Variable.get("ICEBERG_CATALOG_NAME", default_var="iceberg")
    
    #spark.sql(f"use {catalog}.default").show()
    
    
    
    
    
    sql=Template("""SELECT 
    
    CAST(BORM.KEY_1 AS STRING) AS KEY_1,
    
    R.R_KEY1,
    
    DLCD.ORIG_BRCH_CODE,
    
    DLCD.RECOURSE,
    
    DLCD.CREDIT_LINE,
    
    DLCD.LOCAL_INCORPN,
    
    DLCD.PRE_TRFR_APP_DT,
    
    DLCD.PRE_TRFR_APPR,
    
    DLCD.DLR_CODE,
    
    DLCD.DLR_NAME,
    
    DLCD.DLR_LOCN_CODE,
    
    DLCD.CONTACT_PERSON
    
    FROM {{dbdir.pODS_SCHM}}.BORM BORM
    
    LEFT OUTER JOIN (
    
        SELECT 
    
            X.R_KEY1,
    
            X.R_KEY2,
    
            ROW_NUMBER() OVER (PARTITION BY X.R_KEY1 ORDER BY X.KEY_2) AS RNK
    
        FROM (
    
            SELECT 
    
                KEY_2,
    
                SUBSTRING(RELM.KEY_2, 1, 19) AS R_KEY1,
    
                SUBSTRING(RELM.KEY_2, 27, 19) AS R_KEY2
    
            FROM {{dbdir.pODS_SCHM}}.RELM RELM 
    
            WHERE SUBSTRING(RELM.KEY_2, 20, 3) = 'LON' 
    
                AND SUBSTRING(RELM.KEY_2, 23, 4) IN ('3001')
    
            
    
            UNION ALL
    
            
    
            SELECT 
    
                KEY_2,
    
                SUBSTRING(RELM.KEY_2, 27, 19) AS R_KEY1,
    
                SUBSTRING(RELM.KEY_2, 1, 19) AS R_KEY2
    
            FROM {{dbdir.pODS_SCHM}}.RELM RELM 
    
            WHERE SUBSTRING(RELM.KEY_2, 46, 3) = 'LON' 
    
                AND SUBSTRING(RELM.KEY_2, 23, 4) IN ('3001')
    
        ) X
    
    ) R ON R.R_KEY1 = BORM.KEY_1
    
    LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.DLCD DLCD 
    
        ON CONCAT(DLCD.INST_NO, DLCD.DLR_CODE) = R.R_KEY2
    
    WHERE (R.RNK = 1 OR R.RNK IS NULL)
    
    -- AND SUBSTRING(BORM.KEY_1, 4, 16) IN ('0088820000101387')""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    NETZ_SRC_RELM_DLCD_v = spark.sql(sql)
    
    
    
    
    
    #spark.sql(f"use spark_catalog.default").show()
    
    NETZ_SRC_RELM_DLCD_lnk_Source_v=NETZ_SRC_RELM_DLCD_v.select(NETZ_SRC_RELM_DLCD_v[0].cast('string').alias('KEY_1'),NETZ_SRC_RELM_DLCD_v[1].cast('string').alias('R_KEY1'),NETZ_SRC_RELM_DLCD_v[2].cast('string').alias('ORIG_BRCH_CODE'),NETZ_SRC_RELM_DLCD_v[3].cast('string').alias('RECOURSE'),NETZ_SRC_RELM_DLCD_v[4].cast('decimal(17,3)').alias('CREDIT_LINE'),NETZ_SRC_RELM_DLCD_v[5].cast('string').alias('LOCAL_INCORPN'),NETZ_SRC_RELM_DLCD_v[6].cast('integer').alias('PRE_TRFR_APP_DT'),NETZ_SRC_RELM_DLCD_v[7].cast('decimal(17,3)').alias('PRE_TRFR_APPR'),NETZ_SRC_RELM_DLCD_v[8].cast('string').alias('DLR_CODE'),NETZ_SRC_RELM_DLCD_v[9].cast('string').alias('DLR_NAME'),NETZ_SRC_RELM_DLCD_v[10].cast('string').alias('DLR_LOCN_CODE'),NETZ_SRC_RELM_DLCD_v[11].cast('string').alias('CONTACT_PERSON'))
    
    NETZ_SRC_RELM_DLCD_lnk_Source_v = NETZ_SRC_RELM_DLCD_lnk_Source_v.selectExpr("KEY_1","R_KEY1","RTRIM(ORIG_BRCH_CODE) AS ORIG_BRCH_CODE","RTRIM(RECOURSE) AS RECOURSE","CREDIT_LINE","RTRIM(LOCAL_INCORPN) AS LOCAL_INCORPN","PRE_TRFR_APP_DT","PRE_TRFR_APPR","RTRIM(DLR_CODE) AS DLR_CODE","RTRIM(DLR_NAME) AS DLR_NAME","RTRIM(DLR_LOCN_CODE) AS DLR_LOCN_CODE","RTRIM(CONTACT_PERSON) AS CONTACT_PERSON").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'KEY_1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'R_KEY1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'ORIG_BRCH_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'RECOURSE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'CREDIT_LINE', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'LOCAL_INCORPN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'PRE_TRFR_APP_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'PRE_TRFR_APPR', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'DLR_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(16)'}}, {'name': 'DLR_NAME', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(60)'}}, {'name': 'DLR_LOCN_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'CONTACT_PERSON', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(40)'}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__NETZ_SRC_RELM_DLCD_lnk_Source_v PURGE").show()
    
    print("NETZ_SRC_RELM_DLCD_lnk_Source_v")
    
    print(NETZ_SRC_RELM_DLCD_lnk_Source_v.schema.json())
    
    print("count:{}".format(NETZ_SRC_RELM_DLCD_lnk_Source_v.count()))
    
    NETZ_SRC_RELM_DLCD_lnk_Source_v.show(1000,False)
    
    NETZ_SRC_RELM_DLCD_lnk_Source_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__NETZ_SRC_RELM_DLCD_lnk_Source_v")
    

@task
def V0A13(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task.pyspark(conn_id="spark-local")
def NETZ_SRC_COLM_CHPM_INSU(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    catalog=Variable.get("ICEBERG_CATALOG_NAME", default_var="iceberg")
    
    #spark.sql(f"use {catalog}.default").show()
    
    
    
    
    
    sql=Template("""SELECT
    
        CAST(BORM.KEY_1 AS STRING) AS KEY_1,
    
        CASE 
    
            WHEN SUBSTRING(INSU.KEY_2, 17, 4) = 'DRIV' THEN SUBSTRING(INSU.KEY_2, 1, 16)
    
            ELSE NULL 
    
        END AS KEY_2,
    
        CASE 
    
            WHEN SUBSTRING(INSU.KEY_2, 17, 4) = 'DRIV' THEN INSU.PREMIUM
    
            ELSE NULL 
    
        END AS PREMIUM,
    
        INSU.EXPIRY_DATE,
    
        CASE 
    
            WHEN SUBSTRING(INSU.KEY_2, 17, 4) = 'DRIV' THEN INSU.POLICY_NO_FIXED
    
            ELSE NULL 
    
        END AS POLICY_NO_FIXED,
    
        CASE 
    
            WHEN SUBSTRING(INSU.KEY_2, 17, 4) = 'DRIV' THEN INSU.START_DATE
    
            ELSE NULL 
    
        END AS START_DATE,
    
        CHPM.CONDITON,
    
        CHPM.TENDR_RSLT_AMT,
    
        R.R_KEY1,
    
        'X' AS IND,
    
        CASE 
    
            WHEN (SUBSTRING(INSU.KEY_2, 17, 4) = 'DRIV' AND INSU.KEY_1 IS NOT NULL) THEN 'Y'
    
            ELSE NULL 
    
        END AS INS_IND,
    
        CASE 
    
            WHEN (CPRO.KEY_1 IS NOT NULL) THEN 'Y'
    
            ELSE NULL 
    
        END AS CPRO
    
    FROM {{dbdir.pODS_SCHM}}.BORM BORM
    
    LEFT OUTER JOIN (
    
        SELECT 
    
            X.R_KEY1,
    
            X.R_KEY2,
    
            ROW_NUMBER() OVER (PARTITION BY X.R_KEY1 ORDER BY X.KEY_2) AS RNK
    
        FROM (
    
            SELECT 
    
                KEY_2,
    
                SUBSTRING(RELM.KEY_2, 1, 19) AS R_KEY1,
    
                SUBSTRING(RELM.KEY_2, 27, 19) AS R_KEY2
    
            FROM {{dbdir.pODS_SCHM}}.RELM RELM
    
            WHERE SUBSTRING(RELM.KEY_2, 20, 3) = 'LON' 
    
                AND SUBSTRING(RELM.KEY_1, 23, 4) IN ('9035')
    
            
    
            UNION ALL
    
            
    
            SELECT 
    
                KEY_2,
    
                SUBSTRING(RELM.KEY_2, 27, 19) AS R_KEY1,
    
                SUBSTRING(RELM.KEY_2, 1, 19) AS R_KEY2
    
            FROM {{dbdir.pODS_SCHM}}.RELM RELM
    
            WHERE SUBSTRING(RELM.KEY_2, 46, 3) = 'LON' 
    
                AND SUBSTRING(RELM.KEY_1, 23, 4) IN ('9035')
    
        ) X
    
    ) R ON R.R_KEY1 = BORM.KEY_1
    
    LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.COLM COLM ON COLM.KEY_1 = R.R_KEY2
    
    LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.CHPM CHPM 
    
        ON SUBSTRING(COLM.KEY_1, 1, 3) = CHPM.INST_NO 
    
        AND SUBSTRING(COLM.KEY_1, 4, 16) = CHPM.CLTR_NO
    
    LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.CPRO CPRO 
    
        ON SUBSTRING(CPRO.KEY_1, 1, 3) = SUBSTRING(COLM.KEY_1, 1, 3)
    
        AND SUBSTRING(CPRO.KEY_1, 4, 16) = SUBSTRING(COLM.KEY_1, 4, 16)
    
        AND SUBSTRING(CPRO.KEY_1, 20, 4) = '0001'
    
    LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.INSU INSU 
    
        ON SUBSTRING(INSU.KEY_1, 1, 3) = SUBSTRING(COLM.KEY_1, 1, 3)
    
        AND SUBSTRING(INSU.KEY_1, 4, 3) = 'COL'
    
        AND SUBSTRING(INSU.KEY_1, 7, 16) = SUBSTRING(COLM.KEY_1, 4, 16)
    
        AND INSU.INSURANCE_SEQ = '0001'
    
    WHERE (R.RNK = 1 OR R.RNK IS NULL)""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    NETZ_SRC_COLM_CHPM_INSU_v = spark.sql(sql)
    
    
    
    
    
    #spark.sql(f"use spark_catalog.default").show()
    
    NETZ_SRC_COLM_CHPM_INSU_Colm_v=NETZ_SRC_COLM_CHPM_INSU_v.select(NETZ_SRC_COLM_CHPM_INSU_v[0].cast('string').alias('KEY_1'),NETZ_SRC_COLM_CHPM_INSU_v[1].cast('string').alias('KEY_2'),NETZ_SRC_COLM_CHPM_INSU_v[2].cast('decimal(17,3)').alias('PREMIUM'),NETZ_SRC_COLM_CHPM_INSU_v[3].cast('integer').alias('EXPIRY_DATE'),NETZ_SRC_COLM_CHPM_INSU_v[4].cast('string').alias('POLICY_NO_FIXED'),NETZ_SRC_COLM_CHPM_INSU_v[5].cast('integer').alias('START_DATE'),NETZ_SRC_COLM_CHPM_INSU_v[6].cast('string').alias('CONDITON'),NETZ_SRC_COLM_CHPM_INSU_v[7].cast('decimal(17,3)').alias('TENDR_RSLT_AMT'),NETZ_SRC_COLM_CHPM_INSU_v[8].cast('string').alias('R_KEY1'),NETZ_SRC_COLM_CHPM_INSU_v[9].cast('string').alias('IND'),NETZ_SRC_COLM_CHPM_INSU_v[10].cast('string').alias('INS_IND'),NETZ_SRC_COLM_CHPM_INSU_v[11].cast('string').alias('CPRO'))
    
    NETZ_SRC_COLM_CHPM_INSU_Colm_v = NETZ_SRC_COLM_CHPM_INSU_Colm_v.selectExpr("KEY_1","KEY_2","PREMIUM","EXPIRY_DATE","RTRIM(POLICY_NO_FIXED) AS POLICY_NO_FIXED","START_DATE","CONDITON","TENDR_RSLT_AMT","R_KEY1","IND","INS_IND","CPRO").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'KEY_1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'KEY_2', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'PREMIUM', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'EXPIRY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'POLICY_NO_FIXED', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'START_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'CONDITON', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'TENDR_RSLT_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'R_KEY1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'INS_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'CPRO', 'type': 'string', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__NETZ_SRC_COLM_CHPM_INSU_Colm_v PURGE").show()
    
    print("NETZ_SRC_COLM_CHPM_INSU_Colm_v")
    
    print(NETZ_SRC_COLM_CHPM_INSU_Colm_v.schema.json())
    
    print("count:{}".format(NETZ_SRC_COLM_CHPM_INSU_Colm_v.count()))
    
    NETZ_SRC_COLM_CHPM_INSU_Colm_v.show(1000,False)
    
    NETZ_SRC_COLM_CHPM_INSU_Colm_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__NETZ_SRC_COLM_CHPM_INSU_Colm_v")
    

@task.pyspark(conn_id="spark-local")
def NETZ_SRC_COLT(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    catalog=Variable.get("ICEBERG_CATALOG_NAME", default_var="iceberg")
    
    #spark.sql(f"use {catalog}.default").show()
    
    
    
    
    
    sql=Template("""SELECT C.KEY_1,
    
           C.MI006_TOTAL_REALISABLE_VAL,
    
           C.MI006_LTV_VAL
    
    FROM (
    
        SELECT DISTINCT
    
            CAST(BORM.KEY_1 AS VARCHAR(19)) AS KEY_1,
    
            ROUND(
    
                COALESCE(
    
                    CAST(
    
                        (
    
                            CAST(
    
                                (
    
                                    CASE 
    
                                        WHEN (COLM.CEF = '' OR COLM.CEF IS NULL) THEN COLT.SAFE_LEND_MARGIN
    
                                        WHEN COLM.CEF = '1' THEN COLT.CEF1
    
                                        WHEN COLM.CEF = '2' THEN COLT.CEF2
    
                                        WHEN COLM.CEF = '3' THEN COLT.CEF3
    
                                        WHEN COLM.CEF = '4' THEN COLM.CEF_VALUE
    
                                        ELSE 0
    
                                    END
    
                                ) AS DECIMAL(18,3)
    
                            ) * COLM.BANK_VALUATION / 100 * RELM.PERCENTAGE / 100
    
                        ) AS DECIMAL(20,5)
    
                    ),
    
                    0
    
                ),
    
                3
    
            ) AS MI006_TOTAL_REALISABLE_VAL,
    
            COLT.SAFE_LEND_MARGIN AS MI006_LTV_VAL,
    
            RANK() OVER (PARTITION BY BORM.KEY_1 ORDER BY RELM.KEY_1) AS RNK
    
        FROM {{dbdir.pODS_SCHM}}.BORM BORM
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.RELM RELM 
    
            ON SUBSTRING(RELM.KEY_2, 1, 19) = BORM.KEY_1 
    
            AND SUBSTRING(RELM.KEY_2, 20, 7) = 'LON9035' 
    
            AND SUBSTRING(RELM.KEY_2, 46, 3) = 'COL'
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.COLM COLM 
    
            ON SUBSTRING(RELM.KEY_1, 1, 19) = COLM.KEY_1
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.COLT COLT 
    
            ON COLT.COL_TYPE = COLM.TYPE 
    
            AND COLT.COL_SUB_TYPE = COLM.COL_SUB_TYPE
    
    ) C
    
    WHERE C.RNK = 1""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    NETZ_SRC_COLT_v = spark.sql(sql)
    
    
    
    
    
    #spark.sql(f"use spark_catalog.default").show()
    
    NETZ_SRC_COLT_Colt_v=NETZ_SRC_COLT_v.select(NETZ_SRC_COLT_v[0].cast('string').alias('KEY_1'),NETZ_SRC_COLT_v[1].cast('decimal(18,3)').alias('MI006_TOTAL_REALISABLE_VAL'),NETZ_SRC_COLT_v[2].cast('decimal(18,3)').alias('MI006_LTV_VAL'))
    
    NETZ_SRC_COLT_Colt_v = NETZ_SRC_COLT_Colt_v.selectExpr("KEY_1","MI006_TOTAL_REALISABLE_VAL","MI006_LTV_VAL").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'KEY_1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TOTAL_REALISABLE_VAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LTV_VAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__NETZ_SRC_COLT_Colt_v PURGE").show()
    
    print("NETZ_SRC_COLT_Colt_v")
    
    print(NETZ_SRC_COLT_Colt_v.schema.json())
    
    print("count:{}".format(NETZ_SRC_COLT_Colt_v.count()))
    
    NETZ_SRC_COLT_Colt_v.show(1000,False)
    
    NETZ_SRC_COLT_Colt_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__NETZ_SRC_COLT_Colt_v")
    

@task
def V0A26(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def V4A0(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def V5A0(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task.pyspark(conn_id="spark-local")
def Join_18_lnk_Source_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    NETZ_SRC_RELM_DLCD_lnk_Source_v=spark.table('datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__NETZ_SRC_RELM_DLCD_lnk_Source_v')
    
    Join_18_lnk_Source_Part_v=NETZ_SRC_RELM_DLCD_lnk_Source_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__Join_18_lnk_Source_Part_v PURGE").show()
    
    print("Join_18_lnk_Source_Part_v")
    
    print(Join_18_lnk_Source_Part_v.schema.json())
    
    print("count:{}".format(Join_18_lnk_Source_Part_v.count()))
    
    Join_18_lnk_Source_Part_v.show(1000,False)
    
    Join_18_lnk_Source_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__Join_18_lnk_Source_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Join_18_Colm_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    NETZ_SRC_COLM_CHPM_INSU_Colm_v=spark.table('datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__NETZ_SRC_COLM_CHPM_INSU_Colm_v')
    
    Join_18_Colm_Part_v=NETZ_SRC_COLM_CHPM_INSU_Colm_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__Join_18_Colm_Part_v PURGE").show()
    
    print("Join_18_Colm_Part_v")
    
    print(Join_18_Colm_Part_v.schema.json())
    
    print("count:{}".format(Join_18_Colm_Part_v.count()))
    
    Join_18_Colm_Part_v.show(1000,False)
    
    Join_18_Colm_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__Join_18_Colm_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Join_18_Colt_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    NETZ_SRC_COLT_Colt_v=spark.table('datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__NETZ_SRC_COLT_Colt_v')
    
    Join_18_Colt_Part_v=NETZ_SRC_COLT_Colt_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__Join_18_Colt_Part_v PURGE").show()
    
    print("Join_18_Colt_Part_v")
    
    print(Join_18_Colt_Part_v.schema.json())
    
    print("count:{}".format(Join_18_Colt_Part_v.count()))
    
    Join_18_Colt_Part_v.show(1000,False)
    
    Join_18_Colt_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__Join_18_Colt_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Join_18(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Join_18_lnk_Source_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__Join_18_lnk_Source_Part_v')
    
    Join_18_Colt_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__Join_18_Colt_Part_v')
    
    Join_18_Colm_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__Join_18_Colm_Part_v')
    
    Join_18_v=Join_18_lnk_Source_Part_v.join(Join_18_Colt_Part_v,['KEY_1'],'left').join(Join_18_Colm_Part_v,['KEY_1'],'left')
    
    Join_18_DSLink22_v = Join_18_v.select(Join_18_lnk_Source_Part_v.KEY_1.cast('string').alias('KEY_1'),Join_18_lnk_Source_Part_v.R_KEY1.cast('string').alias('R_KEY1'),Join_18_lnk_Source_Part_v.ORIG_BRCH_CODE.cast('string').alias('ORIG_BRCH_CODE'),Join_18_lnk_Source_Part_v.RECOURSE.cast('string').alias('RECOURSE'),Join_18_lnk_Source_Part_v.CREDIT_LINE.cast('decimal(17,3)').alias('CREDIT_LINE'),Join_18_lnk_Source_Part_v.LOCAL_INCORPN.cast('string').alias('LOCAL_INCORPN'),Join_18_lnk_Source_Part_v.PRE_TRFR_APP_DT.cast('integer').alias('PRE_TRFR_APP_DT'),Join_18_lnk_Source_Part_v.PRE_TRFR_APPR.cast('decimal(17,3)').alias('PRE_TRFR_APPR'),Join_18_lnk_Source_Part_v.DLR_CODE.cast('string').alias('DLR_CODE'),Join_18_lnk_Source_Part_v.DLR_NAME.cast('string').alias('DLR_NAME'),Join_18_lnk_Source_Part_v.DLR_LOCN_CODE.cast('string').alias('DLR_LOCN_CODE'),Join_18_lnk_Source_Part_v.CONTACT_PERSON.cast('string').alias('CONTACT_PERSON'),Join_18_Colm_Part_v.KEY_2.cast('string').alias('KEY_2'),Join_18_Colm_Part_v.PREMIUM.cast('decimal(17,3)').alias('PREMIUM'),Join_18_Colm_Part_v.EXPIRY_DATE.cast('integer').alias('EXPIRY_DATE'),Join_18_Colm_Part_v.POLICY_NO_FIXED.cast('string').alias('POLICY_NO_FIXED'),Join_18_Colm_Part_v.START_DATE.cast('integer').alias('START_DATE'),Join_18_Colm_Part_v.CONDITON.cast('string').alias('CONDITON'),Join_18_Colm_Part_v.TENDR_RSLT_AMT.cast('decimal(17,3)').alias('TENDR_RSLT_AMT'),Join_18_Colm_Part_v.IND.cast('string').alias('IND'),Join_18_Colm_Part_v.INS_IND.cast('string').alias('INS_IND'),Join_18_Colt_Part_v.MI006_TOTAL_REALISABLE_VAL.cast('decimal(18,3)').alias('MI006_TOTAL_REALISABLE_VAL'),Join_18_Colt_Part_v.MI006_LTV_VAL.cast('decimal(18,3)').alias('MI006_LTV_VAL'),Join_18_Colm_Part_v.CPRO.cast('string').alias('CPRO'))
    
    Join_18_DSLink22_v = Join_18_DSLink22_v.selectExpr("RTRIM(KEY_1) AS KEY_1","R_KEY1","RTRIM(ORIG_BRCH_CODE) AS ORIG_BRCH_CODE","RTRIM(RECOURSE) AS RECOURSE","CREDIT_LINE","RTRIM(LOCAL_INCORPN) AS LOCAL_INCORPN","PRE_TRFR_APP_DT","PRE_TRFR_APPR","RTRIM(DLR_CODE) AS DLR_CODE","RTRIM(DLR_NAME) AS DLR_NAME","RTRIM(DLR_LOCN_CODE) AS DLR_LOCN_CODE","RTRIM(CONTACT_PERSON) AS CONTACT_PERSON","KEY_2","PREMIUM","EXPIRY_DATE","RTRIM(POLICY_NO_FIXED) AS POLICY_NO_FIXED","START_DATE","CONDITON","TENDR_RSLT_AMT","IND","INS_IND","MI006_TOTAL_REALISABLE_VAL","MI006_LTV_VAL","CPRO").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'KEY_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(19)'}}, {'name': 'R_KEY1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'ORIG_BRCH_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'RECOURSE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'CREDIT_LINE', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'LOCAL_INCORPN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'PRE_TRFR_APP_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'PRE_TRFR_APPR', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'DLR_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(16)'}}, {'name': 'DLR_NAME', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(60)'}}, {'name': 'DLR_LOCN_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'CONTACT_PERSON', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(40)'}}, {'name': 'KEY_2', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'PREMIUM', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'EXPIRY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'POLICY_NO_FIXED', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'START_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'CONDITON', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'TENDR_RSLT_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'INS_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TOTAL_REALISABLE_VAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LTV_VAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'CPRO', 'type': 'string', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__Join_18_DSLink22_v PURGE").show()
    
    print("Join_18_DSLink22_v")
    
    print(Join_18_DSLink22_v.schema.json())
    
    print("count:{}".format(Join_18_DSLink22_v.count()))
    
    Join_18_DSLink22_v.show(1000,False)
    
    Join_18_DSLink22_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__Join_18_DSLink22_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_24_DSLink22_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Join_18_DSLink22_v=spark.table('datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__Join_18_DSLink22_v')
    
    Transformer_24_DSLink22_Part_v=Join_18_DSLink22_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__Transformer_24_DSLink22_Part_v PURGE").show()
    
    print("Transformer_24_DSLink22_Part_v")
    
    print(Transformer_24_DSLink22_Part_v.schema.json())
    
    print("count:{}".format(Transformer_24_DSLink22_Part_v.count()))
    
    Transformer_24_DSLink22_Part_v.show(1000,False)
    
    Transformer_24_DSLink22_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__Transformer_24_DSLink22_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_24(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_24_DSLink22_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__Transformer_24_DSLink22_Part_v')
    
    Transformer_24_v = Transformer_24_DSLink22_Part_v.withColumn('COLM', expr("""IF(ISNULL(IND), 0, 1)""").cast('string').alias('COLM'))
    
    Transformer_24_lnk_RELM_Tgt_v = Transformer_24_v.select(expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM KEY_1))""").cast('string').alias('B_KEY'),expr("""IF(COLM = 1, (IF(ISNULL(TENDR_RSLT_AMT), 0, TENDR_RSLT_AMT)), 0)""").cast('decimal(13,4)').alias('MI006_CURM_RATE_MID'),expr("""IF(COLM = 1, (IF(ISNULL(POLICY_NO_FIXED), '', POLICY_NO_FIXED)), '')""").cast('string').alias('MI006_AM_AUTOLIFE_INS_CMPNY'),expr("""IF(COLM = 1, (IF(ISNULL(CONDITON), '', CONDITON)), '')""").cast('string').alias('MI006_COND_GOODS'),expr("""IF(ISNULL(CREDIT_LINE), 0, CREDIT_LINE)""").cast('decimal(18,3)').alias('MI006_CREDIT_LINE'),expr("""CASE WHEN DLR_CODE IS NULL THEN '' ELSE LPAD(DLR_CODE, 17, '0') END""").cast('string').alias('MI006_DEALER_CODE'),expr("""IF(ISNULL(DLR_NAME), '', DLR_NAME)""").cast('string').alias('MI006_DEALER_NAME'),expr("""IF(ISNULL(DLR_LOCN_CODE), '', DLR_LOCN_CODE)""").cast('string').alias('MI006_DLR_LOCN_CODE'),expr("""IF(COLM = 1, (IF(ISNULL(START_DATE), 0, START_DATE)), 0)""").cast('integer').alias('MI006_DRIVEC_EFF_DATE'),expr("""IF(COLM = 1, (IF(ISNULL(INS_IND), 'N', 'Y')), 'N')""").cast('string').alias('MI006_DRIVEC_INS_CMPNY'),expr("""IF(COLM = 1, (IF(ISNULL(PREMIUM), 0, PREMIUM)), 0)""").cast('decimal(18,3)').alias('MI006_EWP_AMT'),expr("""IF(COLM = 1, (IF(ISNULL(KEY_2), '', KEY_2)), '')""").cast('string').alias('MI006_INS_CMPNY'),expr("""IF(COLM = 1, (IF(INS_IND = 'Y' AND ISNOTNULL(EXPIRY_DATE), EXPIRY_DATE, 0)), 0)""").cast('integer').alias('MI006_INSURANCE_DISPOSED_DATE'),expr("""IF(ISNULL(PRE_TRFR_APPR), '', RIGHT(CONCAT_WS('', REPEAT('0', 7), (IF(PRE_TRFR_APPR = 0, '0', PRE_TRFR_APPR))), 7))""").cast('string').alias('MI006_PRE_TRFR_APPR'),expr("""IF(ISNULL(ORIG_BRCH_CODE), '', ORIG_BRCH_CODE)""").cast('string').alias('MI006_DLCD_ORIG_BRCH_CODE'),expr("""IF(ISNULL(RECOURSE), '', RECOURSE)""").cast('string').alias('MI006_DLCD_RECOURSE'),expr("""IF(ISNULL(CREDIT_LINE), '', CREDIT_LINE)""").cast('decimal(18,3)').alias('MI006_DLCD_CREDIT_LINE'),expr("""IF(ISNULL(LOCAL_INCORPN), '', LOCAL_INCORPN)""").cast('string').alias('MI006_DLCD_LOCAL_INCORPN'),expr("""IF(ISNULL(PRE_TRFR_APP_DT), 0, PRE_TRFR_APP_DT)""").cast('integer').alias('MI006_DLCD_PRE_TRFR_APP_DT'),expr("""IF(ISNULL(PRE_TRFR_APPR), '', RIGHT(CONCAT_WS('', REPEAT('0', 7), (IF(PRE_TRFR_APPR = 0, '0', PRE_TRFR_APPR))), 7))""").cast('string').alias('MI006_DLCD_PRE_TRFR_APPR'),expr("""IF(ISNULL(CONTACT_PERSON), '', CONTACT_PERSON)""").cast('string').alias('MI006_DLCD_CONTACT_PRSN'),col('MI006_TOTAL_REALISABLE_VAL').cast('decimal(18,3)').alias('MI006_TOTAL_REALISABLE_VAL'),col('MI006_LTV_VAL').cast('decimal(18,3)').alias('MI006_LTV_VAL'),expr("""IF(CPRO = 'Y', (IF(ISNULL(EXPIRY_DATE) OR (EXPIRY_DATE = 0), 0, EXPIRY_DATE)), 0)""").cast('integer').alias('MI006_PROP_INS_EXP_DT'))
    
    Transformer_24_lnk_RELM_Tgt_v = Transformer_24_lnk_RELM_Tgt_v.selectExpr("B_KEY","MI006_CURM_RATE_MID","RTRIM(MI006_AM_AUTOLIFE_INS_CMPNY) AS MI006_AM_AUTOLIFE_INS_CMPNY","RTRIM(MI006_COND_GOODS) AS MI006_COND_GOODS","MI006_CREDIT_LINE","MI006_DEALER_CODE","MI006_DEALER_NAME","MI006_DLR_LOCN_CODE","MI006_DRIVEC_EFF_DATE","RTRIM(MI006_DRIVEC_INS_CMPNY) AS MI006_DRIVEC_INS_CMPNY","MI006_EWP_AMT","RTRIM(MI006_INS_CMPNY) AS MI006_INS_CMPNY","MI006_INSURANCE_DISPOSED_DATE","MI006_PRE_TRFR_APPR","MI006_DLCD_ORIG_BRCH_CODE","RTRIM(MI006_DLCD_RECOURSE) AS MI006_DLCD_RECOURSE","MI006_DLCD_CREDIT_LINE","RTRIM(MI006_DLCD_LOCAL_INCORPN) AS MI006_DLCD_LOCAL_INCORPN","MI006_DLCD_PRE_TRFR_APP_DT","MI006_DLCD_PRE_TRFR_APPR","MI006_DLCD_CONTACT_PRSN","MI006_TOTAL_REALISABLE_VAL","MI006_LTV_VAL","MI006_PROP_INS_EXP_DT").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CURM_RATE_MID', 'type': 'decimal(13,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AM_AUTOLIFE_INS_CMPNY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_COND_GOODS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_CREDIT_LINE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DEALER_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DEALER_NAME', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DLR_LOCN_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DRIVEC_EFF_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DRIVEC_INS_CMPNY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_EWP_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INS_CMPNY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_INSURANCE_DISPOSED_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRE_TRFR_APPR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DLCD_ORIG_BRCH_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DLCD_RECOURSE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_DLCD_CREDIT_LINE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DLCD_LOCAL_INCORPN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_DLCD_PRE_TRFR_APP_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DLCD_PRE_TRFR_APPR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DLCD_CONTACT_PRSN', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TOTAL_REALISABLE_VAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LTV_VAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PROP_INS_EXP_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__Transformer_24_lnk_RELM_Tgt_v PURGE").show()
    
    print("Transformer_24_lnk_RELM_Tgt_v")
    
    print(Transformer_24_lnk_RELM_Tgt_v.schema.json())
    
    print("count:{}".format(Transformer_24_lnk_RELM_Tgt_v.count()))
    
    Transformer_24_lnk_RELM_Tgt_v.show(1000,False)
    
    Transformer_24_lnk_RELM_Tgt_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__Transformer_24_lnk_RELM_Tgt_v")
    

@task.pyspark(conn_id="spark-local")
def DS_TGT_RELM_RELX_lnk_RELM_Tgt_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_24_lnk_RELM_Tgt_v=spark.table('datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__Transformer_24_lnk_RELM_Tgt_v')
    
    DS_TGT_RELM_RELX_lnk_RELM_Tgt_Part_v=Transformer_24_lnk_RELM_Tgt_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__DS_TGT_RELM_RELX_lnk_RELM_Tgt_Part_v PURGE").show()
    
    print("DS_TGT_RELM_RELX_lnk_RELM_Tgt_Part_v")
    
    print(DS_TGT_RELM_RELX_lnk_RELM_Tgt_Part_v.schema.json())
    
    print("count:{}".format(DS_TGT_RELM_RELX_lnk_RELM_Tgt_Part_v.count()))
    
    DS_TGT_RELM_RELX_lnk_RELM_Tgt_Part_v.show(1000,False)
    
    DS_TGT_RELM_RELX_lnk_RELM_Tgt_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__DS_TGT_RELM_RELX_lnk_RELM_Tgt_Part_v")
    

@task.pyspark(conn_id="spark-local")
def DS_TGT_RELM_RELX(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    DS_TGT_RELM_RELX_lnk_RELM_Tgt_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_RELM_Extr_POC__DS_TGT_RELM_RELX_lnk_RELM_Tgt_Part_v')
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    locations = Template('{{dbdir.pPROCESSING_DIR}}MIS006_RELM.ds').render(job_params)
    
    log.info("write dataset files to "+locations)
    
    DS_TGT_RELM_RELX_lnk_RELM_Tgt_Part_v.write.mode("overwrite").format("parquet").save(locations)
    

####################################[Main]###################################
import airflow
with DAG(
    dag_id="job_DBdirect_MIS006_RELM_Extr_POC",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None,
    tags=['datastage'],
) as dag:
    
    job_DBdirect_MIS006_RELM_Extr_POC_task = job_DBdirect_MIS006_RELM_Extr_POC()
    
    Job_VIEW_task = Job_VIEW()
    
    NETZ_SRC_RELM_DLCD_task = NETZ_SRC_RELM_DLCD()
    
    V0A13_task = V0A13()
    
    NETZ_SRC_COLM_CHPM_INSU_task = NETZ_SRC_COLM_CHPM_INSU()
    
    NETZ_SRC_COLT_task = NETZ_SRC_COLT()
    
    V0A26_task = V0A26()
    
    V4A0_task = V4A0()
    
    V5A0_task = V5A0()
    
    Join_18_lnk_Source_Part_task = Join_18_lnk_Source_Part()
    
    Join_18_Colm_Part_task = Join_18_Colm_Part()
    
    Join_18_Colt_Part_task = Join_18_Colt_Part()
    
    Join_18_task = Join_18()
    
    Transformer_24_DSLink22_Part_task = Transformer_24_DSLink22_Part()
    
    Transformer_24_task = Transformer_24()
    
    DS_TGT_RELM_RELX_lnk_RELM_Tgt_Part_task = DS_TGT_RELM_RELX_lnk_RELM_Tgt_Part()
    
    DS_TGT_RELM_RELX_task = DS_TGT_RELM_RELX()
    
    
    job_DBdirect_MIS006_RELM_Extr_POC_task >> Job_VIEW_task
    
    Job_VIEW_task >> NETZ_SRC_RELM_DLCD_task
    
    Job_VIEW_task >> V0A13_task
    
    Job_VIEW_task >> NETZ_SRC_COLM_CHPM_INSU_task
    
    Job_VIEW_task >> NETZ_SRC_COLT_task
    
    Job_VIEW_task >> V0A26_task
    
    Job_VIEW_task >> V4A0_task
    
    Job_VIEW_task >> V5A0_task
    
    NETZ_SRC_RELM_DLCD_task >> Join_18_lnk_Source_Part_task
    
    NETZ_SRC_COLM_CHPM_INSU_task >> Join_18_Colm_Part_task
    
    NETZ_SRC_COLT_task >> Join_18_Colt_Part_task
    
    Join_18_lnk_Source_Part_task >> Join_18_task
    
    Join_18_Colm_Part_task >> Join_18_task
    
    Join_18_Colt_Part_task >> Join_18_task
    
    Join_18_task >> Transformer_24_DSLink22_Part_task
    
    Transformer_24_DSLink22_Part_task >> Transformer_24_task
    
    Transformer_24_task >> DS_TGT_RELM_RELX_lnk_RELM_Tgt_Part_task
    
    DS_TGT_RELM_RELX_lnk_RELM_Tgt_Part_task >> DS_TGT_RELM_RELX_task
    



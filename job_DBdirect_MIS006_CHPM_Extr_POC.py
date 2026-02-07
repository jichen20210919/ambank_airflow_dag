
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-02 22:43:51
# @Author  : cloudera
# @File    : job_DBdirect_MIS006_CHPM_Extr_POC.py
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
from pyspark.sql import functions as F
from pyspark.sql import Window
import json
import logging
import pendulum
import textwrap

@task
def job_DBdirect_MIS006_CHPM_Extr_POC(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def Job_VIEW(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task.pyspark(conn_id="spark-local")
def NETZ_SRC_BORM_BOAF(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    catalog=Variable.get("ICEBERG_CATALOG_NAME", default_var="iceberg")
    
    #spark.sql(f"use {catalog}.default").show()
    
    
    
    
    
    sql=Template("""SELECT
    
        A.B_KEY,
    
        A.MI006_ARRS_INT_1,
    
        CASE 
    
            WHEN A.MI006_BRTH_EFF_DATE > 401767 THEN 0 
    
            ELSE A.MI006_BRTH_EFF_DATE 
    
        END AS MI006_BRTH_EFF_DATE,
    
        A.MI006_REASON_CD,
    
        A.DUEC,
    
        A.DUET,
    
        A.MI006_BORM_RM_CODE,
    
        A.MI006_BORH_REBATE_PERC,
    
        A.MI006_BORM_ARR_INT_INCR,
    
        CAST(A.R AS INT) AS R,
    
        CAST(A.T AS INT) AS T,
    
        A.REC_NO
    
    FROM (
    
        SELECT
    
            CAST(BORM.KEY_1 AS STRING) AS B_KEY,
    
            BOAF.ARRS_INT_1 AS MI006_ARRS_INT_1,
    
            RANK() OVER (PARTITION BY BORM.KEY_1 ORDER BY BOAF.DUE_DATE_1) AS R,
    
            RANK() OVER (PARTITION BY BORM.KEY_1 ORDER BY BRTH.EFFECTIVE_DATE) AS T,
    
            BRTH.EFFECTIVE_DATE AS MI006_BRTH_EFF_DATE,
    
            CAST(
    
                CASE 
    
                    WHEN (BORH.REASON_CD IS NULL OR TRIM(BORH.REASON_CD) = '') THEN '0' 
    
                    ELSE BORH.REASON_CD 
    
                END AS INT
    
            ) AS MI006_REASON_CD,
    
            CONCAT_WS(',',
    
                BOAF.DUE_DATE_1, BOAF.DUE_DATE_2, BOAF.DUE_DATE_3, BOAF.DUE_DATE_4, BOAF.DUE_DATE_5,
    
                BOAF.DUE_DATE_6, BOAF.DUE_DATE_7, BOAF.DUE_DATE_8, BOAF.DUE_DATE_9, BOAF.DUE_DATE_10,
    
                BOAF.DUE_DATE_11, BOAF.DUE_DATE_12, BOAF.DUE_DATE_13, BOAF.DUE_DATE_14, BOAF.DUE_DATE_15
    
            ) AS DUEC,
    
            CONCAT_WS(',',
    
                BOAF.TOTAL_INST_1, BOAF.TOTAL_INST_2, BOAF.TOTAL_INST_3, BOAF.TOTAL_INST_4, BOAF.TOTAL_INST_5,
    
                BOAF.TOTAL_INST_6, BOAF.TOTAL_INST_7, BOAF.TOTAL_INST_8, BOAF.TOTAL_INST_9, BOAF.TOTAL_INST_10,
    
                BOAF.TOTAL_INST_11, BOAF.TOTAL_INST_12, BOAF.TOTAL_INST_13, BOAF.TOTAL_INST_14, BOAF.TOTAL_INST_15
    
            ) AS DUET,
    
            BORH.REBATE_PERC AS MI006_BORH_REBATE_PERC,
    
            BORM.ARR_INT_INCR AS MI006_BORM_ARR_INT_INCR,
    
            CAST(RMCD.RM_CODE AS STRING) AS MI006_BORM_RM_CODE,
    
            BORH.REASON_CD,
    
            BOAF.REC_NO
    
        FROM {{dbdir.pODS_SCHM}}.BORM
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.BOAF BOAF 
    
            ON BOAF.INST_NO = SUBSTRING(BORM.KEY_1, 1, 3) 
    
            AND BOAF.ACCT_NO = SUBSTRING(BORM.KEY_1, 4, 16)
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.BRTH BRTH 
    
            ON BRTH.SOC_NO = SUBSTRING(BORM.KEY_1, 1, 3) 
    
            AND BRTH.CUST_AC = SUBSTRING(BORM.KEY_1, 4, 16)
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.BORH BORH 
    
            ON BORH.SOC_NO = SUBSTRING(BORM.KEY_1, 1, 3) 
    
            AND BORH.MEMB_CUST_AC = SUBSTRING(BORM.KEY_1, 4, 16)
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.RMCD RMCD 
    
            ON RMCD.INST_NO = 999 
    
            AND RMCD.CIF_NO = BORM.CUSTOMER_NO 
    
            AND RMCD.ACCT_NO = SUBSTRING(BORM.KEY_1, 4, 16)
    
    ) A 
    
    WHERE A.T = 1
    
    -- AND SUBSTRING(A.B_KEY, 4, 16) IN ('0000340100140195', '0000250200098492')""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    NETZ_SRC_BORM_BOAF_v = spark.sql(sql)
    
    
    
    
    
    #spark.sql(f"use spark_catalog.default").show()
    
    NETZ_SRC_BORM_BOAF_lnk_Source_v=NETZ_SRC_BORM_BOAF_v.select(NETZ_SRC_BORM_BOAF_v[0].cast('string').alias('B_KEY'),NETZ_SRC_BORM_BOAF_v[1].cast('decimal(18,3)').alias('MI006_ARRS_INT_1'),NETZ_SRC_BORM_BOAF_v[2].cast('integer').alias('MI006_BRTH_EFF_DATE'),NETZ_SRC_BORM_BOAF_v[3].cast('integer').alias('MI006_REASON_CD'),NETZ_SRC_BORM_BOAF_v[4].cast('string').alias('DUEC'),NETZ_SRC_BORM_BOAF_v[5].cast('string').alias('DUET'),NETZ_SRC_BORM_BOAF_v[6].cast('string').alias('MI006_BORM_RM_CODE'),NETZ_SRC_BORM_BOAF_v[7].cast('string').alias('MI006_BORH_REBATE_PERC'),NETZ_SRC_BORM_BOAF_v[8].cast('decimal(18,5)').alias('MI006_BORM_ARR_INT_INCR'),NETZ_SRC_BORM_BOAF_v[9].cast('integer').alias('R'),NETZ_SRC_BORM_BOAF_v[10].cast('integer').alias('T'),NETZ_SRC_BORM_BOAF_v[11].cast('integer').alias('REC_NO'))
    
    NETZ_SRC_BORM_BOAF_lnk_Source_v = NETZ_SRC_BORM_BOAF_lnk_Source_v.selectExpr("B_KEY","MI006_ARRS_INT_1","MI006_BRTH_EFF_DATE","MI006_REASON_CD","DUEC","DUET","MI006_BORM_RM_CODE","RTRIM(MI006_BORH_REBATE_PERC) AS MI006_BORH_REBATE_PERC","MI006_BORM_ARR_INT_INCR","R","T","REC_NO").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ARRS_INT_1', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BRTH_EFF_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REASON_CD', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'DUEC', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'DUET', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_RM_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORH_REBATE_PERC', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_BORM_ARR_INT_INCR', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'R', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'T', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REC_NO', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS NETZ_SRC_BORM_BOAF_lnk_Source_v").show()
    
    print("NETZ_SRC_BORM_BOAF_lnk_Source_v")
    
    print(NETZ_SRC_BORM_BOAF_lnk_Source_v.schema.json())
    
    print("count:{}".format(NETZ_SRC_BORM_BOAF_lnk_Source_v.count()))
    
    NETZ_SRC_BORM_BOAF_lnk_Source_v.show(1000,False)
    
    NETZ_SRC_BORM_BOAF_lnk_Source_v.write.mode("overwrite").saveAsTable("NETZ_SRC_BORM_BOAF_lnk_Source_v")
    

@task
def V0A13(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task.pyspark(conn_id="spark-local")
def NETZ_SRC_COLM_CHPM(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    catalog=Variable.get("ICEBERG_CATALOG_NAME", default_var="iceberg")
    
    #spark.sql(f"use {catalog}.default").show()
    
    
    
    
    
    sql=Template("""SELECT DISTINCT 
    
        A.B_KEY,
    
        A.MI006_DESCRIPTION,
    
        A.MI006_CLASSIFIED_DATE,
    
        A.MI006_GUA_CUSTO_NO,
    
        A.MI006_AMORT_FLAG,
    
        CAST(SUM(COALESCE(A.Z, 0)) OVER (PARTITION BY A.B_KEY, A.RNK) AS DECIMAL(18,5)) AS MI006_ZECT_HANDLING_FEE,
    
        CAST(SUM(COALESCE(A.Z1, 0)) OVER (PARTITION BY A.B_KEY, A.RNK) AS DECIMAL(18,5)) AS MI006_ZECT_OTHER_FEE,
    
        CAST(SUM(COALESCE(A.Z2, 0)) OVER (PARTITION BY A.B_KEY, A.RNK) AS DECIMAL(20,5)) AS MI006_ZECT_INPUTTAXNC1,
    
        A.MI006_ZECT_EXP_HANDLING_AMT,
    
        -- Start of New fields [IA AHCR1486]
    
        A.MI006_RIGH_NPL_COUNTER,
    
        A.MI006_RIGH_PL_DATE
    
        -- End of New fields [IA AHCR1486]
    
    FROM (
    
        SELECT
    
            CAST(BORM.KEY_1 AS STRING) AS B_KEY,
    
            COALESCE(ZECT.EXP_AMT, 0) AS MI006_ZECT_EXP_HANDLING_AMT,
    
            EXPN.AMORT_FLAG AS MI006_AMORT_FLAG,
    
            ZECT.VAL_TO_AMRT AS Z,
    
            ZECT1.VAL_TO_AMRT AS Z1,
    
            ZECT2.VAL_TO_AMRT AS Z2,
    
            PURP.DESCRIPTION AS MI006_DESCRIPTION,
    
            R.NPL_DATE AS MI006_CLASSIFIED_DATE,
    
            RANK() OVER (PARTITION BY BORM.KEY_1 ORDER BY ZECT.VAL_TO_AMRT) AS R,
    
            CAST(IADVTT.CUST_NO AS STRING) AS MI006_GUA_CUSTO_NO,
    
            RANK() OVER (
    
                PARTITION BY BORM.KEY_1 
    
                ORDER BY R.INST_NO, R.KEY_1, R.ACT_SYSTEM, R.COUNTER DESC
    
            ) AS RNK,
    
            -- Start of New fields [IA AHCR1486]
    
            CAST(R.COUNTER AS STRING) AS MI006_RIGH_NPL_COUNTER,
    
            R.PL_DATE AS MI006_RIGH_PL_DATE
    
            -- End of New fields [IA AHCR1486]
    
        FROM {{dbdir.pODS_SCHM}}.BORM AS BORM
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.ZECT AS ZECT 
    
            ON SUBSTRING(ZECT.KEY_1, 1, 3) = SUBSTRING(BORM.KEY_1, 1, 3) 
    
            AND SUBSTRING(ZECT.KEY_1, 4, 16) = SUBSTRING(BORM.KEY_1, 4, 16) 
    
            AND ZECT.MNEMONIC = 'HANDLINGFEE'
    
        
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.ZECT AS ZECT1 
    
            ON SUBSTRING(ZECT1.KEY_1, 1, 3) = SUBSTRING(BORM.KEY_1, 1, 3) 
    
            AND SUBSTRING(ZECT1.KEY_1, 4, 16) = SUBSTRING(BORM.KEY_1, 4, 16) 
    
            AND ZECT1.MNEMONIC <> 'HANDLINGFEE'
    
            -- BEGIN: SR907446: H23 Outage issue
    
            AND ZECT1.MNEMONIC <> 'INPUTTAX1'
    
            -- END: SR907446: H23 Outage issue
    
            
    
        -- START: Change to add new mnemonic INPUTTAXNC1 for MIS200 and MIS006 ---- ODS SPEC v9.4
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.ZECT AS ZECT2
    
            ON SUBSTRING(ZECT2.KEY_1, 1, 3) = SUBSTRING(BORM.KEY_1, 1, 3) 
    
            AND SUBSTRING(ZECT2.KEY_1, 4, 16) = SUBSTRING(BORM.KEY_1, 4, 16) 
    
            AND ZECT2.MNEMONIC = 'INPUTTAXNC1'
    
        -- END: Change to add new mnemonic INPUTTAXNC1 for MIS200 and MIS006 ---- ODS SPEC v9.4 
    
        
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.EXPN AS EXPN 
    
            ON EXPN.INST_NO = SUBSTRING(BORM.KEY_1, 1, 3) 
    
            AND EXPN.EXP_TYPE = ZECT.MNEMONIC 
    
        
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.PURP AS PURP 
    
            ON SUBSTRING(PURP.KEY_1, 1, 3) = SUBSTRING(BORM.KEY_1, 1, 3) 
    
            AND SUBSTRING(PURP.KEY_1, 4, 3) = BORM.PURPOSE_CODE_A
    
        
    
        LEFT OUTER JOIN (
    
            SELECT 
    
                RIGH.INST_NO,
    
                RIGH.KEY_1,
    
                RIGH.ACT_SYSTEM,
    
                RIGH.NPL_DATE,
    
                -- Start of New fields [IA AHCR1486]
    
                RIGH.COUNTER,
    
                RIGH.PL_DATE
    
                -- End of New fields [IA AHCR1486]
    
            FROM {{dbdir.pODS_SCHM}}.RIGH AS RIGH
    
        ) AS R 
    
            ON R.INST_NO = SUBSTRING(BORM.KEY_1, 1, 3) 
    
            AND R.KEY_1 = SUBSTRING(BORM.KEY_1, 4, 16)
    
        
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.IADVTT AS IADVTT 
    
            ON BORM.KEY_1 = SUBSTRING(IADVTT.KEY_1, 1, 19) 
    
            AND SUBSTRING(IADVTT.KEY_1, 20, 4) = ''
    
    ) AS A
    
    WHERE A.R = 1 
    
        AND A.RNK = 1
    
    -- Uncomment and modify if needed for testing:
    
    -- AND SUBSTRING(A.B_KEY, 4, 16) IN ('0000340100140195', '0000250200098492')""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    NETZ_SRC_COLM_CHPM_v = spark.sql(sql)
    
    
    
    
    
    #spark.sql(f"use spark_catalog.default").show()
    
    NETZ_SRC_COLM_CHPM_Colm_v=NETZ_SRC_COLM_CHPM_v.select(NETZ_SRC_COLM_CHPM_v[0].cast('string').alias('B_KEY'),NETZ_SRC_COLM_CHPM_v[1].cast('string').alias('MI006_DESCRIPTION'),NETZ_SRC_COLM_CHPM_v[2].cast('integer').alias('MI006_CLASSIFIED_DATE'),NETZ_SRC_COLM_CHPM_v[3].cast('string').alias('MI006_GUA_CUSTO_NO'),NETZ_SRC_COLM_CHPM_v[4].cast('string').alias('MI006_AMORT_FLAG'),NETZ_SRC_COLM_CHPM_v[5].cast('decimal(18,5)').alias('MI006_ZECT_HANDLING_FEE'),NETZ_SRC_COLM_CHPM_v[6].cast('decimal(18,5)').alias('MI006_ZECT_OTHER_FEE'),NETZ_SRC_COLM_CHPM_v[7].cast('decimal(18,3)').alias('MI006_ZECT_EXP_HANDLING_AMT'),NETZ_SRC_COLM_CHPM_v[8].cast('string').alias('MI006_RIGH_NPL_COUNTER'),NETZ_SRC_COLM_CHPM_v[9].cast('integer').alias('MI006_RIGH_PL_DATE'),NETZ_SRC_COLM_CHPM_v[10].cast('decimal(20,5)').alias('MI006_ZECT_INPUTTAXNC1'))
    
    NETZ_SRC_COLM_CHPM_Colm_v = NETZ_SRC_COLM_CHPM_Colm_v.selectExpr("B_KEY","MI006_DESCRIPTION","MI006_CLASSIFIED_DATE","MI006_GUA_CUSTO_NO","RTRIM(MI006_AMORT_FLAG) AS MI006_AMORT_FLAG","MI006_ZECT_HANDLING_FEE","MI006_ZECT_OTHER_FEE","MI006_ZECT_EXP_HANDLING_AMT","MI006_RIGH_NPL_COUNTER","MI006_RIGH_PL_DATE","MI006_ZECT_INPUTTAXNC1").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DESCRIPTION', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CLASSIFIED_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GUA_CUSTO_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AMORT_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ZECT_HANDLING_FEE', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ZECT_OTHER_FEE', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ZECT_EXP_HANDLING_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RIGH_NPL_COUNTER', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RIGH_PL_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ZECT_INPUTTAXNC1', 'type': 'decimal(20,5)', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS NETZ_SRC_COLM_CHPM_Colm_v").show()
    
    print("NETZ_SRC_COLM_CHPM_Colm_v")
    
    print(NETZ_SRC_COLM_CHPM_Colm_v.schema.json())
    
    print("count:{}".format(NETZ_SRC_COLM_CHPM_Colm_v.count()))
    
    NETZ_SRC_COLM_CHPM_Colm_v.show(1000,False)
    
    NETZ_SRC_COLM_CHPM_Colm_v.write.mode("overwrite").saveAsTable("NETZ_SRC_COLM_CHPM_Colm_v")
    

@task.pyspark(conn_id="spark-local")
def NETZ_SRC_BLDVAA(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    catalog=Variable.get("ICEBERG_CATALOG_NAME", default_var="iceberg")
    
    #spark.sql(f"use {catalog}.default").show()
    
    
    
    
    
    sql=Template("""SELECT 
    
        A.B_KEY,
    
        CASE 
    
            WHEN TRIM(A.MI006_NO_OF_DISB_ON_NOTE) = '0 ' THEN NULL 
    
            ELSE A.MI006_NO_OF_DISB_ON_NOTE 
    
        END AS MI006_NO_OF_DISB_ON_NOTE
    
    FROM (
    
        SELECT
    
            CAST(BORM.KEY_1 AS VARCHAR(19)) AS B_KEY,
    
            CAST(COUNT(BLDVAA.KEY_1) OVER (PARTITION BY BORM.KEY_1) AS STRING) AS MI006_NO_OF_DISB_ON_NOTE,
    
            ROW_NUMBER() OVER (PARTITION BY BORM.KEY_1 ORDER BY BLDVAA.KEY_1) AS R
    
        FROM 
    
            {{dbdir.pODS_SCHM}}.BORM BORM
    
        LEFT OUTER JOIN 
    
            {{dbdir.pODS_SCHM}}.BLDVAA BLDVAA 
    
            ON SUBSTRING(BLDVAA.KEY_1, 1, 19) = BORM.KEY_1
    
    ) A
    
    WHERE A.R = 1
    
    -- AND SUBSTRING(A.B_KEY, 4, 16) IN ('0000340100140195', '0000250200098492')""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    NETZ_SRC_BLDVAA_v = spark.sql(sql)
    
    
    
    
    
    #spark.sql(f"use spark_catalog.default").show()
    
    NETZ_SRC_BLDVAA_Colt_v=NETZ_SRC_BLDVAA_v.select(NETZ_SRC_BLDVAA_v[0].cast('string').alias('B_KEY'),NETZ_SRC_BLDVAA_v[1].cast('string').alias('MI006_NO_OF_DISB_ON_NOTE'))
    
    NETZ_SRC_BLDVAA_Colt_v = NETZ_SRC_BLDVAA_Colt_v.selectExpr("B_KEY","RTRIM(MI006_NO_OF_DISB_ON_NOTE) AS MI006_NO_OF_DISB_ON_NOTE").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_OF_DISB_ON_NOTE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}]}))
    
    spark.sql("DROP TABLE IF EXISTS NETZ_SRC_BLDVAA_Colt_v").show()
    
    print("NETZ_SRC_BLDVAA_Colt_v")
    
    print(NETZ_SRC_BLDVAA_Colt_v.schema.json())
    
    print("count:{}".format(NETZ_SRC_BLDVAA_Colt_v.count()))
    
    NETZ_SRC_BLDVAA_Colt_v.show(1000,False)
    
    NETZ_SRC_BLDVAA_Colt_v.write.mode("overwrite").saveAsTable("NETZ_SRC_BLDVAA_Colt_v")
    

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

@task
def V88A0(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task.pyspark(conn_id="spark-local")
def NETZ_SRC_Zect(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    catalog=Variable.get("ICEBERG_CATALOG_NAME", default_var="iceberg")
    
    #spark.sql(f"use {catalog}.default").show()
    
    
    
    
    
    sql=Template("""SELECT 
    
        B_KEY,
    
        CAST(SUM(MI006_ZECT_OTHER_FEE) AS DECIMAL(18,5)) AS MI006_ZECT_OTHER_FEE_sum
    
    FROM (
    
        SELECT 
    
            CAST(BORM.KEY_1 AS VARCHAR(19)) AS B_KEY,
    
            SUBSTR(ZECT.KEY_1, 1, 19) AS ZECT_K,
    
            CONCAT(EXPN.INST_NO, EXPN.EXP_TYPE) AS expn_k,
    
            SUBSTR(GLDM.KEY_1, 1, 19) AS gldm_K,
    
            SUSP.KEY_1 AS susp_k,
    
            COALESCE(TRIM(SUBSTR(GLDM.GL_CLASS_CODE, 15, 8)), '') AS GL_CLASS_CODE_chK,
    
            ZECT.VAL_TO_AMRT,
    
            CASE 
    
                WHEN (ZECT_K IS NOT NULL) 
    
                    AND (expn_k IS NOT NULL) 
    
                    AND (susp_k IS NOT NULL) 
    
                    AND (gldm_K IS NOT NULL) 
    
                    AND (GL_CLASS_CODE_chK != '') 
    
                    AND (GL_CLASS_CODE_chK IN ('00155406'))
    
                THEN COALESCE(ZECT.VAL_TO_AMRT, 0)
    
                ELSE 0 
    
            END AS MI006_ZECT_OTHER_FEE
    
        
    
        FROM {{dbdir.pODS_SCHM}}.BORM
    
        
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.ZECT 
    
            ON SUBSTR(ZECT.KEY_1, 1, 3) = SUBSTR(BORM.KEY_1, 1, 3)
    
            AND SUBSTR(ZECT.KEY_1, 4, 16) = SUBSTR(BORM.KEY_1, 4, 16)
    
            AND ZECT.VAL_TO_AMRT > 0
    
        
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.EXPN
    
            ON EXPN.INST_NO = SUBSTR(ZECT.KEY_1, 1, 3)
    
            AND EXPN.EXP_TYPE = ZECT.MNEMONIC
    
        
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.SUSP
    
            ON SUBSTR(SUSP.KEY_1, 1, 3) = SUBSTR(BORM.KEY_1, 1, 3)
    
            AND SUBSTR(SUSP.KEY_1, 4, 5) = BORM.BR_NO
    
            AND SUBSTR(SUSP.KEY_1, 9, 3) = BORM.CURRENCY_IND
    
            AND SUBSTR(SUSP.KEY_1, 13, 13) = EXPN.MNEMONIC_DR
    
        
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.GLDM
    
            ON SUBSTR(GLDM.KEY_1, 1, 3) = SUBSTR(SUSP.KEY_1, 1, 3)
    
            AND SUBSTR(GLDM.KEY_1, 4, 16) = SUSP.SUSP_ACCT_NO
    
    ) A 
    
    GROUP BY A.B_KEY""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    NETZ_SRC_Zect_v = spark.sql(sql)
    
    
    
    
    
    #spark.sql(f"use spark_catalog.default").show()
    
    NETZ_SRC_Zect_Zect_v=NETZ_SRC_Zect_v.select(NETZ_SRC_Zect_v[0].cast('string').alias('B_KEY'),NETZ_SRC_Zect_v[1].cast('decimal(18,5)').alias('MI006_ZECT_OTHER_FEE_sum'))
    
    NETZ_SRC_Zect_Zect_v = NETZ_SRC_Zect_Zect_v.selectExpr("B_KEY","MI006_ZECT_OTHER_FEE_sum").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ZECT_OTHER_FEE_sum', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS NETZ_SRC_Zect_Zect_v").show()
    
    print("NETZ_SRC_Zect_Zect_v")
    
    print(NETZ_SRC_Zect_Zect_v.schema.json())
    
    print("count:{}".format(NETZ_SRC_Zect_Zect_v.count()))
    
    NETZ_SRC_Zect_Zect_v.show(1000,False)
    
    NETZ_SRC_Zect_Zect_v.write.mode("overwrite").saveAsTable("NETZ_SRC_Zect_Zect_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_J_lnk_Source_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    NETZ_SRC_BORM_BOAF_lnk_Source_v=spark.table('NETZ_SRC_BORM_BOAF_lnk_Source_v')
    
    Transformer_J_lnk_Source_Part_v=NETZ_SRC_BORM_BOAF_lnk_Source_v
    
    spark.sql("DROP TABLE IF EXISTS Transformer_J_lnk_Source_Part_v").show()
    
    print("Transformer_J_lnk_Source_Part_v")
    
    print(Transformer_J_lnk_Source_Part_v.schema.json())
    
    print("count:{}".format(Transformer_J_lnk_Source_Part_v.count()))
    
    Transformer_J_lnk_Source_Part_v.show(1000,False)
    
    Transformer_J_lnk_Source_Part_v.write.mode("overwrite").saveAsTable("Transformer_J_lnk_Source_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Join_18_Colm_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    NETZ_SRC_COLM_CHPM_Colm_v=spark.table('NETZ_SRC_COLM_CHPM_Colm_v')
    
    Join_18_Colm_Part_v=NETZ_SRC_COLM_CHPM_Colm_v
    
    spark.sql("DROP TABLE IF EXISTS Join_18_Colm_Part_v").show()
    
    print("Join_18_Colm_Part_v")
    
    print(Join_18_Colm_Part_v.schema.json())
    
    print("count:{}".format(Join_18_Colm_Part_v.count()))
    
    Join_18_Colm_Part_v.show(1000,False)
    
    Join_18_Colm_Part_v.write.mode("overwrite").saveAsTable("Join_18_Colm_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Join_18_Colt_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    NETZ_SRC_BLDVAA_Colt_v=spark.table('NETZ_SRC_BLDVAA_Colt_v')
    
    Join_18_Colt_Part_v=NETZ_SRC_BLDVAA_Colt_v
    
    spark.sql("DROP TABLE IF EXISTS Join_18_Colt_Part_v").show()
    
    print("Join_18_Colt_Part_v")
    
    print(Join_18_Colt_Part_v.schema.json())
    
    print("count:{}".format(Join_18_Colt_Part_v.count()))
    
    Join_18_Colt_Part_v.show(1000,False)
    
    Join_18_Colt_Part_v.write.mode("overwrite").saveAsTable("Join_18_Colt_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Join_18_Zect_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    NETZ_SRC_Zect_Zect_v=spark.table('NETZ_SRC_Zect_Zect_v')
    
    Join_18_Zect_Part_v=NETZ_SRC_Zect_Zect_v
    
    spark.sql("DROP TABLE IF EXISTS Join_18_Zect_Part_v").show()
    
    print("Join_18_Zect_Part_v")
    
    print(Join_18_Zect_Part_v.schema.json())
    
    print("count:{}".format(Join_18_Zect_Part_v.count()))
    
    Join_18_Zect_Part_v.show(1000,False)
    
    Join_18_Zect_Part_v.write.mode("overwrite").saveAsTable("Join_18_Zect_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_J(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_J_lnk_Source_Part_v=spark.table('Transformer_J_lnk_Source_Part_v')
    # 1. Define the Window (Equivalent to the Sort/Partition in DS)
    # DataStage sorted by B_KEY and REC_NO
    window_spec = Window.partitionBy("B_KEY").orderBy("REC_NO")
    window_unbounded = window_spec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    # 2. Stage Variable Logic: Aggregate columns into arrays (Replacing the comma-string accumulation)
    df_transformed = Transformer_J_lnk_Source_Part_v.withColumn("DUDE", F.collect_list("DUEC").over(window_unbounded)) \
                    .withColumn("DUET", F.collect_list("DUET").over(window_unbounded))

    # 3. Calculate DC (Dcount equivalent)
    df_transformed = df_transformed.withColumn("DC", F.size(F.col("DUDE"))).withColumn(
    "FC",
    F.when(
        F.col("DC").cast("int").isNotNull(),
        # Count elements in DUDE_arr that are exactly '0'
        F.col("DC") - F.size(F.filter(F.col("DUDE"), lambda x: x == '0'))
    ).otherwise(F.lit(-99)))

    # 4. Looping Logic / Total Calculation
    # In DS, the loop calculates a running total based on a flag 'T'.
    # We can use a Python UDF or Spark's aggregate function for complex array logic.

    # This logic mimics the @ITERATION loop:
    # If Field(DUDE, i) == '0' -> C = 0, else C = DUET[i]
    # If C == 0 -> T = 0, else T (persists)
    # Total = Running sum of C as long as T isn't 0

    expr_total = """
    aggregate(
        zip_with(DUDE, DUET, (de, dt) -> struct(de as duec, dt as duet)),
        cast(struct(0.0 as total, 1 as t_flag) as struct<total:double, t_flag:int>),
        (acc, x) -> 
            case 
                when acc.t_flag = 0 then acc
                when x.duec = '0' then struct(acc.total as total, 0 as t_flag)
                else struct(acc.total + cast(x.duet as double) as total, 1 as t_flag)
            end,
        acc -> acc.total
    )
    """

    final_df = df_transformed.withColumn("Total", F.expr(expr_total)) \
                            .withColumn("ROW_INDEX__",F.row_number().over(window_spec.orderBy(F.desc("REC_NO"))))    
    
    Transformer_J_Remove_Dupe_v = final_df.filter("ROW_INDEX__ = 1").select(col('B_KEY').cast('string').alias('B_KEY'),col('MI006_ARRS_INT_1').cast('decimal(18,3)').alias('MI006_ARRS_INT_1'),col('MI006_BRTH_EFF_DATE').cast('integer').alias('MI006_BRTH_EFF_DATE'),col('MI006_REASON_CD').cast('integer').alias('MI006_REASON_CD'),col('DUEC').cast('string').alias('DUEC'),col('DUET').cast('string').alias('DUET'),col('MI006_BORM_RM_CODE').cast('string').alias('MI006_BORM_RM_CODE'),col('MI006_BORH_REBATE_PERC').cast('string').alias('MI006_BORH_REBATE_PERC'),col('MI006_BORM_ARR_INT_INCR').cast('decimal(18,5)').alias('MI006_BORM_ARR_INT_INCR'),expr("""IF(FC = -99, NULL, FC)""").cast('integer').alias('MI006_NO_OF_DUES_BOAF'),col('Total').cast('decimal(18,3)').alias('MI006_BILLED_AMT_UNPD_BOAF'))
    
    Transformer_J_Remove_Dupe_v = Transformer_J_Remove_Dupe_v.selectExpr("B_KEY","MI006_ARRS_INT_1","MI006_BRTH_EFF_DATE","MI006_REASON_CD","DUEC","DUET","MI006_BORM_RM_CODE","RTRIM(MI006_BORH_REBATE_PERC) AS MI006_BORH_REBATE_PERC","MI006_BORM_ARR_INT_INCR","MI006_NO_OF_DUES_BOAF","MI006_BILLED_AMT_UNPD_BOAF").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ARRS_INT_1', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BRTH_EFF_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REASON_CD', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'DUEC', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'DUET', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_RM_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORH_REBATE_PERC', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_BORM_ARR_INT_INCR', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_OF_DUES_BOAF', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BILLED_AMT_UNPD_BOAF', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS Transformer_J_Remove_Dupe_v").show()
    
    print("Transformer_J_Remove_Dupe_v")
    
    print(Transformer_J_Remove_Dupe_v.schema.json())
    
    print("count:{}".format(Transformer_J_Remove_Dupe_v.count()))
    
    Transformer_J_Remove_Dupe_v.show(1000,False)
    
    Transformer_J_Remove_Dupe_v.write.mode("overwrite").saveAsTable("Transformer_J_Remove_Dupe_v")
    

@task.pyspark(conn_id="spark-local")
def Join_18_Remove_Dupe_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_J_Remove_Dupe_v=spark.table('Transformer_J_Remove_Dupe_v')
    
    Join_18_Remove_Dupe_Part_v=Transformer_J_Remove_Dupe_v
    
    spark.sql("DROP TABLE IF EXISTS Join_18_Remove_Dupe_Part_v").show()
    
    print("Join_18_Remove_Dupe_Part_v")
    
    print(Join_18_Remove_Dupe_Part_v.schema.json())
    
    print("count:{}".format(Join_18_Remove_Dupe_Part_v.count()))
    
    Join_18_Remove_Dupe_Part_v.show(1000,False)
    
    Join_18_Remove_Dupe_Part_v.write.mode("overwrite").saveAsTable("Join_18_Remove_Dupe_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Join_18(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Join_18_Colt_Part_v=spark.table('Join_18_Colt_Part_v')
    
    Join_18_Colm_Part_v=spark.table('Join_18_Colm_Part_v')
    
    Join_18_Remove_Dupe_Part_v=spark.table('Join_18_Remove_Dupe_Part_v')
    
    Join_18_Zect_Part_v=spark.table('Join_18_Zect_Part_v')
    
    print(Join_18_Colt_Part_v.schema)
    print(Join_18_Colm_Part_v.schema)
    print(Join_18_Remove_Dupe_Part_v.schema)
    print(Join_18_Zect_Part_v.schema)

    Join_18_v=Join_18_Colt_Part_v.join(Join_18_Colm_Part_v,['B_KEY'],'inner').join(Join_18_Remove_Dupe_Part_v,['B_KEY'],'inner').join(Join_18_Zect_Part_v,['B_KEY'],'inner')
    
    Join_18_DSLink22_v = Join_18_v.select(Join_18_Colt_Part_v["B_KEY"].cast('string').alias('B_KEY'),Join_18_Remove_Dupe_Part_v.MI006_ARRS_INT_1.cast('decimal(18,3)').alias('MI006_ARRS_INT_1'),Join_18_Remove_Dupe_Part_v.MI006_BRTH_EFF_DATE.cast('integer').alias('MI006_BRTH_EFF_DATE'),Join_18_Remove_Dupe_Part_v.MI006_REASON_CD.cast('integer').alias('MI006_REASON_CD'),Join_18_Remove_Dupe_Part_v.DUEC.cast('string').alias('DUEC'),Join_18_Remove_Dupe_Part_v.DUET.cast('string').alias('DUET'),Join_18_Remove_Dupe_Part_v.MI006_BORM_RM_CODE.cast('string').alias('MI006_BORM_RM_CODE'),Join_18_Remove_Dupe_Part_v.MI006_BORH_REBATE_PERC.cast('string').alias('MI006_BORH_REBATE_PERC'),Join_18_Remove_Dupe_Part_v.MI006_BORM_ARR_INT_INCR.cast('decimal(18,5)').alias('MI006_BORM_ARR_INT_INCR'),Join_18_Colt_Part_v["MI006_NO_OF_DISB_ON_NOTE"].cast('string').alias('MI006_NO_OF_DISB_ON_NOTE'),Join_18_Colm_Part_v.MI006_DESCRIPTION.cast('string').alias('MI006_DESCRIPTION'),Join_18_Colm_Part_v.MI006_CLASSIFIED_DATE.cast('integer').alias('MI006_CLASSIFIED_DATE'),Join_18_Colm_Part_v.MI006_GUA_CUSTO_NO.cast('string').alias('MI006_GUA_CUSTO_NO'),Join_18_Colm_Part_v.MI006_AMORT_FLAG.cast('string').alias('MI006_AMORT_FLAG'),Join_18_Colm_Part_v.MI006_ZECT_HANDLING_FEE.cast('decimal(18,5)').alias('MI006_ZECT_HANDLING_FEE'),Join_18_Zect_Part_v.MI006_ZECT_OTHER_FEE_sum.cast('decimal(18,5)').alias('MI006_ZECT_OTHER_FEE'),Join_18_Colm_Part_v.MI006_ZECT_EXP_HANDLING_AMT.cast('decimal(18,3)').alias('MI006_ZECT_EXP_HANDLING_AMT'),Join_18_Remove_Dupe_Part_v.MI006_NO_OF_DUES_BOAF.cast('integer').alias('MI006_NO_OF_DUES_BOAF'),Join_18_Remove_Dupe_Part_v.MI006_BILLED_AMT_UNPD_BOAF.cast('decimal(18,3)').alias('MI006_BILLED_AMT_UNPD_BOAF'),Join_18_Colm_Part_v.MI006_RIGH_NPL_COUNTER.cast('string').alias('MI006_RIGH_NPL_COUNTER'),Join_18_Colm_Part_v.MI006_RIGH_PL_DATE.cast('integer').alias('MI006_RIGH_PL_DATE'),Join_18_Colm_Part_v.MI006_ZECT_INPUTTAXNC1.cast('decimal(18,5)').alias('MI006_ZECT_INPUTTAXNC1'))
    
    Join_18_DSLink22_v = Join_18_DSLink22_v.selectExpr("B_KEY","MI006_ARRS_INT_1","MI006_BRTH_EFF_DATE","MI006_REASON_CD","DUEC","DUET","MI006_BORM_RM_CODE","RTRIM(MI006_BORH_REBATE_PERC) AS MI006_BORH_REBATE_PERC","MI006_BORM_ARR_INT_INCR","RTRIM(MI006_NO_OF_DISB_ON_NOTE) AS MI006_NO_OF_DISB_ON_NOTE","MI006_DESCRIPTION","MI006_CLASSIFIED_DATE","MI006_GUA_CUSTO_NO","RTRIM(MI006_AMORT_FLAG) AS MI006_AMORT_FLAG","MI006_ZECT_HANDLING_FEE","MI006_ZECT_OTHER_FEE","MI006_ZECT_EXP_HANDLING_AMT","MI006_NO_OF_DUES_BOAF","MI006_BILLED_AMT_UNPD_BOAF","MI006_RIGH_NPL_COUNTER","MI006_RIGH_PL_DATE","MI006_ZECT_INPUTTAXNC1").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ARRS_INT_1', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BRTH_EFF_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REASON_CD', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'DUEC', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'DUET', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_RM_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORH_REBATE_PERC', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_BORM_ARR_INT_INCR', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_OF_DISB_ON_NOTE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_DESCRIPTION', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CLASSIFIED_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GUA_CUSTO_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AMORT_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ZECT_HANDLING_FEE', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ZECT_OTHER_FEE', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ZECT_EXP_HANDLING_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_OF_DUES_BOAF', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BILLED_AMT_UNPD_BOAF', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RIGH_NPL_COUNTER', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RIGH_PL_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ZECT_INPUTTAXNC1', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS Join_18_DSLink22_v").show()
    
    print("Join_18_DSLink22_v")
    
    print(Join_18_DSLink22_v.schema.json())
    
    print("count:{}".format(Join_18_DSLink22_v.count()))
    
    Join_18_DSLink22_v.show(1000,False)
    
    Join_18_DSLink22_v.write.mode("overwrite").saveAsTable("Join_18_DSLink22_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_24_DSLink22_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Join_18_DSLink22_v=spark.table('Join_18_DSLink22_v')
    
    Transformer_24_DSLink22_Part_v=Join_18_DSLink22_v
    
    spark.sql("DROP TABLE IF EXISTS Transformer_24_DSLink22_Part_v").show()
    
    print("Transformer_24_DSLink22_Part_v")
    
    print(Transformer_24_DSLink22_Part_v.schema.json())
    
    print("count:{}".format(Transformer_24_DSLink22_Part_v.count()))
    
    Transformer_24_DSLink22_Part_v.show(1000,False)
    
    Transformer_24_DSLink22_Part_v.write.mode("overwrite").saveAsTable("Transformer_24_DSLink22_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_24(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_24_DSLink22_Part_v=spark.table('Transformer_24_DSLink22_Part_v')
    
    Transformer_24_v = Transformer_24_DSLink22_Part_v
    
    Transformer_24_lnk_CHPM_Tgt_v = Transformer_24_v.select(expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM B_KEY))""").cast('string').alias('B_KEY'),col('MI006_ARRS_INT_1').cast('decimal(18,3)').alias('MI006_ARRS_INT_1'),col('MI006_BRTH_EFF_DATE').cast('integer').alias('MI006_BRTH_EFF_DATE'),col('MI006_REASON_CD').cast('integer').alias('MI006_REASON_CD'),col('MI006_BORM_RM_CODE').cast('string').alias('MI006_BORM_RM_CODE'),col('MI006_BORH_REBATE_PERC').cast('string').alias('MI006_BORH_REBATE_PERC'),col('MI006_BORM_ARR_INT_INCR').cast('decimal(18,5)').alias('MI006_BORM_ARR_INT_INCR'),expr("""LPAD(TRIM(MI006_NO_OF_DISB_ON_NOTE), 3, '0')""").cast('string').alias('MI006_NO_OF_DISB_ON_NOTE'),col('MI006_DESCRIPTION').cast('string').alias('MI006_DESCRIPTION'),col('MI006_CLASSIFIED_DATE').cast('integer').alias('MI006_CLASSIFIED_DATE'),col('MI006_GUA_CUSTO_NO').cast('string').alias('MI006_GUA_CUSTO_NO'),lit(None).cast('string').alias('MI006_AMORT_FLAG'),col('MI006_ZECT_HANDLING_FEE').cast('decimal(18,5)').alias('MI006_ZECT_HANDLING_FEE'),col('MI006_ZECT_OTHER_FEE').cast('decimal(18,5)').alias('MI006_ZECT_OTHER_FEE'),col('MI006_ZECT_EXP_HANDLING_AMT').cast('decimal(18,3)').alias('MI006_ZECT_EXP_HANDLING_AMT'),col('MI006_CLASSIFIED_DATE').cast('integer').alias('MI006_NPL_CLASS_DATE'),col('MI006_CLASSIFIED_DATE').cast('integer').alias('MI006_AM_NPL_DATE'),expr("""LPAD(CAST(MI006_NO_OF_DUES_BOAF AS STRING), 3, '0')""").cast('string').alias('MI006_NO_OF_DUES_BOAF'),col('MI006_BILLED_AMT_UNPD_BOAF').cast('decimal(18,3)').alias('MI006_BILLED_AMT_UNPD_BOAF'),col('MI006_RIGH_NPL_COUNTER').cast('string').alias('MI006_RIGH_NPL_COUNTER'),col('MI006_RIGH_PL_DATE').cast('integer').alias('MI006_RIGH_PL_DATE'),col('MI006_ZECT_INPUTTAXNC1').cast('decimal(20,5)').alias('MI006_ZECT_INPUTTAXNC1'))
    
    Transformer_24_lnk_CHPM_Tgt_v = Transformer_24_lnk_CHPM_Tgt_v.selectExpr("B_KEY","MI006_ARRS_INT_1","MI006_BRTH_EFF_DATE","MI006_REASON_CD","MI006_BORM_RM_CODE","RTRIM(MI006_BORH_REBATE_PERC) AS MI006_BORH_REBATE_PERC","MI006_BORM_ARR_INT_INCR","RTRIM(MI006_NO_OF_DISB_ON_NOTE) AS MI006_NO_OF_DISB_ON_NOTE","MI006_DESCRIPTION","MI006_CLASSIFIED_DATE","MI006_GUA_CUSTO_NO","RTRIM(MI006_AMORT_FLAG) AS MI006_AMORT_FLAG","MI006_ZECT_HANDLING_FEE","MI006_ZECT_OTHER_FEE","MI006_ZECT_EXP_HANDLING_AMT","MI006_NPL_CLASS_DATE","MI006_AM_NPL_DATE","RTRIM(MI006_NO_OF_DUES_BOAF) AS MI006_NO_OF_DUES_BOAF","MI006_BILLED_AMT_UNPD_BOAF","MI006_RIGH_NPL_COUNTER","MI006_RIGH_PL_DATE","MI006_ZECT_INPUTTAXNC1").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ARRS_INT_1', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BRTH_EFF_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REASON_CD', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_RM_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORH_REBATE_PERC', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_BORM_ARR_INT_INCR', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_OF_DISB_ON_NOTE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_DESCRIPTION', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CLASSIFIED_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GUA_CUSTO_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AMORT_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ZECT_HANDLING_FEE', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ZECT_OTHER_FEE', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ZECT_EXP_HANDLING_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NPL_CLASS_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AM_NPL_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_OF_DUES_BOAF', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_BILLED_AMT_UNPD_BOAF', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RIGH_NPL_COUNTER', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RIGH_PL_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ZECT_INPUTTAXNC1', 'type': 'decimal(20,5)', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS Transformer_24_lnk_CHPM_Tgt_v").show()
    
    print("Transformer_24_lnk_CHPM_Tgt_v")
    
    print(Transformer_24_lnk_CHPM_Tgt_v.schema.json())
    
    print("count:{}".format(Transformer_24_lnk_CHPM_Tgt_v.count()))
    
    Transformer_24_lnk_CHPM_Tgt_v.show(1000,False)
    
    Transformer_24_lnk_CHPM_Tgt_v.write.mode("overwrite").saveAsTable("Transformer_24_lnk_CHPM_Tgt_v")
    

@task.pyspark(conn_id="spark-local")
def DS_TGT_CHPM_COLT_lnk_CHPM_Tgt_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_24_lnk_CHPM_Tgt_v=spark.table('Transformer_24_lnk_CHPM_Tgt_v')
    
    DS_TGT_CHPM_COLT_lnk_CHPM_Tgt_Part_v=Transformer_24_lnk_CHPM_Tgt_v
    
    spark.sql("DROP TABLE IF EXISTS DS_TGT_CHPM_COLT_lnk_CHPM_Tgt_Part_v").show()
    
    print("DS_TGT_CHPM_COLT_lnk_CHPM_Tgt_Part_v")
    
    print(DS_TGT_CHPM_COLT_lnk_CHPM_Tgt_Part_v.schema.json())
    
    print("count:{}".format(DS_TGT_CHPM_COLT_lnk_CHPM_Tgt_Part_v.count()))
    
    DS_TGT_CHPM_COLT_lnk_CHPM_Tgt_Part_v.show(1000,False)
    
    DS_TGT_CHPM_COLT_lnk_CHPM_Tgt_Part_v.write.mode("overwrite").saveAsTable("DS_TGT_CHPM_COLT_lnk_CHPM_Tgt_Part_v")
    

@task.pyspark(conn_id="spark-local")
def DS_TGT_CHPM_COLT(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    DS_TGT_CHPM_COLT_lnk_CHPM_Tgt_Part_v=spark.table('DS_TGT_CHPM_COLT_lnk_CHPM_Tgt_Part_v')
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    locations = Template('{{dbdir.pPROCESSING_DIR}}MIS006_CHPM.ds').render(job_params)
    
    log.info("write dataset files to "+locations)
    
    spark.table("DS_TGT_CHPM_COLT_lnk_CHPM_Tgt_Part_v").write.mode("overwrite").format("parquet").save(locations)
    

####################################[Main]###################################
import airflow
with DAG(
    dag_id="job_DBdirect_MIS006_CHPM_Extr_POC",
    start_date=airflow.utils.dates.days_ago(1),
    tags=['datastage'],
) as dag:
    
    job_DBdirect_MIS006_CHPM_Extr_POC_task = job_DBdirect_MIS006_CHPM_Extr_POC()
    
    Job_VIEW_task = Job_VIEW()
    
    NETZ_SRC_BORM_BOAF_task = NETZ_SRC_BORM_BOAF()
    
    V0A13_task = V0A13()
    
    NETZ_SRC_COLM_CHPM_task = NETZ_SRC_COLM_CHPM()
    
    NETZ_SRC_BLDVAA_task = NETZ_SRC_BLDVAA()
    
    V0A26_task = V0A26()
    
    V4A0_task = V4A0()
    
    V5A0_task = V5A0()
    
    V88A0_task = V88A0()
    
    NETZ_SRC_Zect_task = NETZ_SRC_Zect()
    
    Transformer_J_lnk_Source_Part_task = Transformer_J_lnk_Source_Part()
    
    Join_18_Colm_Part_task = Join_18_Colm_Part()
    
    Join_18_Colt_Part_task = Join_18_Colt_Part()
    
    Join_18_Zect_Part_task = Join_18_Zect_Part()
    
    Transformer_J_task = Transformer_J()
    
    Join_18_Remove_Dupe_Part_task = Join_18_Remove_Dupe_Part()
    
    Join_18_task = Join_18()
    
    Transformer_24_DSLink22_Part_task = Transformer_24_DSLink22_Part()
    
    Transformer_24_task = Transformer_24()
    
    DS_TGT_CHPM_COLT_lnk_CHPM_Tgt_Part_task = DS_TGT_CHPM_COLT_lnk_CHPM_Tgt_Part()
    
    DS_TGT_CHPM_COLT_task = DS_TGT_CHPM_COLT()
    
    
    job_DBdirect_MIS006_CHPM_Extr_POC_task >> Job_VIEW_task
    
    Job_VIEW_task >> NETZ_SRC_BORM_BOAF_task
    
    Job_VIEW_task >> V0A13_task
    
    Job_VIEW_task >> NETZ_SRC_COLM_CHPM_task
    
    Job_VIEW_task >> NETZ_SRC_BLDVAA_task
    
    Job_VIEW_task >> V0A26_task
    
    Job_VIEW_task >> V4A0_task
    
    Job_VIEW_task >> V5A0_task
    
    Job_VIEW_task >> V88A0_task
    
    Job_VIEW_task >> NETZ_SRC_Zect_task
    
    NETZ_SRC_BORM_BOAF_task >> Transformer_J_lnk_Source_Part_task
    
    NETZ_SRC_COLM_CHPM_task >> Join_18_Colm_Part_task
    
    NETZ_SRC_BLDVAA_task >> Join_18_Colt_Part_task
    
    NETZ_SRC_Zect_task >> Join_18_Zect_Part_task
    
    Transformer_J_lnk_Source_Part_task >> Transformer_J_task
    
    Join_18_Colm_Part_task >> Join_18_task
    
    Join_18_Colt_Part_task >> Join_18_task
    
    Join_18_Zect_Part_task >> Join_18_task
    
    Transformer_J_task >> Join_18_Remove_Dupe_Part_task
    
    Join_18_Remove_Dupe_Part_task >> Join_18_task
    
    Join_18_task >> Transformer_24_DSLink22_Part_task
    
    Transformer_24_DSLink22_Part_task >> Transformer_24_task
    
    Transformer_24_task >> DS_TGT_CHPM_COLT_lnk_CHPM_Tgt_Part_task
    
    DS_TGT_CHPM_COLT_lnk_CHPM_Tgt_Part_task >> DS_TGT_CHPM_COLT_task
    



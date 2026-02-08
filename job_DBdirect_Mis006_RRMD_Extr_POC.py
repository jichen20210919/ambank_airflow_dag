
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-08 14:40:00
# @Author  : cloudera
# @File    : job_DBdirect_Mis006_RRMD_Extr_POC.py
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
def job_DBdirect_Mis006_RRMD_Extr_POC(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def Job_VIEW(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task.pyspark(conn_id="spark-local")
def NETZ_SRC_TBL_NM(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    catalog=Variable.get("ICEBERG_CATALOG_NAME", default_var="iceberg")
    
    #spark.sql(f"use {catalog}.default").show()
    
    
    
    
    
    sql=Template("""SELECT 
    
        BORM_KEY_1,
    
        MI006_MEMB_CUST_AC,
    
        rec_no,
    
        CNT_CAT_1,
    
        CNT_CAT_NOT_1,
    
        MaX_RR_DATE_CAT_1,
    
        MaX_RR_DATE_CAT_2,
    
        ACCT_NO,
    
        SEQ_NO,
    
        RR_TYPE,
    
        RR_REASON,
    
        RR_CAT,
    
        RR_SUB_CAT,
    
        RR_COUNT,
    
        MP_EXP_DATE,
    
        RR_DATE,
    
        -- Changes as per AHCR 1538 - New BNM Requirement on Hardship Moratorium Scheme
    
        CAST(VIRTUAL_MIA AS VARCHAR(3)) AS VIRTUAL_MIA,
    
        VIRTUAL_DPD,
    
        VIRTUAL_PRIN_ARR,
    
        VIRTUAL_INT_ARR,
    
        MI006_RNR_REPORTING_FLAG,
    
        -- ODS SPEC v9.9 START
    
        MI006_PROMPT_PAY_CNT,
    
        MI006_ORI_RR_DATE,
    
        -- ODS SPEC v9.9 END
    
        REHABILITAION_FLAG,     -- ODS SPEC v15.9
    
        RR_PROMPT_PAY_CNT,      -- ODS SPEC v15.9
    
        RNR_AMORT_MONTH,        -- ODS SPEC v16.6
    
        RNR_ARR_AMORT_AMT,      -- ODS SPEC v16.6
    
        NPL_MP_EXP_ST           -- ODS SPEC v20.0
    
    FROM (
    
        SELECT
    
            BORM.KEY_1 AS BORM_KEY_1,
    
            SUBSTRING(BORM.KEY_1, 4, 16) AS MI006_MEMB_CUST_AC,
    
            CAST(ROW_NUMBER() OVER (PARTITION BY BORM.KEY_1 ORDER BY RRMD.SEQ_NO DESC) AS INT) AS rec_no,
    
            CASE WHEN RRMD.RR_CAT = 1 THEN 1 ELSE 0 END AS C_CAT_1,
    
            CASE WHEN RRMD.RR_CAT <> 1 THEN 1 ELSE 0 END AS C_CAT_NOT_1,
    
            CASE WHEN RRMD.RR_CAT = 1 THEN RRMD.RR_DATE ELSE 0 END AS RR_DATE_CAT_1,
    
            CASE WHEN RRMD.RR_CAT = 2 THEN RRMD.RR_DATE ELSE 0 END AS RR_DATE_CAT_2,
    
            CAST(SUM(CASE WHEN RRMD.RR_CAT <> 1 THEN 1 ELSE 0 END) OVER (PARTITION BY BORM.KEY_1) AS INT) AS CNT_CAT_NOT_1,
    
            CAST(SUM(CASE WHEN RRMD.RR_CAT = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY BORM.KEY_1) AS INT) AS CNT_CAT_1,
    
            CAST(MAX(CASE WHEN RRMD.RR_CAT = 1 THEN RRMD.RR_DATE ELSE 0 END) OVER (PARTITION BY BORM.KEY_1) AS INT) AS MaX_RR_DATE_CAT_1,
    
            CAST(MAX(CASE WHEN RRMD.RR_CAT = 2 THEN RRMD.RR_DATE ELSE 0 END) OVER (PARTITION BY BORM.KEY_1) AS INT) AS MaX_RR_DATE_CAT_2,
    
            RRMD.ACCT_NO,
    
            RRMD.SEQ_NO,
    
            RRMD.RR_TYPE,
    
            COALESCE(CAST(RRMD.RR_CAT AS STRING), ' ') AS RR_CAT,
    
            RRMD.RR_REASON,
    
            RRMD.RR_DATE,
    
            RRMD.RR_COUNT,
    
            RRMD.RR_SUB_CAT,
    
            RRMD.MP_EXP_DATE,
    
            -- Changes as per AHCR 1538 - New BNM Requirement on Hardship Moratorium Scheme
    
            RRMD.VIRTUAL_MIA,
    
            RRMD.VIRTUAL_DPD,
    
            RRMD.VIRTUAL_PRIN_ARR,
    
            RRMD.VIRTUAL_INT_ARR,
    
            CASE 
    
                WHEN RRMD.INST_NO IS NOT NULL AND RRMD.ACCT_NO IS NOT NULL THEN 
    
                    CASE 
    
                        WHEN RRMD.MP_EXP_DATE <= (DATEDIFF(TO_DATE('{{dbdir.pBUSINESS_DATE}}', 'yyyyMMdd'), TO_DATE('1899-12-31', 'yyyy-MM-dd'))) 
    
                             AND (RRMD.MP_EXP_DATE > 0) THEN 'N'
    
                        ELSE 'Y'
    
                    END
    
                ELSE ' '
    
            END AS MI006_RNR_REPORTING_FLAG,
    
            -- ODS SPEC v9.9 START
    
            RRMD.PROMPT_PAYMENT_CNT AS MI006_PROMPT_PAY_CNT,
    
            B.RR_DATE AS MI006_ORI_RR_DATE,
    
            -- ODS SPEC v9.9 END
    
            RRMD.REHABILITAION_FLAG AS REHABILITAION_FLAG,   -- ODS SPEC v15.9
    
            RRMD.RR_PROMPT_PAY_CNT AS RR_PROMPT_PAY_CNT,    -- ODS SPEC v15.9
    
            RRMD.RNR_AMORT_MONTH,      -- ODS SPEC v16.6
    
            RRMD.RNR_ARR_AMORT_AMT,    -- ODS SPEC v16.6
    
            CAST(RRMD.NPL_MP_EXP_ST AS STRING) AS NPL_MP_EXP_ST        -- ODS SPEC v20.0
    
        FROM {{dbdir.pODS_SCHM}}.BORM BORM
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.RRMD RRMD 
    
            ON RRMD.INST_NO = SUBSTRING(BORM.KEY_1, 1, 3) 
    
            AND RRMD.ACCT_NO = SUBSTRING(BORM.KEY_1, 4, 16)
    
        -- ODS SPEC v9.9 START
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.RRMD B 
    
            ON B.INST_NO = SUBSTRING(BORM.KEY_1, 1, 3) 
    
            AND B.ACCT_NO = SUBSTRING(BORM.KEY_1, 4, 16)
    
            AND B.SEQ_NO = '000001'
    
        -- ODS SPEC v9.9 END
    
    ) A 
    
    WHERE rec_no = 1""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    NETZ_SRC_TBL_NM_v = spark.sql(sql)
    
    
    
    
    
    #spark.sql(f"use spark_catalog.default").show()
    
    NETZ_SRC_TBL_NM_lnk_Source__v=NETZ_SRC_TBL_NM_v.select(NETZ_SRC_TBL_NM_v[0].cast('string').alias('BORM_KEY_1'),NETZ_SRC_TBL_NM_v[1].cast('string').alias('MI006_MEMB_CUST_AC'),NETZ_SRC_TBL_NM_v[2].cast('integer').alias('rec_no'),NETZ_SRC_TBL_NM_v[3].cast('integer').alias('CNT_CAT_1'),NETZ_SRC_TBL_NM_v[4].cast('integer').alias('CNT_CAT_NOT_1'),NETZ_SRC_TBL_NM_v[5].cast('integer').alias('MAX_RR_DATE_CAT_1'),NETZ_SRC_TBL_NM_v[6].cast('integer').alias('MAX_RR_DATE_CAT_2'),NETZ_SRC_TBL_NM_v[7].cast('string').alias('ACCT_NO'),NETZ_SRC_TBL_NM_v[8].cast('string').alias('SEQ_NO'),NETZ_SRC_TBL_NM_v[9].cast('string').alias('RR_TYPE'),NETZ_SRC_TBL_NM_v[10].cast('string').alias('RR_REASON'),NETZ_SRC_TBL_NM_v[11].cast('string').alias('RR_CAT'),NETZ_SRC_TBL_NM_v[12].cast('string').alias('RR_SUB_CAT'),NETZ_SRC_TBL_NM_v[13].cast('string').alias('RR_COUNT'),NETZ_SRC_TBL_NM_v[14].cast('integer').alias('MP_EXP_DATE'),NETZ_SRC_TBL_NM_v[15].cast('integer').alias('RR_DATE'),NETZ_SRC_TBL_NM_v[16].cast('string').alias('VIRTUAL_MIA'),NETZ_SRC_TBL_NM_v[17].cast('decimal(5,0)').alias('VIRTUAL_DPD'),NETZ_SRC_TBL_NM_v[18].cast('decimal(18,3)').alias('VIRTUAL_PRIN_ARR'),NETZ_SRC_TBL_NM_v[19].cast('decimal(18,3)').alias('VIRTUAL_INT_ARR'),NETZ_SRC_TBL_NM_v[20].cast('string').alias('MI006_RNR_REPORTING_FLAG'),NETZ_SRC_TBL_NM_v[21].cast('string').alias('MI006_PROMPT_PAY_CNT'),NETZ_SRC_TBL_NM_v[22].cast('integer').alias('MI006_ORI_RR_DATE'),NETZ_SRC_TBL_NM_v[23].cast('string').alias('REHABILITAION_FLAG'),NETZ_SRC_TBL_NM_v[24].cast('decimal(2,0)').alias('RR_PROMPT_PAY_CNT'),NETZ_SRC_TBL_NM_v[25].cast('decimal(3,0)').alias('RNR_AMORT_MONTH'),NETZ_SRC_TBL_NM_v[26].cast('decimal(17,3)').alias('RNR_ARR_AMORT_AMT'),NETZ_SRC_TBL_NM_v[27].cast('string').alias('NPL_MP_EXP_ST'))
    
    NETZ_SRC_TBL_NM_lnk_Source__v = NETZ_SRC_TBL_NM_lnk_Source__v.selectExpr("RTRIM(BORM_KEY_1) AS BORM_KEY_1","RTRIM(MI006_MEMB_CUST_AC) AS MI006_MEMB_CUST_AC","rec_no","CNT_CAT_1","CNT_CAT_NOT_1","MAX_RR_DATE_CAT_1","MAX_RR_DATE_CAT_2","RTRIM(ACCT_NO) AS ACCT_NO","RTRIM(SEQ_NO) AS SEQ_NO","RTRIM(RR_TYPE) AS RR_TYPE","RTRIM(RR_REASON) AS RR_REASON","RTRIM(RR_CAT) AS RR_CAT","RTRIM(RR_SUB_CAT) AS RR_SUB_CAT","RTRIM(RR_COUNT) AS RR_COUNT","MP_EXP_DATE","RR_DATE","VIRTUAL_MIA","VIRTUAL_DPD","VIRTUAL_PRIN_ARR","VIRTUAL_INT_ARR","MI006_RNR_REPORTING_FLAG","RTRIM(MI006_PROMPT_PAY_CNT) AS MI006_PROMPT_PAY_CNT","MI006_ORI_RR_DATE","RTRIM(REHABILITAION_FLAG) AS REHABILITAION_FLAG","RR_PROMPT_PAY_CNT","RNR_AMORT_MONTH","RNR_ARR_AMORT_AMT","RTRIM(NPL_MP_EXP_ST) AS NPL_MP_EXP_ST").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'BORM_KEY_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(19)'}}, {'name': 'MI006_MEMB_CUST_AC', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(16)'}}, {'name': 'rec_no', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'CNT_CAT_1', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'CNT_CAT_NOT_1', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MAX_RR_DATE_CAT_1', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MAX_RR_DATE_CAT_2', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(16)'}}, {'name': 'SEQ_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(6)'}}, {'name': 'RR_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'RR_REASON', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'RR_CAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'RR_SUB_CAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'RR_COUNT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MP_EXP_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'RR_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'VIRTUAL_MIA', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'VIRTUAL_DPD', 'type': 'decimal(5,0)', 'nullable': True, 'metadata': {}}, {'name': 'VIRTUAL_PRIN_ARR', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'VIRTUAL_INT_ARR', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RNR_REPORTING_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PROMPT_PAY_CNT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_ORI_RR_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REHABILITAION_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'RR_PROMPT_PAY_CNT', 'type': 'decimal(2,0)', 'nullable': True, 'metadata': {}}, {'name': 'RNR_AMORT_MONTH', 'type': 'decimal(3,0)', 'nullable': True, 'metadata': {}}, {'name': 'RNR_ARR_AMORT_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'NPL_MP_EXP_ST', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_RRMD_Extr_POC__NETZ_SRC_TBL_NM_lnk_Source__v PURGE").show()
    
    print("NETZ_SRC_TBL_NM_lnk_Source__v")
    
    print(NETZ_SRC_TBL_NM_lnk_Source__v.schema.json())
    
    print("count:{}".format(NETZ_SRC_TBL_NM_lnk_Source__v.count()))
    
    NETZ_SRC_TBL_NM_lnk_Source__v.show(1000,False)
    
    NETZ_SRC_TBL_NM_lnk_Source__v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_RRMD_Extr_POC__NETZ_SRC_TBL_NM_lnk_Source__v")
    

@task
def V0A58(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task.pyspark(conn_id="spark-local")
def Sort_56_lnk_Source__Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    NETZ_SRC_TBL_NM_lnk_Source__v=spark.table('datastage_temp_job_DBdirect_Mis006_RRMD_Extr_POC__NETZ_SRC_TBL_NM_lnk_Source__v')
    
    Sort_56_lnk_Source__Part_v=NETZ_SRC_TBL_NM_lnk_Source__v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_RRMD_Extr_POC__Sort_56_lnk_Source__Part_v PURGE").show()
    
    print("Sort_56_lnk_Source__Part_v")
    
    print(Sort_56_lnk_Source__Part_v.schema.json())
    
    print("count:{}".format(Sort_56_lnk_Source__Part_v.count()))
    
    Sort_56_lnk_Source__Part_v.show(1000,False)
    
    Sort_56_lnk_Source__Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_RRMD_Extr_POC__Sort_56_lnk_Source__Part_v")
    

@task.pyspark(conn_id="spark-local")
def Sort_56(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Sort_56_lnk_Source__Part_v=spark.table('datastage_temp_job_DBdirect_Mis006_RRMD_Extr_POC__Sort_56_lnk_Source__Part_v')
    
    Sort_56_v = Sort_56_lnk_Source__Part_v
    
    Sort_56_lnk_Source_v_0 = Sort_56_v.orderBy(col('BORM_KEY_1').asc())
    
    Sort_56_lnk_Source_v = Sort_56_lnk_Source_v_0.select(col('BORM_KEY_1').cast('string').alias('BORM_KEY_1'),col('MI006_MEMB_CUST_AC').cast('string').alias('MI006_MEMB_CUST_AC'),col('rec_no').cast('integer').alias('rec_no'),col('CNT_CAT_1').cast('integer').alias('CNT_CAT_1'),col('CNT_CAT_NOT_1').cast('integer').alias('CNT_CAT_NOT_1'),col('MAX_RR_DATE_CAT_1').cast('integer').alias('MAX_RR_DATE_CAT_1'),col('MAX_RR_DATE_CAT_2').cast('integer').alias('MAX_RR_DATE_CAT_2'),col('ACCT_NO').cast('string').alias('ACCT_NO'),col('SEQ_NO').cast('string').alias('SEQ_NO'),col('RR_TYPE').cast('string').alias('RR_TYPE'),col('RR_REASON').cast('string').alias('RR_REASON'),col('RR_CAT').cast('string').alias('RR_CAT'),col('RR_SUB_CAT').cast('string').alias('RR_SUB_CAT'),col('RR_COUNT').cast('string').alias('RR_COUNT'),col('MP_EXP_DATE').cast('integer').alias('MP_EXP_DATE'),col('RR_DATE').cast('integer').alias('RR_DATE'),col('VIRTUAL_MIA').cast('string').alias('VIRTUAL_MIA'),col('VIRTUAL_DPD').cast('decimal(5,0)').alias('VIRTUAL_DPD'),col('VIRTUAL_PRIN_ARR').cast('decimal(18,3)').alias('VIRTUAL_PRIN_ARR'),col('VIRTUAL_INT_ARR').cast('decimal(18,3)').alias('VIRTUAL_INT_ARR'),col('MI006_RNR_REPORTING_FLAG').cast('string').alias('MI006_RNR_REPORTING_FLAG'),col('MI006_PROMPT_PAY_CNT').cast('string').alias('MI006_PROMPT_PAY_CNT'),col('MI006_ORI_RR_DATE').cast('integer').alias('MI006_ORI_RR_DATE'),col('REHABILITAION_FLAG').cast('string').alias('REHABILITAION_FLAG'),col('RR_PROMPT_PAY_CNT').cast('decimal(2,0)').alias('RR_PROMPT_PAY_CNT'),col('RNR_AMORT_MONTH').cast('decimal(3,0)').alias('RNR_AMORT_MONTH'),col('RNR_ARR_AMORT_AMT').cast('decimal(17,3)').alias('RNR_ARR_AMORT_AMT'),col('NPL_MP_EXP_ST').cast('string').alias('NPL_MP_EXP_ST'))
    
    Sort_56_lnk_Source_v = Sort_56_lnk_Source_v.selectExpr("RTRIM(BORM_KEY_1) AS BORM_KEY_1","RTRIM(MI006_MEMB_CUST_AC) AS MI006_MEMB_CUST_AC","rec_no","CNT_CAT_1","CNT_CAT_NOT_1","MAX_RR_DATE_CAT_1","MAX_RR_DATE_CAT_2","RTRIM(ACCT_NO) AS ACCT_NO","RTRIM(SEQ_NO) AS SEQ_NO","RTRIM(RR_TYPE) AS RR_TYPE","RTRIM(RR_REASON) AS RR_REASON","RTRIM(RR_CAT) AS RR_CAT","RTRIM(RR_SUB_CAT) AS RR_SUB_CAT","RTRIM(RR_COUNT) AS RR_COUNT","MP_EXP_DATE","RR_DATE","VIRTUAL_MIA","VIRTUAL_DPD","VIRTUAL_PRIN_ARR","VIRTUAL_INT_ARR","MI006_RNR_REPORTING_FLAG","RTRIM(MI006_PROMPT_PAY_CNT) AS MI006_PROMPT_PAY_CNT","MI006_ORI_RR_DATE","RTRIM(REHABILITAION_FLAG) AS REHABILITAION_FLAG","RR_PROMPT_PAY_CNT","RNR_AMORT_MONTH","RNR_ARR_AMORT_AMT","RTRIM(NPL_MP_EXP_ST) AS NPL_MP_EXP_ST").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'BORM_KEY_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(19)'}}, {'name': 'MI006_MEMB_CUST_AC', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(16)'}}, {'name': 'rec_no', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'CNT_CAT_1', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'CNT_CAT_NOT_1', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MAX_RR_DATE_CAT_1', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MAX_RR_DATE_CAT_2', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(16)'}}, {'name': 'SEQ_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(6)'}}, {'name': 'RR_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'RR_REASON', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'RR_CAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'RR_SUB_CAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'RR_COUNT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MP_EXP_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'RR_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'VIRTUAL_MIA', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'VIRTUAL_DPD', 'type': 'decimal(5,0)', 'nullable': True, 'metadata': {}}, {'name': 'VIRTUAL_PRIN_ARR', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'VIRTUAL_INT_ARR', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RNR_REPORTING_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PROMPT_PAY_CNT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_ORI_RR_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REHABILITAION_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'RR_PROMPT_PAY_CNT', 'type': 'decimal(2,0)', 'nullable': True, 'metadata': {}}, {'name': 'RNR_AMORT_MONTH', 'type': 'decimal(3,0)', 'nullable': True, 'metadata': {}}, {'name': 'RNR_ARR_AMORT_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'NPL_MP_EXP_ST', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_RRMD_Extr_POC__Sort_56_lnk_Source_v PURGE").show()
    
    print("Sort_56_lnk_Source_v")
    
    print(Sort_56_lnk_Source_v.schema.json())
    
    print("count:{}".format(Sort_56_lnk_Source_v.count()))
    
    Sort_56_lnk_Source_v.show(1000,False)
    
    Sort_56_lnk_Source_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_RRMD_Extr_POC__Sort_56_lnk_Source_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_52_lnk_Source_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Sort_56_lnk_Source_v=spark.table('datastage_temp_job_DBdirect_Mis006_RRMD_Extr_POC__Sort_56_lnk_Source_v')
    
    Transformer_52_lnk_Source_Part_v=Sort_56_lnk_Source_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_RRMD_Extr_POC__Transformer_52_lnk_Source_Part_v PURGE").show()
    
    print("Transformer_52_lnk_Source_Part_v")
    
    print(Transformer_52_lnk_Source_Part_v.schema.json())
    
    print("count:{}".format(Transformer_52_lnk_Source_Part_v.count()))
    
    Transformer_52_lnk_Source_Part_v.show(1000,False)
    
    Transformer_52_lnk_Source_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_RRMD_Extr_POC__Transformer_52_lnk_Source_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_52(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_52_lnk_Source_Part_v=spark.table('datastage_temp_job_DBdirect_Mis006_RRMD_Extr_POC__Transformer_52_lnk_Source_Part_v')
    
    Transformer_52_v = Transformer_52_lnk_Source_Part_v
    
    Transformer_52_Lnk_RRMD_Tgt_v = Transformer_52_v.select(expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM BORM_KEY_1))""").cast('string').alias('B_KEY'),col('ACCT_NO').cast('string').alias('MI006_MEMB_CUST_AC'),expr("""IF(ISNOTNULL(ACCT_NO), (IF(RR_TYPE = 1, 'Y', 'N')), NULL)""").cast('string').alias('MI006_DISTRESS_STATUS'),expr("""CONCAT_WS('', RPAD('', 5 - LENGTH(CNT_CAT_1), '0'), CNT_CAT_1)""").cast('string').alias('MI006_NO_OF_RESCHEDULE'),expr("""CONCAT_WS('', RPAD('', 5 - LENGTH(CNT_CAT_NOT_1), '0'), CNT_CAT_NOT_1)""").cast('string').alias('MI006_NO_OF_RESTRUCTURE'),col('RR_DATE').cast('integer').alias('MI006_NPL_BFR_RR_DATE'),col('MP_EXP_DATE').cast('integer').alias('MI006_RE_AGED_DATE'),expr("""IF(RR_CAT = 1 AND ISNOTNULL(ACCT_NO), 'Y', REPEAT(' ', 1))""").cast('string').alias('MI006_RESCHEDULE'),col('MAX_RR_DATE_CAT_1').cast('integer').alias('MI006_RESCHUDLE_DATE'),expr("""IF(RR_CAT = 1 AND ISNOTNULL(ACCT_NO), 'Y', REPEAT(' ', 1))""").cast('string').alias('MI006_RESCHUDLE_FLAG'),expr("""IF(RR_CAT <> 1 AND ISNOTNULL(ACCT_NO), 'Y', REPEAT(' ', 1))""").cast('string').alias('MI006_RESTRUCTURE'),expr("""IF(RR_CAT = '2', RR_DATE, 0)""").cast('integer').alias('MI006_RESTRUCTURE_DATE'),expr("""IF(RR_CAT <> 1 AND ISNOTNULL(ACCT_NO), 'Y', REPEAT(' ', 1))""").cast('string').alias('MI006_RESTRUCTURE_FLAG'),col('RR_REASON').cast('string').alias('MI006_RR_REASON'),expr("""IF(RR_CAT = 1 AND ISNOTNULL(ACCT_NO), RR_REASON, REPEAT(' ', 1))""").cast('string').alias('MI006_RESCHEDULE_CODE'),expr("""IF(RR_CAT = 1 AND ISNOTNULL(ACCT_NO), RR_DATE, 0)""").cast('integer').alias('MI006_RESCHEDULE_DATE_TAG'),expr("""IF(ISNOTNULL(ACCT_NO) AND rec_no = 1, RR_TYPE, REPEAT(' ', 1))""").cast('string').alias('MI006_RR_TYPE'),expr("""IF(ISNOTNULL(ACCT_NO) AND rec_no = 1, RR_CAT, REPEAT(' ', 1))""").cast('string').alias('MI006_RR_CAT'),expr("""IF(ISNOTNULL(ACCT_NO) AND rec_no = 1, RR_COUNT, REPEAT(' ', 1))""").cast('integer').alias('MI006_RR_COUNT'),expr("""IF(ISNOTNULL(ACCT_NO) AND rec_no = 1, RR_SUB_CAT, REPEAT(' ', 1))""").cast('string').alias('MI006_RR_SUB_CAT'),col('VIRTUAL_MIA').cast('string').alias('MI006_RRMD_V_MIA'),col('VIRTUAL_DPD').cast('decimal(5,0)').alias('MI006_RRMD_V_DPD'),col('VIRTUAL_PRIN_ARR').cast('decimal(18,3)').alias('MI006_RRMD_V_PRIN_ARR'),col('VIRTUAL_INT_ARR').cast('decimal(18,3)').alias('MI006_RRMD_INT_ARR'),col('MI006_RNR_REPORTING_FLAG').cast('string').alias('MI006_RNR_REPORTING_FLAG'),col('MI006_PROMPT_PAY_CNT').cast('string').alias('MI006_PROMPT_PAY_CNT'),col('MI006_ORI_RR_DATE').cast('integer').alias('MI006_ORI_RR_DATE'),col('REHABILITAION_FLAG').cast('string').alias('MI006_RNR_PL_REHAB_FLAG'),col('RR_PROMPT_PAY_CNT').cast('integer').alias('MI006_RNR_PL_PROMPT_PAY_CNT'),col('RNR_AMORT_MONTH').cast('decimal(3,0)').alias('MI006_RNR_AMORT_MONTH'),col('RNR_ARR_AMORT_AMT').cast('decimal(17,3)').alias('MI006_RNR_ARR_AMORT_AMT'),col('NPL_MP_EXP_ST').cast('string').alias('MI006_NPL_MP_EXP_ST'))
    
    Transformer_52_Lnk_RRMD_Tgt_v = Transformer_52_Lnk_RRMD_Tgt_v.selectExpr("B_KEY","RTRIM(MI006_MEMB_CUST_AC) AS MI006_MEMB_CUST_AC","RTRIM(MI006_DISTRESS_STATUS) AS MI006_DISTRESS_STATUS","RTRIM(MI006_NO_OF_RESCHEDULE) AS MI006_NO_OF_RESCHEDULE","RTRIM(MI006_NO_OF_RESTRUCTURE) AS MI006_NO_OF_RESTRUCTURE","MI006_NPL_BFR_RR_DATE","MI006_RE_AGED_DATE","RTRIM(MI006_RESCHEDULE) AS MI006_RESCHEDULE","MI006_RESCHUDLE_DATE","RTRIM(MI006_RESCHUDLE_FLAG) AS MI006_RESCHUDLE_FLAG","RTRIM(MI006_RESTRUCTURE) AS MI006_RESTRUCTURE","MI006_RESTRUCTURE_DATE","RTRIM(MI006_RESTRUCTURE_FLAG) AS MI006_RESTRUCTURE_FLAG","RTRIM(MI006_RR_REASON) AS MI006_RR_REASON","RTRIM(MI006_RESCHEDULE_CODE) AS MI006_RESCHEDULE_CODE","MI006_RESCHEDULE_DATE_TAG","RTRIM(MI006_RR_TYPE) AS MI006_RR_TYPE","RTRIM(MI006_RR_CAT) AS MI006_RR_CAT","MI006_RR_COUNT","RTRIM(MI006_RR_SUB_CAT) AS MI006_RR_SUB_CAT","MI006_RRMD_V_MIA","MI006_RRMD_V_DPD","MI006_RRMD_V_PRIN_ARR","MI006_RRMD_INT_ARR","MI006_RNR_REPORTING_FLAG","RTRIM(MI006_PROMPT_PAY_CNT) AS MI006_PROMPT_PAY_CNT","MI006_ORI_RR_DATE","MI006_RNR_PL_REHAB_FLAG","MI006_RNR_PL_PROMPT_PAY_CNT","MI006_RNR_AMORT_MONTH","MI006_RNR_ARR_AMORT_AMT","MI006_NPL_MP_EXP_ST").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MEMB_CUST_AC', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(16)'}}, {'name': 'MI006_DISTRESS_STATUS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NO_OF_RESCHEDULE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_NO_OF_RESTRUCTURE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_NPL_BFR_RR_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RE_AGED_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RESCHEDULE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_RESCHUDLE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RESCHUDLE_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_RESTRUCTURE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_RESTRUCTURE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RESTRUCTURE_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_RR_REASON', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_RESCHEDULE_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_RESCHEDULE_DATE_TAG', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RR_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_RR_CAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_RR_COUNT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RR_SUB_CAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_RRMD_V_MIA', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RRMD_V_DPD', 'type': 'decimal(5,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RRMD_V_PRIN_ARR', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RRMD_INT_ARR', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RNR_REPORTING_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PROMPT_PAY_CNT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_ORI_RR_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RNR_PL_REHAB_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RNR_PL_PROMPT_PAY_CNT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RNR_AMORT_MONTH', 'type': 'decimal(3,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RNR_ARR_AMORT_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NPL_MP_EXP_ST', 'type': 'string', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_RRMD_Extr_POC__Transformer_52_Lnk_RRMD_Tgt_v PURGE").show()
    
    print("Transformer_52_Lnk_RRMD_Tgt_v")
    
    print(Transformer_52_Lnk_RRMD_Tgt_v.schema.json())
    
    print("count:{}".format(Transformer_52_Lnk_RRMD_Tgt_v.count()))
    
    Transformer_52_Lnk_RRMD_Tgt_v.show(1000,False)
    
    Transformer_52_Lnk_RRMD_Tgt_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_RRMD_Extr_POC__Transformer_52_Lnk_RRMD_Tgt_v")
    

@task.pyspark(conn_id="spark-local")
def TGT_RRMD_Lnk_RRMD_Tgt_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_52_Lnk_RRMD_Tgt_v=spark.table('datastage_temp_job_DBdirect_Mis006_RRMD_Extr_POC__Transformer_52_Lnk_RRMD_Tgt_v')
    
    TGT_RRMD_Lnk_RRMD_Tgt_Part_v=Transformer_52_Lnk_RRMD_Tgt_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_Mis006_RRMD_Extr_POC__TGT_RRMD_Lnk_RRMD_Tgt_Part_v PURGE").show()
    
    print("TGT_RRMD_Lnk_RRMD_Tgt_Part_v")
    
    print(TGT_RRMD_Lnk_RRMD_Tgt_Part_v.schema.json())
    
    print("count:{}".format(TGT_RRMD_Lnk_RRMD_Tgt_Part_v.count()))
    
    TGT_RRMD_Lnk_RRMD_Tgt_Part_v.show(1000,False)
    
    TGT_RRMD_Lnk_RRMD_Tgt_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_Mis006_RRMD_Extr_POC__TGT_RRMD_Lnk_RRMD_Tgt_Part_v")
    

@task.pyspark(conn_id="spark-local")
def TGT_RRMD(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    TGT_RRMD_Lnk_RRMD_Tgt_Part_v=spark.table('datastage_temp_job_DBdirect_Mis006_RRMD_Extr_POC__TGT_RRMD_Lnk_RRMD_Tgt_Part_v')
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    locations = Template('{{dbdir.pPROCESSING_DIR}}MIS006_RRMD.ds').render(job_params)
    
    log.info("write dataset files to "+locations)
    
    TGT_RRMD_Lnk_RRMD_Tgt_Part_v.write.mode("overwrite").format("parquet").save(locations)
    

####################################[Main]###################################
import airflow
with DAG(
    dag_id="job_DBdirect_Mis006_RRMD_Extr_POC",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None,
    tags=['datastage'],
) as dag:
    
    job_DBdirect_Mis006_RRMD_Extr_POC_task = job_DBdirect_Mis006_RRMD_Extr_POC()
    
    Job_VIEW_task = Job_VIEW()
    
    NETZ_SRC_TBL_NM_task = NETZ_SRC_TBL_NM()
    
    V0A58_task = V0A58()
    
    Sort_56_lnk_Source__Part_task = Sort_56_lnk_Source__Part()
    
    Sort_56_task = Sort_56()
    
    Transformer_52_lnk_Source_Part_task = Transformer_52_lnk_Source_Part()
    
    Transformer_52_task = Transformer_52()
    
    TGT_RRMD_Lnk_RRMD_Tgt_Part_task = TGT_RRMD_Lnk_RRMD_Tgt_Part()
    
    TGT_RRMD_task = TGT_RRMD()
    
    
    job_DBdirect_Mis006_RRMD_Extr_POC_task >> Job_VIEW_task
    
    Job_VIEW_task >> NETZ_SRC_TBL_NM_task
    
    Job_VIEW_task >> V0A58_task
    
    NETZ_SRC_TBL_NM_task >> Sort_56_lnk_Source__Part_task
    
    Sort_56_lnk_Source__Part_task >> Sort_56_task
    
    Sort_56_task >> Transformer_52_lnk_Source_Part_task
    
    Transformer_52_lnk_Source_Part_task >> Transformer_52_task
    
    Transformer_52_task >> TGT_RRMD_Lnk_RRMD_Tgt_Part_task
    
    TGT_RRMD_Lnk_RRMD_Tgt_Part_task >> TGT_RRMD_task
    




#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-08 14:39:15
# @Author  : cloudera
# @File    : job_DBdirect_MIS006_BOIS_Extr_POC.py
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
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import json
import logging
import pendulum
import textwrap

@task
def job_DBdirect_MIS006_BOIS_Extr_POC(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def Job_VIEW(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task.pyspark(conn_id="spark-local")
def NETZ_SRC_BASM(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    catalog=Variable.get("ICEBERG_CATALOG_NAME", default_var="iceberg")
    
    #spark.sql(f"use {catalog}.default").show()
    
    
    
    
    
    sql=Template("""SELECT 
    
        A.SOC_NO,
    
        A.BASE_ID,
    
        A.RATE_ID,
    
        (999999999 - A.BASM_DATE) AS BASM_DATE,
    
        A.BASM_TIME,
    
        CAST(A.RATE AS DECIMAL(16,3)) AS RATE,
    
        A.KEY_POINTER,
    
        CAST(
    
            CASE 
    
                WHEN A.KEY_POINTER <> '' AND A.KEY_POINTER IS NOT NULL 
    
                THEN COALESCE(A.RATE, 0) + COALESCE(X.RATE, 0) 
    
                ELSE A.RATE 
    
            END AS DECIMAL(16,4)
    
        ) AS EFF_RATE,
    
        L.INST_NO,
    
        L.BASM_BASE_ID AS L_BASM_BASE_ID,
    
        L.BASM_RATE_ID AS L_BASM_RATE_ID,
    
        CAST(
    
            CASE 
    
                WHEN A.BASE_ID <> '' AND A.BASE_ID IS NOT NULL 
    
                THEN A.BASE_ID 
    
                ELSE '' 
    
            END AS STRING
    
        ) AS MI006_BASM_BASE_ID,
    
        CAST(
    
            CASE 
    
                WHEN (A.RATE <> '' AND A.RATE IS NOT NULL) 
    
                THEN A.RATE 
    
                ELSE 0 
    
            END AS DECIMAL(9,5)
    
        ) AS MI006_PRIME_RATE,
    
        CAST(A.DESCRIPTION AS STRING) AS MI006_BASM_DESCRIPTION
    
    FROM (
    
        SELECT 
    
            BASM.SOC_NO,
    
            BASM.BASE_ID,
    
            BASM.RATE_ID,
    
            BASM.BASM_DATE,
    
            BASM.BASM_TIME,
    
            BASM.RATE,
    
            BASM.KEY_POINTER,
    
            BASM.DESCRIPTION
    
        FROM {{dbdir.pODS_SCHM}}.BASM
    
        WHERE {{Curr_Date}} > 999999999 - BASM.BASM_DATE
    
    ) A 
    
    LEFT OUTER JOIN (
    
        SELECT * FROM (
    
            SELECT 
    
                SOC_NO,
    
                BASE_ID,
    
                RATE_ID,
    
                RATE,
    
                KEY_POINTER,
    
                BASM_DATE,
    
                BASM_TIME,
    
                ROW_NUMBER() OVER(
    
                    PARTITION BY SOC_NO, BASE_ID, RATE_ID 
    
                    ORDER BY (999999999 - BASM_DATE) DESC, BASM_TIME
    
                ) AS Row
    
            FROM {{dbdir.pODS_SCHM}}.BASM
    
            WHERE (41941 > 999999999 - BASM.BASM_DATE)
    
            AND BASE_ID IN (
    
                SELECT DISTINCT KEY_POINTER
    
                FROM {{dbdir.pODS_SCHM}}.BASM
    
                WHERE KEY_POINTER <> '' AND KEY_POINTER IS NOT NULL
    
            )
    
        ) K
    
        WHERE Row = 1
    
    ) X 
    
        ON A.SOC_NO = X.SOC_NO 
    
        AND X.BASE_ID = A.KEY_POINTER 
    
        AND A.RATE_ID = X.RATE_ID
    
    LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.LONP L 
    
        ON A.SOC_NO = L.INST_NO 
    
        AND A.BASE_ID = L.BASM_BASE_ID 
    
        AND A.RATE_ID = L.BASM_RATE_ID""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    NETZ_SRC_BASM_v = spark.sql(sql)
    
    
    
    
    
    #spark.sql(f"use spark_catalog.default").show()
    
    NETZ_SRC_BASM_srt_keys_v=NETZ_SRC_BASM_v.select(NETZ_SRC_BASM_v[0].cast('string').alias('SOC_NO'),NETZ_SRC_BASM_v[1].cast('string').alias('BASE_ID'),NETZ_SRC_BASM_v[2].cast('string').alias('RATE_ID'),NETZ_SRC_BASM_v[3].cast('integer').alias('BASM_DATE'),NETZ_SRC_BASM_v[4].cast('integer').alias('BASM_TIME'),NETZ_SRC_BASM_v[5].cast('decimal(16,3)').alias('RATE'),NETZ_SRC_BASM_v[6].cast('string').alias('KEY_POINTER'),NETZ_SRC_BASM_v[7].cast('decimal(16,4)').alias('EFF_RATE'),NETZ_SRC_BASM_v[8].cast('string').alias('INST_NO'),NETZ_SRC_BASM_v[9].cast('string').alias('L_BASM_BASE_ID'),NETZ_SRC_BASM_v[10].cast('string').alias('L_BASM_RATE_ID'),NETZ_SRC_BASM_v[11].cast('string').alias('MI006_BASM_BASE_ID'),NETZ_SRC_BASM_v[12].cast('decimal(9,5)').alias('MI006_PRIME_RATE'),NETZ_SRC_BASM_v[13].cast('string').alias('MI006_BASM_DESCRIPTION'))
    
    NETZ_SRC_BASM_srt_keys_v = NETZ_SRC_BASM_srt_keys_v.selectExpr("RTRIM(SOC_NO) AS SOC_NO","RTRIM(BASE_ID) AS BASE_ID","RTRIM(RATE_ID) AS RATE_ID","BASM_DATE","BASM_TIME","RATE","RTRIM(KEY_POINTER) AS KEY_POINTER","EFF_RATE","RTRIM(INST_NO) AS INST_NO","RTRIM(L_BASM_BASE_ID) AS L_BASM_BASE_ID","RTRIM(L_BASM_RATE_ID) AS L_BASM_RATE_ID","MI006_BASM_BASE_ID","MI006_PRIME_RATE","MI006_BASM_DESCRIPTION").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'SOC_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'BASM_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'BASM_TIME', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'RATE', 'type': 'decimal(16,3)', 'nullable': True, 'metadata': {}}, {'name': 'KEY_POINTER', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'EFF_RATE', 'type': 'decimal(16,4)', 'nullable': True, 'metadata': {}}, {'name': 'INST_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'L_BASM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'L_BASM_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_BASM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRIME_RATE', 'type': 'decimal(9,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BASM_DESCRIPTION', 'type': 'string', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__NETZ_SRC_BASM_srt_keys_v PURGE").show()
    
    print("NETZ_SRC_BASM_srt_keys_v")
    
    print(NETZ_SRC_BASM_srt_keys_v.schema.json())
    
    print("count:{}".format(NETZ_SRC_BASM_srt_keys_v.count()))
    
    NETZ_SRC_BASM_srt_keys_v.show(1000,False)
    
    NETZ_SRC_BASM_srt_keys_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__NETZ_SRC_BASM_srt_keys_v")
    

@task.pyspark(conn_id="spark-local")
def NETZ_SRC_TBL_NM(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    catalog=Variable.get("ICEBERG_CATALOG_NAME", default_var="iceberg")
    
    #spark.sql(f"use {catalog}.default").show()
    
    
    
    
    
    sql=Template("""SELECT 
    
    BORM.KEY_1 AS B_KEY,
    
    SUBSTRING(BORM.KEY_1, 1, 3) AS KEY1,
    
    BOIS.UNPAID_SOLCA_BAL,
    
    BOIS.PRE_DISBURSE_DT,
    
    BOIS.AUTO_DR_STRT_DT,
    
    BOIS.RECALL_IND,
    
    BOIS.INST_COMM_DATE,
    
    BOIS.ACCT_SOLD_TO,
    
    TRIM(BOIS.WRITE_OFF_FLAG) AS WRITE_OFF_FLAG,
    
    BOIS.WDV_PRINCIPAL,
    
    BOIS.WDV_INTEREST,
    
    BOIS.WDV_CHARGES,
    
    BOIS.WDV_LPI,
    
    CAST(BORM.STAT AS INT) AS STAT,
    
    BORM.APP_AMT,
    
    BORM.ADV_VAL,
    
    BORM.ADV_DATE,
    
    BORM.INT_RATE,
    
    BORM.STORE_RATE,
    
    BORM.CAP_THEO_UNPD_INT,
    
    BORM.UNPD_ARRS_INT_BAL,
    
    BORM.UNPD_CHRG_BAL,
    
    BORM.UNPD_PRIN_BAL,
    
    BORM.UNPD_PRIN_BAL AS THEO_UNPD_PRIN_BAL,
    
    BORM.COLLECTIBILITY,
    
    TRIM(BORM.RATE_ID) AS RATE_ID,
    
    BORM.CAP_UNPD_INT,
    
    BORM.BASE_ID,
    
    LONP.INST_NO,
    
    LONP.NPA_VALUE,
    
    LONP.WRITE_OFF_VALUE,
    
    LONP.UNDRAWN_COMP,
    
    LONP.BASM_BASE_ID,
    
    LONP.BASM_RATE_ID,
    
    BLDVNN.START_DATE_01,
    
    CASE WHEN (BORM.BASE_ID != '' AND BORM.RATE_ID != '') THEN BORM.BASE_ID ELSE LONP.BASM_BASE_ID END AS E_BASE_ID,
    
    CASE WHEN (BORM.BASE_ID != '' AND BORM.RATE_ID != '') THEN BORM.RATE_ID ELSE LONP.BASM_RATE_ID END AS E_RATE_ID,
    
    CAST(ROUND(COALESCE(BOIS.YTD_ACCR, 0) + COALESCE(BORM.INT_ACCR, 0), 3) AS DECIMAL(18, 3)) AS MI006_ACCR_INCEPT,
    
    CAST(ABS((COALESCE(BORM.LOAN_TRM, 0) - COALESCE(BORM.REM_REPAYS, 0) - COALESCE(CAST(BOIS.MIA AS INT), 0))) AS STRING) AS MI006_NO_INST_PAID,
    
    --DIRECT COLUMNS START
    
    LONP.CAPN_METHOD AS MI006_CAPN_METHOD,
    
    BOIS.CCC_REGULATED_IND AS MI006_CCC_REGULATED_IND,
    
    CAST(BOIS.FUND_NO AS INT) AS MI006_FUND_CODE,
    
    BOIS.EXTRACTED_DATE AS MI006_EXTRACTED_DATE,
    
    BOIS.INST_COMM_DATE AS MI006_INT_ONLY_EXP_DATE,
    
    LONP.REVOLVING_CR_IND AS MI006_REVOLVING_CR_IND,
    
    BOIS.IP_PRO_AMOUNT AS MI006_PROV_AMT,
    
    BOIS.ORIGINAL_EXP_DATE AS M1006_ORIGINAL_EXP_DT,
    
    BOIS.FIRST_ADV_DATE AS MI006_FIRST_ADV_DATE,
    
    CAST(BOIS.PUR_CONTRACT_NO AS STRING) AS MI006_PUR_CONTRACT_NO,
    
    BOIS.COMMISSION_AMT AS MI006_COMMISSION_AMT,
    
    CAST(BOIS.ASSET_TYPE AS STRING) AS MI006_TYPE,
    
    BOIS.STOP_ACCRUAL AS MI006_STOP_ACCRUAL,
    
    BOIS.SHADOW_INT_ACCRUAL AS MI006_SHADOW_INT_ACCURAL,
    
    BOIS.SHADOW_CURR_YR_INT AS MI006_SHADOW_CURR_YR_INT,
    
    CAST(LONP.TIER_GROUP_ID AS STRING) AS MI006_TIER_GROUP_ID,
    
    CAST(BOIS.SETTLE_ACCT_NO AS STRING) AS MI006_SETELMENT_ACCT_NO,
    
    BOIS.DATE_OF_SALE AS MI006_DATE_OF_SALE,
    
    BOIS.RETENTION_AMOUNT AS MI006_RETENTION_AMOUNT,
    
    CAST(BOIS.RETENTION_PERIOD AS STRING) AS MI006_RETENTION_PERIOD,
    
    BOIS.SECURITY_IND AS MI006_SECURITY_IND,
    
    LONP.IO_TRM_METHOD AS MI006_ACCR_TYPE,
    
    BOIS.ACQUISITION_FEE AS MI006_ACQUISITION_FEE,
    
    BOIS.AKPK_TAG AS MI006_AKPK_CODE,
    
    CAST(BOIS.APPROVED_BY AS STRING) AS MI006_APPROVED_AUTH,
    
    BOIS.FFTH_SCHD_ISS_DT AS MI006_BOIS_5TH_SCHED_ISS_DT,
    
    BOIS.ACQUISITION_FEE AS MI006_BOIS_ACQUISITION_FEE,
    
    CAST(BOIS.APPROVED_BY AS STRING) AS MI006_BOIS_APPROVED_BY,
    
    BOIS.GROSS_EFF_YIELD AS MI006_BOIS_GROSS_EFF_YIELD,
    
    BOIS.LAST_ADV_DATE AS MI006_BOIS_LAST_ADV_DATE,
    
    BOIS.NET_EFF_YIELD AS MI006_BOIS_NET_EFF_YEILD,
    
    BOIS.NPA_BAL AS MI006_BOIS_NPA_BAL,
    
    BOIS.PROVISIONING_AMT AS MI006_BOIS_PROV_AMT,
    
    CAST(BOIS.PUR_CONTRACT_NO AS STRING) AS MI006_BOIS_PUR_CNTRT_NO,
    
    CAST(BOIS.SETTLE_ACCT_NO AS STRING) AS MI006_BOIS_SETTLE_ACCT,
    
    BOIS.SHADOW_CURR_YR_INT AS MI006_BOIS_SHDW_CURR_YR_INT,
    
    BOIS.SHADOW_INT_ACCRUAL AS MI006_BOIS_SHDW_INT_ACCR,
    
    CAST(BOIS.TOT_ACCR_CAP AS DECIMAL(18, 3)) AS MI006_BOIS_TOT_ACCR_CAP,
    
    CAST(BOIS.BUSINESS_TYPE AS STRING) AS MI006_BUS_TYPE,
    
    BOIS.NEXT_INSTLLMT_DATE AS MI006_DUE_DATE,
    
    BOIS.FFTH_SCHD_ISS_DT AS MI006_FIFTH_SCHD_ISS_DAT,
    
    CAST(BOIS.WRITE_OFF_FLAG AS STRING) AS MI006_INDEX_CODE,
    
    LONP.ISLAMIC_PRODUCT AS MI006_ISLAMIC_BANK,
    
    BOIS.MIA AS MI006_MONTHS_IN_ARR,
    
    LONP.MULTITIER_ENABLED AS MI006_MULTI_TIER_FLG,
    
    BOIS.STOP_ACCRUAL AS MI006_NON_ACCR_IND,
    
    BOIS.TOP_DOWN_AMOUNT AS MI006_NON_WF_AMT,
    
    CAST(BOIS.ORIGIN_BRANCH AS STRING) AS MI006_ORIG_BRCH_CODE,
    
    BOIS.OTHER_CHARGES AS MI006_OTHER_CHARGES,
    
    BOIS.EX_ATTRIBUTE_2 AS MI006_PERS_BNK_ID,
    
    BOIS.INST_COMM_DATE AS MI006_RTC_EXP_DATE,
    
    BOIS.MORATORIUM_TYPE AS MI006_RTC_METHOD_IND,
    
    BOIS.SALE_INDICATOR AS MI006_SALE_REPUR_CODE,
    
    BOIS.SEC_IND_DATE AS MI006_SECURITY_IND_DATE,
    
    LONP.STAFF_PROD_IND AS MI006_STAFF_PRODUCT_IND,
    
    BOIS.STMP_DUTY AS MI006_STAMPING_FEE,
    
    LONP.TIER_METH AS MI006_TIER_METHOD,
    
    BOIS.WATCH_FLAG AS MI006_WATCHLIST_TAG,
    
    BOIS.WRITE_OFF_FLAG AS MI006_WRITEOFF_STATUS_CODE,
    
    CASE WHEN BORM.INT_REPAY_FREQ != '' THEN BORM.INT_REPAY_FREQ ELSE LONP.INT_REPAY_FREQ END AS MI006_INT_REPAY_FREQ,
    
    BOIS.NEXT_INSTLLMT_DATE AS MI006_BLDVNN_NXT_PRIN_REP,
    
    BOIS.NEXT_INSTLLMT_DATE AS MI006_BLDVNN_NXT_INT_REPA,
    
    BOIS.SOLD_AMT AS MI006_BOIS_SOLD_AMOUNT,
    
    BOIS.LOAN_TYPE AS MI006_BOIS_LOAN_TYPE,
    
    CAST(BOIS.EX_FLAG_3 AS STRING) AS MI006_BOIS_EX_FLAG_3,
    
    BOIS.WATCH_FLAG_DATE AS MI006_WATCHLIST_DT,
    
    BOIS.IP_PRO_AMOUNT AS MI006_IP_PROVISION_AMOUNT,
    
    LONP.NEG_RATE_IND AS MI006_INT_TYPE,
    
    BOIS.INS_PRMUM AS MI006_BINS_PREMIUM,
    
    BOIS.CP1_PRO_AMOUNT AS MI006_BOIS_CP1_PROV_AMT,
    
    BOIS.CP3_PRO_AMOUNT AS MI006_BOIS_CP3_PROV_AMT,
    
    CASE WHEN BOIS.TOP_DOWN_FLAG = 'D' THEN BOIS.TOP_DOWN_AMOUNT ELSE 0 END AS MI006_P_WOFF_OVERDUE,
    
    BOIS.OFF_BAL_AMOUNT AS MI006_TOTAL_SUB_LEDGER_AMT,
    
    BOIS.RECALL_IND AS MI006_BOIS_RECALL_IND,
    
    CAST(BOIS.MIA AS INT) AS MI006_NO_OF_INST_ARR,
    
    BOIS.OUTSTANDING_PRINCI AS MI006_OUTSTANDING_PRINCI,
    
    BOIS.PRIMRY_NPL AS MI006_PRIMRY_NPL,
    
    BOIS.REVERSED_INT AS MI006_BOIS_REVERSED_INT,
    
    BOIS.PROVISIONING_DT AS MI006_BOIS_PROVISIONING_DT,
    
    BOIS.LITIGATION_DATE AS MI006_LITIGATION_DATE,
    
    BOIS.ADV_PREP_TXN_AMNT AS MI006_ADV_PREP_TXN_AMNT,
    
    BOIS.DWN_PAYMENT AS MI006_BORM_PRINC_REDUCT_EFF,
    
    CAST(BOIS.RETENTION_PERIOD AS STRING) AS MI006_BOIS_RETENTION_PERIOD,
    
    CAST(BOIS.ACF_NUMBER AS STRING) AS MI006_ACF_SL_NO,
    
    BOIS.STAF_GRAC_END_DT AS MI006_BOIS_RECALL_IND_DT,
    
    CAST(SUBSTRING(RIGHT(CONCAT('000', CAST(BOIS.LOCK_IN_PERIOD AS STRING)), 3), 1, 1) AS STRING) AS MI006_BOIS_LOCK_IN,
    
    BOIS.CP7_PR0_AMOUNT AS MI006_CP7_PROV,
    
    BOIS.BAD_DEBT_AMOUNT AS MI006_RECOV_AMT,
    
    SUBSTRING(BOIS.WORKOUT_TYPE, 2, 1) AS MI006_WORKOUT,
    
    BOIS.WDV_PRINCIPAL AS MI006_WDV_PRIN,
    
    CAST(BOIS.WDV_INTEREST AS DECIMAL(18, 3)) AS MI006_WDV_INT,
    
    CAST(BOIS.WDV_LPI AS DECIMAL(18, 3)) AS MI006_WDV_LPI,
    
    BOIS.WDV_CHARGES AS MI006_WDV_OC,
    
    BOIS.OFF_BAL_AMOUNT AS MI006_OFF_BS_AMT,
    
    BOIS.LOCK_IN_SRT_DT AS MI006_LOCK_IN_SRT_DT,
    
    CAST(BOIS.LOCK_IN_PERIOD AS INT) AS MI006_LOCK_IN_PERIOD,
    
    BOIS.DISCH_PEN_AMT1 AS MI006_DISCH_PEN_AMT1,
    
    BOIS.DISCH_PEN_AMT2 AS MI006_DISCH_PEN_AMT2,
    
    BOIS.DISCH_PEN_AMT3 AS MI006_DISCH_PEN_AMT3,
    
    CAST(BOIS.HSE_MRKT_CD AS STRING) AS MI006_HOUSE_MRKT_CODE,
    
    BOIS.CITIZEN_CODE AS MI006_BOIS_CITIZEN_CODE,
    
    BOIS.ATTRITION_DATE AS MI006_BOIS_ATTRITION_DT,
    
    BOIS.ATTRITION_CODE AS MI006_BOIS_ATTRITION_CODE,
    
    BOIS.STAF_GRAC_END_DT AS MI006_BOIS_STAF_GRAC_END_DT,
    
    BOIS.PREV_CAP_BAL AS MI006_BOIS_PREV_CAP_BAL,
    
    BOIS.SHORTFALL_DATE AS MI006_SHORTFALL_DATE,
    
    BOIS.RECALL_IND AS MI006_TYPE_OF_RECALL,
    
    CAST(CHCD.COLLECN_HUB_CD AS STRING) AS MI006_CHCD_COLLECN_HUB_CD,
    
    LONP.COMPANY_CODE,
    
    BORM.REM_REPAYS,
    
    BORM.FINE_CDE,
    
    BORM.RT_INCR,
    
    BORM.PENALTY_RATE,
    
    LONP.FINE_RATE,
    
    LONP.FINE_METH,
    
    LONP.FINE_CONDT,
    
    LONP.RECALL_BASE_ID,
    
    LONP.RECALL_MARGIN_ID,
    
    LONP.MAT_RECALL_ACC_RT,
    
    BOIS.RECALL_RATE,
    
    DATE_FORMAT(
    
        DATE_ADD(TO_DATE('1899-12-31'), 
    
            CASE 
    
                WHEN BORM.DUE_STRT_DATE = 0 THEN 
    
                    CASE 
    
                        WHEN BORM.INT_STRT_DATE = 0 THEN BORM.APPLIC_ISSUE_DATE 
    
                        ELSE BORM.INT_STRT_DATE 
    
                    END
    
                ELSE BORM.DUE_STRT_DATE 
    
            END
    
        ), 'yyyyMMdd'
    
    ) AS DD,
    
    CASE 
    
        WHEN BORM.TERM_BASIS = 'D' THEN 
    
            DATE_FORMAT(
    
                DATE_ADD(
    
                    DATE_ADD(TO_DATE('1899-12-31'), 
    
                        CASE 
    
                            WHEN BORM.DUE_STRT_DATE = 0 THEN 
    
                                CASE 
    
                                    WHEN BORM.INT_STRT_DATE = 0 THEN BORM.APPLIC_ISSUE_DATE 
    
                                    ELSE BORM.INT_STRT_DATE 
    
                                END
    
                            ELSE BORM.DUE_STRT_DATE 
    
                        END
    
                    ), 
    
                    CAST(BORM.LOAN_TRM AS INT)
    
                ), 
    
                'yyyyMMdd'
    
            )
    
        WHEN BORM.TERM_BASIS = 'M' THEN 
    
            DATE_FORMAT(
    
                ADD_MONTHS(
    
                    DATE_ADD(TO_DATE('1899-12-31'), 
    
                        CASE 
    
                            WHEN BORM.DUE_STRT_DATE = 0 THEN 
    
                                CASE 
    
                                    WHEN BORM.INT_STRT_DATE = 0 THEN BORM.APPLIC_ISSUE_DATE 
    
                                    ELSE BORM.INT_STRT_DATE 
    
                                END
    
                            ELSE BORM.DUE_STRT_DATE 
    
                        END
    
                    ), 
    
                    CAST(BORM.LOAN_TRM AS INT)
    
                ), 
    
                'yyyyMMdd'
    
            )
    
        ELSE '0' 
    
    END AS MD,
    
    CASE 
    
        WHEN BORM.REPAY_DAY = '00' THEN 
    
            CASE 
    
                WHEN RIGHT(CONCAT('00', CAST(LONP.REPAY_DAY AS STRING)), 2) = '00' THEN SUBSTRING(MD, 7, 2)
    
                ELSE RIGHT(CONCAT('00', CAST(LONP.REPAY_DAY AS STRING)), 2)
    
            END
    
        ELSE BORM.REPAY_DAY 
    
    END AS RD,
    
    CASE 
    
        WHEN BOIS.ORIGINAL_EXP_DATE != 0 THEN 
    
            CAST(DATE_FORMAT(DATE_ADD(TO_DATE('1899-12-31'), BOIS.ORIGINAL_EXP_DATE), 'yyyyMMdd') AS INT)
    
        ELSE 
    
            CAST(
    
                CASE 
    
                    WHEN CAST(RD AS INT) <= CAST(SUBSTRING(DATE_FORMAT(LAST_DAY(TO_DATE(CONCAT(SUBSTRING(
    
                        CASE 
    
                            WHEN CAST(SUBSTRING(DD, 7, 2) AS INT) < CAST(RD AS INT) THEN 
    
                                DATE_FORMAT(ADD_MONTHS(TO_DATE(MD, 'yyyyMMdd'), -1), 'yyyyMMdd')
    
                            ELSE MD
    
                        END, 1, 6), '01'), 'yyyyMMdd')), 'yyyyMMdd'), 7, 2) AS INT) 
    
                    THEN CONCAT(SUBSTRING(
    
                        CASE 
    
                            WHEN CAST(SUBSTRING(DD, 7, 2) AS INT) < CAST(RD AS INT) THEN 
    
                                DATE_FORMAT(ADD_MONTHS(TO_DATE(MD, 'yyyyMMdd'), -1), 'yyyyMMdd')
    
                            ELSE MD
    
                        END, 1, 6), RD)
    
                    ELSE CONCAT(SUBSTRING(
    
                        CASE 
    
                            WHEN CAST(SUBSTRING(DD, 7, 2) AS INT) < CAST(RD AS INT) THEN 
    
                                DATE_FORMAT(ADD_MONTHS(TO_DATE(MD, 'yyyyMMdd'), -1), 'yyyyMMdd')
    
                            ELSE MD
    
                        END, 1, 6), 
    
                        CAST(SUBSTRING(DATE_FORMAT(LAST_DAY(TO_DATE(CONCAT(SUBSTRING(
    
                            CASE 
    
                                WHEN CAST(SUBSTRING(DD, 7, 2) AS INT) < CAST(RD AS INT) THEN 
    
                                    DATE_FORMAT(ADD_MONTHS(TO_DATE(MD, 'yyyyMMdd'), -1), 'yyyyMMdd')
    
                                ELSE MD
    
                            END, 1, 6), '01'), 'yyyyMMdd')), 'yyyyMMdd'), 7, 2) AS INT)
    
                    )
    
                END AS INT
    
            )
    
    END AS MI006_MATURITY_DATE,
    
    BOIS.WRITE_OFF_AMOUNT AS MI006_WRITEOFF_AMOUNT,
    
    CASE 
    
        WHEN BOIS.RECALL_IND = 'D' THEN BORM.INT_RATE
    
        ELSE 
    
            CASE 
    
                WHEN BORM.FINE_CDE IN (0, 99) THEN 
    
                    CASE 
    
                        WHEN LONP.FINE_CONDT IN (0, 99) THEN 0
    
                        ELSE 
    
                            CASE 
    
                                WHEN BORM.PENALTY_RATE > 0 THEN BORM.PENALTY_RATE
    
                                ELSE 0 
    
                            END
    
                    END
    
                ELSE 
    
                    CASE 
    
                        WHEN BORM.PENALTY_RATE > 0 THEN BORM.PENALTY_RATE 
    
                        ELSE LONP.FINE_RATE 
    
                    END
    
            END
    
    END AS P_RATE,
    
    CASE 
    
        WHEN (BORM.REM_REPAYS = 0 OR BOIS.RECALL_IND IN ('C', 'D', 'E', 'F')) THEN 
    
            CASE 
    
                WHEN LONP.MAT_RECALL_ACC_RT IN ('02', '06') THEN 'R'
    
                ELSE 
    
                    CASE 
    
                        WHEN LONP.MAT_RECALL_ACC_RT IN ('03', '07') THEN 'A1'
    
                        ELSE 
    
                            CASE 
    
                                WHEN LONP.MAT_RECALL_ACC_RT IN ('04', '08') THEN 'A2'
    
                                ELSE 
    
                                    CASE 
    
                                        WHEN MI006_MATURITY_DATE != 0 THEN 
    
                                            CASE 
    
                                                WHEN LONP.FINE_METH = 18 AND {{Curr_Date}} > (CAST(DATE_FORMAT(TO_DATE(CAST(MI006_MATURITY_DATE AS STRING), 'yyyyMMdd'), 'yyyyMMdd') AS INT) - 2415020) THEN 
    
                                                    CASE 
    
                                                        WHEN (BOIS.PENALTY_RATE_ID != '' AND BOIS.PENALTY_BASE_ID != '') THEN 'P'
    
                                                        ELSE 'A'
    
                                                    END
    
                                                ELSE 'F'
    
                                            END
    
                                        ELSE 'S2'
    
                                    END
    
                            END
    
                    END
    
            END
    
        ELSE 'S2'
    
    END AS P_IND,
    
    CASE 
    
        WHEN P_IND = 'R' THEN LONP.RECALL_BASE_ID
    
        ELSE 
    
            CASE 
    
                WHEN P_IND = 'P' THEN BOIS.PENALTY_BASE_ID
    
                ELSE 
    
                    CASE 
    
                        WHEN P_IND = 'A' THEN LONP.ARR_BASM_BASE_ID
    
                        ELSE NULL 
    
                    END
    
            END
    
    END AS P_BASE_ID,
    
    CASE 
    
        WHEN P_IND = 'R' THEN LONP.RECALL_MARGIN_ID
    
        ELSE 
    
            CASE 
    
                WHEN P_IND = 'P' THEN BOIS.PENALTY_RATE_ID
    
                ELSE 
    
                    CASE 
    
                        WHEN P_IND = 'A' THEN LONP.ARR_BASM_RATE_ID
    
                        ELSE NULL 
    
                    END
    
            END
    
    END AS P_RATE_ID,
    
    LONP.CAPN_FREQ AS MI006_CAPN_FREQ,
    
    LONP.REPAY_METHOD AS MI006_BORM_REPAY_METHOD,
    
    CASE 
    
        WHEN BOIS.STOP_ACCRUAL = 'N' THEN 
    
            CAST(DATE_FORMAT(DATE_ADD(TO_DATE('1899-12-31'), DATE_DIFF({{Curr_Date}},'1900-01-01') + 1), 'yyyyMMdd') AS INT)
    
        ELSE 0 
    
    END AS MIS006_NEXT_ACCR_DT,
    
    -- Start: ODS Spec 9.8 --
    
    BOIS.ORIG_MAT_DATE AS MI006_ORIG_MAT_DATE,
    
    -- End: ODS Spec 9.8 --
    
    --ODS SPEC 9.9 START
    
    BOIS.EX_FLAG_4 AS MI006_NOTICE_FLAG,
    
    BOIS.AUTO_DR_ACCT_NO AS MI006_AUTO_DBT_ACCT_NO,
    
    BOIS.AUTO_DR_STRT_DT AS MI006_AUTO_DBT_STRT_DT,
    
    BOIS.AUTO_DR_END_DT AS MI006_AUTO_DBT_END_DT,
    
    BOIS.MON_PRD_SRT_DT AS MI006_MON_PRD_STRT_DT,
    
    BOIS.MON_PRD_END_DT AS MI006_MON_PRD_END_DT,
    
    BOIS.RR_MARGIN AS MI006_RR_MARGIN,
    
    BOIS.AKPK_DATE AS MI006_AKPK_DATE,
    
    --ODS SPEC 9.9 and 10.8 also START
    
    BOIS.FOUR_SCHD_ISS_DT,
    
    BOIS.RULE_THRE_ISS_DT,
    
    BOIS.REPO_ORDER_DATE,
    
    BOIS.FFTH_SCHD_ISS_DT,
    
    BOIS.NOT_ISS_DATE,
    
    --ODS SPEC 9.9 and 10.8 also END
    
    --ODS SPEC 9.9 END
    
    --ODS SPEC 10.2 START
    
    BOIS.MIN_REPAY_AMT AS MI006_MIN_REPAY_AMT,
    
    --ODS SPEC 10.2 END
    
    --ODS SPEC 10.3 START
    
    BOIS.PROMPT_PAY_CNTR,
    
    BOIS.BUY_BACK_CNTR,
    
    --ODS SPEC 10.3 END
    
    --ODS SPEC 10.7 START
    
    BOIS.STEP_UP_PERIOD,
    
    --ODS SPEC 10.7 END
    
    --ODS SPEC 11.3 START
    
    BOIS.REDRAW_AMT,
    
    --ODS SPEC 11.3 END
    
    --ODS SPEC 11.13 START
    
    BOIS.WRITE_OFF_VAL_TAG,
    
    BOIS.WRITE_OFF_JSTFCATN,
    
    BOIS.WRITE_OFF_TAG_DT,
    
    BOIS.WRITE_OFF_EXCL_TAG,
    
    BOIS.WRITE_OFF_EXCL_RSN,
    
    BOIS.WRITE_OFF_EXCL_DT,
    
    --ODS SPEC 11.13 END
    
    --ODS SPEC 11.17 START
    
    BOIS.ORIG_REM_REPAYS,
    
    --ODS SPEC 11.17 END
    
    BORM.STMT_DELI,
    
    BOIS.EMAIL_ADDRESS,
    
    BOIS.CHRG_RPY_AMT,
    
    BOIS.PRIN_RPY_AMT,
    
    BOIS.COURT_DEP_RPY_AMT,
    
    BOIS.INT_RPY_AMT,
    
    BOIS.BUY_BACK_DATE,
    
    BOIS.SALE_BUY_BACK,
    
    --ODS SPEC 14.2 START
    
    IFRM.IFRS_EIR_RATE,
    
    IFRM.IFRS_ESTM_LOAN_TRM,
    
    --ODS SPEC 14.2 END
    
    --ODS SPEC 14.0 START
    
    CAST(BOIS.MAX_REPAY_AGE AS STRING) AS MAX_REPAY_AGE,
    
    BORM.MORTGAGE_FACILITY,
    
    --ODS SPEC 14.0 END
    
    BOIS.SPGA_NO,         --ODS SPEC 15.2
    
    BOIS.DEDUCT_CODE,     --ODS SPEC 15.2
    
    BOIS.DED_STRT_MON,    --ODS SPEC 15.2
    
    BOIS.DED_END_MON,     --ODS SPEC 15.2
    
    BOIS.LAW_SUIT_FLAG,   --ODS SPEC 15.1
    
    BOIS.GPP_INT_CAP_AMT,       --ODS SPEC 16.2
    
    BOIS.GPP_MON_AMORT_AMT,     --ODS SPEC 16.2
    
    BOIS.RNR_REM_PROF,          --ODS SPEC 16.3
    
    BOIS.RNR_MONTHLY_PROF,      --ODS SPEC 16.3
    
    BOIS.INT_THRES_CAP_PER,     --ODS SPEC 16.4
    
    BOIS.PP_ADD_INT_AMT,        --ODS SPECT 16.7
    
    BOIS.PP_PROMPT_PAY_CNTR,    --ODS SPECT 16.7
    
    BOIS.PP_MON_PRD_END_DT,     --ODS SPECT 16.7
    
    BOIS.REL_RED_RSN_CD,        --ODS SPECT 16.8
    
    BOIS.RSN_CD_MAINTNCE_DT,    --ODS SPECT 16.8
    
    BOIS.INST_CHG_EXP_DT,       --ODS SPECT 16.6
    
    BOIS.AKPK_SPC_TAG,          --ODS SPECT 17.9
    
    BOIS.AKPK_SPC_START_DT,     --ODS SPECT 17.9
    
    BOIS.AKPK_SPC_END_DT,       --ODS SPECT 17.9
    
    BOIS.AKPK_MATRIX,           --ODS SPECT 17.9
    
    BOIS.MORA_ACCR_INT,         --ODS SPECT 17.8
    
    BOIS.MANUAL_IMPAIR,         --ODS SPECT 18.2
    
    BOIS.UMA_NOTC_GEN_DT,       --ODC SPECT 18.4
    
    BOIS.ACC_DUE_DEF_INST,      --ODC SPECT 18.7
    
    BOIS.EX_FLAG_1,             --ODS SPECT 19.6
    
    BOIS.INSTALLMENT_NO,        --ODS SPECT 19.6
    
    BOIS.ADV_PREPAY_FLAG,       --ODS SPECT 19.6
    
    BOIS.REL_RED_RSN_FL_CD,     --ODS SPECT 19.6
    
    BOIS.REN_ENHC_NEW_FL,       --ODS SPECT 19.7
    
    BOIS.INST_REINSTATE,        --ODS SPECT 19.8
    
    BOIS.REINSTATE_DATE,        --ODS SPECT 19.8
    
    BOIS.REINSTATE_COUNT,       --ODS SPECT 19.8
    
    BOIS.MFA_FLAG,              --ODS SPECT 22.0
    
    BOIS.MFA_COUNTER,           --ODS SPECT 22.0
    
    BOIS.MFA_EFF_DT,            --ODS SPECT 22.0
    
    BOIS.MFA_MAINT_DT,          --ODS SPECT 22.0
    
    BOIS.EIY_RATE               --ODS SPECT 23.0
    
    FROM {{dbdir.pODS_SCHM}}.BORM
    
    LEFT JOIN {{dbdir.pODS_SCHM}}.BOIS ON BOIS.KEY_1 = BORM.KEY_1
    
    LEFT JOIN {{dbdir.pODS_SCHM}}.LONP ON LONP.INST_NO = SUBSTRING(BORM.KEY_1, 1, 3) 
    
        AND LONP.SYST = 'BOR' 
    
        AND LONP.ACCT_TYPE = BORM.ACT_TYPE 
    
        AND LONP.INT_CAT = BORM.CAT
    
    LEFT JOIN (
    
        SELECT 
    
            B.BL_KEY,
    
            B.START_DATE_01
    
        FROM (
    
            SELECT 
    
                SUBSTRING(BLDVNN.KEY_1, 1, 19) AS BL_KEY,
    
                RANK() OVER (PARTITION BY SUBSTRING(BLDVNN.KEY_1, 1, 19) ORDER BY HEX(SUBSTRING(BLDVNN.KEY_1, 20, 4))) AS RNK,
    
                BLDVNN.START_DATE_01
    
            FROM {{dbdir.pODS_SCHM}}.BLDVNN
    
        ) B
    
        WHERE B.RNK = 1
    
    ) BLDVNN ON BLDVNN.BL_KEY = BORM.KEY_1
    
    LEFT JOIN {{dbdir.pODS_SCHM}}.CHCD ON CHCD.INST_NO = SUBSTRING(BORM.KEY_1, 1, 3)  
    
        AND CHCD.BRANCH_NO = BOIS.ORIGIN_BRANCH 
    
        AND CHCD.ACCT_TYP = BORM.ACT_TYPE 
    
        AND CHCD.INT_CAT = BORM.CAT
    
    LEFT JOIN {{dbdir.pODS_SCHM}}.IFRM ON SUBSTRING(BORM.KEY_1, 1, 3) = IFRM.INST_NO
    
        AND SUBSTRING(BORM.KEY_1, 4, 16) = IFRM.ACCT_NO""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    NETZ_SRC_TBL_NM_v = spark.sql(sql)
    
    
    
    
    
    #spark.sql(f"use spark_catalog.default").show()
    
    NETZ_SRC_TBL_NM_lnk_Source_v=NETZ_SRC_TBL_NM_v.select(NETZ_SRC_TBL_NM_v[0].cast('string').alias('B_KEY'),NETZ_SRC_TBL_NM_v[1].cast('string').alias('KEY1'),NETZ_SRC_TBL_NM_v[2].cast('decimal(17,3)').alias('UNPAID_SOLCA_BAL'),NETZ_SRC_TBL_NM_v[3].cast('integer').alias('PRE_DISBURSE_DT'),NETZ_SRC_TBL_NM_v[4].cast('integer').alias('AUTO_DR_STRT_DT'),NETZ_SRC_TBL_NM_v[5].cast('string').alias('RECALL_IND'),NETZ_SRC_TBL_NM_v[6].cast('integer').alias('INST_COMM_DATE'),NETZ_SRC_TBL_NM_v[7].cast('string').alias('ACCT_SOLD_TO'),NETZ_SRC_TBL_NM_v[8].cast('string').alias('WRITE_OFF_FLAG'),NETZ_SRC_TBL_NM_v[9].cast('decimal(17,3)').alias('WDV_PRINCIPAL'),NETZ_SRC_TBL_NM_v[10].cast('decimal(17,5)').alias('WDV_INTEREST'),NETZ_SRC_TBL_NM_v[11].cast('decimal(17,3)').alias('WDV_CHARGES'),NETZ_SRC_TBL_NM_v[12].cast('decimal(17,5)').alias('WDV_LPI'),NETZ_SRC_TBL_NM_v[13].cast('integer').alias('STAT'),NETZ_SRC_TBL_NM_v[14].cast('decimal(17,3)').alias('APP_AMT'),NETZ_SRC_TBL_NM_v[15].cast('decimal(17,3)').alias('ADV_VAL'),NETZ_SRC_TBL_NM_v[16].cast('integer').alias('ADV_DATE'),NETZ_SRC_TBL_NM_v[17].cast('decimal(7,4)').alias('INT_RATE'),NETZ_SRC_TBL_NM_v[18].cast('decimal(7,4)').alias('STORE_RATE'),NETZ_SRC_TBL_NM_v[19].cast('decimal(17,5)').alias('CAP_THEO_UNPD_INT'),NETZ_SRC_TBL_NM_v[20].cast('decimal(17,5)').alias('UNPD_ARRS_INT_BAL'),NETZ_SRC_TBL_NM_v[21].cast('decimal(17,3)').alias('UNPD_CHRG_BAL'),NETZ_SRC_TBL_NM_v[22].cast('decimal(17,3)').alias('UNPD_PRIN_BAL'),NETZ_SRC_TBL_NM_v[23].cast('decimal(17,3)').alias('THEO_UNPD_PRIN_BAL'),NETZ_SRC_TBL_NM_v[24].cast('string').alias('COLLECTIBILITY'),NETZ_SRC_TBL_NM_v[25].cast('string').alias('RATE_ID'),NETZ_SRC_TBL_NM_v[26].cast('decimal(17,5)').alias('CAP_UNPD_INT'),NETZ_SRC_TBL_NM_v[27].cast('string').alias('BASE_ID'),NETZ_SRC_TBL_NM_v[28].cast('string').alias('INST_NO'),NETZ_SRC_TBL_NM_v[29].cast('string').alias('NPA_VALUE'),NETZ_SRC_TBL_NM_v[30].cast('string').alias('WRITE_OFF_VALUE'),NETZ_SRC_TBL_NM_v[31].cast('string').alias('UNDRAWN_COMP'),NETZ_SRC_TBL_NM_v[32].cast('string').alias('BASM_BASE_ID'),NETZ_SRC_TBL_NM_v[33].cast('string').alias('BASM_RATE_ID'),NETZ_SRC_TBL_NM_v[34].cast('integer').alias('START_DATE_01'),NETZ_SRC_TBL_NM_v[35].cast('string').alias('E_BASE_ID'),NETZ_SRC_TBL_NM_v[36].cast('string').alias('E_RATE_ID'),NETZ_SRC_TBL_NM_v[37].cast('decimal(18,3)').alias('MI006_ACCR_INCEPT'),NETZ_SRC_TBL_NM_v[38].cast('string').alias('MI006_NO_INST_PAID'),NETZ_SRC_TBL_NM_v[39].cast('string').alias('MI006_CAPN_METHOD'),NETZ_SRC_TBL_NM_v[40].cast('string').alias('MI006_CCC_REGULATED_IND'),NETZ_SRC_TBL_NM_v[41].cast('integer').alias('MI006_FUND_CODE'),NETZ_SRC_TBL_NM_v[42].cast('integer').alias('MI006_EXTRACTED_DATE'),NETZ_SRC_TBL_NM_v[43].cast('integer').alias('MI006_INT_ONLY_EXP_DATE'),NETZ_SRC_TBL_NM_v[44].cast('string').alias('MI006_REVOLVING_CR_IND'),NETZ_SRC_TBL_NM_v[45].cast('decimal(18,3)').alias('MI006_PROV_AMT'),NETZ_SRC_TBL_NM_v[46].cast('integer').alias('M1006_ORIGINAL_EXP_DT'),NETZ_SRC_TBL_NM_v[47].cast('integer').alias('MI006_FIRST_ADV_DATE'),NETZ_SRC_TBL_NM_v[48].cast('string').alias('MI006_PUR_CONTRACT_NO'),NETZ_SRC_TBL_NM_v[49].cast('decimal(18,3)').alias('MI006_COMMISSION_AMT'),NETZ_SRC_TBL_NM_v[50].cast('string').alias('MI006_TYPE'),NETZ_SRC_TBL_NM_v[51].cast('string').alias('MI006_STOP_ACCRUAL'),NETZ_SRC_TBL_NM_v[52].cast('decimal(18,3)').alias('MI006_SHADOW_INT_ACCURAL'),NETZ_SRC_TBL_NM_v[53].cast('decimal(18,3)').alias('MI006_SHADOW_CURR_YR_INT'),NETZ_SRC_TBL_NM_v[54].cast('string').alias('MI006_TIER_GROUP_ID'),NETZ_SRC_TBL_NM_v[55].cast('string').alias('MI006_SETELMENT_ACCT_NO'),NETZ_SRC_TBL_NM_v[56].cast('integer').alias('MI006_DATE_OF_SALE'),NETZ_SRC_TBL_NM_v[57].cast('decimal(18,3)').alias('MI006_RETENTION_AMOUNT'),NETZ_SRC_TBL_NM_v[58].cast('string').alias('MI006_RETENTION_PERIOD'),NETZ_SRC_TBL_NM_v[59].cast('string').alias('MI006_SECURITY_IND'),NETZ_SRC_TBL_NM_v[60].cast('string').alias('MI006_ACCR_TYPE'),NETZ_SRC_TBL_NM_v[61].cast('decimal(18,3)').alias('MI006_ACQUISITION_FEE'),NETZ_SRC_TBL_NM_v[62].cast('string').alias('MI006_AKPK_CODE'),NETZ_SRC_TBL_NM_v[63].cast('string').alias('MI006_APPROVED_AUTH'),NETZ_SRC_TBL_NM_v[64].cast('integer').alias('MI006_BOIS_5TH_SCHED_ISS_DT'),NETZ_SRC_TBL_NM_v[65].cast('decimal(18,3)').alias('MI006_BOIS_ACQUISITION_FEE'),NETZ_SRC_TBL_NM_v[66].cast('string').alias('MI006_BOIS_APPROVED_BY'),NETZ_SRC_TBL_NM_v[67].cast('decimal(4,2)').alias('MI006_BOIS_GROSS_EFF_YIELD'),NETZ_SRC_TBL_NM_v[68].cast('integer').alias('MI006_BOIS_LAST_ADV_DATE'),NETZ_SRC_TBL_NM_v[69].cast('decimal(4,2)').alias('MI006_BOIS_NET_EFF_YEILD'),NETZ_SRC_TBL_NM_v[70].cast('decimal(18,3)').alias('MI006_BOIS_NPA_BAL'),NETZ_SRC_TBL_NM_v[71].cast('decimal(18,3)').alias('MI006_BOIS_PROV_AMT'),NETZ_SRC_TBL_NM_v[72].cast('string').alias('MI006_BOIS_PUR_CNTRT_NO'),NETZ_SRC_TBL_NM_v[73].cast('string').alias('MI006_BOIS_SETTLE_ACCT'),NETZ_SRC_TBL_NM_v[74].cast('decimal(18,3)').alias('MI006_BOIS_SHDW_CURR_YR_INT'),NETZ_SRC_TBL_NM_v[75].cast('decimal(18,3)').alias('MI006_BOIS_SHDW_INT_ACCR'),NETZ_SRC_TBL_NM_v[76].cast('decimal(18,3)').alias('MI006_BOIS_TOT_ACCR_CAP'),NETZ_SRC_TBL_NM_v[77].cast('string').alias('MI006_BUS_TYPE'),NETZ_SRC_TBL_NM_v[78].cast('integer').alias('MI006_DUE_DATE'),NETZ_SRC_TBL_NM_v[79].cast('integer').alias('MI006_FIFTH_SCHD_ISS_DAT'),NETZ_SRC_TBL_NM_v[80].cast('string').alias('MI006_INDEX_CODE'),NETZ_SRC_TBL_NM_v[81].cast('string').alias('MI006_ISLAMIC_BANK'),NETZ_SRC_TBL_NM_v[82].cast('string').alias('MI006_MONTHS_IN_ARR'),NETZ_SRC_TBL_NM_v[83].cast('string').alias('MI006_MULTI_TIER_FLG'),NETZ_SRC_TBL_NM_v[84].cast('string').alias('MI006_NON_ACCR_IND'),NETZ_SRC_TBL_NM_v[85].cast('decimal(18,3)').alias('MI006_NON_WF_AMT'),NETZ_SRC_TBL_NM_v[86].cast('string').alias('MI006_ORIG_BRCH_CODE'),NETZ_SRC_TBL_NM_v[87].cast('decimal(18,3)').alias('MI006_OTHER_CHARGES'),NETZ_SRC_TBL_NM_v[88].cast('string').alias('MI006_PERS_BNK_ID'),NETZ_SRC_TBL_NM_v[89].cast('integer').alias('MI006_RTC_EXP_DATE'),NETZ_SRC_TBL_NM_v[90].cast('string').alias('MI006_RTC_METHOD_IND'),NETZ_SRC_TBL_NM_v[91].cast('string').alias('MI006_SALE_REPUR_CODE'),NETZ_SRC_TBL_NM_v[92].cast('integer').alias('MI006_SECURITY_IND_DATE'),NETZ_SRC_TBL_NM_v[93].cast('string').alias('MI006_STAFF_PRODUCT_IND'),NETZ_SRC_TBL_NM_v[94].cast('decimal(18,3)').alias('MI006_STAMPING_FEE'),NETZ_SRC_TBL_NM_v[95].cast('string').alias('MI006_TIER_METHOD'),NETZ_SRC_TBL_NM_v[96].cast('string').alias('MI006_WATCHLIST_TAG'),NETZ_SRC_TBL_NM_v[97].cast('string').alias('MI006_WRITEOFF_STATUS_CODE'),NETZ_SRC_TBL_NM_v[98].cast('string').alias('MI006_INT_REPAY_FREQ'),NETZ_SRC_TBL_NM_v[99].cast('integer').alias('MI006_BLDVNN_NXT_PRIN_REP'),NETZ_SRC_TBL_NM_v[100].cast('integer').alias('MI006_BLDVNN_NXT_INT_REPA'),NETZ_SRC_TBL_NM_v[101].cast('decimal(18,3)').alias('MI006_BOIS_SOLD_AMOUNT'),NETZ_SRC_TBL_NM_v[102].cast('string').alias('MI006_BOIS_LOAN_TYPE'),NETZ_SRC_TBL_NM_v[103].cast('string').alias('MI006_BOIS_EX_FLAG_3'),NETZ_SRC_TBL_NM_v[104].cast('integer').alias('MI006_WATCHLIST_DT'),NETZ_SRC_TBL_NM_v[105].cast('decimal(18,3)').alias('MI006_IP_PROVISION_AMOUNT'),NETZ_SRC_TBL_NM_v[106].cast('string').alias('MI006_INT_TYPE'),NETZ_SRC_TBL_NM_v[107].cast('decimal(18,3)').alias('MI006_BINS_PREMIUM'),NETZ_SRC_TBL_NM_v[108].cast('decimal(18,3)').alias('MI006_BOIS_CP1_PROV_AMT'),NETZ_SRC_TBL_NM_v[109].cast('decimal(18,3)').alias('MI006_BOIS_CP3_PROV_AMT'),NETZ_SRC_TBL_NM_v[110].cast('decimal(18,3)').alias('MI006_P_WOFF_OVERDUE'),NETZ_SRC_TBL_NM_v[111].cast('decimal(18,3)').alias('MI006_TOTAL_SUB_LEDGER_AMT'),NETZ_SRC_TBL_NM_v[112].cast('string').alias('MI006_BOIS_RECALL_IND'),NETZ_SRC_TBL_NM_v[113].cast('integer').alias('MI006_NO_OF_INST_ARR'),NETZ_SRC_TBL_NM_v[114].cast('decimal(18,3)').alias('MI006_OUTSTANDING_PRINCI'),NETZ_SRC_TBL_NM_v[115].cast('string').alias('MI006_PRIMRY_NPL'),NETZ_SRC_TBL_NM_v[116].cast('decimal(18,3)').alias('MI006_BOIS_REVERSED_INT'),NETZ_SRC_TBL_NM_v[117].cast('integer').alias('MI006_BOIS_PROVISIONING_DT'),NETZ_SRC_TBL_NM_v[118].cast('integer').alias('MI006_LITIGATION_DATE'),NETZ_SRC_TBL_NM_v[119].cast('decimal(18,3)').alias('MI006_ADV_PREP_TXN_AMNT'),NETZ_SRC_TBL_NM_v[120].cast('decimal(18,3)').alias('MI006_BORM_PRINC_REDUCT_EFF'),NETZ_SRC_TBL_NM_v[121].cast('string').alias('MI006_BOIS_RETENTION_PERIOD'),NETZ_SRC_TBL_NM_v[122].cast('string').alias('MI006_ACF_SL_NO'),NETZ_SRC_TBL_NM_v[123].cast('integer').alias('MI006_BOIS_RECALL_IND_DT'),NETZ_SRC_TBL_NM_v[124].cast('string').alias('MI006_BOIS_LOCK_IN'),NETZ_SRC_TBL_NM_v[125].cast('decimal(18,3)').alias('MI006_CP7_PROV'),NETZ_SRC_TBL_NM_v[126].cast('decimal(18,3)').alias('MI006_RECOV_AMT'),NETZ_SRC_TBL_NM_v[127].cast('string').alias('MI006_WORKOUT'),NETZ_SRC_TBL_NM_v[128].cast('decimal(18,3)').alias('MI006_WDV_PRIN'),NETZ_SRC_TBL_NM_v[129].cast('decimal(18,3)').alias('MI006_WDV_INT'),NETZ_SRC_TBL_NM_v[130].cast('decimal(18,3)').alias('MI006_WDV_LPI'),NETZ_SRC_TBL_NM_v[131].cast('decimal(18,3)').alias('MI006_WDV_OC'),NETZ_SRC_TBL_NM_v[132].cast('decimal(18,3)').alias('MI006_OFF_BS_AMT'),NETZ_SRC_TBL_NM_v[133].cast('string').alias('MI006_LOCK_IN_SRT_DT'),NETZ_SRC_TBL_NM_v[134].cast('integer').alias('MI006_LOCK_IN_PERIOD'),NETZ_SRC_TBL_NM_v[135].cast('decimal(18,3)').alias('MI006_DISCH_PEN_AMT1'),NETZ_SRC_TBL_NM_v[136].cast('decimal(18,3)').alias('MI006_DISCH_PEN_AMT2'),NETZ_SRC_TBL_NM_v[137].cast('decimal(18,3)').alias('MI006_DISCH_PEN_AMT3'),NETZ_SRC_TBL_NM_v[138].cast('string').alias('MI006_HOUSE_MRKT_CODE'),NETZ_SRC_TBL_NM_v[139].cast('string').alias('MI006_BOIS_CITIZEN_CODE'),NETZ_SRC_TBL_NM_v[140].cast('integer').alias('MI006_BOIS_ATTRITION_DT'),NETZ_SRC_TBL_NM_v[141].cast('string').alias('MI006_BOIS_ATTRITION_CODE'),NETZ_SRC_TBL_NM_v[142].cast('integer').alias('MI006_BOIS_STAF_GRAC_END_DT'),NETZ_SRC_TBL_NM_v[143].cast('decimal(18,3)').alias('MI006_BOIS_PREV_CAP_BAL'),NETZ_SRC_TBL_NM_v[144].cast('integer').alias('MI006_SHORTFALL_DATE'),NETZ_SRC_TBL_NM_v[145].cast('string').alias('MI006_TYPE_OF_RECALL'),NETZ_SRC_TBL_NM_v[146].cast('string').alias('MI006_CHCD_COLLECN_HUB_CD'),NETZ_SRC_TBL_NM_v[147].cast('string').alias('COMPANY_CODE'),NETZ_SRC_TBL_NM_v[148].cast('decimal(5,0)').alias('REM_REPAYS'),NETZ_SRC_TBL_NM_v[149].cast('string').alias('FINE_CDE'),NETZ_SRC_TBL_NM_v[150].cast('decimal(7,5)').alias('RT_INCR'),NETZ_SRC_TBL_NM_v[151].cast('decimal(7,4)').alias('PENALTY_RATE'),NETZ_SRC_TBL_NM_v[152].cast('decimal(6,4)').alias('FINE_RATE'),NETZ_SRC_TBL_NM_v[153].cast('string').alias('FINE_METH'),NETZ_SRC_TBL_NM_v[154].cast('string').alias('FINE_CONDT'),NETZ_SRC_TBL_NM_v[155].cast('string').alias('RECALL_BASE_ID'),NETZ_SRC_TBL_NM_v[156].cast('string').alias('RECALL_MARGIN_ID'),NETZ_SRC_TBL_NM_v[157].cast('string').alias('MAT_RECALL_ACC_RT'),NETZ_SRC_TBL_NM_v[158].cast('decimal(6,4)').alias('RECALL_RATE'),NETZ_SRC_TBL_NM_v[159].cast('string').alias('DD'),NETZ_SRC_TBL_NM_v[160].cast('string').alias('MD'),NETZ_SRC_TBL_NM_v[161].cast('string').alias('RD'),NETZ_SRC_TBL_NM_v[162].cast('integer').alias('MI006_MATURITY_DATE'),NETZ_SRC_TBL_NM_v[163].cast('decimal(18,3)').alias('MI006_WRITEOFF_AMOUNT'),NETZ_SRC_TBL_NM_v[164].cast('decimal(7,4)').alias('P_RATE'),NETZ_SRC_TBL_NM_v[165].cast('string').alias('P_IND'),NETZ_SRC_TBL_NM_v[166].cast('string').alias('P_BASE_ID'),NETZ_SRC_TBL_NM_v[167].cast('string').alias('P_RATE_ID'),NETZ_SRC_TBL_NM_v[168].cast('string').alias('MI006_CAPN_FREQ'),NETZ_SRC_TBL_NM_v[169].cast('string').alias('MI006_BORM_REPAY_METHOD'),NETZ_SRC_TBL_NM_v[170].cast('integer').alias('MIS006_NEXT_ACCR_DT'),NETZ_SRC_TBL_NM_v[171].cast('integer').alias('MI006_ORIG_MAT_DATE'),NETZ_SRC_TBL_NM_v[172].cast('string').alias('MI006_NOTICE_FLAG'),NETZ_SRC_TBL_NM_v[173].cast('string').alias('MI006_AUTO_DBT_ACCT_NO'),NETZ_SRC_TBL_NM_v[174].cast('integer').alias('MI006_AUTO_DBT_STRT_DT'),NETZ_SRC_TBL_NM_v[175].cast('integer').alias('MI006_AUTO_DBT_END_DT'),NETZ_SRC_TBL_NM_v[176].cast('integer').alias('MI006_MON_PRD_STRT_DT'),NETZ_SRC_TBL_NM_v[177].cast('integer').alias('MI006_MON_PRD_END_DT'),NETZ_SRC_TBL_NM_v[178].cast('decimal(4,2)').alias('MI006_RR_MARGIN'),NETZ_SRC_TBL_NM_v[179].cast('integer').alias('MI006_AKPK_DATE'),NETZ_SRC_TBL_NM_v[180].cast('integer').alias('FOUR_SCHD_ISS_DT'),NETZ_SRC_TBL_NM_v[181].cast('integer').alias('RULE_THRE_ISS_DT'),NETZ_SRC_TBL_NM_v[182].cast('integer').alias('REPO_ORDER_DATE'),NETZ_SRC_TBL_NM_v[183].cast('integer').alias('FFTH_SCHD_ISS_DT'),NETZ_SRC_TBL_NM_v[184].cast('integer').alias('NOT_ISS_DATE'),NETZ_SRC_TBL_NM_v[185].cast('decimal(17,3)').alias('MI006_MIN_REPAY_AMT'),NETZ_SRC_TBL_NM_v[186].cast('decimal(3,0)').alias('PROMPT_PAY_CNTR'),NETZ_SRC_TBL_NM_v[187].cast('decimal(2,0)').alias('BUY_BACK_CNTR'),NETZ_SRC_TBL_NM_v[188].cast('decimal(2,0)').alias('STEP_UP_PERIOD'),NETZ_SRC_TBL_NM_v[189].cast('decimal(17,3)').alias('REDRAW_AMT'),NETZ_SRC_TBL_NM_v[190].cast('string').alias('WRITE_OFF_VAL_TAG'),NETZ_SRC_TBL_NM_v[191].cast('string').alias('WRITE_OFF_JSTFCATN'),NETZ_SRC_TBL_NM_v[192].cast('integer').alias('WRITE_OFF_TAG_DT'),NETZ_SRC_TBL_NM_v[193].cast('string').alias('WRITE_OFF_EXCL_TAG'),NETZ_SRC_TBL_NM_v[194].cast('string').alias('WRITE_OFF_EXCL_RSN'),NETZ_SRC_TBL_NM_v[195].cast('integer').alias('WRITE_OFF_EXCL_DT'),NETZ_SRC_TBL_NM_v[196].cast('decimal(5,0)').alias('ORIG_REM_REPAYS'),NETZ_SRC_TBL_NM_v[197].cast('string').alias('STMT_DELI'),NETZ_SRC_TBL_NM_v[198].cast('string').alias('EMAIL_ADDRESS'),NETZ_SRC_TBL_NM_v[199].cast('decimal(17,3)').alias('CHRG_RPY_AMT'),NETZ_SRC_TBL_NM_v[200].cast('decimal(17,3)').alias('PRIN_RPY_AMT'),NETZ_SRC_TBL_NM_v[201].cast('decimal(17,3)').alias('COURT_DEP_RPY_AMT'),NETZ_SRC_TBL_NM_v[202].cast('decimal(17,3)').alias('INT_RPY_AMT'),NETZ_SRC_TBL_NM_v[203].cast('integer').alias('BUY_BACK_DATE'),NETZ_SRC_TBL_NM_v[204].cast('string').alias('SALE_BUY_BACK'),NETZ_SRC_TBL_NM_v[205].cast('decimal(9,6)').alias('IFRS_EIR_RATE'),NETZ_SRC_TBL_NM_v[206].cast('decimal(5,0)').alias('IFRS_ESTM_LOAN_TRM'),NETZ_SRC_TBL_NM_v[207].cast('string').alias('MAX_REPAY_AGE'),NETZ_SRC_TBL_NM_v[208].cast('string').alias('MORTGAGE_FACILITY'),NETZ_SRC_TBL_NM_v[209].cast('string').alias('SPGA_NO'),NETZ_SRC_TBL_NM_v[210].cast('string').alias('DEDUCT_CODE'),NETZ_SRC_TBL_NM_v[211].cast('string').alias('DED_STRT_MON'),NETZ_SRC_TBL_NM_v[212].cast('string').alias('DED_END_MON'),NETZ_SRC_TBL_NM_v[213].cast('string').alias('LAW_SUIT_FLAG'),NETZ_SRC_TBL_NM_v[214].cast('decimal(17,3)').alias('GPP_INT_CAP_AMT'),NETZ_SRC_TBL_NM_v[215].cast('decimal(17,3)').alias('GPP_MON_AMORT_AMT'),NETZ_SRC_TBL_NM_v[216].cast('decimal(17,3)').alias('RNR_REM_PROF'),NETZ_SRC_TBL_NM_v[217].cast('decimal(17,3)').alias('RNR_MONTHLY_PROF'),NETZ_SRC_TBL_NM_v[218].cast('decimal(5,2)').alias('INT_THRES_CAP_PER'),NETZ_SRC_TBL_NM_v[219].cast('decimal(17,3)').alias('PP_ADD_INT_AMT'),NETZ_SRC_TBL_NM_v[220].cast('decimal(2,0)').alias('PP_PROMPT_PAY_CNTR'),NETZ_SRC_TBL_NM_v[221].cast('integer').alias('PP_MON_PRD_END_DT'),NETZ_SRC_TBL_NM_v[222].cast('string').alias('REL_RED_RSN_CD'),NETZ_SRC_TBL_NM_v[223].cast('integer').alias('RSN_CD_MAINTNCE_DT'),NETZ_SRC_TBL_NM_v[224].cast('integer').alias('INST_CHG_EXP_DT'),NETZ_SRC_TBL_NM_v[225].cast('string').alias('AKPK_SPC_TAG'),NETZ_SRC_TBL_NM_v[226].cast('integer').alias('AKPK_SPC_START_DT'),NETZ_SRC_TBL_NM_v[227].cast('integer').alias('AKPK_SPC_END_DT'),NETZ_SRC_TBL_NM_v[228].cast('string').alias('AKPK_MATRIX'),NETZ_SRC_TBL_NM_v[229].cast('decimal(17,3)').alias('MORA_ACCR_INT'),NETZ_SRC_TBL_NM_v[230].cast('string').alias('MANUAL_IMPAIR'),NETZ_SRC_TBL_NM_v[231].cast('integer').alias('UMA_NOTC_GEN_DT'),NETZ_SRC_TBL_NM_v[232].cast('decimal(2,0)').alias('ACC_DUE_DEF_INST'),NETZ_SRC_TBL_NM_v[233].cast('string').alias('EX_FLAG_1'),NETZ_SRC_TBL_NM_v[234].cast('decimal(3,0)').alias('INSTALLMENT_NO'),NETZ_SRC_TBL_NM_v[235].cast('string').alias('ADV_PREPAY_FLAG'),NETZ_SRC_TBL_NM_v[236].cast('string').alias('REL_RED_RSN_FL_CD'),NETZ_SRC_TBL_NM_v[237].cast('string').alias('REN_ENHC_NEW_FL'),NETZ_SRC_TBL_NM_v[238].cast('string').alias('INST_REINSTATE'),NETZ_SRC_TBL_NM_v[239].cast('integer').alias('REINSTATE_DATE'),NETZ_SRC_TBL_NM_v[240].cast('decimal(2,0)').alias('REINSTATE_COUNT'),NETZ_SRC_TBL_NM_v[241].cast('string').alias('MFA_FLAG'),NETZ_SRC_TBL_NM_v[242].cast('decimal(2,0)').alias('MFA_COUNTER'),NETZ_SRC_TBL_NM_v[243].cast('integer').alias('MFA_EFF_DT'),NETZ_SRC_TBL_NM_v[244].cast('integer').alias('MFA_MAINT_DT'),NETZ_SRC_TBL_NM_v[245].cast('decimal(4,2)').alias('EIY_RATE'))
    
    NETZ_SRC_TBL_NM_lnk_Source_v = NETZ_SRC_TBL_NM_lnk_Source_v.selectExpr("RTRIM(B_KEY) AS B_KEY","KEY1","UNPAID_SOLCA_BAL","PRE_DISBURSE_DT","AUTO_DR_STRT_DT","RTRIM(RECALL_IND) AS RECALL_IND","INST_COMM_DATE","RTRIM(ACCT_SOLD_TO) AS ACCT_SOLD_TO","WRITE_OFF_FLAG","WDV_PRINCIPAL","WDV_INTEREST","WDV_CHARGES","WDV_LPI","STAT","APP_AMT","ADV_VAL","ADV_DATE","INT_RATE","STORE_RATE","CAP_THEO_UNPD_INT","UNPD_ARRS_INT_BAL","UNPD_CHRG_BAL","UNPD_PRIN_BAL","THEO_UNPD_PRIN_BAL","RTRIM(COLLECTIBILITY) AS COLLECTIBILITY","RATE_ID","CAP_UNPD_INT","RTRIM(BASE_ID) AS BASE_ID","RTRIM(INST_NO) AS INST_NO","RTRIM(NPA_VALUE) AS NPA_VALUE","RTRIM(WRITE_OFF_VALUE) AS WRITE_OFF_VALUE","RTRIM(UNDRAWN_COMP) AS UNDRAWN_COMP","RTRIM(BASM_BASE_ID) AS BASM_BASE_ID","RTRIM(BASM_RATE_ID) AS BASM_RATE_ID","START_DATE_01","RTRIM(E_BASE_ID) AS E_BASE_ID","RTRIM(E_RATE_ID) AS E_RATE_ID","MI006_ACCR_INCEPT","MI006_NO_INST_PAID","RTRIM(MI006_CAPN_METHOD) AS MI006_CAPN_METHOD","RTRIM(MI006_CCC_REGULATED_IND) AS MI006_CCC_REGULATED_IND","MI006_FUND_CODE","MI006_EXTRACTED_DATE","MI006_INT_ONLY_EXP_DATE","RTRIM(MI006_REVOLVING_CR_IND) AS MI006_REVOLVING_CR_IND","MI006_PROV_AMT","M1006_ORIGINAL_EXP_DT","MI006_FIRST_ADV_DATE","MI006_PUR_CONTRACT_NO","MI006_COMMISSION_AMT","MI006_TYPE","RTRIM(MI006_STOP_ACCRUAL) AS MI006_STOP_ACCRUAL","MI006_SHADOW_INT_ACCURAL","MI006_SHADOW_CURR_YR_INT","MI006_TIER_GROUP_ID","MI006_SETELMENT_ACCT_NO","MI006_DATE_OF_SALE","MI006_RETENTION_AMOUNT","RTRIM(MI006_RETENTION_PERIOD) AS MI006_RETENTION_PERIOD","RTRIM(MI006_SECURITY_IND) AS MI006_SECURITY_IND","RTRIM(MI006_ACCR_TYPE) AS MI006_ACCR_TYPE","MI006_ACQUISITION_FEE","RTRIM(MI006_AKPK_CODE) AS MI006_AKPK_CODE","MI006_APPROVED_AUTH","MI006_BOIS_5TH_SCHED_ISS_DT","MI006_BOIS_ACQUISITION_FEE","MI006_BOIS_APPROVED_BY","MI006_BOIS_GROSS_EFF_YIELD","MI006_BOIS_LAST_ADV_DATE","MI006_BOIS_NET_EFF_YEILD","MI006_BOIS_NPA_BAL","MI006_BOIS_PROV_AMT","MI006_BOIS_PUR_CNTRT_NO","MI006_BOIS_SETTLE_ACCT","MI006_BOIS_SHDW_CURR_YR_INT","MI006_BOIS_SHDW_INT_ACCR","MI006_BOIS_TOT_ACCR_CAP","MI006_BUS_TYPE","MI006_DUE_DATE","MI006_FIFTH_SCHD_ISS_DAT","MI006_INDEX_CODE","RTRIM(MI006_ISLAMIC_BANK) AS MI006_ISLAMIC_BANK","RTRIM(MI006_MONTHS_IN_ARR) AS MI006_MONTHS_IN_ARR","RTRIM(MI006_MULTI_TIER_FLG) AS MI006_MULTI_TIER_FLG","RTRIM(MI006_NON_ACCR_IND) AS MI006_NON_ACCR_IND","MI006_NON_WF_AMT","MI006_ORIG_BRCH_CODE","MI006_OTHER_CHARGES","MI006_PERS_BNK_ID","MI006_RTC_EXP_DATE","RTRIM(MI006_RTC_METHOD_IND) AS MI006_RTC_METHOD_IND","RTRIM(MI006_SALE_REPUR_CODE) AS MI006_SALE_REPUR_CODE","MI006_SECURITY_IND_DATE","RTRIM(MI006_STAFF_PRODUCT_IND) AS MI006_STAFF_PRODUCT_IND","MI006_STAMPING_FEE","RTRIM(MI006_TIER_METHOD) AS MI006_TIER_METHOD","RTRIM(MI006_WATCHLIST_TAG) AS MI006_WATCHLIST_TAG","RTRIM(MI006_WRITEOFF_STATUS_CODE) AS MI006_WRITEOFF_STATUS_CODE","RTRIM(MI006_INT_REPAY_FREQ) AS MI006_INT_REPAY_FREQ","MI006_BLDVNN_NXT_PRIN_REP","MI006_BLDVNN_NXT_INT_REPA","MI006_BOIS_SOLD_AMOUNT","RTRIM(MI006_BOIS_LOAN_TYPE) AS MI006_BOIS_LOAN_TYPE","RTRIM(MI006_BOIS_EX_FLAG_3) AS MI006_BOIS_EX_FLAG_3","MI006_WATCHLIST_DT","MI006_IP_PROVISION_AMOUNT","RTRIM(MI006_INT_TYPE) AS MI006_INT_TYPE","MI006_BINS_PREMIUM","MI006_BOIS_CP1_PROV_AMT","MI006_BOIS_CP3_PROV_AMT","MI006_P_WOFF_OVERDUE","MI006_TOTAL_SUB_LEDGER_AMT","RTRIM(MI006_BOIS_RECALL_IND) AS MI006_BOIS_RECALL_IND","MI006_NO_OF_INST_ARR","MI006_OUTSTANDING_PRINCI","RTRIM(MI006_PRIMRY_NPL) AS MI006_PRIMRY_NPL","MI006_BOIS_REVERSED_INT","MI006_BOIS_PROVISIONING_DT","MI006_LITIGATION_DATE","MI006_ADV_PREP_TXN_AMNT","MI006_BORM_PRINC_REDUCT_EFF","RTRIM(MI006_BOIS_RETENTION_PERIOD) AS MI006_BOIS_RETENTION_PERIOD","MI006_ACF_SL_NO","MI006_BOIS_RECALL_IND_DT","RTRIM(MI006_BOIS_LOCK_IN) AS MI006_BOIS_LOCK_IN","MI006_CP7_PROV","MI006_RECOV_AMT","RTRIM(MI006_WORKOUT) AS MI006_WORKOUT","MI006_WDV_PRIN","MI006_WDV_INT","MI006_WDV_LPI","MI006_WDV_OC","MI006_OFF_BS_AMT","RTRIM(MI006_LOCK_IN_SRT_DT) AS MI006_LOCK_IN_SRT_DT","MI006_LOCK_IN_PERIOD","MI006_DISCH_PEN_AMT1","MI006_DISCH_PEN_AMT2","MI006_DISCH_PEN_AMT3","MI006_HOUSE_MRKT_CODE","RTRIM(MI006_BOIS_CITIZEN_CODE) AS MI006_BOIS_CITIZEN_CODE","MI006_BOIS_ATTRITION_DT","RTRIM(MI006_BOIS_ATTRITION_CODE) AS MI006_BOIS_ATTRITION_CODE","MI006_BOIS_STAF_GRAC_END_DT","MI006_BOIS_PREV_CAP_BAL","MI006_SHORTFALL_DATE","RTRIM(MI006_TYPE_OF_RECALL) AS MI006_TYPE_OF_RECALL","MI006_CHCD_COLLECN_HUB_CD","RTRIM(COMPANY_CODE) AS COMPANY_CODE","REM_REPAYS","RTRIM(FINE_CDE) AS FINE_CDE","RT_INCR","PENALTY_RATE","FINE_RATE","RTRIM(FINE_METH) AS FINE_METH","RTRIM(FINE_CONDT) AS FINE_CONDT","RTRIM(RECALL_BASE_ID) AS RECALL_BASE_ID","RTRIM(RECALL_MARGIN_ID) AS RECALL_MARGIN_ID","RTRIM(MAT_RECALL_ACC_RT) AS MAT_RECALL_ACC_RT","RECALL_RATE","DD","MD","RD","MI006_MATURITY_DATE","MI006_WRITEOFF_AMOUNT","P_RATE","P_IND","RTRIM(P_BASE_ID) AS P_BASE_ID","RTRIM(P_RATE_ID) AS P_RATE_ID","RTRIM(MI006_CAPN_FREQ) AS MI006_CAPN_FREQ","RTRIM(MI006_BORM_REPAY_METHOD) AS MI006_BORM_REPAY_METHOD","MIS006_NEXT_ACCR_DT","MI006_ORIG_MAT_DATE","MI006_NOTICE_FLAG","RTRIM(MI006_AUTO_DBT_ACCT_NO) AS MI006_AUTO_DBT_ACCT_NO","MI006_AUTO_DBT_STRT_DT","MI006_AUTO_DBT_END_DT","MI006_MON_PRD_STRT_DT","MI006_MON_PRD_END_DT","MI006_RR_MARGIN","MI006_AKPK_DATE","FOUR_SCHD_ISS_DT","RULE_THRE_ISS_DT","REPO_ORDER_DATE","FFTH_SCHD_ISS_DT","NOT_ISS_DATE","MI006_MIN_REPAY_AMT","PROMPT_PAY_CNTR","BUY_BACK_CNTR","STEP_UP_PERIOD","REDRAW_AMT","RTRIM(WRITE_OFF_VAL_TAG) AS WRITE_OFF_VAL_TAG","RTRIM(WRITE_OFF_JSTFCATN) AS WRITE_OFF_JSTFCATN","WRITE_OFF_TAG_DT","RTRIM(WRITE_OFF_EXCL_TAG) AS WRITE_OFF_EXCL_TAG","RTRIM(WRITE_OFF_EXCL_RSN) AS WRITE_OFF_EXCL_RSN","WRITE_OFF_EXCL_DT","ORIG_REM_REPAYS","RTRIM(STMT_DELI) AS STMT_DELI","EMAIL_ADDRESS","CHRG_RPY_AMT","PRIN_RPY_AMT","COURT_DEP_RPY_AMT","INT_RPY_AMT","BUY_BACK_DATE","RTRIM(SALE_BUY_BACK) AS SALE_BUY_BACK","IFRS_EIR_RATE","IFRS_ESTM_LOAN_TRM","MAX_REPAY_AGE","RTRIM(MORTGAGE_FACILITY) AS MORTGAGE_FACILITY","RTRIM(SPGA_NO) AS SPGA_NO","RTRIM(DEDUCT_CODE) AS DEDUCT_CODE","RTRIM(DED_STRT_MON) AS DED_STRT_MON","RTRIM(DED_END_MON) AS DED_END_MON","RTRIM(LAW_SUIT_FLAG) AS LAW_SUIT_FLAG","GPP_INT_CAP_AMT","GPP_MON_AMORT_AMT","RNR_REM_PROF","RNR_MONTHLY_PROF","INT_THRES_CAP_PER","PP_ADD_INT_AMT","PP_PROMPT_PAY_CNTR","PP_MON_PRD_END_DT","RTRIM(REL_RED_RSN_CD) AS REL_RED_RSN_CD","RSN_CD_MAINTNCE_DT","INST_CHG_EXP_DT","RTRIM(AKPK_SPC_TAG) AS AKPK_SPC_TAG","AKPK_SPC_START_DT","AKPK_SPC_END_DT","RTRIM(AKPK_MATRIX) AS AKPK_MATRIX","MORA_ACCR_INT","RTRIM(MANUAL_IMPAIR) AS MANUAL_IMPAIR","UMA_NOTC_GEN_DT","ACC_DUE_DEF_INST","EX_FLAG_1","INSTALLMENT_NO","RTRIM(ADV_PREPAY_FLAG) AS ADV_PREPAY_FLAG","RTRIM(REL_RED_RSN_FL_CD) AS REL_RED_RSN_FL_CD","RTRIM(REN_ENHC_NEW_FL) AS REN_ENHC_NEW_FL","RTRIM(INST_REINSTATE) AS INST_REINSTATE","REINSTATE_DATE","REINSTATE_COUNT","RTRIM(MFA_FLAG) AS MFA_FLAG","MFA_COUNTER","MFA_EFF_DT","MFA_MAINT_DT","EIY_RATE").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(19)'}}, {'name': 'KEY1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'UNPAID_SOLCA_BAL', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'PRE_DISBURSE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'AUTO_DR_STRT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'RECALL_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'INST_COMM_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'ACCT_SOLD_TO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'WRITE_OFF_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'WDV_PRINCIPAL', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'WDV_INTEREST', 'type': 'decimal(17,5)', 'nullable': True, 'metadata': {}}, {'name': 'WDV_CHARGES', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'WDV_LPI', 'type': 'decimal(17,5)', 'nullable': True, 'metadata': {}}, {'name': 'STAT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'APP_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'ADV_VAL', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'INT_RATE', 'type': 'decimal(7,4)', 'nullable': True, 'metadata': {}}, {'name': 'STORE_RATE', 'type': 'decimal(7,4)', 'nullable': True, 'metadata': {}}, {'name': 'CAP_THEO_UNPD_INT', 'type': 'decimal(17,5)', 'nullable': True, 'metadata': {}}, {'name': 'UNPD_ARRS_INT_BAL', 'type': 'decimal(17,5)', 'nullable': True, 'metadata': {}}, {'name': 'UNPD_CHRG_BAL', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'UNPD_PRIN_BAL', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'THEO_UNPD_PRIN_BAL', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'COLLECTIBILITY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'CAP_UNPD_INT', 'type': 'decimal(17,5)', 'nullable': True, 'metadata': {}}, {'name': 'BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'INST_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'NPA_VALUE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'WRITE_OFF_VALUE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'UNDRAWN_COMP', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'BASM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'BASM_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'START_DATE_01', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'E_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'E_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_ACCR_INCEPT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_INST_PAID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAPN_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_CCC_REGULATED_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_FUND_CODE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EXTRACTED_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_ONLY_EXP_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REVOLVING_CR_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'M1006_ORIGINAL_EXP_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FIRST_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PUR_CONTRACT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_COMMISSION_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TYPE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STOP_ACCRUAL', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SHADOW_INT_ACCURAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SHADOW_CURR_YR_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TIER_GROUP_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SETELMENT_ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DATE_OF_SALE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RETENTION_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RETENTION_PERIOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_SECURITY_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ACCR_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ACQUISITION_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_APPROVED_AUTH', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_5TH_SCHED_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_ACQUISITION_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_APPROVED_BY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_GROSS_EFF_YIELD', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LAST_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_NET_EFF_YEILD', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_NPA_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PUR_CNTRT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SETTLE_ACCT', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SHDW_CURR_YR_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SHDW_INT_ACCR', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_TOT_ACCR_CAP', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BUS_TYPE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DUE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FIFTH_SCHD_ISS_DAT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ISLAMIC_BANK', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_MONTHS_IN_ARR', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_MULTI_TIER_FLG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NON_ACCR_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NON_WF_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ORIG_BRCH_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OTHER_CHARGES', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PERS_BNK_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RTC_EXP_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RTC_METHOD_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SALE_REPUR_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SECURITY_IND_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STAFF_PRODUCT_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_STAMPING_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TIER_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_WATCHLIST_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_WRITEOFF_STATUS_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_INT_REPAY_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BLDVNN_NXT_PRIN_REP', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_NXT_INT_REPA', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SOLD_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LOAN_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_BOIS_EX_FLAG_3', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_WATCHLIST_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_IP_PROVISION_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BINS_PREMIUM', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_CP1_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_CP3_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WOFF_OVERDUE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TOTAL_SUB_LEDGER_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_RECALL_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_NO_OF_INST_ARR', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OUTSTANDING_PRINCI', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRIMRY_NPL', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOIS_REVERSED_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PROVISIONING_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LITIGATION_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADV_PREP_TXN_AMNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_PRINC_REDUCT_EFF', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_RETENTION_PERIOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_ACF_SL_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_RECALL_IND_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LOCK_IN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_CP7_PROV', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RECOV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WORKOUT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_WDV_PRIN', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WDV_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WDV_LPI', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WDV_OC', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OFF_BS_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOCK_IN_SRT_DT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_LOCK_IN_PERIOD', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_PEN_AMT1', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_PEN_AMT2', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_PEN_AMT3', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_HOUSE_MRKT_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_CITIZEN_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOIS_ATTRITION_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_ATTRITION_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BOIS_STAF_GRAC_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PREV_CAP_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SHORTFALL_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TYPE_OF_RECALL', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_CHCD_COLLECN_HUB_CD', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'COMPANY_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'REM_REPAYS', 'type': 'decimal(5,0)', 'nullable': True, 'metadata': {}}, {'name': 'FINE_CDE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'RT_INCR', 'type': 'decimal(7,5)', 'nullable': True, 'metadata': {}}, {'name': 'PENALTY_RATE', 'type': 'decimal(7,4)', 'nullable': True, 'metadata': {}}, {'name': 'FINE_RATE', 'type': 'decimal(6,4)', 'nullable': True, 'metadata': {}}, {'name': 'FINE_METH', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'FINE_CONDT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'RECALL_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'RECALL_MARGIN_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MAT_RECALL_ACC_RT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'RECALL_RATE', 'type': 'decimal(6,4)', 'nullable': True, 'metadata': {}}, {'name': 'DD', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MD', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'RD', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MATURITY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITEOFF_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'P_RATE', 'type': 'decimal(7,4)', 'nullable': True, 'metadata': {}}, {'name': 'P_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'P_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'P_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_CAPN_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BORM_REPAY_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MIS006_NEXT_ACCR_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ORIG_MAT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NOTICE_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AUTO_DBT_ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(16)'}}, {'name': 'MI006_AUTO_DBT_STRT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AUTO_DBT_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MON_PRD_STRT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MON_PRD_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RR_MARGIN', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'FOUR_SCHD_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'RULE_THRE_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REPO_ORDER_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'FFTH_SCHD_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'NOT_ISS_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MIN_REPAY_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'PROMPT_PAY_CNTR', 'type': 'decimal(3,0)', 'nullable': True, 'metadata': {}}, {'name': 'BUY_BACK_CNTR', 'type': 'decimal(2,0)', 'nullable': True, 'metadata': {}}, {'name': 'STEP_UP_PERIOD', 'type': 'decimal(2,0)', 'nullable': True, 'metadata': {}}, {'name': 'REDRAW_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'WRITE_OFF_VAL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'WRITE_OFF_JSTFCATN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(30)'}}, {'name': 'WRITE_OFF_TAG_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'WRITE_OFF_EXCL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'WRITE_OFF_EXCL_RSN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(30)'}}, {'name': 'WRITE_OFF_EXCL_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'ORIG_REM_REPAYS', 'type': 'decimal(5,0)', 'nullable': True, 'metadata': {}}, {'name': 'STMT_DELI', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'EMAIL_ADDRESS', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'CHRG_RPY_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'PRIN_RPY_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'COURT_DEP_RPY_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'INT_RPY_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'BUY_BACK_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'SALE_BUY_BACK', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'IFRS_EIR_RATE', 'type': 'decimal(9,6)', 'nullable': True, 'metadata': {}}, {'name': 'IFRS_ESTM_LOAN_TRM', 'type': 'decimal(5,0)', 'nullable': True, 'metadata': {}}, {'name': 'MAX_REPAY_AGE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MORTGAGE_FACILITY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'SPGA_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(12)'}}, {'name': 'DEDUCT_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'DED_STRT_MON', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(6)'}}, {'name': 'DED_END_MON', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(6)'}}, {'name': 'LAW_SUIT_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'GPP_INT_CAP_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'GPP_MON_AMORT_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'RNR_REM_PROF', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'RNR_MONTHLY_PROF', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'INT_THRES_CAP_PER', 'type': 'decimal(5,2)', 'nullable': True, 'metadata': {}}, {'name': 'PP_ADD_INT_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'PP_PROMPT_PAY_CNTR', 'type': 'decimal(2,0)', 'nullable': True, 'metadata': {}}, {'name': 'PP_MON_PRD_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REL_RED_RSN_CD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'RSN_CD_MAINTNCE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'INST_CHG_EXP_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'AKPK_SPC_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'AKPK_SPC_START_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'AKPK_SPC_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'AKPK_MATRIX', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MORA_ACCR_INT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MANUAL_IMPAIR', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'UMA_NOTC_GEN_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'ACC_DUE_DEF_INST', 'type': 'decimal(2,0)', 'nullable': True, 'metadata': {}}, {'name': 'EX_FLAG_1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'INSTALLMENT_NO', 'type': 'decimal(3,0)', 'nullable': True, 'metadata': {}}, {'name': 'ADV_PREPAY_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'REL_RED_RSN_FL_CD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'REN_ENHC_NEW_FL', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'INST_REINSTATE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'REINSTATE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REINSTATE_COUNT', 'type': 'decimal(2,0)', 'nullable': True, 'metadata': {}}, {'name': 'MFA_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MFA_COUNTER', 'type': 'decimal(2,0)', 'nullable': True, 'metadata': {}}, {'name': 'MFA_EFF_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MFA_MAINT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'EIY_RATE', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__NETZ_SRC_TBL_NM_lnk_Source_v PURGE").show()
    
    print("NETZ_SRC_TBL_NM_lnk_Source_v")
    
    print(NETZ_SRC_TBL_NM_lnk_Source_v.schema.json())
    
    print("count:{}".format(NETZ_SRC_TBL_NM_lnk_Source_v.count()))
    
    NETZ_SRC_TBL_NM_lnk_Source_v.show(1000,False)
    
    NETZ_SRC_TBL_NM_lnk_Source_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__NETZ_SRC_TBL_NM_lnk_Source_v")
    

@task
def V0A105(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def V25A0(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task.pyspark(conn_id="spark-local")
def srt_keys_srt_keys_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    NETZ_SRC_BASM_srt_keys_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__NETZ_SRC_BASM_srt_keys_v')
    
    srt_keys_srt_keys_Part_v=NETZ_SRC_BASM_srt_keys_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__srt_keys_srt_keys_Part_v PURGE").show()
    
    print("srt_keys_srt_keys_Part_v")
    
    print(srt_keys_srt_keys_Part_v.schema.json())
    
    print("count:{}".format(srt_keys_srt_keys_Part_v.count()))
    
    srt_keys_srt_keys_Part_v.show(1000,False)
    
    srt_keys_srt_keys_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__srt_keys_srt_keys_Part_v")
    

@task.pyspark(conn_id="spark-local")
def TRN_CONVERT_lnk_Source_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    NETZ_SRC_TBL_NM_lnk_Source_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__NETZ_SRC_TBL_NM_lnk_Source_v')
    
    TRN_CONVERT_lnk_Source_Part_v=NETZ_SRC_TBL_NM_lnk_Source_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__TRN_CONVERT_lnk_Source_Part_v PURGE").show()
    
    print("TRN_CONVERT_lnk_Source_Part_v")
    
    print(TRN_CONVERT_lnk_Source_Part_v.schema.json())
    
    print("count:{}".format(TRN_CONVERT_lnk_Source_Part_v.count()))
    
    TRN_CONVERT_lnk_Source_Part_v.show(1000,False)
    
    TRN_CONVERT_lnk_Source_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__TRN_CONVERT_lnk_Source_Part_v")
    

@task.pyspark(conn_id="spark-local")
def srt_keys(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    srt_keys_srt_keys_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__srt_keys_srt_keys_Part_v')
    
    srt_keys_v = srt_keys_srt_keys_Part_v
    
    srt_keys_srt_KeyChange_v_0 = srt_keys_v.orderBy(col('SOC_NO').asc(),col('BASE_ID').asc(),col('RATE_ID').asc(),col('BASM_DATE').desc(),col('BASM_TIME').asc())
    
    srt_keys_srt_KeyChange_v = srt_keys_srt_KeyChange_v_0.select(col('SOC_NO').cast('string').alias('SOC_NO'),col('BASE_ID').cast('string').alias('BASE_ID'),col('RATE_ID').cast('string').alias('RATE_ID'),col('BASM_DATE').cast('integer').alias('BASM_DATE'),col('BASM_TIME').cast('integer').alias('BASM_TIME'),col('RATE').cast('decimal(16,3)').alias('RATE'),col('KEY_POINTER').cast('string').alias('KEY_POINTER'),col('EFF_RATE').cast('decimal(16,4)').alias('EFF_RATE'),col('INST_NO').cast('string').alias('INST_NO'),col('L_BASM_BASE_ID').cast('string').alias('L_BASM_BASE_ID'),col('L_BASM_RATE_ID').cast('string').alias('L_BASM_RATE_ID'),col('MI006_BASM_BASE_ID').cast('string').alias('MI006_BASM_BASE_ID'),col('MI006_PRIME_RATE').cast('decimal(9,5)').alias('MI006_PRIME_RATE'),col('MI006_BASM_DESCRIPTION').cast('string').alias('MI006_BASM_DESCRIPTION'))
    
    srt_keys_srt_KeyChange_v = srt_keys_srt_KeyChange_v.selectExpr("RTRIM(SOC_NO) AS SOC_NO","RTRIM(BASE_ID) AS BASE_ID","RTRIM(RATE_ID) AS RATE_ID","BASM_DATE","BASM_TIME","RATE","RTRIM(KEY_POINTER) AS KEY_POINTER","EFF_RATE","RTRIM(INST_NO) AS INST_NO","RTRIM(L_BASM_BASE_ID) AS L_BASM_BASE_ID","RTRIM(L_BASM_RATE_ID) AS L_BASM_RATE_ID","MI006_BASM_BASE_ID","MI006_PRIME_RATE","MI006_BASM_DESCRIPTION").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'SOC_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'BASM_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'BASM_TIME', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'RATE', 'type': 'decimal(16,3)', 'nullable': True, 'metadata': {}}, {'name': 'KEY_POINTER', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'EFF_RATE', 'type': 'decimal(16,4)', 'nullable': True, 'metadata': {}}, {'name': 'INST_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'L_BASM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'L_BASM_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_BASM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRIME_RATE', 'type': 'decimal(9,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BASM_DESCRIPTION', 'type': 'string', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__srt_keys_srt_KeyChange_v PURGE").show()
    
    print("srt_keys_srt_KeyChange_v")
    
    print(srt_keys_srt_KeyChange_v.schema.json())
    
    print("count:{}".format(srt_keys_srt_KeyChange_v.count()))
    
    srt_keys_srt_KeyChange_v.show(1000,False)
    
    srt_keys_srt_KeyChange_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__srt_keys_srt_KeyChange_v")
    

@task.pyspark(conn_id="spark-local")
def TRN_CONVERT(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    TRN_CONVERT_lnk_Source_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__TRN_CONVERT_lnk_Source_Part_v')
    
    TRN_CONVERT_v = TRN_CONVERT_lnk_Source_Part_v.withColumn('NPL', expr("""IF(COLLECTIBILITY < WRITE_OFF_VALUE, (IF(COLLECTIBILITY < NPA_VALUE, 'N', 'Y')), 'Y')""").cast('string').alias('NPL')).withColumn('WRTFLG', expr("""CASE WHEN TRIM(WRITE_OFF_FLAG) = '' OR WRITE_OFF_FLAG IS NULL THEN 'XX' WHEN WRITE_OFF_FLAG = 'P' THEN 'X' ELSE 'Y' END""").cast('string').alias('WRTFLG')).withColumn('ACT', expr("""IF(ACCT_SOLD_TO = '1', 'X', (IF(ACCT_SOLD_TO = '2', 'XX', 'Y')))""").cast('string').alias('ACT')).withColumn('S1', expr("""IF(RECALL_IND = 'D', INT_RATE, 0)""").cast('string').alias('S1')).withColumn('S2', expr("""IF(FINE_CDE = 0 OR FINE_CDE = 99, (IF(FINE_CONDT = 0 OR FINE_CONDT = 99, 0, (IF(PENALTY_RATE > 0, PENALTY_RATE, 0)))), (IF(PENALTY_RATE > 0, PENALTY_RATE, FINE_RATE)))""").cast('string').alias('S2')).withColumn('S3', lit(0).cast('string').alias('S3')).withColumn('S4', lit(0).cast('string').alias('S4'))
    
    TRN_CONVERT_Left_v = TRN_CONVERT_v.select(col('INT_RATE').cast('decimal(7,4)').alias('INT_RATE'),col('B_KEY').cast('string').alias('B_KEY'),col('KEY1').cast('string').alias('SOC_NO'),expr("""IF(RECALL_IND = 'D', INT_RATE, STORE_RATE)""").cast('decimal(8,4)').alias('MI006_INT_RATE'),expr("""IF(RECALL_IND = 'D', INT_RATE, STORE_RATE)""").cast('decimal(8,4)').alias('MI006_STORE_RATE'),expr("""IF(STAT = 22 OR STAT = 40, UNPAID_SOLCA_BAL, 0)""").cast('decimal(18,3)').alias('MI006_DISCH_AMOUNT'),expr("""IF(ACT = 'X' OR ACT = 'XX', 'Y', 'N')""").cast('string').alias('MI006_ACCT_SOLD_TO'),col('MI006_ACCR_INCEPT').cast('decimal(18,3)').alias('MI006_ACCR_INCEPT'),expr("""CASE WHEN (stat > 1 AND stat < 9 AND TRIM(write_off_flag) = '' AND pre_disburse_dt <> 0 AND TRIM(pre_disburse_dt) <> '' AND undrawn_comp = 'Y') THEN (app_amt - COALESCE(adv_val, 0)) ELSE 0 END AS result_column""").cast('decimal(18,3)').alias('MI006_AMT_UNDRAWN'),expr("""IF(AUTO_DR_STRT_DT <> 0, 'Y', 'N')""").cast('string').alias('MI006_AUTO_DEBIT_IND'),expr("""IF(STAT >= 6, ADV_DATE, 0)""").cast('integer').alias('MI006_BOIS_LST_ADV_DATE'),expr("""IF(NPL = 'Y', 'NP', (IF(NPL = 'N', 'PL', '')))""").cast('string').alias('MI006_CLASS_CODE'),expr("""IF(NPL = 'Y', 'C', '')""").cast('string').alias('MI006_CLASSIFY_FLAG'),expr("""IF(INST_COMM_DATE > 0, INST_COMM_DATE, (IF(NOT (ISNULL(START_DATE_01)), START_DATE_01, 0)))""").cast('integer').alias('MI006_FIRST_INST_DATE'),expr("""IF(INST_COMM_DATE > 0, INST_COMM_DATE, (IF(NOT (ISNULL(START_DATE_01)), START_DATE_01, 0)))""").cast('integer').alias('MI006_FIRST_REPAY_DATE'),expr("""IF(ACCT_SOLD_TO = '1', 'Y', 'N')""").cast('string').alias('MI006_LOAN_SOLD_CAGAMAS_FLAG'),col('MI006_NO_INST_PAID').cast('string').alias('MI006_NO_INST_PAID'),col('NPL').cast('string').alias('MI006_NPA_IND'),col('NPL').cast('string').alias('MI006_NPL_CLASS_STAT'),col('NPL').cast('string').alias('MI006_NPL_STATUS'),expr("""IF(WRTFLG = 'X', (UNPD_CHRG_BAL - (IF(ISNOTNULL((WDV_CHARGES)), (WDV_CHARGES), 0))), 0)""").cast('decimal(18,3)').alias('MI006_P_WF_FEE_AMT'),expr("""IF(WRTFLG = 'X', (CAP_UNPD_INT - (IF(ISNOTNULL((WDV_INTEREST)), (WDV_INTEREST), 0))), 0)""").cast('decimal(18,3)').alias('MI006_P_WF_INT_AMT'),expr("""IF(WRTFLG = 'X', (UNPD_ARRS_INT_BAL - (IF(ISNOTNULL((WDV_LPI)), (WDV_LPI), 0))), 0)""").cast('decimal(18,3)').alias('MI006_P_WF_LT_CHRG_AMT'),expr("""IF(WRTFLG = 'X', (UNPD_PRIN_BAL - (IF(ISNOTNULL((WDV_PRINCIPAL)), (WDV_PRINCIPAL), 0))), 0)""").cast('decimal(18,3)').alias('MI006_P_WF_PRIN_AMT'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM RATE_ID)) <> '', RATE_ID, BASM_RATE_ID)""").cast('string').alias('MI006_RATE_ID'),expr("""IF(TRIM(WRITE_OFF_FLAG) = '', 'N', 'Y')""").cast('string').alias('MI006_WRITEOFF_FLAG'),expr("""CASE WHEN TRIM(WRITE_OFF_FLAG) = 'F2' THEN 'Y' ELSE 'N' END""").cast('string').alias('MI006_BORM_CHARGE_OFF_FLAG'),expr("""IF(ACT = 'X', 'CAGAMAS', (IF(ACT = 'XX', 'AmMortgage', '')))""").cast('string').alias('MI006_AM_ASSEC_TAG'),expr("""IF(ACT = 'X', 'CAGAMAS', (IF(ACT = 'XX', 'AmMortgage', '')))""").cast('string').alias('MI006_ACCT_SOLD_TO_NAME'),expr("""IF(WRTFLG <> 'XX', (CAP_UNPD_INT - (IF(ISNOTNULL((WDV_INTEREST)), (WDV_INTEREST), 0))), '')""").cast('decimal(18,3)').alias('MI006_WRIT_OFF_INT'),expr("""IF(WRTFLG <> 'XX', (UNPD_PRIN_BAL - (IF(ISNOTNULL((WDV_PRINCIPAL)), (WDV_PRINCIPAL), 0))), '')""").cast('decimal(18,3)').alias('MI006_WRIT_OFF_PRIN'),expr("""IF(WRTFLG <> 'XX', (UNPD_CHRG_BAL - (IF(ISNOTNULL((WDV_CHARGES)), (WDV_CHARGES), 0))), '')""").cast('decimal(18,3)').alias('MI006_WRIT_OFF_CHARGE'),col('BASM_BASE_ID').cast('string').alias('BASM_BASE_ID'),col('BASM_RATE_ID').cast('string').alias('BASM_RATE_ID'),col('E_RATE_ID').cast('string').alias('RATE_ID'),col('E_BASE_ID').cast('string').alias('BASE_ID'),col('RATE_ID').cast('string').alias('L_RATE_ID'),col('MI006_CAPN_METHOD').cast('string').alias('MI006_CAPN_METHOD'),col('MI006_CCC_REGULATED_IND').cast('string').alias('MI006_CCC_REGULATED_IND'),col('MI006_FUND_CODE').cast('integer').alias('MI006_FUND_CODE'),col('MI006_EXTRACTED_DATE').cast('integer').alias('MI006_EXTRACTED_DATE'),col('MI006_INT_ONLY_EXP_DATE').cast('integer').alias('MI006_INT_ONLY_EXP_DATE'),col('MI006_REVOLVING_CR_IND').cast('string').alias('MI006_REVOLVING_CR_IND'),col('MI006_PROV_AMT').cast('decimal(18,3)').alias('MI006_PROV_AMT'),col('M1006_ORIGINAL_EXP_DT').cast('integer').alias('M1006_ORIGINAL_EXP_DT'),col('MI006_FIRST_ADV_DATE').cast('integer').alias('MI006_FIRST_ADV_DATE'),col('MI006_PUR_CONTRACT_NO').cast('string').alias('MI006_PUR_CONTRACT_NO'),col('MI006_COMMISSION_AMT').cast('decimal(18,3)').alias('MI006_COMMISSION_AMT'),col('MI006_TYPE').cast('string').alias('MI006_TYPE'),col('MI006_STOP_ACCRUAL').cast('string').alias('MI006_STOP_ACCRUAL'),col('MI006_SHADOW_INT_ACCURAL').cast('decimal(18,3)').alias('MI006_SHADOW_INT_ACCURAL'),col('MI006_SHADOW_CURR_YR_INT').cast('decimal(18,3)').alias('MI006_SHADOW_CURR_YR_INT'),col('MI006_TIER_GROUP_ID').cast('string').alias('MI006_TIER_GROUP_ID'),col('MI006_SETELMENT_ACCT_NO').cast('string').alias('MI006_SETELMENT_ACCT_NO'),col('MI006_DATE_OF_SALE').cast('integer').alias('MI006_DATE_OF_SALE'),col('MI006_RETENTION_AMOUNT').cast('decimal(18,3)').alias('MI006_RETENTION_AMOUNT'),col('MI006_RETENTION_PERIOD').cast('string').alias('MI006_RETENTION_PERIOD'),col('MI006_SECURITY_IND').cast('string').alias('MI006_SECURITY_IND'),col('MI006_ACCR_TYPE').cast('string').alias('MI006_ACCR_TYPE'),col('MI006_ACQUISITION_FEE').cast('decimal(18,3)').alias('MI006_ACQUISITION_FEE'),col('MI006_AKPK_CODE').cast('string').alias('MI006_AKPK_CODE'),col('MI006_APPROVED_AUTH').cast('string').alias('MI006_APPROVED_AUTH'),col('MI006_BOIS_5TH_SCHED_ISS_DT').cast('integer').alias('MI006_BOIS_5TH_SCHED_ISS_DT'),col('MI006_BOIS_ACQUISITION_FEE').cast('decimal(18,3)').alias('MI006_BOIS_ACQUISITION_FEE'),col('MI006_BOIS_APPROVED_BY').cast('string').alias('MI006_BOIS_APPROVED_BY'),col('MI006_BOIS_GROSS_EFF_YIELD').cast('decimal(4,2)').alias('MI006_BOIS_GROSS_EFF_YIELD'),col('MI006_BOIS_LAST_ADV_DATE').cast('integer').alias('MI006_BOIS_LAST_ADV_DATE'),col('MI006_BOIS_NET_EFF_YEILD').cast('decimal(4,2)').alias('MI006_BOIS_NET_EFF_YEILD'),col('MI006_BOIS_NPA_BAL').cast('decimal(18,3)').alias('MI006_BOIS_NPA_BAL'),col('MI006_BOIS_PROV_AMT').cast('decimal(18,3)').alias('MI006_BOIS_PROV_AMT'),col('MI006_BOIS_PUR_CNTRT_NO').cast('string').alias('MI006_BOIS_PUR_CNTRT_NO'),col('MI006_BOIS_SETTLE_ACCT').cast('string').alias('MI006_BOIS_SETTLE_ACCT'),col('MI006_BOIS_SHDW_CURR_YR_INT').cast('decimal(18,3)').alias('MI006_BOIS_SHDW_CURR_YR_INT'),col('MI006_BOIS_SHDW_INT_ACCR').cast('decimal(18,3)').alias('MI006_BOIS_SHDW_INT_ACCR'),col('MI006_BOIS_TOT_ACCR_CAP').cast('decimal(18,3)').alias('MI006_BOIS_TOT_ACCR_CAP'),col('MI006_BUS_TYPE').cast('string').alias('MI006_BUS_TYPE'),col('MI006_DUE_DATE').cast('integer').alias('MI006_DUE_DATE'),col('MI006_FIFTH_SCHD_ISS_DAT').cast('integer').alias('MI006_FIFTH_SCHD_ISS_DAT'),col('MI006_INDEX_CODE').cast('string').alias('MI006_INDEX_CODE'),col('MI006_ISLAMIC_BANK').cast('string').alias('MI006_ISLAMIC_BANK'),col('MI006_MONTHS_IN_ARR').cast('string').alias('MI006_MONTHS_IN_ARR'),col('MI006_MULTI_TIER_FLG').cast('string').alias('MI006_MULTI_TIER_FLG'),col('MI006_NON_ACCR_IND').cast('string').alias('MI006_NON_ACCR_IND'),col('MI006_NON_WF_AMT').cast('decimal(18,3)').alias('MI006_NON_WF_AMT'),col('MI006_ORIG_BRCH_CODE').cast('string').alias('MI006_ORIG_BRCH_CODE'),col('MI006_OTHER_CHARGES').cast('decimal(18,3)').alias('MI006_OTHER_CHARGES'),col('MI006_PERS_BNK_ID').cast('string').alias('MI006_PERS_BNK_ID'),col('MI006_RTC_EXP_DATE').cast('integer').alias('MI006_RTC_EXP_DATE'),col('MI006_RTC_METHOD_IND').cast('string').alias('MI006_RTC_METHOD_IND'),col('MI006_SALE_REPUR_CODE').cast('string').alias('MI006_SALE_REPUR_CODE'),col('MI006_SECURITY_IND_DATE').cast('integer').alias('MI006_SECURITY_IND_DATE'),col('MI006_STAFF_PRODUCT_IND').cast('string').alias('MI006_STAFF_PRODUCT_IND'),col('MI006_STAMPING_FEE').cast('decimal(18,3)').alias('MI006_STAMPING_FEE'),col('MI006_TIER_METHOD').cast('string').alias('MI006_TIER_METHOD'),col('MI006_WATCHLIST_TAG').cast('string').alias('MI006_WATCHLIST_TAG'),col('MI006_WRITEOFF_STATUS_CODE').cast('string').alias('MI006_WRITEOFF_STATUS_CODE'),col('MI006_INT_REPAY_FREQ').cast('string').alias('MI006_INT_REPAY_FREQ'),col('MI006_BLDVNN_NXT_PRIN_REP').cast('integer').alias('MI006_BLDVNN_NXT_PRIN_REP'),col('MI006_BLDVNN_NXT_INT_REPA').cast('integer').alias('MI006_BLDVNN_NXT_INT_REPA'),col('MI006_BOIS_SOLD_AMOUNT').cast('decimal(18,3)').alias('MI006_BOIS_SOLD_AMOUNT'),col('MI006_BOIS_LOAN_TYPE').cast('string').alias('MI006_BOIS_LOAN_TYPE'),col('MI006_BOIS_EX_FLAG_3').cast('string').alias('MI006_BOIS_EX_FLAG_3'),col('MI006_WATCHLIST_DT').cast('integer').alias('MI006_WATCHLIST_DT'),col('MI006_IP_PROVISION_AMOUNT').cast('decimal(18,3)').alias('MI006_IP_PROVISION_AMOUNT'),col('MI006_INT_TYPE').cast('string').alias('MI006_INT_TYPE'),col('MI006_BINS_PREMIUM').cast('decimal(18,3)').alias('MI006_BINS_PREMIUM'),col('MI006_BOIS_CP1_PROV_AMT').cast('decimal(18,3)').alias('MI006_BOIS_CP1_PROV_AMT'),col('MI006_BOIS_CP3_PROV_AMT').cast('decimal(18,3)').alias('MI006_BOIS_CP3_PROV_AMT'),col('MI006_P_WOFF_OVERDUE').cast('decimal(18,3)').alias('MI006_P_WOFF_OVERDUE'),col('MI006_TOTAL_SUB_LEDGER_AMT').cast('decimal(18,3)').alias('MI006_TOTAL_SUB_LEDGER_AMT'),col('MI006_BOIS_RECALL_IND').cast('string').alias('MI006_BOIS_RECALL_IND'),col('MI006_NO_OF_INST_ARR').cast('integer').alias('MI006_NO_OF_INST_ARR'),col('MI006_OUTSTANDING_PRINCI').cast('decimal(18,3)').alias('MI006_OUTSTANDING_PRINCI'),col('MI006_PRIMRY_NPL').cast('string').alias('MI006_PRIMRY_NPL'),col('MI006_BOIS_REVERSED_INT').cast('decimal(18,3)').alias('MI006_BOIS_REVERSED_INT'),col('MI006_BOIS_PROVISIONING_DT').cast('integer').alias('MI006_BOIS_PROVISIONING_DT'),col('MI006_LITIGATION_DATE').cast('integer').alias('MI006_LITIGATION_DATE'),col('MI006_ADV_PREP_TXN_AMNT').cast('decimal(18,3)').alias('MI006_ADV_PREP_TXN_AMNT'),col('MI006_BORM_PRINC_REDUCT_EFF').cast('decimal(18,3)').alias('MI006_BORM_PRINC_REDUCT_EFF'),col('MI006_BOIS_RETENTION_PERIOD').cast('string').alias('MI006_BOIS_RETENTION_PERIOD'),col('MI006_ACF_SL_NO').cast('string').alias('MI006_ACF_SL_NO'),col('MI006_BOIS_RECALL_IND_DT').cast('integer').alias('MI006_BOIS_RECALL_IND_DT'),col('MI006_BOIS_LOCK_IN').cast('string').alias('MI006_BOIS_LOCK_IN'),col('MI006_CP7_PROV').cast('decimal(18,3)').alias('MI006_CP7_PROV'),col('MI006_RECOV_AMT').cast('decimal(18,3)').alias('MI006_RECOV_AMT'),col('MI006_WORKOUT').cast('string').alias('MI006_WORKOUT'),col('MI006_WDV_PRIN').cast('decimal(18,3)').alias('MI006_WDV_PRIN'),col('MI006_WDV_INT').cast('decimal(18,3)').alias('MI006_WDV_INT'),col('MI006_WDV_LPI').cast('decimal(18,3)').alias('MI006_WDV_LPI'),col('MI006_WDV_OC').cast('decimal(18,3)').alias('MI006_WDV_OC'),col('MI006_OFF_BS_AMT').cast('decimal(18,3)').alias('MI006_OFF_BS_AMT'),col('MI006_LOCK_IN_SRT_DT').cast('string').alias('MI006_LOCK_IN_SRT_DT'),col('MI006_LOCK_IN_PERIOD').cast('integer').alias('MI006_LOCK_IN_PERIOD'),col('MI006_DISCH_PEN_AMT1').cast('decimal(18,3)').alias('MI006_DISCH_PEN_AMT1'),col('MI006_DISCH_PEN_AMT2').cast('decimal(18,3)').alias('MI006_DISCH_PEN_AMT2'),col('MI006_DISCH_PEN_AMT3').cast('decimal(18,3)').alias('MI006_DISCH_PEN_AMT3'),col('MI006_HOUSE_MRKT_CODE').cast('string').alias('MI006_HOUSE_MRKT_CODE'),col('MI006_BOIS_CITIZEN_CODE').cast('string').alias('MI006_BOIS_CITIZEN_CODE'),col('MI006_BOIS_ATTRITION_DT').cast('integer').alias('MI006_BOIS_ATTRITION_DT'),col('MI006_BOIS_ATTRITION_CODE').cast('string').alias('MI006_BOIS_ATTRITION_CODE'),col('MI006_BOIS_STAF_GRAC_END_DT').cast('integer').alias('MI006_BOIS_STAF_GRAC_END_DT'),col('MI006_BOIS_PREV_CAP_BAL').cast('decimal(18,3)').alias('MI006_BOIS_PREV_CAP_BAL'),col('MI006_SHORTFALL_DATE').cast('integer').alias('MI006_SHORTFALL_DATE'),col('MI006_TYPE_OF_RECALL').cast('string').alias('MI006_TYPE_OF_RECALL'),col('MI006_CHCD_COLLECN_HUB_CD').cast('string').alias('MI006_CHCD_COLLECN_HUB_CD'),col('COMPANY_CODE').cast('string').alias('COMPANY_CODE'),col('MI006_WRITEOFF_AMOUNT').cast('decimal(18,3)').alias('MI006_WRITEOFF_AMOUNT'),col('P_RATE').cast('decimal(7,4)').alias('P_RATE'),col('P_IND').cast('string').alias('P_IND'),col('P_BASE_ID').cast('string').alias('P_BASE_ID'),col('P_RATE_ID').cast('string').alias('P_RATE_ID'),col('RECALL_BASE_ID').cast('string').alias('RECALL_BASE_ID'),col('RECALL_MARGIN_ID').cast('string').alias('RECALL_MARGIN_ID'),col('MAT_RECALL_ACC_RT').cast('string').alias('MAT_RECALL_ACC_RT'),col('RECALL_RATE').cast('decimal(6,4)').alias('RECALL_RATE'),col('RECALL_IND').cast('string').alias('RECALL_IND'),col('FINE_RATE').cast('decimal(6,4)').alias('FINE_RATE'),col('RT_INCR').cast('decimal(7,5)').alias('RT_INCR'),col('MI006_MATURITY_DATE').cast('integer').alias('MI006_MATURITY_DATE'),col('MI006_CAPN_FREQ').cast('string').alias('MI006_CAPN_FREQ'),col('MI006_BORM_REPAY_METHOD').cast('string').alias('MI006_BORM_REPAY_METHOD'),col('MIS006_NEXT_ACCR_DT').cast('integer').alias('MIS006_NEXT_ACCR_DT'),col('MI006_ORIG_MAT_DATE').cast('integer').alias('MI006_ORIG_MAT_DATE'),col('MI006_NOTICE_FLAG').cast('string').alias('MI006_NOTICE_FLAG'),expr("""IF(MI006_NOTICE_FLAG = '1', FOUR_SCHD_ISS_DT, IF(MI006_NOTICE_FLAG = '2', RULE_THRE_ISS_DT, IF(MI006_NOTICE_FLAG = '3' OR MI006_NOTICE_FLAG = '6', REPO_ORDER_DATE, IF(MI006_NOTICE_FLAG = '4' OR MI006_NOTICE_FLAG = '7', FFTH_SCHD_ISS_DT, NOT_ISS_DATE))))""").cast('integer').alias('MI006_NOTICE_DATE'),col('MI006_AUTO_DBT_ACCT_NO').cast('string').alias('MI006_AUTO_DBT_ACCT_NO'),col('MI006_AUTO_DBT_STRT_DT').cast('integer').alias('MI006_AUTO_DBT_STRT_DT'),col('MI006_AUTO_DBT_END_DT').cast('integer').alias('MI006_AUTO_DBT_END_DT'),col('MI006_MON_PRD_STRT_DT').cast('integer').alias('MI006_MON_PRD_STRT_DT'),col('MI006_MON_PRD_END_DT').cast('integer').alias('MI006_MON_PRD_END_DT'),col('MI006_RR_MARGIN').cast('decimal(4,2)').alias('MI006_RR_MARGIN'),col('MI006_AKPK_DATE').cast('integer').alias('MI006_AKPK_DATE'),col('MI006_MIN_REPAY_AMT').cast('decimal(18,3)').alias('MI006_MIN_REPAY_AMT'),col('FOUR_SCHD_ISS_DT').cast('integer').alias('MI006_FOUR_SCHD_ISS_DT'),col('RULE_THRE_ISS_DT').cast('integer').alias('MI006_RULE_THREE_ISS_DT'),col('REPO_ORDER_DATE').cast('integer').alias('MI006_REPO_ORDER_DT'),col('NOT_ISS_DATE').cast('integer').alias('MI006_NOT_ISS_DT'),col('PROMPT_PAY_CNTR').cast('integer').alias('MI006_PROMPT_PAY_CNTR'),col('BUY_BACK_CNTR').cast('integer').alias('MI006_BUY_BACK_CNTR'),col('STEP_UP_PERIOD').cast('integer').alias('MI006_STEP_UP_PERIOD'),col('REDRAW_AMT').cast('decimal(18,3)').alias('MI006_REDRAW_AMT'),col('WRITE_OFF_VAL_TAG').cast('string').alias('MI006_WRITE_OFF_VAL_TAG'),col('WRITE_OFF_JSTFCATN').cast('string').alias('MI006_WRITE_OFF_JSTFCATN'),col('WRITE_OFF_TAG_DT').cast('integer').alias('MI006_WRITE_OFF_TAG_DT'),col('WRITE_OFF_EXCL_TAG').cast('string').alias('MI006_WRITE_OFF_EXCL_TAG'),col('WRITE_OFF_EXCL_RSN').cast('string').alias('MI006_WRITE_OFF_EXCL_RSN'),col('WRITE_OFF_EXCL_DT').cast('integer').alias('MI006_WRITE_OFF_EXCL_DT'),col('ORIG_REM_REPAYS').cast('decimal(5,0)').alias('MI006_ORIG_REM_REPAYS'),col('STMT_DELI').cast('string').alias('MI006_STMT_DELIVERY'),col('EMAIL_ADDRESS').cast('string').alias('MI006_EMAIL_ADDR'),col('CHRG_RPY_AMT').cast('decimal(18,3)').alias('MI006_CHRG_RPY_AMT'),col('PRIN_RPY_AMT').cast('decimal(18,3)').alias('MI006_PRIN_RPY_AMT'),col('COURT_DEP_RPY_AMT').cast('decimal(18,3)').alias('MI006_COURT_DEP_RPY_AMT'),col('INT_RPY_AMT').cast('decimal(18,3)').alias('MI006_INT_RPY_AMT'),col('BUY_BACK_DATE').cast('integer').alias('MI006_BUY_BACK_DATE'),col('SALE_BUY_BACK').cast('string').alias('MI006_BUY_BACK_IND'),col('IFRS_EIR_RATE').cast('decimal(9,6)').alias('MI006_PREV_DAY_IFRS_EIR_RATE'),col('IFRS_ESTM_LOAN_TRM').cast('decimal(5,0)').alias('MI006_PREV_DAY_IFRS_ESTM_LOAN_TERM'),col('MAX_REPAY_AGE').cast('string').alias('MI006_MAX_REPAY_AGE'),col('MORTGAGE_FACILITY').cast('string').alias('MI006_MORTGAGE_FACILITY_IND'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM (IF(ISNOTNULL((SPGA_NO)), (SPGA_NO), ''))))""").cast('string').alias('MI006_SPGA_NO'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM (IF(ISNOTNULL((DEDUCT_CODE)), (DEDUCT_CODE), ''))))""").cast('string').alias('MI006_DEDUCT_CODE'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM (IF(ISNOTNULL((DED_STRT_MON)), (DED_STRT_MON), ''))))""").cast('string').alias('MI006_DED_STRT_MON'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM (IF(ISNOTNULL((DED_END_MON)), (DED_END_MON), ''))))""").cast('string').alias('MI006_DED_END_MON'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM (IF(ISNOTNULL((LAW_SUIT_FLAG)), (LAW_SUIT_FLAG), ''))))""").cast('string').alias('MI006_EXC_DPD_WOFF_VAL'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM GPP_INT_CAP_AMT))""").cast('decimal(17,3)').alias('MI006_GPP_INT_CAP_AMT'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM GPP_MON_AMORT_AMT))""").cast('decimal(17,3)').alias('MI006_GPP_AMORT_AMT_MTH'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM RNR_REM_PROF))""").cast('decimal(17,3)').alias('MI006_RNR_REM_PROFIT'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM RNR_MONTHLY_PROF))""").cast('decimal(17,3)').alias('MI006_RNR_MONTHLY_PROFIT'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM INT_THRES_CAP_PER))""").cast('decimal(5,2)').alias('MI006_INT_THRES_CAP_PER'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM PP_ADD_INT_AMT))""").cast('decimal(17,3)').alias('MI006_PP_ADD_INT_AMT'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM PP_PROMPT_PAY_CNTR))""").cast('decimal(2,0)').alias('MI006_PP_PROMPT_PAY_COUNTER'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM PP_MON_PRD_END_DT))""").cast('integer').alias('MI006_PP_MON_PRD_END_DATE'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM REL_RED_RSN_CD))""").cast('string').alias('MI006_REL_RED_RSN_CD'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM RSN_CD_MAINTNCE_DT))""").cast('integer').alias('MI006_RSN_CD_MAINTNCE_DT'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM INST_CHG_EXP_DT))""").cast('integer').alias('MI006_INST_CHG_EXP_DT'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM AKPK_SPC_TAG))""").cast('string').alias('MI006_AKPK_SPC_TAG'),col('AKPK_SPC_START_DT').cast('integer').alias('MI006_AKPK_SPC_START_DT'),col('AKPK_SPC_END_DT').cast('integer').alias('MI006_AKPK_SPC_END_DT'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM AKPK_MATRIX))""").cast('string').alias('MI006_AKPK_MATRIX'),col('MORA_ACCR_INT').cast('decimal(17,3)').alias('MI006_MORA_ACCR_INT'),expr("""IF(PRE_DISBURSE_DT > 0, PRE_DISBURSE_DT, (IF(NOT (ISNULL(PRE_DISBURSE_DT)), PRE_DISBURSE_DT, 0)))""").cast('integer').alias('MI006_PRE_DISB_DOC_STAT_DT'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM MANUAL_IMPAIR))""").cast('string').alias('MI006_MANUAL_IMPAIR'),expr("""IF(UMA_NOTC_GEN_DT > 0, UMA_NOTC_GEN_DT, (IF(NOT (ISNULL(UMA_NOTC_GEN_DT)), UMA_NOTC_GEN_DT, 0)))""").cast('integer').alias('MI006_UMA_NOTC_GEN_DT'),col('ACC_DUE_DEF_INST').cast('string').alias('MI006_ACC_DUE_DEF_INST'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM (IF(ISNOTNULL((EX_FLAG_1)), (EX_FLAG_1), ''))))""").cast('string').alias('MI006_EX_FLAG_1'),expr("""DS_DECIMALTOSTRING(INSTALLMENT_NO, 'suppress_zero')""").cast('string').alias('MI006_INSTALLMENT_NO'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM (IF(ISNOTNULL((ADV_PREPAY_FLAG)), (ADV_PREPAY_FLAG), ''))))""").cast('string').alias('MI006_ADV_PREPAY_FLAG'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM (IF(ISNOTNULL((REL_RED_RSN_FL_CD)), (REL_RED_RSN_FL_CD), ''))))""").cast('string').alias('MI006_REL_RED_RSN_FL_CD'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM (IF(ISNOTNULL((REN_ENHC_NEW_FL)), (REN_ENHC_NEW_FL), ''))))""").cast('string').alias('MI006_SETLEMENT_WAIVER'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM (IF(ISNOTNULL((INST_REINSTATE)), (INST_REINSTATE), ''))))""").cast('string').alias('MI006_INST_REINSTATE'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM (IF(ISNOTNULL((REINSTATE_DATE)), (REINSTATE_DATE), ''))))""").cast('integer').alias('MI006_REINSTATE_DATE'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM (IF(ISNOTNULL((REINSTATE_COUNT)), (REINSTATE_COUNT), ''))))""").cast('string').alias('MI006_REINSTATE_COUNT'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM MFA_FLAG))""").cast('string').alias('MI006_MFA_FLAG'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM MFA_COUNTER))""").cast('string').alias('MI006_MFA_COUNTER'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM MFA_EFF_DT))""").cast('integer').alias('MI006_MFA_EFF_DT'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM MFA_MAINT_DT))""").cast('integer').alias('MI006_MFA_MAINT_DT'),expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM EIY_RATE))""").cast('decimal(5,2)').alias('MI006_EIY'))
    
    TRN_CONVERT_Left_v = TRN_CONVERT_Left_v.selectExpr("INT_RATE","B_KEY","RTRIM(SOC_NO) AS SOC_NO","MI006_INT_RATE","MI006_STORE_RATE","MI006_DISCH_AMOUNT","RTRIM(MI006_ACCT_SOLD_TO) AS MI006_ACCT_SOLD_TO","MI006_ACCR_INCEPT","MI006_AMT_UNDRAWN","RTRIM(MI006_AUTO_DEBIT_IND) AS MI006_AUTO_DEBIT_IND","MI006_BOIS_LST_ADV_DATE","MI006_CLASS_CODE","RTRIM(MI006_CLASSIFY_FLAG) AS MI006_CLASSIFY_FLAG","MI006_FIRST_INST_DATE","MI006_FIRST_REPAY_DATE","RTRIM(MI006_LOAN_SOLD_CAGAMAS_FLAG) AS MI006_LOAN_SOLD_CAGAMAS_FLAG","MI006_NO_INST_PAID","RTRIM(MI006_NPA_IND) AS MI006_NPA_IND","RTRIM(MI006_NPL_CLASS_STAT) AS MI006_NPL_CLASS_STAT","RTRIM(MI006_NPL_STATUS) AS MI006_NPL_STATUS","MI006_P_WF_FEE_AMT","MI006_P_WF_INT_AMT","MI006_P_WF_LT_CHRG_AMT","MI006_P_WF_PRIN_AMT","MI006_RATE_ID","RTRIM(MI006_WRITEOFF_FLAG) AS MI006_WRITEOFF_FLAG","MI006_BORM_CHARGE_OFF_FLAG","MI006_AM_ASSEC_TAG","MI006_ACCT_SOLD_TO_NAME","MI006_WRIT_OFF_INT","MI006_WRIT_OFF_PRIN","MI006_WRIT_OFF_CHARGE","RTRIM(BASM_BASE_ID) AS BASM_BASE_ID","RTRIM(BASM_RATE_ID) AS BASM_RATE_ID","RTRIM(RATE_ID) AS RATE_ID","RTRIM(BASE_ID) AS BASE_ID","RTRIM(L_RATE_ID) AS L_RATE_ID","RTRIM(MI006_CAPN_METHOD) AS MI006_CAPN_METHOD","RTRIM(MI006_CCC_REGULATED_IND) AS MI006_CCC_REGULATED_IND","MI006_FUND_CODE","MI006_EXTRACTED_DATE","MI006_INT_ONLY_EXP_DATE","RTRIM(MI006_REVOLVING_CR_IND) AS MI006_REVOLVING_CR_IND","MI006_PROV_AMT","M1006_ORIGINAL_EXP_DT","MI006_FIRST_ADV_DATE","MI006_PUR_CONTRACT_NO","MI006_COMMISSION_AMT","MI006_TYPE","RTRIM(MI006_STOP_ACCRUAL) AS MI006_STOP_ACCRUAL","MI006_SHADOW_INT_ACCURAL","MI006_SHADOW_CURR_YR_INT","MI006_TIER_GROUP_ID","MI006_SETELMENT_ACCT_NO","MI006_DATE_OF_SALE","MI006_RETENTION_AMOUNT","RTRIM(MI006_RETENTION_PERIOD) AS MI006_RETENTION_PERIOD","RTRIM(MI006_SECURITY_IND) AS MI006_SECURITY_IND","RTRIM(MI006_ACCR_TYPE) AS MI006_ACCR_TYPE","MI006_ACQUISITION_FEE","RTRIM(MI006_AKPK_CODE) AS MI006_AKPK_CODE","MI006_APPROVED_AUTH","MI006_BOIS_5TH_SCHED_ISS_DT","MI006_BOIS_ACQUISITION_FEE","MI006_BOIS_APPROVED_BY","MI006_BOIS_GROSS_EFF_YIELD","MI006_BOIS_LAST_ADV_DATE","MI006_BOIS_NET_EFF_YEILD","MI006_BOIS_NPA_BAL","MI006_BOIS_PROV_AMT","MI006_BOIS_PUR_CNTRT_NO","MI006_BOIS_SETTLE_ACCT","MI006_BOIS_SHDW_CURR_YR_INT","MI006_BOIS_SHDW_INT_ACCR","MI006_BOIS_TOT_ACCR_CAP","MI006_BUS_TYPE","MI006_DUE_DATE","MI006_FIFTH_SCHD_ISS_DAT","MI006_INDEX_CODE","RTRIM(MI006_ISLAMIC_BANK) AS MI006_ISLAMIC_BANK","RTRIM(MI006_MONTHS_IN_ARR) AS MI006_MONTHS_IN_ARR","RTRIM(MI006_MULTI_TIER_FLG) AS MI006_MULTI_TIER_FLG","RTRIM(MI006_NON_ACCR_IND) AS MI006_NON_ACCR_IND","MI006_NON_WF_AMT","MI006_ORIG_BRCH_CODE","MI006_OTHER_CHARGES","MI006_PERS_BNK_ID","MI006_RTC_EXP_DATE","RTRIM(MI006_RTC_METHOD_IND) AS MI006_RTC_METHOD_IND","RTRIM(MI006_SALE_REPUR_CODE) AS MI006_SALE_REPUR_CODE","MI006_SECURITY_IND_DATE","RTRIM(MI006_STAFF_PRODUCT_IND) AS MI006_STAFF_PRODUCT_IND","MI006_STAMPING_FEE","RTRIM(MI006_TIER_METHOD) AS MI006_TIER_METHOD","RTRIM(MI006_WATCHLIST_TAG) AS MI006_WATCHLIST_TAG","RTRIM(MI006_WRITEOFF_STATUS_CODE) AS MI006_WRITEOFF_STATUS_CODE","RTRIM(MI006_INT_REPAY_FREQ) AS MI006_INT_REPAY_FREQ","MI006_BLDVNN_NXT_PRIN_REP","MI006_BLDVNN_NXT_INT_REPA","MI006_BOIS_SOLD_AMOUNT","RTRIM(MI006_BOIS_LOAN_TYPE) AS MI006_BOIS_LOAN_TYPE","RTRIM(MI006_BOIS_EX_FLAG_3) AS MI006_BOIS_EX_FLAG_3","MI006_WATCHLIST_DT","MI006_IP_PROVISION_AMOUNT","RTRIM(MI006_INT_TYPE) AS MI006_INT_TYPE","MI006_BINS_PREMIUM","MI006_BOIS_CP1_PROV_AMT","MI006_BOIS_CP3_PROV_AMT","MI006_P_WOFF_OVERDUE","MI006_TOTAL_SUB_LEDGER_AMT","RTRIM(MI006_BOIS_RECALL_IND) AS MI006_BOIS_RECALL_IND","MI006_NO_OF_INST_ARR","MI006_OUTSTANDING_PRINCI","RTRIM(MI006_PRIMRY_NPL) AS MI006_PRIMRY_NPL","MI006_BOIS_REVERSED_INT","MI006_BOIS_PROVISIONING_DT","MI006_LITIGATION_DATE","MI006_ADV_PREP_TXN_AMNT","MI006_BORM_PRINC_REDUCT_EFF","RTRIM(MI006_BOIS_RETENTION_PERIOD) AS MI006_BOIS_RETENTION_PERIOD","MI006_ACF_SL_NO","MI006_BOIS_RECALL_IND_DT","RTRIM(MI006_BOIS_LOCK_IN) AS MI006_BOIS_LOCK_IN","MI006_CP7_PROV","MI006_RECOV_AMT","RTRIM(MI006_WORKOUT) AS MI006_WORKOUT","MI006_WDV_PRIN","MI006_WDV_INT","MI006_WDV_LPI","MI006_WDV_OC","MI006_OFF_BS_AMT","RTRIM(MI006_LOCK_IN_SRT_DT) AS MI006_LOCK_IN_SRT_DT","MI006_LOCK_IN_PERIOD","MI006_DISCH_PEN_AMT1","MI006_DISCH_PEN_AMT2","MI006_DISCH_PEN_AMT3","MI006_HOUSE_MRKT_CODE","RTRIM(MI006_BOIS_CITIZEN_CODE) AS MI006_BOIS_CITIZEN_CODE","MI006_BOIS_ATTRITION_DT","RTRIM(MI006_BOIS_ATTRITION_CODE) AS MI006_BOIS_ATTRITION_CODE","MI006_BOIS_STAF_GRAC_END_DT","MI006_BOIS_PREV_CAP_BAL","MI006_SHORTFALL_DATE","RTRIM(MI006_TYPE_OF_RECALL) AS MI006_TYPE_OF_RECALL","MI006_CHCD_COLLECN_HUB_CD","RTRIM(COMPANY_CODE) AS COMPANY_CODE","MI006_WRITEOFF_AMOUNT","P_RATE","P_IND","RTRIM(P_BASE_ID) AS P_BASE_ID","RTRIM(P_RATE_ID) AS P_RATE_ID","RTRIM(RECALL_BASE_ID) AS RECALL_BASE_ID","RTRIM(RECALL_MARGIN_ID) AS RECALL_MARGIN_ID","RTRIM(MAT_RECALL_ACC_RT) AS MAT_RECALL_ACC_RT","RECALL_RATE","RTRIM(RECALL_IND) AS RECALL_IND","FINE_RATE","RT_INCR","MI006_MATURITY_DATE","RTRIM(MI006_CAPN_FREQ) AS MI006_CAPN_FREQ","RTRIM(MI006_BORM_REPAY_METHOD) AS MI006_BORM_REPAY_METHOD","MIS006_NEXT_ACCR_DT","MI006_ORIG_MAT_DATE","MI006_NOTICE_FLAG","MI006_NOTICE_DATE","MI006_AUTO_DBT_ACCT_NO","MI006_AUTO_DBT_STRT_DT","MI006_AUTO_DBT_END_DT","MI006_MON_PRD_STRT_DT","MI006_MON_PRD_END_DT","MI006_RR_MARGIN","MI006_AKPK_DATE","MI006_MIN_REPAY_AMT","MI006_FOUR_SCHD_ISS_DT","MI006_RULE_THREE_ISS_DT","MI006_REPO_ORDER_DT","MI006_NOT_ISS_DT","MI006_PROMPT_PAY_CNTR","MI006_BUY_BACK_CNTR","MI006_STEP_UP_PERIOD","MI006_REDRAW_AMT","RTRIM(MI006_WRITE_OFF_VAL_TAG) AS MI006_WRITE_OFF_VAL_TAG","RTRIM(MI006_WRITE_OFF_JSTFCATN) AS MI006_WRITE_OFF_JSTFCATN","MI006_WRITE_OFF_TAG_DT","RTRIM(MI006_WRITE_OFF_EXCL_TAG) AS MI006_WRITE_OFF_EXCL_TAG","RTRIM(MI006_WRITE_OFF_EXCL_RSN) AS MI006_WRITE_OFF_EXCL_RSN","MI006_WRITE_OFF_EXCL_DT","MI006_ORIG_REM_REPAYS","RTRIM(MI006_STMT_DELIVERY) AS MI006_STMT_DELIVERY","MI006_EMAIL_ADDR","MI006_CHRG_RPY_AMT","MI006_PRIN_RPY_AMT","MI006_COURT_DEP_RPY_AMT","MI006_INT_RPY_AMT","MI006_BUY_BACK_DATE","RTRIM(MI006_BUY_BACK_IND) AS MI006_BUY_BACK_IND","MI006_PREV_DAY_IFRS_EIR_RATE","MI006_PREV_DAY_IFRS_ESTM_LOAN_TERM","MI006_MAX_REPAY_AGE","MI006_MORTGAGE_FACILITY_IND","MI006_SPGA_NO","MI006_DEDUCT_CODE","MI006_DED_STRT_MON","MI006_DED_END_MON","MI006_EXC_DPD_WOFF_VAL","MI006_GPP_INT_CAP_AMT","MI006_GPP_AMORT_AMT_MTH","MI006_RNR_REM_PROFIT","MI006_RNR_MONTHLY_PROFIT","MI006_INT_THRES_CAP_PER","MI006_PP_ADD_INT_AMT","MI006_PP_PROMPT_PAY_COUNTER","MI006_PP_MON_PRD_END_DATE","MI006_REL_RED_RSN_CD","MI006_RSN_CD_MAINTNCE_DT","MI006_INST_CHG_EXP_DT","MI006_AKPK_SPC_TAG","MI006_AKPK_SPC_START_DT","MI006_AKPK_SPC_END_DT","MI006_AKPK_MATRIX","MI006_MORA_ACCR_INT","MI006_PRE_DISB_DOC_STAT_DT","MI006_MANUAL_IMPAIR","MI006_UMA_NOTC_GEN_DT","MI006_ACC_DUE_DEF_INST","MI006_EX_FLAG_1","MI006_INSTALLMENT_NO","MI006_ADV_PREPAY_FLAG","MI006_REL_RED_RSN_FL_CD","MI006_SETLEMENT_WAIVER","MI006_INST_REINSTATE","MI006_REINSTATE_DATE","MI006_REINSTATE_COUNT","MI006_MFA_FLAG","MI006_MFA_COUNTER","MI006_MFA_EFF_DT","MI006_MFA_MAINT_DT","MI006_EIY").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'INT_RATE', 'type': 'decimal(7,4)', 'nullable': True, 'metadata': {}}, {'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'SOC_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_INT_RATE', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STORE_RATE', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACCT_SOLD_TO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ACCR_INCEPT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AMT_UNDRAWN', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AUTO_DEBIT_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOIS_LST_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CLASS_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CLASSIFY_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_FIRST_INST_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FIRST_REPAY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_SOLD_CAGAMAS_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NO_INST_PAID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NPA_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NPL_CLASS_STAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NPL_STATUS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_P_WF_FEE_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WF_INT_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WF_LT_CHRG_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WF_PRIN_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITEOFF_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BORM_CHARGE_OFF_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AM_ASSEC_TAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACCT_SOLD_TO_NAME', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRIT_OFF_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRIT_OFF_PRIN', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRIT_OFF_CHARGE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'BASM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'BASM_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'L_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_CAPN_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_CCC_REGULATED_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_FUND_CODE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EXTRACTED_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_ONLY_EXP_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REVOLVING_CR_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'M1006_ORIGINAL_EXP_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FIRST_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PUR_CONTRACT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_COMMISSION_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TYPE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STOP_ACCRUAL', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SHADOW_INT_ACCURAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SHADOW_CURR_YR_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TIER_GROUP_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SETELMENT_ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DATE_OF_SALE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RETENTION_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RETENTION_PERIOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_SECURITY_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ACCR_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ACQUISITION_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_APPROVED_AUTH', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_5TH_SCHED_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_ACQUISITION_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_APPROVED_BY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_GROSS_EFF_YIELD', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LAST_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_NET_EFF_YEILD', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_NPA_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PUR_CNTRT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SETTLE_ACCT', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SHDW_CURR_YR_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SHDW_INT_ACCR', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_TOT_ACCR_CAP', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BUS_TYPE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DUE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FIFTH_SCHD_ISS_DAT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ISLAMIC_BANK', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_MONTHS_IN_ARR', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_MULTI_TIER_FLG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NON_ACCR_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NON_WF_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ORIG_BRCH_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OTHER_CHARGES', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PERS_BNK_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RTC_EXP_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RTC_METHOD_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SALE_REPUR_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SECURITY_IND_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STAFF_PRODUCT_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_STAMPING_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TIER_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_WATCHLIST_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_WRITEOFF_STATUS_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_INT_REPAY_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BLDVNN_NXT_PRIN_REP', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_NXT_INT_REPA', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SOLD_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LOAN_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_BOIS_EX_FLAG_3', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_WATCHLIST_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_IP_PROVISION_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BINS_PREMIUM', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_CP1_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_CP3_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WOFF_OVERDUE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TOTAL_SUB_LEDGER_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_RECALL_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_NO_OF_INST_ARR', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OUTSTANDING_PRINCI', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRIMRY_NPL', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOIS_REVERSED_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PROVISIONING_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LITIGATION_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADV_PREP_TXN_AMNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_PRINC_REDUCT_EFF', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_RETENTION_PERIOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_ACF_SL_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_RECALL_IND_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LOCK_IN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_CP7_PROV', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RECOV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WORKOUT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_WDV_PRIN', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WDV_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WDV_LPI', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WDV_OC', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OFF_BS_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOCK_IN_SRT_DT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_LOCK_IN_PERIOD', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_PEN_AMT1', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_PEN_AMT2', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_PEN_AMT3', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_HOUSE_MRKT_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_CITIZEN_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOIS_ATTRITION_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_ATTRITION_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BOIS_STAF_GRAC_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PREV_CAP_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SHORTFALL_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TYPE_OF_RECALL', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_CHCD_COLLECN_HUB_CD', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'COMPANY_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_WRITEOFF_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'P_RATE', 'type': 'decimal(7,4)', 'nullable': True, 'metadata': {}}, {'name': 'P_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'P_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'P_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'RECALL_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'RECALL_MARGIN_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MAT_RECALL_ACC_RT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'RECALL_RATE', 'type': 'decimal(6,4)', 'nullable': True, 'metadata': {}}, {'name': 'RECALL_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'FINE_RATE', 'type': 'decimal(6,4)', 'nullable': True, 'metadata': {}}, {'name': 'RT_INCR', 'type': 'decimal(7,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MATURITY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAPN_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BORM_REPAY_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MIS006_NEXT_ACCR_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ORIG_MAT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NOTICE_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NOTICE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AUTO_DBT_ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AUTO_DBT_STRT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AUTO_DBT_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MON_PRD_STRT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MON_PRD_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RR_MARGIN', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MIN_REPAY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FOUR_SCHD_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RULE_THREE_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REPO_ORDER_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NOT_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PROMPT_PAY_CNTR', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BUY_BACK_CNTR', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STEP_UP_PERIOD', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REDRAW_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITE_OFF_VAL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_WRITE_OFF_JSTFCATN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(30)'}}, {'name': 'MI006_WRITE_OFF_TAG_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITE_OFF_EXCL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_WRITE_OFF_EXCL_RSN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(30)'}}, {'name': 'MI006_WRITE_OFF_EXCL_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ORIG_REM_REPAYS', 'type': 'decimal(5,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STMT_DELIVERY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_EMAIL_ADDR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CHRG_RPY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRIN_RPY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_COURT_DEP_RPY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_RPY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BUY_BACK_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BUY_BACK_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_PREV_DAY_IFRS_EIR_RATE', 'type': 'decimal(9,6)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PREV_DAY_IFRS_ESTM_LOAN_TERM', 'type': 'decimal(5,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MAX_REPAY_AGE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MORTGAGE_FACILITY_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SPGA_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DEDUCT_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DED_STRT_MON', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DED_END_MON', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EXC_DPD_WOFF_VAL', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GPP_INT_CAP_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GPP_AMORT_AMT_MTH', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RNR_REM_PROFIT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RNR_MONTHLY_PROFIT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_THRES_CAP_PER', 'type': 'decimal(5,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PP_ADD_INT_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PP_PROMPT_PAY_COUNTER', 'type': 'decimal(2,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PP_MON_PRD_END_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REL_RED_RSN_CD', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RSN_CD_MAINTNCE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INST_CHG_EXP_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_SPC_TAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_SPC_START_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_SPC_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_MATRIX', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MORA_ACCR_INT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRE_DISB_DOC_STAT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MANUAL_IMPAIR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UMA_NOTC_GEN_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACC_DUE_DEF_INST', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EX_FLAG_1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INSTALLMENT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADV_PREPAY_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REL_RED_RSN_FL_CD', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SETLEMENT_WAIVER', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INST_REINSTATE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REINSTATE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REINSTATE_COUNT', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MFA_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MFA_COUNTER', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MFA_EFF_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MFA_MAINT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EIY', 'type': 'decimal(5,2)', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__TRN_CONVERT_Left_v PURGE").show()
    
    print("TRN_CONVERT_Left_v")
    
    print(TRN_CONVERT_Left_v.schema.json())
    
    print("count:{}".format(TRN_CONVERT_Left_v.count()))
    
    TRN_CONVERT_Left_v.show(1000,False)
    
    TRN_CONVERT_Left_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__TRN_CONVERT_Left_v")
    

@task.pyspark(conn_id="spark-local")
def srt_KeyChange_srt_KeyChange_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    srt_keys_srt_KeyChange_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__srt_keys_srt_KeyChange_v')
    
    srt_KeyChange_srt_KeyChange_Part_v=srt_keys_srt_KeyChange_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__srt_KeyChange_srt_KeyChange_Part_v PURGE").show()
    
    print("srt_KeyChange_srt_KeyChange_Part_v")
    
    print(srt_KeyChange_srt_KeyChange_Part_v.schema.json())
    
    print("count:{}".format(srt_KeyChange_srt_KeyChange_Part_v.count()))
    
    srt_KeyChange_srt_KeyChange_Part_v.show(1000,False)
    
    srt_KeyChange_srt_KeyChange_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__srt_KeyChange_srt_KeyChange_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Join_57_Left_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    TRN_CONVERT_Left_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__TRN_CONVERT_Left_v')
    
    Join_57_Left_Part_v=TRN_CONVERT_Left_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Join_57_Left_Part_v PURGE").show()
    
    print("Join_57_Left_Part_v")
    
    print(Join_57_Left_Part_v.schema.json())
    
    print("count:{}".format(Join_57_Left_Part_v.count()))
    
    Join_57_Left_Part_v.show(1000,False)
    
    Join_57_Left_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Join_57_Left_Part_v")
    

@task.pyspark(conn_id="spark-local")
def srt_KeyChange(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    srt_KeyChange_srt_KeyChange_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__srt_KeyChange_srt_KeyChange_Part_v')
    
    srt_KeyChange_v = srt_KeyChange_srt_KeyChange_Part_v
    
    window_spec = Window.orderBy("SOC_NO","BASE_ID","RATE_ID")
    columns_to_hash = ["SOC_NO","BASE_ID","RATE_ID"]
    srt_KeyChange_fil_KeyChange_v_0 = srt_KeyChange_v.orderBy(col('SOC_NO').asc(),col('BASE_ID').asc(),col('RATE_ID').asc()).withColumn("KEY_HASH", F.hash(*columns_to_hash))
    df = srt_KeyChange_fil_KeyChange_v_0.withColumn("_PREV_KEY_HASH", F.lag("SOC_NO").over(window_spec))

    print(df.schema)
    # KeyChange() logic: 1 if changed (or first row), 0 if same
    df = df.withColumn("KeyChange", 
        F.when(F.col("_PREV_KEY_HASH").isNull() | (F.col("_PREV_KEY_HASH") != F.col("KEY_HASH")), 1)
        .otherwise(0)
    )
    srt_KeyChange_fil_KeyChange_v = df.select(col('SOC_NO').cast('string').alias('SOC_NO'),col('BASE_ID').cast('string').alias('BASE_ID'),col('RATE_ID').cast('string').alias('RATE_ID'),col('BASM_DATE').cast('integer').alias('BASM_DATE'),col('BASM_TIME').cast('integer').alias('BASM_TIME'),col('RATE').cast('decimal(16,3)').alias('RATE'),col('KEY_POINTER').cast('string').alias('KEY_POINTER'),col('EFF_RATE').cast('decimal(16,4)').alias('EFF_RATE'),col('INST_NO').cast('string').alias('INST_NO'),col('L_BASM_BASE_ID').cast('string').alias('L_BASM_BASE_ID'),col('L_BASM_RATE_ID').cast('string').alias('L_BASM_RATE_ID'),col('MI006_BASM_BASE_ID').cast('string').alias('MI006_BASM_BASE_ID'),col('MI006_PRIME_RATE').cast('decimal(9,5)').alias('MI006_PRIME_RATE'),col('MI006_BASM_DESCRIPTION').cast('string').alias('MI006_BASM_DESCRIPTION'),expr("""KeyChange""").cast('integer').alias('keyChange'))
    
    srt_KeyChange_fil_KeyChange_v = srt_KeyChange_fil_KeyChange_v.selectExpr("RTRIM(SOC_NO) AS SOC_NO","RTRIM(BASE_ID) AS BASE_ID","RTRIM(RATE_ID) AS RATE_ID","BASM_DATE","BASM_TIME","RATE","RTRIM(KEY_POINTER) AS KEY_POINTER","EFF_RATE","RTRIM(INST_NO) AS INST_NO","RTRIM(L_BASM_BASE_ID) AS L_BASM_BASE_ID","RTRIM(L_BASM_RATE_ID) AS L_BASM_RATE_ID","MI006_BASM_BASE_ID","MI006_PRIME_RATE","MI006_BASM_DESCRIPTION","keyChange").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'SOC_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'BASM_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'BASM_TIME', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'RATE', 'type': 'decimal(16,3)', 'nullable': True, 'metadata': {}}, {'name': 'KEY_POINTER', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'EFF_RATE', 'type': 'decimal(16,4)', 'nullable': True, 'metadata': {}}, {'name': 'INST_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'L_BASM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'L_BASM_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_BASM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRIME_RATE', 'type': 'decimal(9,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BASM_DESCRIPTION', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'keyChange', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__srt_KeyChange_fil_KeyChange_v PURGE").show()
    
    print("srt_KeyChange_fil_KeyChange_v")
    
    print(srt_KeyChange_fil_KeyChange_v.schema.json())
    
    print("count:{}".format(srt_KeyChange_fil_KeyChange_v.count()))
    
    srt_KeyChange_fil_KeyChange_v.show(1000,False)
    
    srt_KeyChange_fil_KeyChange_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__srt_KeyChange_fil_KeyChange_v")
    

@task.pyspark(conn_id="spark-local")
def fil_KeyChange_fil_KeyChange_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    srt_KeyChange_fil_KeyChange_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__srt_KeyChange_fil_KeyChange_v')
    
    fil_KeyChange_fil_KeyChange_Part_v=srt_KeyChange_fil_KeyChange_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__fil_KeyChange_fil_KeyChange_Part_v PURGE").show()
    
    print("fil_KeyChange_fil_KeyChange_Part_v")
    
    print(fil_KeyChange_fil_KeyChange_Part_v.schema.json())
    
    print("count:{}".format(fil_KeyChange_fil_KeyChange_Part_v.count()))
    
    fil_KeyChange_fil_KeyChange_Part_v.show(1000,False)
    
    fil_KeyChange_fil_KeyChange_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__fil_KeyChange_fil_KeyChange_Part_v")
    

@task.pyspark(conn_id="spark-local")
def fil_KeyChange(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    fil_KeyChange_fil_KeyChange_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__fil_KeyChange_fil_KeyChange_Part_v')
    
    fil_KeyChange_v = fil_KeyChange_fil_KeyChange_Part_v
    
    fil_KeyChange_DSLink148_v_0 = fil_KeyChange_v
    
    fil_KeyChange_DSLink148_v = fil_KeyChange_DSLink148_v_0.select(col('SOC_NO').cast('string').alias('SOC_NO'),col('BASE_ID').cast('string').alias('BASE_ID'),col('RATE_ID').cast('string').alias('RATE_ID'),col('BASM_DATE').cast('integer').alias('BASM_DATE'),col('BASM_TIME').cast('integer').alias('BASM_TIME'),col('RATE').cast('decimal(16,3)').alias('RATE'),col('KEY_POINTER').cast('string').alias('KEY_POINTER'),col('EFF_RATE').cast('decimal(16,4)').alias('EFF_RATE'),col('INST_NO').cast('string').alias('INST_NO'),col('L_BASM_BASE_ID').cast('string').alias('L_BASM_BASE_ID'),col('L_BASM_RATE_ID').cast('string').alias('L_BASM_RATE_ID'),col('MI006_BASM_BASE_ID').cast('string').alias('MI006_BASM_BASE_ID'),col('MI006_PRIME_RATE').cast('decimal(9,5)').alias('MI006_PRIME_RATE'),col('MI006_BASM_DESCRIPTION').cast('string').alias('MI006_BASM_DESCRIPTION'),col('keyChange').cast('integer').alias('keyChange'))
    
    fil_KeyChange_DSLink148_v = fil_KeyChange_DSLink148_v.selectExpr("RTRIM(SOC_NO) AS SOC_NO","RTRIM(BASE_ID) AS BASE_ID","RTRIM(RATE_ID) AS RATE_ID","BASM_DATE","BASM_TIME","RATE","RTRIM(KEY_POINTER) AS KEY_POINTER","EFF_RATE","RTRIM(INST_NO) AS INST_NO","RTRIM(L_BASM_BASE_ID) AS L_BASM_BASE_ID","RTRIM(L_BASM_RATE_ID) AS L_BASM_RATE_ID","MI006_BASM_BASE_ID","MI006_PRIME_RATE","MI006_BASM_DESCRIPTION","keyChange").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'SOC_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'BASM_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'BASM_TIME', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'RATE', 'type': 'decimal(16,3)', 'nullable': True, 'metadata': {}}, {'name': 'KEY_POINTER', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'EFF_RATE', 'type': 'decimal(16,4)', 'nullable': True, 'metadata': {}}, {'name': 'INST_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'L_BASM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'L_BASM_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_BASM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRIME_RATE', 'type': 'decimal(9,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BASM_DESCRIPTION', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'keyChange', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__fil_KeyChange_DSLink148_v PURGE").show()
    
    print("fil_KeyChange_DSLink148_v")
    
    print(fil_KeyChange_DSLink148_v.schema.json())
    
    print("count:{}".format(fil_KeyChange_DSLink148_v.count()))
    
    fil_KeyChange_DSLink148_v.show(1000,False)
    
    fil_KeyChange_DSLink148_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__fil_KeyChange_DSLink148_v")
    

@task.pyspark(conn_id="spark-local")
def Copy_149_DSLink148_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    fil_KeyChange_DSLink148_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__fil_KeyChange_DSLink148_v')
    
    Copy_149_DSLink148_Part_v=fil_KeyChange_DSLink148_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Copy_149_DSLink148_Part_v PURGE").show()
    
    print("Copy_149_DSLink148_Part_v")
    
    print(Copy_149_DSLink148_Part_v.schema.json())
    
    print("count:{}".format(Copy_149_DSLink148_Part_v.count()))
    
    Copy_149_DSLink148_Part_v.show(1000,False)
    
    Copy_149_DSLink148_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Copy_149_DSLink148_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Copy_149(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Copy_149_DSLink148_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Copy_149_DSLink148_Part_v')
    
    Copy_149_v=Copy_149_DSLink148_Part_v
    
    Copy_149_Right_v = Copy_149_v.select(col('SOC_NO').cast('string').alias('SOC_NO'),col('BASE_ID').cast('string').alias('BASE_ID'),col('RATE_ID').cast('string').alias('RATE_ID'),col('BASM_DATE').cast('integer').alias('BASM_DATE'),col('BASM_TIME').cast('integer').alias('BASM_TIME'),col('RATE').cast('decimal(16,3)').alias('RATE'),col('KEY_POINTER').cast('string').alias('KEY_POINTER'),col('EFF_RATE').cast('decimal(16,4)').alias('EFF_RATE'),col('INST_NO').cast('string').alias('INST_NO'),col('L_BASM_BASE_ID').cast('string').alias('L_BASM_BASE_ID'),col('L_BASM_RATE_ID').cast('string').alias('L_BASM_RATE_ID'),col('MI006_BASM_BASE_ID').cast('string').alias('MI006_BASM_BASE_ID'),col('MI006_PRIME_RATE').cast('decimal(9,5)').alias('MI006_PRIME_RATE'),col('MI006_BASM_DESCRIPTION').cast('string').alias('MI006_BASM_DESCRIPTION'),col('keyChange').cast('integer').alias('keyChange'))
    
    Copy_149_Right_v = Copy_149_Right_v.selectExpr("RTRIM(SOC_NO) AS SOC_NO","RTRIM(BASE_ID) AS BASE_ID","RTRIM(RATE_ID) AS RATE_ID","BASM_DATE","BASM_TIME","RATE","RTRIM(KEY_POINTER) AS KEY_POINTER","EFF_RATE","RTRIM(INST_NO) AS INST_NO","RTRIM(L_BASM_BASE_ID) AS L_BASM_BASE_ID","RTRIM(L_BASM_RATE_ID) AS L_BASM_RATE_ID","MI006_BASM_BASE_ID","MI006_PRIME_RATE","MI006_BASM_DESCRIPTION","keyChange").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'SOC_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'BASM_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'BASM_TIME', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'RATE', 'type': 'decimal(16,3)', 'nullable': True, 'metadata': {}}, {'name': 'KEY_POINTER', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'EFF_RATE', 'type': 'decimal(16,4)', 'nullable': True, 'metadata': {}}, {'name': 'INST_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'L_BASM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'L_BASM_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_BASM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRIME_RATE', 'type': 'decimal(9,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BASM_DESCRIPTION', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'keyChange', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Copy_149_Right_v PURGE").show()
    
    print("Copy_149_Right_v")
    
    print(Copy_149_Right_v.schema.json())
    
    print("count:{}".format(Copy_149_Right_v.count()))
    
    Copy_149_Right_v.show(1000,False)
    
    Copy_149_Right_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Copy_149_Right_v")
    
    Copy_149_Penalty_v = Copy_149_v.select(col('SOC_NO').cast('string').alias('SOC_NO'),col('BASE_ID').cast('string').alias('P_BASE_ID'),col('RATE_ID').cast('string').alias('P_RATE_ID'),col('EFF_RATE').cast('decimal(16,4)').alias('P_EFF_RATE'))
    
    Copy_149_Penalty_v = Copy_149_Penalty_v.selectExpr("RTRIM(SOC_NO) AS SOC_NO","RTRIM(P_BASE_ID) AS P_BASE_ID","RTRIM(P_RATE_ID) AS P_RATE_ID","P_EFF_RATE").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'SOC_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'P_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'P_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'P_EFF_RATE', 'type': 'decimal(16,4)', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Copy_149_Penalty_v PURGE").show()
    
    print("Copy_149_Penalty_v")
    
    print(Copy_149_Penalty_v.schema.json())
    
    print("count:{}".format(Copy_149_Penalty_v.count()))
    
    Copy_149_Penalty_v.show(1000,False)
    
    Copy_149_Penalty_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Copy_149_Penalty_v")
    

@task.pyspark(conn_id="spark-local")
def Join_57_Right_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Copy_149_Right_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Copy_149_Right_v')
    
    Join_57_Right_Part_v=Copy_149_Right_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Join_57_Right_Part_v PURGE").show()
    
    print("Join_57_Right_Part_v")
    
    print(Join_57_Right_Part_v.schema.json())
    
    print("count:{}".format(Join_57_Right_Part_v.count()))
    
    Join_57_Right_Part_v.show(1000,False)
    
    Join_57_Right_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Join_57_Right_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Join_152_Penalty_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Copy_149_Penalty_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Copy_149_Penalty_v')
    
    Join_152_Penalty_Part_v=Copy_149_Penalty_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Join_152_Penalty_Part_v PURGE").show()
    
    print("Join_152_Penalty_Part_v")
    
    print(Join_152_Penalty_Part_v.schema.json())
    
    print("count:{}".format(Join_152_Penalty_Part_v.count()))
    
    Join_152_Penalty_Part_v.show(1000,False)
    
    Join_152_Penalty_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Join_152_Penalty_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Join_57(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Join_57_Left_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Join_57_Left_Part_v')
    
    Join_57_Right_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Join_57_Right_Part_v')
    
    Join_57_v=Join_57_Left_Part_v.join(Join_57_Right_Part_v,['SOC_NO', 'BASE_ID', 'RATE_ID'],'left')
    
    Join_57_P_Join_v = Join_57_v.select(Join_57_Left_Part_v.B_KEY.cast('string').alias('B_KEY'),Join_57_Left_Part_v.INT_RATE.cast('decimal(7,4)').alias('INT_RATE'),Join_57_Left_Part_v.MI006_INT_RATE.cast('decimal(8,4)').alias('MI006_INT_RATE'),Join_57_Left_Part_v.MI006_STORE_RATE.cast('decimal(8,4)').alias('MI006_STORE_RATE'),Join_57_Left_Part_v.MI006_DISCH_AMOUNT.cast('decimal(18,3)').alias('MI006_DISCH_AMOUNT'),Join_57_Right_Part_v.KEY_POINTER.cast('string').alias('MI006_KEY_POINTER'),Join_57_Left_Part_v.MI006_ACCT_SOLD_TO.cast('string').alias('MI006_ACCT_SOLD_TO'),Join_57_Left_Part_v.MI006_ACCR_INCEPT.cast('decimal(18,3)').alias('MI006_ACCR_INCEPT'),Join_57_Left_Part_v.MI006_AMT_UNDRAWN.cast('decimal(18,3)').alias('MI006_AMT_UNDRAWN'),Join_57_Left_Part_v.MI006_AUTO_DEBIT_IND.cast('string').alias('MI006_AUTO_DEBIT_IND'),Join_57_Right_Part_v.RATE.cast('decimal(16,3)').alias('MI006_BASE_RATE'),Join_57_Left_Part_v.MI006_BOIS_LST_ADV_DATE.cast('integer').alias('MI006_BOIS_LST_ADV_DATE'),Join_57_Left_Part_v.MI006_CLASS_CODE.cast('string').alias('MI006_CLASS_CODE'),Join_57_Left_Part_v.MI006_CLASSIFY_FLAG.cast('string').alias('MI006_CLASSIFY_FLAG'),Join_57_Left_Part_v.MI006_FIRST_INST_DATE.cast('integer').alias('MI006_FIRST_INST_DATE'),Join_57_Left_Part_v.MI006_FIRST_REPAY_DATE.cast('integer').alias('MI006_FIRST_REPAY_DATE'),Join_57_Left_Part_v.MI006_LOAN_SOLD_CAGAMAS_FLAG.cast('string').alias('MI006_LOAN_SOLD_CAGAMAS_FLAG'),Join_57_Left_Part_v.MI006_NO_INST_PAID.cast('string').alias('MI006_NO_INST_PAID'),Join_57_Left_Part_v.MI006_NPA_IND.cast('string').alias('MI006_NPA_IND'),Join_57_Left_Part_v.MI006_NPL_CLASS_STAT.cast('string').alias('MI006_NPL_CLASS_STAT'),Join_57_Left_Part_v.MI006_NPL_STATUS.cast('string').alias('MI006_NPL_STATUS'),Join_57_Left_Part_v.MI006_P_WF_FEE_AMT.cast('decimal(18,3)').alias('MI006_P_WF_FEE_AMT'),Join_57_Left_Part_v.MI006_P_WF_INT_AMT.cast('decimal(18,3)').alias('MI006_P_WF_INT_AMT'),Join_57_Left_Part_v.MI006_P_WF_LT_CHRG_AMT.cast('decimal(18,3)').alias('MI006_P_WF_LT_CHRG_AMT'),Join_57_Left_Part_v.MI006_P_WF_PRIN_AMT.cast('decimal(18,3)').alias('MI006_P_WF_PRIN_AMT'),Join_57_Left_Part_v.MI006_RATE_ID.cast('string').alias('MI006_RATE_ID'),Join_57_Left_Part_v.MI006_WRITEOFF_FLAG.cast('string').alias('MI006_WRITEOFF_FLAG'),Join_57_Left_Part_v.MI006_BORM_CHARGE_OFF_FLAG.cast('string').alias('MI006_BORM_CHARGE_OFF_FLAG'),Join_57_Left_Part_v.MI006_AM_ASSEC_TAG.cast('string').alias('MI006_AM_ASSEC_TAG'),Join_57_Left_Part_v.MI006_ACCT_SOLD_TO_NAME.cast('string').alias('MI006_ACCT_SOLD_TO_NAME'),Join_57_Left_Part_v.MI006_WRIT_OFF_INT.cast('decimal(18,3)').alias('MI006_WRIT_OFF_INT'),Join_57_Left_Part_v.MI006_WRIT_OFF_PRIN.cast('decimal(18,3)').alias('MI006_WRIT_OFF_PRIN'),Join_57_Left_Part_v.MI006_WRIT_OFF_CHARGE.cast('decimal(18,3)').alias('MI006_WRIT_OFF_CHARGE'),Join_57_Right_Part_v.BASM_DATE.cast('integer').alias('MI006_BASM_DATE'),Join_57_Right_Part_v.MI006_BASM_BASE_ID.cast('string').alias('MI006_BASM_BASE_ID'),Join_57_Right_Part_v.MI006_PRIME_RATE.cast('decimal(9,5)').alias('MI006_PRIME_RATE'),Join_57_Right_Part_v.MI006_BASM_DESCRIPTION.cast('string').alias('MI006_BASM_DESCRIPTION'),Join_57_Left_Part_v.MI006_CAPN_METHOD.cast('string').alias('MI006_CAPN_METHOD'),Join_57_Left_Part_v.MI006_CCC_REGULATED_IND.cast('string').alias('MI006_CCC_REGULATED_IND'),Join_57_Left_Part_v.MI006_FUND_CODE.cast('integer').alias('MI006_FUND_CODE'),Join_57_Left_Part_v.MI006_EXTRACTED_DATE.cast('integer').alias('MI006_EXTRACTED_DATE'),Join_57_Left_Part_v.MI006_INT_ONLY_EXP_DATE.cast('integer').alias('MI006_INT_ONLY_EXP_DATE'),Join_57_Left_Part_v.MI006_REVOLVING_CR_IND.cast('string').alias('MI006_REVOLVING_CR_IND'),Join_57_Left_Part_v.MI006_PROV_AMT.cast('decimal(18,3)').alias('MI006_PROV_AMT'),Join_57_Left_Part_v.M1006_ORIGINAL_EXP_DT.cast('integer').alias('M1006_ORIGINAL_EXP_DT'),Join_57_Left_Part_v.MI006_FIRST_ADV_DATE.cast('integer').alias('MI006_FIRST_ADV_DATE'),Join_57_Left_Part_v.MI006_PUR_CONTRACT_NO.cast('string').alias('MI006_PUR_CONTRACT_NO'),Join_57_Left_Part_v.MI006_COMMISSION_AMT.cast('decimal(18,3)').alias('MI006_COMMISSION_AMT'),Join_57_Left_Part_v.MI006_TYPE.cast('string').alias('MI006_TYPE'),Join_57_Left_Part_v.MI006_STOP_ACCRUAL.cast('string').alias('MI006_STOP_ACCRUAL'),Join_57_Left_Part_v.MI006_SHADOW_INT_ACCURAL.cast('decimal(18,3)').alias('MI006_SHADOW_INT_ACCURAL'),Join_57_Left_Part_v.MI006_SHADOW_CURR_YR_INT.cast('decimal(18,3)').alias('MI006_SHADOW_CURR_YR_INT'),Join_57_Left_Part_v.MI006_TIER_GROUP_ID.cast('string').alias('MI006_TIER_GROUP_ID'),Join_57_Left_Part_v.MI006_SETELMENT_ACCT_NO.cast('string').alias('MI006_SETELMENT_ACCT_NO'),Join_57_Left_Part_v.MI006_DATE_OF_SALE.cast('integer').alias('MI006_DATE_OF_SALE'),Join_57_Left_Part_v.MI006_RETENTION_AMOUNT.cast('decimal(18,3)').alias('MI006_RETENTION_AMOUNT'),Join_57_Left_Part_v.MI006_RETENTION_PERIOD.cast('string').alias('MI006_RETENTION_PERIOD'),Join_57_Left_Part_v.MI006_SECURITY_IND.cast('string').alias('MI006_SECURITY_IND'),Join_57_Left_Part_v.MI006_ACCR_TYPE.cast('string').alias('MI006_ACCR_TYPE'),Join_57_Left_Part_v.MI006_ACQUISITION_FEE.cast('decimal(18,3)').alias('MI006_ACQUISITION_FEE'),Join_57_Left_Part_v.MI006_AKPK_CODE.cast('string').alias('MI006_AKPK_CODE'),Join_57_Left_Part_v.MI006_APPROVED_AUTH.cast('string').alias('MI006_APPROVED_AUTH'),Join_57_Left_Part_v.MI006_BOIS_5TH_SCHED_ISS_DT.cast('integer').alias('MI006_BOIS_5TH_SCHED_ISS_DT'),Join_57_Left_Part_v.MI006_BOIS_ACQUISITION_FEE.cast('decimal(18,3)').alias('MI006_BOIS_ACQUISITION_FEE'),Join_57_Left_Part_v.MI006_BOIS_APPROVED_BY.cast('string').alias('MI006_BOIS_APPROVED_BY'),Join_57_Left_Part_v.MI006_BOIS_GROSS_EFF_YIELD.cast('decimal(4,2)').alias('MI006_BOIS_GROSS_EFF_YIELD'),Join_57_Left_Part_v.MI006_BOIS_LAST_ADV_DATE.cast('integer').alias('MI006_BOIS_LAST_ADV_DATE'),Join_57_Left_Part_v.MI006_BOIS_NET_EFF_YEILD.cast('decimal(4,2)').alias('MI006_BOIS_NET_EFF_YEILD'),Join_57_Left_Part_v.MI006_BOIS_NPA_BAL.cast('decimal(18,3)').alias('MI006_BOIS_NPA_BAL'),Join_57_Left_Part_v.MI006_BOIS_PROV_AMT.cast('decimal(18,3)').alias('MI006_BOIS_PROV_AMT'),Join_57_Left_Part_v.MI006_BOIS_PUR_CNTRT_NO.cast('string').alias('MI006_BOIS_PUR_CNTRT_NO'),Join_57_Left_Part_v.MI006_BOIS_SETTLE_ACCT.cast('string').alias('MI006_BOIS_SETTLE_ACCT'),Join_57_Left_Part_v.MI006_BOIS_SHDW_CURR_YR_INT.cast('decimal(18,3)').alias('MI006_BOIS_SHDW_CURR_YR_INT'),Join_57_Left_Part_v.MI006_BOIS_SHDW_INT_ACCR.cast('decimal(18,3)').alias('MI006_BOIS_SHDW_INT_ACCR'),Join_57_Left_Part_v.MI006_BOIS_TOT_ACCR_CAP.cast('decimal(18,3)').alias('MI006_BOIS_TOT_ACCR_CAP'),Join_57_Left_Part_v.MI006_BUS_TYPE.cast('string').alias('MI006_BUS_TYPE'),Join_57_Left_Part_v.MI006_DUE_DATE.cast('integer').alias('MI006_DUE_DATE'),Join_57_Left_Part_v.MI006_FIFTH_SCHD_ISS_DAT.cast('integer').alias('MI006_FIFTH_SCHD_ISS_DAT'),Join_57_Left_Part_v.MI006_INDEX_CODE.cast('string').alias('MI006_INDEX_CODE'),Join_57_Left_Part_v.MI006_ISLAMIC_BANK.cast('string').alias('MI006_ISLAMIC_BANK'),Join_57_Left_Part_v.MI006_MONTHS_IN_ARR.cast('string').alias('MI006_MONTHS_IN_ARR'),Join_57_Left_Part_v.MI006_MULTI_TIER_FLG.cast('string').alias('MI006_MULTI_TIER_FLG'),Join_57_Left_Part_v.MI006_NON_ACCR_IND.cast('string').alias('MI006_NON_ACCR_IND'),Join_57_Left_Part_v.MI006_NON_WF_AMT.cast('decimal(18,3)').alias('MI006_NON_WF_AMT'),Join_57_Left_Part_v.MI006_ORIG_BRCH_CODE.cast('string').alias('MI006_ORIG_BRCH_CODE'),Join_57_Left_Part_v.MI006_OTHER_CHARGES.cast('decimal(18,3)').alias('MI006_OTHER_CHARGES'),Join_57_Left_Part_v.MI006_PERS_BNK_ID.cast('string').alias('MI006_PERS_BNK_ID'),Join_57_Left_Part_v.MI006_RTC_EXP_DATE.cast('integer').alias('MI006_RTC_EXP_DATE'),Join_57_Left_Part_v.MI006_RTC_METHOD_IND.cast('string').alias('MI006_RTC_METHOD_IND'),Join_57_Left_Part_v.MI006_SALE_REPUR_CODE.cast('string').alias('MI006_SALE_REPUR_CODE'),Join_57_Left_Part_v.MI006_SECURITY_IND_DATE.cast('integer').alias('MI006_SECURITY_IND_DATE'),Join_57_Left_Part_v.MI006_STAFF_PRODUCT_IND.cast('string').alias('MI006_STAFF_PRODUCT_IND'),Join_57_Left_Part_v.MI006_STAMPING_FEE.cast('decimal(18,3)').alias('MI006_STAMPING_FEE'),Join_57_Left_Part_v.MI006_TIER_METHOD.cast('string').alias('MI006_TIER_METHOD'),Join_57_Left_Part_v.MI006_WATCHLIST_TAG.cast('string').alias('MI006_WATCHLIST_TAG'),Join_57_Left_Part_v.MI006_WRITEOFF_STATUS_CODE.cast('string').alias('MI006_WRITEOFF_STATUS_CODE'),Join_57_Left_Part_v.MI006_INT_REPAY_FREQ.cast('string').alias('MI006_INT_REPAY_FREQ'),Join_57_Left_Part_v.MI006_BLDVNN_NXT_PRIN_REP.cast('integer').alias('MI006_BLDVNN_NXT_PRIN_REP'),Join_57_Left_Part_v.MI006_BLDVNN_NXT_INT_REPA.cast('integer').alias('MI006_BLDVNN_NXT_INT_REPA'),Join_57_Left_Part_v.MI006_BOIS_SOLD_AMOUNT.cast('decimal(18,3)').alias('MI006_BOIS_SOLD_AMOUNT'),Join_57_Left_Part_v.MI006_BOIS_LOAN_TYPE.cast('string').alias('MI006_BOIS_LOAN_TYPE'),Join_57_Left_Part_v.MI006_BOIS_EX_FLAG_3.cast('string').alias('MI006_BOIS_EX_FLAG_3'),Join_57_Left_Part_v.MI006_WATCHLIST_DT.cast('integer').alias('MI006_WATCHLIST_DT'),Join_57_Left_Part_v.MI006_IP_PROVISION_AMOUNT.cast('decimal(18,3)').alias('MI006_IP_PROVISION_AMOUNT'),Join_57_Left_Part_v.MI006_INT_TYPE.cast('string').alias('MI006_INT_TYPE'),Join_57_Left_Part_v.MI006_BINS_PREMIUM.cast('decimal(18,3)').alias('MI006_BINS_PREMIUM'),Join_57_Left_Part_v.MI006_BOIS_CP1_PROV_AMT.cast('decimal(18,3)').alias('MI006_BOIS_CP1_PROV_AMT'),Join_57_Left_Part_v.MI006_BOIS_CP3_PROV_AMT.cast('decimal(18,3)').alias('MI006_BOIS_CP3_PROV_AMT'),Join_57_Left_Part_v.MI006_P_WOFF_OVERDUE.cast('decimal(18,3)').alias('MI006_P_WOFF_OVERDUE'),Join_57_Left_Part_v.MI006_TOTAL_SUB_LEDGER_AMT.cast('decimal(18,3)').alias('MI006_TOTAL_SUB_LEDGER_AMT'),Join_57_Left_Part_v.MI006_BOIS_RECALL_IND.cast('string').alias('MI006_BOIS_RECALL_IND'),Join_57_Left_Part_v.MI006_NO_OF_INST_ARR.cast('integer').alias('MI006_NO_OF_INST_ARR'),Join_57_Left_Part_v.MI006_OUTSTANDING_PRINCI.cast('decimal(18,3)').alias('MI006_OUTSTANDING_PRINCI'),Join_57_Left_Part_v.MI006_PRIMRY_NPL.cast('string').alias('MI006_PRIMRY_NPL'),Join_57_Left_Part_v.MI006_BOIS_REVERSED_INT.cast('decimal(18,3)').alias('MI006_BOIS_REVERSED_INT'),Join_57_Left_Part_v.MI006_BOIS_PROVISIONING_DT.cast('integer').alias('MI006_BOIS_PROVISIONING_DT'),Join_57_Left_Part_v.MI006_LITIGATION_DATE.cast('integer').alias('MI006_LITIGATION_DATE'),Join_57_Left_Part_v.MI006_ADV_PREP_TXN_AMNT.cast('decimal(18,3)').alias('MI006_ADV_PREP_TXN_AMNT'),Join_57_Left_Part_v.MI006_BORM_PRINC_REDUCT_EFF.cast('decimal(18,3)').alias('MI006_BORM_PRINC_REDUCT_EFF'),Join_57_Left_Part_v.MI006_BOIS_RETENTION_PERIOD.cast('string').alias('MI006_BOIS_RETENTION_PERIOD'),Join_57_Left_Part_v.MI006_ACF_SL_NO.cast('string').alias('MI006_ACF_SL_NO'),Join_57_Left_Part_v.MI006_BOIS_RECALL_IND_DT.cast('integer').alias('MI006_BOIS_RECALL_IND_DT'),Join_57_Left_Part_v.MI006_BOIS_LOCK_IN.cast('string').alias('MI006_BOIS_LOCK_IN'),Join_57_Left_Part_v.MI006_CP7_PROV.cast('decimal(18,3)').alias('MI006_CP7_PROV'),Join_57_Left_Part_v.MI006_RECOV_AMT.cast('decimal(18,3)').alias('MI006_RECOV_AMT'),Join_57_Left_Part_v.MI006_WORKOUT.cast('string').alias('MI006_WORKOUT'),Join_57_Left_Part_v.MI006_WDV_PRIN.cast('decimal(18,3)').alias('MI006_WDV_PRIN'),Join_57_Left_Part_v.MI006_WDV_INT.cast('decimal(18,3)').alias('MI006_WDV_INT'),Join_57_Left_Part_v.MI006_WDV_LPI.cast('decimal(18,3)').alias('MI006_WDV_LPI'),Join_57_Left_Part_v.MI006_WDV_OC.cast('decimal(18,3)').alias('MI006_WDV_OC'),Join_57_Left_Part_v.MI006_OFF_BS_AMT.cast('decimal(18,3)').alias('MI006_OFF_BS_AMT'),Join_57_Left_Part_v.MI006_LOCK_IN_SRT_DT.cast('string').alias('MI006_LOCK_IN_SRT_DT'),Join_57_Left_Part_v.MI006_LOCK_IN_PERIOD.cast('integer').alias('MI006_LOCK_IN_PERIOD'),Join_57_Left_Part_v.MI006_DISCH_PEN_AMT1.cast('decimal(18,3)').alias('MI006_DISCH_PEN_AMT1'),Join_57_Left_Part_v.MI006_DISCH_PEN_AMT2.cast('decimal(18,3)').alias('MI006_DISCH_PEN_AMT2'),Join_57_Left_Part_v.MI006_DISCH_PEN_AMT3.cast('decimal(18,3)').alias('MI006_DISCH_PEN_AMT3'),Join_57_Left_Part_v.MI006_HOUSE_MRKT_CODE.cast('string').alias('MI006_HOUSE_MRKT_CODE'),Join_57_Left_Part_v.MI006_BOIS_CITIZEN_CODE.cast('string').alias('MI006_BOIS_CITIZEN_CODE'),Join_57_Left_Part_v.MI006_BOIS_ATTRITION_DT.cast('integer').alias('MI006_BOIS_ATTRITION_DT'),Join_57_Left_Part_v.MI006_BOIS_ATTRITION_CODE.cast('string').alias('MI006_BOIS_ATTRITION_CODE'),Join_57_Left_Part_v.MI006_BOIS_STAF_GRAC_END_DT.cast('integer').alias('MI006_BOIS_STAF_GRAC_END_DT'),Join_57_Left_Part_v.MI006_BOIS_PREV_CAP_BAL.cast('decimal(18,3)').alias('MI006_BOIS_PREV_CAP_BAL'),Join_57_Left_Part_v.MI006_SHORTFALL_DATE.cast('integer').alias('MI006_SHORTFALL_DATE'),Join_57_Left_Part_v.MI006_TYPE_OF_RECALL.cast('string').alias('MI006_TYPE_OF_RECALL'),Join_57_Left_Part_v.MI006_CHCD_COLLECN_HUB_CD.cast('string').alias('MI006_CHCD_COLLECN_HUB_CD'),Join_57_Right_Part_v.L_BASM_BASE_ID.cast('string').alias('L_BASM_BASE_ID'),Join_57_Right_Part_v.L_BASM_RATE_ID.cast('string').alias('L_BASM_RATE_ID'),Join_57_Left_Part_v.BASM_RATE_ID.cast('string').alias('BASM_RATE_ID'),Join_57_Left_Part_v.COMPANY_CODE.cast('string').alias('COMPANY_CODE'),Join_57_Right_Part_v.EFF_RATE.cast('decimal(16,4)').alias('EFF_RATE'),Join_57_Left_Part_v.MI006_WRITEOFF_AMOUNT.cast('decimal(18,3)').alias('MI006_WRITEOFF_AMOUNT'),Join_57_Left_Part_v.SOC_NO.cast('string').alias('SOC_NO'),Join_57_Left_Part_v.P_RATE.cast('decimal(7,4)').alias('P_RATE'),Join_57_Left_Part_v.P_IND.cast('string').alias('P_IND'),Join_57_Left_Part_v.P_BASE_ID.cast('string').alias('P_BASE_ID'),Join_57_Left_Part_v.P_RATE_ID.cast('string').alias('P_RATE_ID'),Join_57_Left_Part_v.RECALL_RATE.cast('decimal(6,4)').alias('RECALL_RATE'),Join_57_Left_Part_v.RECALL_IND.cast('string').alias('RECALL_IND'),Join_57_Left_Part_v.FINE_RATE.cast('decimal(6,4)').alias('FINE_RATE'),Join_57_Left_Part_v.RT_INCR.cast('decimal(7,5)').alias('RT_INCR'),Join_57_Left_Part_v.MI006_MATURITY_DATE.cast('integer').alias('MI006_MATURITY_DATE'),Join_57_Left_Part_v.MI006_CAPN_FREQ.cast('string').alias('MI006_CAPN_FREQ'),Join_57_Left_Part_v.MI006_BORM_REPAY_METHOD.cast('string').alias('MI006_BORM_REPAY_METHOD'),Join_57_Left_Part_v.MIS006_NEXT_ACCR_DT.cast('integer').alias('MIS006_NEXT_ACCR_DT'),Join_57_Left_Part_v.MI006_ORIG_MAT_DATE.cast('integer').alias('MI006_ORIG_MAT_DATE'),Join_57_Left_Part_v.MI006_NOTICE_FLAG.cast('string').alias('MI006_NOTICE_FLAG'),Join_57_Left_Part_v.MI006_NOTICE_DATE.cast('integer').alias('MI006_NOTICE_DATE'),Join_57_Left_Part_v.MI006_AUTO_DBT_ACCT_NO.cast('string').alias('MI006_AUTO_DBT_ACCT_NO'),Join_57_Left_Part_v.MI006_AUTO_DBT_STRT_DT.cast('integer').alias('MI006_AUTO_DBT_STRT_DT'),Join_57_Left_Part_v.MI006_AUTO_DBT_END_DT.cast('integer').alias('MI006_AUTO_DBT_END_DT'),Join_57_Left_Part_v.MI006_MON_PRD_STRT_DT.cast('integer').alias('MI006_MON_PRD_STRT_DT'),Join_57_Left_Part_v.MI006_MON_PRD_END_DT.cast('integer').alias('MI006_MON_PRD_END_DT'),Join_57_Left_Part_v.MI006_RR_MARGIN.cast('decimal(4,2)').alias('MI006_RR_MARGIN'),Join_57_Left_Part_v.MI006_AKPK_DATE.cast('integer').alias('MI006_AKPK_DATE'),Join_57_Left_Part_v.MI006_MIN_REPAY_AMT.cast('decimal(18,3)').alias('MI006_MIN_REPAY_AMT'),Join_57_Left_Part_v.MI006_FOUR_SCHD_ISS_DT.cast('integer').alias('MI006_FOUR_SCHD_ISS_DT'),Join_57_Left_Part_v.MI006_RULE_THREE_ISS_DT.cast('integer').alias('MI006_RULE_THREE_ISS_DT'),Join_57_Left_Part_v.MI006_REPO_ORDER_DT.cast('integer').alias('MI006_REPO_ORDER_DT'),Join_57_Left_Part_v.MI006_NOT_ISS_DT.cast('integer').alias('MI006_NOT_ISS_DT'),Join_57_Left_Part_v.MI006_PROMPT_PAY_CNTR.cast('integer').alias('MI006_PROMPT_PAY_CNTR'),Join_57_Left_Part_v.MI006_BUY_BACK_CNTR.cast('integer').alias('MI006_BUY_BACK_CNTR'),Join_57_Left_Part_v.MI006_STEP_UP_PERIOD.cast('integer').alias('MI006_STEP_UP_PERIOD'),Join_57_Left_Part_v.MI006_REDRAW_AMT.cast('decimal(18,3)').alias('MI006_REDRAW_AMT'),Join_57_Left_Part_v.MI006_WRITE_OFF_VAL_TAG.cast('string').alias('MI006_WRITE_OFF_VAL_TAG'),Join_57_Left_Part_v.MI006_WRITE_OFF_JSTFCATN.cast('string').alias('MI006_WRITE_OFF_JSTFCATN'),Join_57_Left_Part_v.MI006_WRITE_OFF_TAG_DT.cast('integer').alias('MI006_WRITE_OFF_TAG_DT'),Join_57_Left_Part_v.MI006_WRITE_OFF_EXCL_TAG.cast('string').alias('MI006_WRITE_OFF_EXCL_TAG'),Join_57_Left_Part_v.MI006_WRITE_OFF_EXCL_RSN.cast('string').alias('MI006_WRITE_OFF_EXCL_RSN'),Join_57_Left_Part_v.MI006_WRITE_OFF_EXCL_DT.cast('integer').alias('MI006_WRITE_OFF_EXCL_DT'),Join_57_Left_Part_v.MI006_ORIG_REM_REPAYS.cast('decimal(5,0)').alias('MI006_ORIG_REM_REPAYS'),Join_57_Left_Part_v.MI006_STMT_DELIVERY.cast('string').alias('MI006_STMT_DELIVERY'),Join_57_Left_Part_v.MI006_EMAIL_ADDR.cast('string').alias('MI006_EMAIL_ADDR'),Join_57_Left_Part_v.MI006_CHRG_RPY_AMT.cast('decimal(18,3)').alias('MI006_CHRG_RPY_AMT'),Join_57_Left_Part_v.MI006_PRIN_RPY_AMT.cast('decimal(18,3)').alias('MI006_PRIN_RPY_AMT'),Join_57_Left_Part_v.MI006_COURT_DEP_RPY_AMT.cast('decimal(18,3)').alias('MI006_COURT_DEP_RPY_AMT'),Join_57_Left_Part_v.MI006_INT_RPY_AMT.cast('decimal(18,3)').alias('MI006_INT_RPY_AMT'),Join_57_Left_Part_v.MI006_BUY_BACK_DATE.cast('integer').alias('MI006_BUY_BACK_DATE'),Join_57_Left_Part_v.MI006_BUY_BACK_IND.cast('string').alias('MI006_BUY_BACK_IND'),Join_57_Left_Part_v.MI006_PREV_DAY_IFRS_EIR_RATE.cast('decimal(9,6)').alias('MI006_PREV_DAY_IFRS_EIR_RATE'),Join_57_Left_Part_v.MI006_PREV_DAY_IFRS_ESTM_LOAN_TERM.cast('decimal(5,0)').alias('MI006_PREV_DAY_IFRS_ESTM_LOAN_TERM'),Join_57_Left_Part_v.MI006_MAX_REPAY_AGE.cast('string').alias('MI006_MAX_REPAY_AGE'),Join_57_Left_Part_v.MI006_MORTGAGE_FACILITY_IND.cast('string').alias('MI006_MORTGAGE_FACILITY_IND'),Join_57_Left_Part_v.MI006_SPGA_NO.cast('string').alias('MI006_SPGA_NO'),Join_57_Left_Part_v.MI006_DEDUCT_CODE.cast('string').alias('MI006_DEDUCT_CODE'),Join_57_Left_Part_v.MI006_DED_STRT_MON.cast('string').alias('MI006_DED_STRT_MON'),Join_57_Left_Part_v.MI006_DED_END_MON.cast('string').alias('MI006_DED_END_MON'),Join_57_Left_Part_v.MI006_EXC_DPD_WOFF_VAL.cast('string').alias('MI006_EXC_DPD_WOFF_VAL'),Join_57_Left_Part_v.MI006_GPP_INT_CAP_AMT.cast('decimal(17,3)').alias('MI006_GPP_INT_CAP_AMT'),Join_57_Left_Part_v.MI006_GPP_AMORT_AMT_MTH.cast('decimal(17,3)').alias('MI006_GPP_AMORT_AMT_MTH'),Join_57_Left_Part_v.MI006_RNR_REM_PROFIT.cast('decimal(17,3)').alias('MI006_RNR_REM_PROFIT'),Join_57_Left_Part_v.MI006_RNR_MONTHLY_PROFIT.cast('decimal(17,3)').alias('MI006_RNR_MONTHLY_PROFIT'),Join_57_Left_Part_v.MI006_INT_THRES_CAP_PER.cast('decimal(5,2)').alias('MI006_INT_THRES_CAP_PER'),Join_57_Left_Part_v.MI006_PP_ADD_INT_AMT.cast('decimal(17,3)').alias('MI006_PP_ADD_INT_AMT'),Join_57_Left_Part_v.MI006_PP_PROMPT_PAY_COUNTER.cast('decimal(2,0)').alias('MI006_PP_PROMPT_PAY_COUNTER'),Join_57_Left_Part_v.MI006_PP_MON_PRD_END_DATE.cast('integer').alias('MI006_PP_MON_PRD_END_DATE'),Join_57_Left_Part_v.MI006_REL_RED_RSN_CD.cast('string').alias('MI006_REL_RED_RSN_CD'),Join_57_Left_Part_v.MI006_RSN_CD_MAINTNCE_DT.cast('integer').alias('MI006_RSN_CD_MAINTNCE_DT'),Join_57_Left_Part_v.MI006_INST_CHG_EXP_DT.cast('integer').alias('MI006_INST_CHG_EXP_DT'),Join_57_Left_Part_v.MI006_AKPK_SPC_TAG.cast('string').alias('MI006_AKPK_SPC_TAG'),Join_57_Left_Part_v.MI006_AKPK_SPC_START_DT.cast('integer').alias('MI006_AKPK_SPC_START_DT'),Join_57_Left_Part_v.MI006_AKPK_SPC_END_DT.cast('integer').alias('MI006_AKPK_SPC_END_DT'),Join_57_Left_Part_v.MI006_AKPK_MATRIX.cast('string').alias('MI006_AKPK_MATRIX'),Join_57_Left_Part_v.MI006_MORA_ACCR_INT.cast('decimal(17,3)').alias('MI006_MORA_ACCR_INT'),Join_57_Left_Part_v.MI006_MANUAL_IMPAIR.cast('string').alias('MI006_MANUAL_IMPAIR'),Join_57_Left_Part_v.MI006_PRE_DISB_DOC_STAT_DT.cast('integer').alias('MI006_PRE_DISB_DOC_STAT_DT'),Join_57_Left_Part_v.MI006_UMA_NOTC_GEN_DT.cast('integer').alias('MI006_UMA_NOTC_GEN_DT'),Join_57_Left_Part_v.MI006_ACC_DUE_DEF_INST.cast('string').alias('MI006_ACC_DUE_DEF_INST'),Join_57_Left_Part_v.MI006_EX_FLAG_1.cast('string').alias('MI006_EX_FLAG_1'),Join_57_Left_Part_v.MI006_INSTALLMENT_NO.cast('string').alias('MI006_INSTALLMENT_NO'),Join_57_Left_Part_v.MI006_ADV_PREPAY_FLAG.cast('string').alias('MI006_ADV_PREPAY_FLAG'),Join_57_Left_Part_v.MI006_REL_RED_RSN_FL_CD.cast('string').alias('MI006_REL_RED_RSN_FL_CD'),Join_57_Left_Part_v.MI006_SETLEMENT_WAIVER.cast('string').alias('MI006_SETLEMENT_WAIVER'),Join_57_Left_Part_v.MI006_INST_REINSTATE.cast('string').alias('MI006_INST_REINSTATE'),Join_57_Left_Part_v.MI006_REINSTATE_DATE.cast('integer').alias('MI006_REINSTATE_DATE'),Join_57_Left_Part_v.MI006_REINSTATE_COUNT.cast('string').alias('MI006_REINSTATE_COUNT'),Join_57_Left_Part_v.MI006_MFA_FLAG.cast('string').alias('MI006_MFA_FLAG'),Join_57_Left_Part_v.MI006_MFA_COUNTER.cast('string').alias('MI006_MFA_COUNTER'),Join_57_Left_Part_v.MI006_MFA_EFF_DT.cast('integer').alias('MI006_MFA_EFF_DT'),Join_57_Left_Part_v.MI006_MFA_MAINT_DT.cast('integer').alias('MI006_MFA_MAINT_DT'),Join_57_Left_Part_v.MI006_EIY.cast('decimal(5,2)').alias('MI006_EIY'))
    
    Join_57_P_Join_v = Join_57_P_Join_v.selectExpr("B_KEY","INT_RATE","MI006_INT_RATE","MI006_STORE_RATE","MI006_DISCH_AMOUNT","MI006_KEY_POINTER","RTRIM(MI006_ACCT_SOLD_TO) AS MI006_ACCT_SOLD_TO","MI006_ACCR_INCEPT","MI006_AMT_UNDRAWN","RTRIM(MI006_AUTO_DEBIT_IND) AS MI006_AUTO_DEBIT_IND","MI006_BASE_RATE","MI006_BOIS_LST_ADV_DATE","MI006_CLASS_CODE","RTRIM(MI006_CLASSIFY_FLAG) AS MI006_CLASSIFY_FLAG","MI006_FIRST_INST_DATE","MI006_FIRST_REPAY_DATE","RTRIM(MI006_LOAN_SOLD_CAGAMAS_FLAG) AS MI006_LOAN_SOLD_CAGAMAS_FLAG","MI006_NO_INST_PAID","RTRIM(MI006_NPA_IND) AS MI006_NPA_IND","RTRIM(MI006_NPL_CLASS_STAT) AS MI006_NPL_CLASS_STAT","RTRIM(MI006_NPL_STATUS) AS MI006_NPL_STATUS","MI006_P_WF_FEE_AMT","MI006_P_WF_INT_AMT","MI006_P_WF_LT_CHRG_AMT","MI006_P_WF_PRIN_AMT","MI006_RATE_ID","RTRIM(MI006_WRITEOFF_FLAG) AS MI006_WRITEOFF_FLAG","MI006_BORM_CHARGE_OFF_FLAG","MI006_AM_ASSEC_TAG","MI006_ACCT_SOLD_TO_NAME","MI006_WRIT_OFF_INT","MI006_WRIT_OFF_PRIN","MI006_WRIT_OFF_CHARGE","MI006_BASM_DATE","MI006_BASM_BASE_ID","MI006_PRIME_RATE","MI006_BASM_DESCRIPTION","RTRIM(MI006_CAPN_METHOD) AS MI006_CAPN_METHOD","RTRIM(MI006_CCC_REGULATED_IND) AS MI006_CCC_REGULATED_IND","MI006_FUND_CODE","MI006_EXTRACTED_DATE","MI006_INT_ONLY_EXP_DATE","RTRIM(MI006_REVOLVING_CR_IND) AS MI006_REVOLVING_CR_IND","MI006_PROV_AMT","M1006_ORIGINAL_EXP_DT","MI006_FIRST_ADV_DATE","MI006_PUR_CONTRACT_NO","MI006_COMMISSION_AMT","MI006_TYPE","RTRIM(MI006_STOP_ACCRUAL) AS MI006_STOP_ACCRUAL","MI006_SHADOW_INT_ACCURAL","MI006_SHADOW_CURR_YR_INT","MI006_TIER_GROUP_ID","MI006_SETELMENT_ACCT_NO","MI006_DATE_OF_SALE","MI006_RETENTION_AMOUNT","RTRIM(MI006_RETENTION_PERIOD) AS MI006_RETENTION_PERIOD","RTRIM(MI006_SECURITY_IND) AS MI006_SECURITY_IND","RTRIM(MI006_ACCR_TYPE) AS MI006_ACCR_TYPE","MI006_ACQUISITION_FEE","RTRIM(MI006_AKPK_CODE) AS MI006_AKPK_CODE","MI006_APPROVED_AUTH","MI006_BOIS_5TH_SCHED_ISS_DT","MI006_BOIS_ACQUISITION_FEE","MI006_BOIS_APPROVED_BY","MI006_BOIS_GROSS_EFF_YIELD","MI006_BOIS_LAST_ADV_DATE","MI006_BOIS_NET_EFF_YEILD","MI006_BOIS_NPA_BAL","MI006_BOIS_PROV_AMT","MI006_BOIS_PUR_CNTRT_NO","MI006_BOIS_SETTLE_ACCT","MI006_BOIS_SHDW_CURR_YR_INT","MI006_BOIS_SHDW_INT_ACCR","MI006_BOIS_TOT_ACCR_CAP","MI006_BUS_TYPE","MI006_DUE_DATE","MI006_FIFTH_SCHD_ISS_DAT","MI006_INDEX_CODE","RTRIM(MI006_ISLAMIC_BANK) AS MI006_ISLAMIC_BANK","RTRIM(MI006_MONTHS_IN_ARR) AS MI006_MONTHS_IN_ARR","RTRIM(MI006_MULTI_TIER_FLG) AS MI006_MULTI_TIER_FLG","RTRIM(MI006_NON_ACCR_IND) AS MI006_NON_ACCR_IND","MI006_NON_WF_AMT","MI006_ORIG_BRCH_CODE","MI006_OTHER_CHARGES","MI006_PERS_BNK_ID","MI006_RTC_EXP_DATE","RTRIM(MI006_RTC_METHOD_IND) AS MI006_RTC_METHOD_IND","RTRIM(MI006_SALE_REPUR_CODE) AS MI006_SALE_REPUR_CODE","MI006_SECURITY_IND_DATE","RTRIM(MI006_STAFF_PRODUCT_IND) AS MI006_STAFF_PRODUCT_IND","MI006_STAMPING_FEE","RTRIM(MI006_TIER_METHOD) AS MI006_TIER_METHOD","RTRIM(MI006_WATCHLIST_TAG) AS MI006_WATCHLIST_TAG","RTRIM(MI006_WRITEOFF_STATUS_CODE) AS MI006_WRITEOFF_STATUS_CODE","RTRIM(MI006_INT_REPAY_FREQ) AS MI006_INT_REPAY_FREQ","MI006_BLDVNN_NXT_PRIN_REP","MI006_BLDVNN_NXT_INT_REPA","MI006_BOIS_SOLD_AMOUNT","RTRIM(MI006_BOIS_LOAN_TYPE) AS MI006_BOIS_LOAN_TYPE","RTRIM(MI006_BOIS_EX_FLAG_3) AS MI006_BOIS_EX_FLAG_3","MI006_WATCHLIST_DT","MI006_IP_PROVISION_AMOUNT","RTRIM(MI006_INT_TYPE) AS MI006_INT_TYPE","MI006_BINS_PREMIUM","MI006_BOIS_CP1_PROV_AMT","MI006_BOIS_CP3_PROV_AMT","MI006_P_WOFF_OVERDUE","MI006_TOTAL_SUB_LEDGER_AMT","RTRIM(MI006_BOIS_RECALL_IND) AS MI006_BOIS_RECALL_IND","MI006_NO_OF_INST_ARR","MI006_OUTSTANDING_PRINCI","RTRIM(MI006_PRIMRY_NPL) AS MI006_PRIMRY_NPL","MI006_BOIS_REVERSED_INT","MI006_BOIS_PROVISIONING_DT","MI006_LITIGATION_DATE","MI006_ADV_PREP_TXN_AMNT","MI006_BORM_PRINC_REDUCT_EFF","RTRIM(MI006_BOIS_RETENTION_PERIOD) AS MI006_BOIS_RETENTION_PERIOD","MI006_ACF_SL_NO","MI006_BOIS_RECALL_IND_DT","RTRIM(MI006_BOIS_LOCK_IN) AS MI006_BOIS_LOCK_IN","MI006_CP7_PROV","MI006_RECOV_AMT","RTRIM(MI006_WORKOUT) AS MI006_WORKOUT","MI006_WDV_PRIN","MI006_WDV_INT","MI006_WDV_LPI","MI006_WDV_OC","MI006_OFF_BS_AMT","RTRIM(MI006_LOCK_IN_SRT_DT) AS MI006_LOCK_IN_SRT_DT","MI006_LOCK_IN_PERIOD","MI006_DISCH_PEN_AMT1","MI006_DISCH_PEN_AMT2","MI006_DISCH_PEN_AMT3","MI006_HOUSE_MRKT_CODE","RTRIM(MI006_BOIS_CITIZEN_CODE) AS MI006_BOIS_CITIZEN_CODE","MI006_BOIS_ATTRITION_DT","RTRIM(MI006_BOIS_ATTRITION_CODE) AS MI006_BOIS_ATTRITION_CODE","MI006_BOIS_STAF_GRAC_END_DT","MI006_BOIS_PREV_CAP_BAL","MI006_SHORTFALL_DATE","RTRIM(MI006_TYPE_OF_RECALL) AS MI006_TYPE_OF_RECALL","MI006_CHCD_COLLECN_HUB_CD","RTRIM(L_BASM_BASE_ID) AS L_BASM_BASE_ID","RTRIM(L_BASM_RATE_ID) AS L_BASM_RATE_ID","RTRIM(BASM_RATE_ID) AS BASM_RATE_ID","RTRIM(COMPANY_CODE) AS COMPANY_CODE","EFF_RATE","MI006_WRITEOFF_AMOUNT","RTRIM(SOC_NO) AS SOC_NO","P_RATE","P_IND","RTRIM(P_BASE_ID) AS P_BASE_ID","RTRIM(P_RATE_ID) AS P_RATE_ID","RECALL_RATE","RTRIM(RECALL_IND) AS RECALL_IND","FINE_RATE","RT_INCR","MI006_MATURITY_DATE","RTRIM(MI006_CAPN_FREQ) AS MI006_CAPN_FREQ","RTRIM(MI006_BORM_REPAY_METHOD) AS MI006_BORM_REPAY_METHOD","MIS006_NEXT_ACCR_DT","MI006_ORIG_MAT_DATE","MI006_NOTICE_FLAG","MI006_NOTICE_DATE","MI006_AUTO_DBT_ACCT_NO","MI006_AUTO_DBT_STRT_DT","MI006_AUTO_DBT_END_DT","MI006_MON_PRD_STRT_DT","MI006_MON_PRD_END_DT","MI006_RR_MARGIN","MI006_AKPK_DATE","MI006_MIN_REPAY_AMT","MI006_FOUR_SCHD_ISS_DT","MI006_RULE_THREE_ISS_DT","MI006_REPO_ORDER_DT","MI006_NOT_ISS_DT","MI006_PROMPT_PAY_CNTR","MI006_BUY_BACK_CNTR","MI006_STEP_UP_PERIOD","MI006_REDRAW_AMT","RTRIM(MI006_WRITE_OFF_VAL_TAG) AS MI006_WRITE_OFF_VAL_TAG","RTRIM(MI006_WRITE_OFF_JSTFCATN) AS MI006_WRITE_OFF_JSTFCATN","MI006_WRITE_OFF_TAG_DT","RTRIM(MI006_WRITE_OFF_EXCL_TAG) AS MI006_WRITE_OFF_EXCL_TAG","RTRIM(MI006_WRITE_OFF_EXCL_RSN) AS MI006_WRITE_OFF_EXCL_RSN","MI006_WRITE_OFF_EXCL_DT","MI006_ORIG_REM_REPAYS","RTRIM(MI006_STMT_DELIVERY) AS MI006_STMT_DELIVERY","MI006_EMAIL_ADDR","MI006_CHRG_RPY_AMT","MI006_PRIN_RPY_AMT","MI006_COURT_DEP_RPY_AMT","MI006_INT_RPY_AMT","MI006_BUY_BACK_DATE","RTRIM(MI006_BUY_BACK_IND) AS MI006_BUY_BACK_IND","MI006_PREV_DAY_IFRS_EIR_RATE","MI006_PREV_DAY_IFRS_ESTM_LOAN_TERM","MI006_MAX_REPAY_AGE","MI006_MORTGAGE_FACILITY_IND","MI006_SPGA_NO","MI006_DEDUCT_CODE","MI006_DED_STRT_MON","MI006_DED_END_MON","MI006_EXC_DPD_WOFF_VAL","MI006_GPP_INT_CAP_AMT","MI006_GPP_AMORT_AMT_MTH","MI006_RNR_REM_PROFIT","MI006_RNR_MONTHLY_PROFIT","MI006_INT_THRES_CAP_PER","MI006_PP_ADD_INT_AMT","MI006_PP_PROMPT_PAY_COUNTER","MI006_PP_MON_PRD_END_DATE","MI006_REL_RED_RSN_CD","MI006_RSN_CD_MAINTNCE_DT","MI006_INST_CHG_EXP_DT","MI006_AKPK_SPC_TAG","MI006_AKPK_SPC_START_DT","MI006_AKPK_SPC_END_DT","MI006_AKPK_MATRIX","MI006_MORA_ACCR_INT","MI006_MANUAL_IMPAIR","MI006_PRE_DISB_DOC_STAT_DT","MI006_UMA_NOTC_GEN_DT","MI006_ACC_DUE_DEF_INST","MI006_EX_FLAG_1","MI006_INSTALLMENT_NO","MI006_ADV_PREPAY_FLAG","MI006_REL_RED_RSN_FL_CD","MI006_SETLEMENT_WAIVER","MI006_INST_REINSTATE","MI006_REINSTATE_DATE","MI006_REINSTATE_COUNT","MI006_MFA_FLAG","MI006_MFA_COUNTER","MI006_MFA_EFF_DT","MI006_MFA_MAINT_DT","MI006_EIY").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'INT_RATE', 'type': 'decimal(7,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_RATE', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STORE_RATE', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_KEY_POINTER', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACCT_SOLD_TO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ACCR_INCEPT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AMT_UNDRAWN', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AUTO_DEBIT_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BASE_RATE', 'type': 'decimal(16,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LST_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CLASS_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CLASSIFY_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_FIRST_INST_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FIRST_REPAY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_SOLD_CAGAMAS_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NO_INST_PAID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NPA_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NPL_CLASS_STAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NPL_STATUS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_P_WF_FEE_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WF_INT_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WF_LT_CHRG_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WF_PRIN_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITEOFF_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BORM_CHARGE_OFF_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AM_ASSEC_TAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACCT_SOLD_TO_NAME', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRIT_OFF_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRIT_OFF_PRIN', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRIT_OFF_CHARGE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BASM_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BASM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRIME_RATE', 'type': 'decimal(9,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BASM_DESCRIPTION', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAPN_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_CCC_REGULATED_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_FUND_CODE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EXTRACTED_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_ONLY_EXP_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REVOLVING_CR_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'M1006_ORIGINAL_EXP_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FIRST_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PUR_CONTRACT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_COMMISSION_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TYPE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STOP_ACCRUAL', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SHADOW_INT_ACCURAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SHADOW_CURR_YR_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TIER_GROUP_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SETELMENT_ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DATE_OF_SALE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RETENTION_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RETENTION_PERIOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_SECURITY_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ACCR_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ACQUISITION_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_APPROVED_AUTH', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_5TH_SCHED_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_ACQUISITION_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_APPROVED_BY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_GROSS_EFF_YIELD', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LAST_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_NET_EFF_YEILD', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_NPA_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PUR_CNTRT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SETTLE_ACCT', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SHDW_CURR_YR_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SHDW_INT_ACCR', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_TOT_ACCR_CAP', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BUS_TYPE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DUE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FIFTH_SCHD_ISS_DAT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ISLAMIC_BANK', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_MONTHS_IN_ARR', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_MULTI_TIER_FLG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NON_ACCR_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NON_WF_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ORIG_BRCH_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OTHER_CHARGES', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PERS_BNK_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RTC_EXP_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RTC_METHOD_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SALE_REPUR_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SECURITY_IND_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STAFF_PRODUCT_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_STAMPING_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TIER_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_WATCHLIST_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_WRITEOFF_STATUS_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_INT_REPAY_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BLDVNN_NXT_PRIN_REP', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_NXT_INT_REPA', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SOLD_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LOAN_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_BOIS_EX_FLAG_3', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_WATCHLIST_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_IP_PROVISION_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BINS_PREMIUM', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_CP1_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_CP3_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WOFF_OVERDUE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TOTAL_SUB_LEDGER_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_RECALL_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_NO_OF_INST_ARR', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OUTSTANDING_PRINCI', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRIMRY_NPL', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOIS_REVERSED_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PROVISIONING_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LITIGATION_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADV_PREP_TXN_AMNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_PRINC_REDUCT_EFF', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_RETENTION_PERIOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_ACF_SL_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_RECALL_IND_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LOCK_IN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_CP7_PROV', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RECOV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WORKOUT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_WDV_PRIN', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WDV_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WDV_LPI', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WDV_OC', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OFF_BS_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOCK_IN_SRT_DT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_LOCK_IN_PERIOD', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_PEN_AMT1', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_PEN_AMT2', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_PEN_AMT3', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_HOUSE_MRKT_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_CITIZEN_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOIS_ATTRITION_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_ATTRITION_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BOIS_STAF_GRAC_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PREV_CAP_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SHORTFALL_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TYPE_OF_RECALL', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_CHCD_COLLECN_HUB_CD', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'L_BASM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'L_BASM_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'BASM_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'COMPANY_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'EFF_RATE', 'type': 'decimal(16,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITEOFF_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'SOC_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'P_RATE', 'type': 'decimal(7,4)', 'nullable': True, 'metadata': {}}, {'name': 'P_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'P_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'P_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'RECALL_RATE', 'type': 'decimal(6,4)', 'nullable': True, 'metadata': {}}, {'name': 'RECALL_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'FINE_RATE', 'type': 'decimal(6,4)', 'nullable': True, 'metadata': {}}, {'name': 'RT_INCR', 'type': 'decimal(7,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MATURITY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAPN_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BORM_REPAY_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MIS006_NEXT_ACCR_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ORIG_MAT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NOTICE_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NOTICE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AUTO_DBT_ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AUTO_DBT_STRT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AUTO_DBT_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MON_PRD_STRT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MON_PRD_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RR_MARGIN', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MIN_REPAY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FOUR_SCHD_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RULE_THREE_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REPO_ORDER_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NOT_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PROMPT_PAY_CNTR', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BUY_BACK_CNTR', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STEP_UP_PERIOD', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REDRAW_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITE_OFF_VAL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_WRITE_OFF_JSTFCATN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(30)'}}, {'name': 'MI006_WRITE_OFF_TAG_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITE_OFF_EXCL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_WRITE_OFF_EXCL_RSN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(30)'}}, {'name': 'MI006_WRITE_OFF_EXCL_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ORIG_REM_REPAYS', 'type': 'decimal(5,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STMT_DELIVERY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_EMAIL_ADDR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CHRG_RPY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRIN_RPY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_COURT_DEP_RPY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_RPY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BUY_BACK_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BUY_BACK_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_PREV_DAY_IFRS_EIR_RATE', 'type': 'decimal(9,6)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PREV_DAY_IFRS_ESTM_LOAN_TERM', 'type': 'decimal(5,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MAX_REPAY_AGE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MORTGAGE_FACILITY_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SPGA_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DEDUCT_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DED_STRT_MON', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DED_END_MON', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EXC_DPD_WOFF_VAL', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GPP_INT_CAP_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GPP_AMORT_AMT_MTH', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RNR_REM_PROFIT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RNR_MONTHLY_PROFIT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_THRES_CAP_PER', 'type': 'decimal(5,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PP_ADD_INT_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PP_PROMPT_PAY_COUNTER', 'type': 'decimal(2,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PP_MON_PRD_END_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REL_RED_RSN_CD', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RSN_CD_MAINTNCE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INST_CHG_EXP_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_SPC_TAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_SPC_START_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_SPC_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_MATRIX', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MORA_ACCR_INT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MANUAL_IMPAIR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRE_DISB_DOC_STAT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UMA_NOTC_GEN_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACC_DUE_DEF_INST', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EX_FLAG_1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INSTALLMENT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADV_PREPAY_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REL_RED_RSN_FL_CD', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SETLEMENT_WAIVER', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INST_REINSTATE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REINSTATE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REINSTATE_COUNT', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MFA_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MFA_COUNTER', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MFA_EFF_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MFA_MAINT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EIY', 'type': 'decimal(5,2)', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Join_57_P_Join_v PURGE").show()
    
    print("Join_57_P_Join_v")
    
    print(Join_57_P_Join_v.schema.json())
    
    print("count:{}".format(Join_57_P_Join_v.count()))
    
    Join_57_P_Join_v.show(1000,False)
    
    Join_57_P_Join_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Join_57_P_Join_v")
    

@task.pyspark(conn_id="spark-local")
def Join_152_P_Join_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Join_57_P_Join_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Join_57_P_Join_v')
    
    Join_152_P_Join_Part_v=Join_57_P_Join_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Join_152_P_Join_Part_v PURGE").show()
    
    print("Join_152_P_Join_Part_v")
    
    print(Join_152_P_Join_Part_v.schema.json())
    
    print("count:{}".format(Join_152_P_Join_Part_v.count()))
    
    Join_152_P_Join_Part_v.show(1000,False)
    
    Join_152_P_Join_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Join_152_P_Join_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Join_152(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Join_152_P_Join_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Join_152_P_Join_Part_v')
    
    Join_152_Penalty_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Join_152_Penalty_Part_v')
    
    Join_152_v=Join_152_P_Join_Part_v.join(Join_152_Penalty_Part_v,['SOC_NO', 'P_BASE_ID', 'P_RATE_ID'],'left')
    
    Join_152_LnkJoin_v = Join_152_v.select(Join_152_P_Join_Part_v.SOC_NO.cast('string').alias('SOC_NO'),Join_152_P_Join_Part_v.P_BASE_ID.cast('string').alias('P_BASE_ID'),Join_152_P_Join_Part_v.P_RATE_ID.cast('string').alias('P_RATE_ID'),Join_152_Penalty_Part_v.P_EFF_RATE.cast('decimal(16,4)').alias('P_EFF_RATE'),Join_152_P_Join_Part_v.B_KEY.cast('string').alias('B_KEY'),Join_152_P_Join_Part_v.INT_RATE.cast('decimal(7,4)').alias('INT_RATE'),Join_152_P_Join_Part_v.MI006_INT_RATE.cast('decimal(8,4)').alias('MI006_INT_RATE'),Join_152_P_Join_Part_v.MI006_STORE_RATE.cast('decimal(8,4)').alias('MI006_STORE_RATE'),Join_152_P_Join_Part_v.MI006_DISCH_AMOUNT.cast('decimal(18,3)').alias('MI006_DISCH_AMOUNT'),Join_152_P_Join_Part_v.MI006_KEY_POINTER.cast('string').alias('MI006_KEY_POINTER'),Join_152_P_Join_Part_v.MI006_ACCT_SOLD_TO.cast('string').alias('MI006_ACCT_SOLD_TO'),Join_152_P_Join_Part_v.MI006_ACCR_INCEPT.cast('decimal(18,3)').alias('MI006_ACCR_INCEPT'),Join_152_P_Join_Part_v.MI006_AMT_UNDRAWN.cast('decimal(18,3)').alias('MI006_AMT_UNDRAWN'),Join_152_P_Join_Part_v.MI006_AUTO_DEBIT_IND.cast('string').alias('MI006_AUTO_DEBIT_IND'),Join_152_P_Join_Part_v.MI006_BASE_RATE.cast('decimal(16,3)').alias('MI006_BASE_RATE'),Join_152_P_Join_Part_v.MI006_BOIS_LST_ADV_DATE.cast('integer').alias('MI006_BOIS_LST_ADV_DATE'),Join_152_P_Join_Part_v.MI006_CLASS_CODE.cast('string').alias('MI006_CLASS_CODE'),Join_152_P_Join_Part_v.MI006_CLASSIFY_FLAG.cast('string').alias('MI006_CLASSIFY_FLAG'),Join_152_P_Join_Part_v.MI006_FIRST_INST_DATE.cast('integer').alias('MI006_FIRST_INST_DATE'),Join_152_P_Join_Part_v.MI006_FIRST_REPAY_DATE.cast('integer').alias('MI006_FIRST_REPAY_DATE'),Join_152_P_Join_Part_v.MI006_LOAN_SOLD_CAGAMAS_FLAG.cast('string').alias('MI006_LOAN_SOLD_CAGAMAS_FLAG'),Join_152_P_Join_Part_v.MI006_NO_INST_PAID.cast('string').alias('MI006_NO_INST_PAID'),Join_152_P_Join_Part_v.MI006_NPA_IND.cast('string').alias('MI006_NPA_IND'),Join_152_P_Join_Part_v.MI006_NPL_CLASS_STAT.cast('string').alias('MI006_NPL_CLASS_STAT'),Join_152_P_Join_Part_v.MI006_NPL_STATUS.cast('string').alias('MI006_NPL_STATUS'),Join_152_P_Join_Part_v.MI006_P_WF_FEE_AMT.cast('decimal(18,3)').alias('MI006_P_WF_FEE_AMT'),Join_152_P_Join_Part_v.MI006_P_WF_INT_AMT.cast('decimal(18,3)').alias('MI006_P_WF_INT_AMT'),Join_152_P_Join_Part_v.MI006_P_WF_LT_CHRG_AMT.cast('decimal(18,3)').alias('MI006_P_WF_LT_CHRG_AMT'),Join_152_P_Join_Part_v.MI006_P_WF_PRIN_AMT.cast('decimal(18,3)').alias('MI006_P_WF_PRIN_AMT'),Join_152_P_Join_Part_v.MI006_RATE_ID.cast('string').alias('MI006_RATE_ID'),Join_152_P_Join_Part_v.MI006_WRITEOFF_FLAG.cast('string').alias('MI006_WRITEOFF_FLAG'),Join_152_P_Join_Part_v.MI006_BORM_CHARGE_OFF_FLAG.cast('string').alias('MI006_BORM_CHARGE_OFF_FLAG'),Join_152_P_Join_Part_v.MI006_AM_ASSEC_TAG.cast('string').alias('MI006_AM_ASSEC_TAG'),Join_152_P_Join_Part_v.MI006_ACCT_SOLD_TO_NAME.cast('string').alias('MI006_ACCT_SOLD_TO_NAME'),Join_152_P_Join_Part_v.MI006_WRIT_OFF_INT.cast('decimal(18,3)').alias('MI006_WRIT_OFF_INT'),Join_152_P_Join_Part_v.MI006_WRIT_OFF_PRIN.cast('decimal(18,3)').alias('MI006_WRIT_OFF_PRIN'),Join_152_P_Join_Part_v.MI006_WRIT_OFF_CHARGE.cast('decimal(18,3)').alias('MI006_WRIT_OFF_CHARGE'),Join_152_P_Join_Part_v.MI006_BASM_DATE.cast('integer').alias('MI006_BASM_DATE'),Join_152_P_Join_Part_v.MI006_BASM_BASE_ID.cast('string').alias('MI006_BASM_BASE_ID'),Join_152_P_Join_Part_v.MI006_PRIME_RATE.cast('decimal(9,5)').alias('MI006_PRIME_RATE'),Join_152_P_Join_Part_v.MI006_BASM_DESCRIPTION.cast('string').alias('MI006_BASM_DESCRIPTION'),Join_152_P_Join_Part_v.MI006_CAPN_METHOD.cast('string').alias('MI006_CAPN_METHOD'),Join_152_P_Join_Part_v.MI006_CCC_REGULATED_IND.cast('string').alias('MI006_CCC_REGULATED_IND'),Join_152_P_Join_Part_v.MI006_FUND_CODE.cast('integer').alias('MI006_FUND_CODE'),Join_152_P_Join_Part_v.MI006_EXTRACTED_DATE.cast('integer').alias('MI006_EXTRACTED_DATE'),Join_152_P_Join_Part_v.MI006_INT_ONLY_EXP_DATE.cast('integer').alias('MI006_INT_ONLY_EXP_DATE'),Join_152_P_Join_Part_v.MI006_REVOLVING_CR_IND.cast('string').alias('MI006_REVOLVING_CR_IND'),Join_152_P_Join_Part_v.MI006_PROV_AMT.cast('decimal(18,3)').alias('MI006_PROV_AMT'),Join_152_P_Join_Part_v.M1006_ORIGINAL_EXP_DT.cast('integer').alias('M1006_ORIGINAL_EXP_DT'),Join_152_P_Join_Part_v.MI006_FIRST_ADV_DATE.cast('integer').alias('MI006_FIRST_ADV_DATE'),Join_152_P_Join_Part_v.MI006_PUR_CONTRACT_NO.cast('string').alias('MI006_PUR_CONTRACT_NO'),Join_152_P_Join_Part_v.MI006_COMMISSION_AMT.cast('decimal(18,3)').alias('MI006_COMMISSION_AMT'),Join_152_P_Join_Part_v.MI006_TYPE.cast('string').alias('MI006_TYPE'),Join_152_P_Join_Part_v.MI006_STOP_ACCRUAL.cast('string').alias('MI006_STOP_ACCRUAL'),Join_152_P_Join_Part_v.MI006_SHADOW_INT_ACCURAL.cast('decimal(18,3)').alias('MI006_SHADOW_INT_ACCURAL'),Join_152_P_Join_Part_v.MI006_SHADOW_CURR_YR_INT.cast('decimal(18,3)').alias('MI006_SHADOW_CURR_YR_INT'),Join_152_P_Join_Part_v.MI006_TIER_GROUP_ID.cast('string').alias('MI006_TIER_GROUP_ID'),Join_152_P_Join_Part_v.MI006_SETELMENT_ACCT_NO.cast('string').alias('MI006_SETELMENT_ACCT_NO'),Join_152_P_Join_Part_v.MI006_DATE_OF_SALE.cast('integer').alias('MI006_DATE_OF_SALE'),Join_152_P_Join_Part_v.MI006_RETENTION_AMOUNT.cast('decimal(18,3)').alias('MI006_RETENTION_AMOUNT'),Join_152_P_Join_Part_v.MI006_RETENTION_PERIOD.cast('string').alias('MI006_RETENTION_PERIOD'),Join_152_P_Join_Part_v.MI006_SECURITY_IND.cast('string').alias('MI006_SECURITY_IND'),Join_152_P_Join_Part_v.MI006_ACCR_TYPE.cast('string').alias('MI006_ACCR_TYPE'),Join_152_P_Join_Part_v.MI006_ACQUISITION_FEE.cast('decimal(18,3)').alias('MI006_ACQUISITION_FEE'),Join_152_P_Join_Part_v.MI006_AKPK_CODE.cast('string').alias('MI006_AKPK_CODE'),Join_152_P_Join_Part_v.MI006_APPROVED_AUTH.cast('string').alias('MI006_APPROVED_AUTH'),Join_152_P_Join_Part_v.MI006_BOIS_5TH_SCHED_ISS_DT.cast('integer').alias('MI006_BOIS_5TH_SCHED_ISS_DT'),Join_152_P_Join_Part_v.MI006_BOIS_ACQUISITION_FEE.cast('decimal(18,3)').alias('MI006_BOIS_ACQUISITION_FEE'),Join_152_P_Join_Part_v.MI006_BOIS_APPROVED_BY.cast('string').alias('MI006_BOIS_APPROVED_BY'),Join_152_P_Join_Part_v.MI006_BOIS_GROSS_EFF_YIELD.cast('decimal(4,2)').alias('MI006_BOIS_GROSS_EFF_YIELD'),Join_152_P_Join_Part_v.MI006_BOIS_LAST_ADV_DATE.cast('integer').alias('MI006_BOIS_LAST_ADV_DATE'),Join_152_P_Join_Part_v.MI006_BOIS_NET_EFF_YEILD.cast('decimal(4,2)').alias('MI006_BOIS_NET_EFF_YEILD'),Join_152_P_Join_Part_v.MI006_BOIS_NPA_BAL.cast('decimal(18,3)').alias('MI006_BOIS_NPA_BAL'),Join_152_P_Join_Part_v.MI006_BOIS_PROV_AMT.cast('decimal(18,3)').alias('MI006_BOIS_PROV_AMT'),Join_152_P_Join_Part_v.MI006_BOIS_PUR_CNTRT_NO.cast('string').alias('MI006_BOIS_PUR_CNTRT_NO'),Join_152_P_Join_Part_v.MI006_BOIS_SETTLE_ACCT.cast('string').alias('MI006_BOIS_SETTLE_ACCT'),Join_152_P_Join_Part_v.MI006_BOIS_SHDW_CURR_YR_INT.cast('decimal(18,3)').alias('MI006_BOIS_SHDW_CURR_YR_INT'),Join_152_P_Join_Part_v.MI006_BOIS_SHDW_INT_ACCR.cast('decimal(18,3)').alias('MI006_BOIS_SHDW_INT_ACCR'),Join_152_P_Join_Part_v.MI006_BOIS_TOT_ACCR_CAP.cast('decimal(18,3)').alias('MI006_BOIS_TOT_ACCR_CAP'),Join_152_P_Join_Part_v.MI006_BUS_TYPE.cast('string').alias('MI006_BUS_TYPE'),Join_152_P_Join_Part_v.MI006_DUE_DATE.cast('integer').alias('MI006_DUE_DATE'),Join_152_P_Join_Part_v.MI006_FIFTH_SCHD_ISS_DAT.cast('integer').alias('MI006_FIFTH_SCHD_ISS_DAT'),Join_152_P_Join_Part_v.MI006_INDEX_CODE.cast('string').alias('MI006_INDEX_CODE'),Join_152_P_Join_Part_v.MI006_ISLAMIC_BANK.cast('string').alias('MI006_ISLAMIC_BANK'),Join_152_P_Join_Part_v.MI006_MONTHS_IN_ARR.cast('string').alias('MI006_MONTHS_IN_ARR'),Join_152_P_Join_Part_v.MI006_MULTI_TIER_FLG.cast('string').alias('MI006_MULTI_TIER_FLG'),Join_152_P_Join_Part_v.MI006_NON_ACCR_IND.cast('string').alias('MI006_NON_ACCR_IND'),Join_152_P_Join_Part_v.MI006_NON_WF_AMT.cast('decimal(18,3)').alias('MI006_NON_WF_AMT'),Join_152_P_Join_Part_v.MI006_ORIG_BRCH_CODE.cast('string').alias('MI006_ORIG_BRCH_CODE'),Join_152_P_Join_Part_v.MI006_OTHER_CHARGES.cast('decimal(18,3)').alias('MI006_OTHER_CHARGES'),Join_152_P_Join_Part_v.MI006_PERS_BNK_ID.cast('string').alias('MI006_PERS_BNK_ID'),Join_152_P_Join_Part_v.MI006_RTC_EXP_DATE.cast('integer').alias('MI006_RTC_EXP_DATE'),Join_152_P_Join_Part_v.MI006_RTC_METHOD_IND.cast('string').alias('MI006_RTC_METHOD_IND'),Join_152_P_Join_Part_v.MI006_SALE_REPUR_CODE.cast('string').alias('MI006_SALE_REPUR_CODE'),Join_152_P_Join_Part_v.MI006_SECURITY_IND_DATE.cast('integer').alias('MI006_SECURITY_IND_DATE'),Join_152_P_Join_Part_v.MI006_STAFF_PRODUCT_IND.cast('string').alias('MI006_STAFF_PRODUCT_IND'),Join_152_P_Join_Part_v.MI006_STAMPING_FEE.cast('decimal(18,3)').alias('MI006_STAMPING_FEE'),Join_152_P_Join_Part_v.MI006_TIER_METHOD.cast('string').alias('MI006_TIER_METHOD'),Join_152_P_Join_Part_v.MI006_WATCHLIST_TAG.cast('string').alias('MI006_WATCHLIST_TAG'),Join_152_P_Join_Part_v.MI006_WRITEOFF_STATUS_CODE.cast('string').alias('MI006_WRITEOFF_STATUS_CODE'),Join_152_P_Join_Part_v.MI006_INT_REPAY_FREQ.cast('string').alias('MI006_INT_REPAY_FREQ'),Join_152_P_Join_Part_v.MI006_BLDVNN_NXT_PRIN_REP.cast('integer').alias('MI006_BLDVNN_NXT_PRIN_REP'),Join_152_P_Join_Part_v.MI006_BLDVNN_NXT_INT_REPA.cast('integer').alias('MI006_BLDVNN_NXT_INT_REPA'),Join_152_P_Join_Part_v.MI006_BOIS_SOLD_AMOUNT.cast('decimal(18,3)').alias('MI006_BOIS_SOLD_AMOUNT'),Join_152_P_Join_Part_v.MI006_BOIS_LOAN_TYPE.cast('string').alias('MI006_BOIS_LOAN_TYPE'),Join_152_P_Join_Part_v.MI006_BOIS_EX_FLAG_3.cast('string').alias('MI006_BOIS_EX_FLAG_3'),Join_152_P_Join_Part_v.MI006_WATCHLIST_DT.cast('integer').alias('MI006_WATCHLIST_DT'),Join_152_P_Join_Part_v.MI006_IP_PROVISION_AMOUNT.cast('decimal(18,3)').alias('MI006_IP_PROVISION_AMOUNT'),Join_152_P_Join_Part_v.MI006_INT_TYPE.cast('string').alias('MI006_INT_TYPE'),Join_152_P_Join_Part_v.MI006_BINS_PREMIUM.cast('decimal(18,3)').alias('MI006_BINS_PREMIUM'),Join_152_P_Join_Part_v.MI006_BOIS_CP1_PROV_AMT.cast('decimal(18,3)').alias('MI006_BOIS_CP1_PROV_AMT'),Join_152_P_Join_Part_v.MI006_BOIS_CP3_PROV_AMT.cast('decimal(18,3)').alias('MI006_BOIS_CP3_PROV_AMT'),Join_152_P_Join_Part_v.MI006_P_WOFF_OVERDUE.cast('decimal(18,3)').alias('MI006_P_WOFF_OVERDUE'),Join_152_P_Join_Part_v.MI006_TOTAL_SUB_LEDGER_AMT.cast('decimal(18,3)').alias('MI006_TOTAL_SUB_LEDGER_AMT'),Join_152_P_Join_Part_v.MI006_BOIS_RECALL_IND.cast('string').alias('MI006_BOIS_RECALL_IND'),Join_152_P_Join_Part_v.MI006_NO_OF_INST_ARR.cast('integer').alias('MI006_NO_OF_INST_ARR'),Join_152_P_Join_Part_v.MI006_OUTSTANDING_PRINCI.cast('decimal(18,3)').alias('MI006_OUTSTANDING_PRINCI'),Join_152_P_Join_Part_v.MI006_PRIMRY_NPL.cast('string').alias('MI006_PRIMRY_NPL'),Join_152_P_Join_Part_v.MI006_BOIS_REVERSED_INT.cast('decimal(18,3)').alias('MI006_BOIS_REVERSED_INT'),Join_152_P_Join_Part_v.MI006_BOIS_PROVISIONING_DT.cast('integer').alias('MI006_BOIS_PROVISIONING_DT'),Join_152_P_Join_Part_v.MI006_LITIGATION_DATE.cast('integer').alias('MI006_LITIGATION_DATE'),Join_152_P_Join_Part_v.MI006_ADV_PREP_TXN_AMNT.cast('decimal(18,3)').alias('MI006_ADV_PREP_TXN_AMNT'),Join_152_P_Join_Part_v.MI006_BORM_PRINC_REDUCT_EFF.cast('decimal(18,3)').alias('MI006_BORM_PRINC_REDUCT_EFF'),Join_152_P_Join_Part_v.MI006_BOIS_RETENTION_PERIOD.cast('string').alias('MI006_BOIS_RETENTION_PERIOD'),Join_152_P_Join_Part_v.MI006_ACF_SL_NO.cast('string').alias('MI006_ACF_SL_NO'),Join_152_P_Join_Part_v.MI006_BOIS_RECALL_IND_DT.cast('integer').alias('MI006_BOIS_RECALL_IND_DT'),Join_152_P_Join_Part_v.MI006_BOIS_LOCK_IN.cast('string').alias('MI006_BOIS_LOCK_IN'),Join_152_P_Join_Part_v.MI006_CP7_PROV.cast('decimal(18,3)').alias('MI006_CP7_PROV'),Join_152_P_Join_Part_v.MI006_RECOV_AMT.cast('decimal(18,3)').alias('MI006_RECOV_AMT'),Join_152_P_Join_Part_v.MI006_WORKOUT.cast('string').alias('MI006_WORKOUT'),Join_152_P_Join_Part_v.MI006_WDV_PRIN.cast('decimal(18,3)').alias('MI006_WDV_PRIN'),Join_152_P_Join_Part_v.MI006_WDV_INT.cast('decimal(18,3)').alias('MI006_WDV_INT'),Join_152_P_Join_Part_v.MI006_WDV_LPI.cast('decimal(18,3)').alias('MI006_WDV_LPI'),Join_152_P_Join_Part_v.MI006_WDV_OC.cast('decimal(18,3)').alias('MI006_WDV_OC'),Join_152_P_Join_Part_v.MI006_OFF_BS_AMT.cast('decimal(18,3)').alias('MI006_OFF_BS_AMT'),Join_152_P_Join_Part_v.MI006_LOCK_IN_SRT_DT.cast('string').alias('MI006_LOCK_IN_SRT_DT'),Join_152_P_Join_Part_v.MI006_LOCK_IN_PERIOD.cast('integer').alias('MI006_LOCK_IN_PERIOD'),Join_152_P_Join_Part_v.MI006_DISCH_PEN_AMT1.cast('decimal(18,3)').alias('MI006_DISCH_PEN_AMT1'),Join_152_P_Join_Part_v.MI006_DISCH_PEN_AMT2.cast('decimal(18,3)').alias('MI006_DISCH_PEN_AMT2'),Join_152_P_Join_Part_v.MI006_DISCH_PEN_AMT3.cast('decimal(18,3)').alias('MI006_DISCH_PEN_AMT3'),Join_152_P_Join_Part_v.MI006_HOUSE_MRKT_CODE.cast('string').alias('MI006_HOUSE_MRKT_CODE'),Join_152_P_Join_Part_v.MI006_BOIS_CITIZEN_CODE.cast('string').alias('MI006_BOIS_CITIZEN_CODE'),Join_152_P_Join_Part_v.MI006_BOIS_ATTRITION_DT.cast('integer').alias('MI006_BOIS_ATTRITION_DT'),Join_152_P_Join_Part_v.MI006_BOIS_ATTRITION_CODE.cast('string').alias('MI006_BOIS_ATTRITION_CODE'),Join_152_P_Join_Part_v.MI006_BOIS_STAF_GRAC_END_DT.cast('integer').alias('MI006_BOIS_STAF_GRAC_END_DT'),Join_152_P_Join_Part_v.MI006_BOIS_PREV_CAP_BAL.cast('decimal(18,3)').alias('MI006_BOIS_PREV_CAP_BAL'),Join_152_P_Join_Part_v.MI006_SHORTFALL_DATE.cast('integer').alias('MI006_SHORTFALL_DATE'),Join_152_P_Join_Part_v.MI006_TYPE_OF_RECALL.cast('string').alias('MI006_TYPE_OF_RECALL'),Join_152_P_Join_Part_v.MI006_CHCD_COLLECN_HUB_CD.cast('string').alias('MI006_CHCD_COLLECN_HUB_CD'),Join_152_P_Join_Part_v.L_BASM_BASE_ID.cast('string').alias('L_BASM_BASE_ID'),Join_152_P_Join_Part_v.L_BASM_RATE_ID.cast('string').alias('L_BASM_RATE_ID'),Join_152_P_Join_Part_v.BASM_RATE_ID.cast('string').alias('BASM_RATE_ID'),Join_152_P_Join_Part_v.COMPANY_CODE.cast('string').alias('COMPANY_CODE'),Join_152_P_Join_Part_v.EFF_RATE.cast('decimal(16,4)').alias('EFF_RATE'),Join_152_P_Join_Part_v.MI006_WRITEOFF_AMOUNT.cast('decimal(18,3)').alias('MI006_WRITEOFF_AMOUNT'),Join_152_P_Join_Part_v.P_RATE.cast('decimal(7,4)').alias('P_RATE'),Join_152_P_Join_Part_v.P_IND.cast('string').alias('P_IND'),Join_152_P_Join_Part_v.RECALL_IND.cast('string').alias('RECALL_IND'),Join_152_P_Join_Part_v.RECALL_RATE.cast('decimal(6,4)').alias('RECALL_RATE'),Join_152_P_Join_Part_v.FINE_RATE.cast('decimal(6,4)').alias('FINE_RATE'),Join_152_P_Join_Part_v.RT_INCR.cast('decimal(7,5)').alias('RT_INCR'),Join_152_P_Join_Part_v.MI006_MATURITY_DATE.cast('integer').alias('MI006_MATURITY_DATE'),Join_152_P_Join_Part_v.MI006_CAPN_FREQ.cast('string').alias('MI006_CAPN_FREQ'),Join_152_P_Join_Part_v.MI006_BORM_REPAY_METHOD.cast('string').alias('MI006_BORM_REPAY_METHOD'),Join_152_P_Join_Part_v.MIS006_NEXT_ACCR_DT.cast('integer').alias('MIS006_NEXT_ACCR_DT'),Join_152_P_Join_Part_v.MI006_ORIG_MAT_DATE.cast('integer').alias('MI006_ORIG_MAT_DATE'),Join_152_P_Join_Part_v.MI006_NOTICE_FLAG.cast('string').alias('MI006_NOTICE_FLAG'),Join_152_P_Join_Part_v.MI006_NOTICE_DATE.cast('integer').alias('MI006_NOTICE_DATE'),Join_152_P_Join_Part_v.MI006_AUTO_DBT_ACCT_NO.cast('string').alias('MI006_AUTO_DBT_ACCT_NO'),Join_152_P_Join_Part_v.MI006_AUTO_DBT_STRT_DT.cast('integer').alias('MI006_AUTO_DBT_STRT_DT'),Join_152_P_Join_Part_v.MI006_AUTO_DBT_END_DT.cast('integer').alias('MI006_AUTO_DBT_END_DT'),Join_152_P_Join_Part_v.MI006_MON_PRD_STRT_DT.cast('integer').alias('MI006_MON_PRD_STRT_DT'),Join_152_P_Join_Part_v.MI006_MON_PRD_END_DT.cast('integer').alias('MI006_MON_PRD_END_DT'),Join_152_P_Join_Part_v.MI006_RR_MARGIN.cast('decimal(4,2)').alias('MI006_RR_MARGIN'),Join_152_P_Join_Part_v.MI006_AKPK_DATE.cast('integer').alias('MI006_AKPK_DATE'),Join_152_P_Join_Part_v.MI006_MIN_REPAY_AMT.cast('decimal(18,3)').alias('MI006_MIN_REPAY_AMT'),Join_152_P_Join_Part_v.MI006_FOUR_SCHD_ISS_DT.cast('integer').alias('MI006_FOUR_SCHD_ISS_DT'),Join_152_P_Join_Part_v.MI006_RULE_THREE_ISS_DT.cast('integer').alias('MI006_RULE_THREE_ISS_DT'),Join_152_P_Join_Part_v.MI006_REPO_ORDER_DT.cast('integer').alias('MI006_REPO_ORDER_DT'),Join_152_P_Join_Part_v.MI006_NOT_ISS_DT.cast('integer').alias('MI006_NOT_ISS_DT'),Join_152_P_Join_Part_v.MI006_PROMPT_PAY_CNTR.cast('integer').alias('MI006_PROMPT_PAY_CNTR'),Join_152_P_Join_Part_v.MI006_BUY_BACK_CNTR.cast('integer').alias('MI006_BUY_BACK_CNTR'),Join_152_P_Join_Part_v.MI006_STEP_UP_PERIOD.cast('integer').alias('MI006_STEP_UP_PERIOD'),Join_152_P_Join_Part_v.MI006_REDRAW_AMT.cast('decimal(18,3)').alias('MI006_REDRAW_AMT'),Join_152_P_Join_Part_v.MI006_WRITE_OFF_VAL_TAG.cast('string').alias('MI006_WRITE_OFF_VAL_TAG'),Join_152_P_Join_Part_v.MI006_WRITE_OFF_JSTFCATN.cast('string').alias('MI006_WRITE_OFF_JSTFCATN'),Join_152_P_Join_Part_v.MI006_WRITE_OFF_TAG_DT.cast('integer').alias('MI006_WRITE_OFF_TAG_DT'),Join_152_P_Join_Part_v.MI006_WRITE_OFF_EXCL_TAG.cast('string').alias('MI006_WRITE_OFF_EXCL_TAG'),Join_152_P_Join_Part_v.MI006_WRITE_OFF_EXCL_RSN.cast('string').alias('MI006_WRITE_OFF_EXCL_RSN'),Join_152_P_Join_Part_v.MI006_WRITE_OFF_EXCL_DT.cast('integer').alias('MI006_WRITE_OFF_EXCL_DT'),Join_152_P_Join_Part_v.MI006_ORIG_REM_REPAYS.cast('decimal(5,0)').alias('MI006_ORIG_REM_REPAYS'),Join_152_P_Join_Part_v.MI006_STMT_DELIVERY.cast('string').alias('MI006_STMT_DELIVERY'),Join_152_P_Join_Part_v.MI006_EMAIL_ADDR.cast('string').alias('MI006_EMAIL_ADDR'),Join_152_P_Join_Part_v.MI006_CHRG_RPY_AMT.cast('decimal(18,3)').alias('MI006_CHRG_RPY_AMT'),Join_152_P_Join_Part_v.MI006_PRIN_RPY_AMT.cast('decimal(18,3)').alias('MI006_PRIN_RPY_AMT'),Join_152_P_Join_Part_v.MI006_COURT_DEP_RPY_AMT.cast('decimal(18,3)').alias('MI006_COURT_DEP_RPY_AMT'),Join_152_P_Join_Part_v.MI006_INT_RPY_AMT.cast('decimal(18,3)').alias('MI006_INT_RPY_AMT'),Join_152_P_Join_Part_v.MI006_BUY_BACK_DATE.cast('integer').alias('MI006_BUY_BACK_DATE'),Join_152_P_Join_Part_v.MI006_BUY_BACK_IND.cast('string').alias('MI006_BUY_BACK_IND'),Join_152_P_Join_Part_v.MI006_PREV_DAY_IFRS_EIR_RATE.cast('decimal(9,6)').alias('MI006_PREV_DAY_IFRS_EIR_RATE'),Join_152_P_Join_Part_v.MI006_PREV_DAY_IFRS_ESTM_LOAN_TERM.cast('decimal(5,0)').alias('MI006_PREV_DAY_IFRS_ESTM_LOAN_TERM'),Join_152_P_Join_Part_v.MI006_MAX_REPAY_AGE.cast('string').alias('MI006_MAX_REPAY_AGE'),Join_152_P_Join_Part_v.MI006_MORTGAGE_FACILITY_IND.cast('string').alias('MI006_MORTGAGE_FACILITY_IND'),Join_152_P_Join_Part_v.MI006_SPGA_NO.cast('string').alias('MI006_SPGA_NO'),Join_152_P_Join_Part_v.MI006_DEDUCT_CODE.cast('string').alias('MI006_DEDUCT_CODE'),Join_152_P_Join_Part_v.MI006_DED_STRT_MON.cast('string').alias('MI006_DED_STRT_MON'),Join_152_P_Join_Part_v.MI006_DED_END_MON.cast('string').alias('MI006_DED_END_MON'),Join_152_P_Join_Part_v.MI006_EXC_DPD_WOFF_VAL.cast('string').alias('MI006_EXC_DPD_WOFF_VAL'),Join_152_P_Join_Part_v.MI006_GPP_INT_CAP_AMT.cast('decimal(17,3)').alias('MI006_GPP_INT_CAP_AMT'),Join_152_P_Join_Part_v.MI006_GPP_AMORT_AMT_MTH.cast('decimal(17,3)').alias('MI006_GPP_AMORT_AMT_MTH'),Join_152_P_Join_Part_v.MI006_RNR_REM_PROFIT.cast('decimal(17,3)').alias('MI006_RNR_REM_PROFIT'),Join_152_P_Join_Part_v.MI006_RNR_MONTHLY_PROFIT.cast('decimal(17,3)').alias('MI006_RNR_MONTHLY_PROFIT'),Join_152_P_Join_Part_v.MI006_INT_THRES_CAP_PER.cast('decimal(5,2)').alias('MI006_INT_THRES_CAP_PER'),Join_152_P_Join_Part_v.MI006_PP_ADD_INT_AMT.cast('decimal(17,3)').alias('MI006_PP_ADD_INT_AMT'),Join_152_P_Join_Part_v.MI006_PP_PROMPT_PAY_COUNTER.cast('decimal(2,0)').alias('MI006_PP_PROMPT_PAY_COUNTER'),Join_152_P_Join_Part_v.MI006_PP_MON_PRD_END_DATE.cast('integer').alias('MI006_PP_MON_PRD_END_DATE'),Join_152_P_Join_Part_v.MI006_REL_RED_RSN_CD.cast('string').alias('MI006_REL_RED_RSN_CD'),Join_152_P_Join_Part_v.MI006_RSN_CD_MAINTNCE_DT.cast('integer').alias('MI006_RSN_CD_MAINTNCE_DT'),Join_152_P_Join_Part_v.MI006_INST_CHG_EXP_DT.cast('integer').alias('MI006_INST_CHG_EXP_DT'),Join_152_P_Join_Part_v.MI006_AKPK_SPC_TAG.cast('string').alias('MI006_AKPK_SPC_TAG'),Join_152_P_Join_Part_v.MI006_AKPK_SPC_START_DT.cast('integer').alias('MI006_AKPK_SPC_START_DT'),Join_152_P_Join_Part_v.MI006_AKPK_SPC_END_DT.cast('integer').alias('MI006_AKPK_SPC_END_DT'),Join_152_P_Join_Part_v.MI006_AKPK_MATRIX.cast('string').alias('MI006_AKPK_MATRIX'),Join_152_P_Join_Part_v.MI006_MORA_ACCR_INT.cast('decimal(17,3)').alias('MI006_MORA_ACCR_INT'),Join_152_P_Join_Part_v.MI006_MANUAL_IMPAIR.cast('string').alias('MI006_MANUAL_IMPAIR'),Join_152_P_Join_Part_v.MI006_PRE_DISB_DOC_STAT_DT.cast('integer').alias('MI006_PRE_DISB_DOC_STAT_DT'),Join_152_P_Join_Part_v.MI006_UMA_NOTC_GEN_DT.cast('integer').alias('MI006_UMA_NOTC_GEN_DT'),Join_152_P_Join_Part_v.MI006_ACC_DUE_DEF_INST.cast('string').alias('MI006_ACC_DUE_DEF_INST'),Join_152_P_Join_Part_v.MI006_EX_FLAG_1.cast('string').alias('MI006_EX_FLAG_1'),Join_152_P_Join_Part_v.MI006_INSTALLMENT_NO.cast('string').alias('MI006_INSTALLMENT_NO'),Join_152_P_Join_Part_v.MI006_ADV_PREPAY_FLAG.cast('string').alias('MI006_ADV_PREPAY_FLAG'),Join_152_P_Join_Part_v.MI006_REL_RED_RSN_FL_CD.cast('string').alias('MI006_REL_RED_RSN_FL_CD'),Join_152_P_Join_Part_v.MI006_SETLEMENT_WAIVER.cast('string').alias('MI006_SETLEMENT_WAIVER'),Join_152_P_Join_Part_v.MI006_INST_REINSTATE.cast('string').alias('MI006_INST_REINSTATE'),Join_152_P_Join_Part_v.MI006_REINSTATE_DATE.cast('integer').alias('MI006_REINSTATE_DATE'),Join_152_P_Join_Part_v.MI006_REINSTATE_COUNT.cast('string').alias('MI006_REINSTATE_COUNT'),Join_152_P_Join_Part_v.MI006_MFA_FLAG.cast('string').alias('MI006_MFA_FLAG'),Join_152_P_Join_Part_v.MI006_MFA_COUNTER.cast('string').alias('MI006_MFA_COUNTER'),Join_152_P_Join_Part_v.MI006_MFA_EFF_DT.cast('integer').alias('MI006_MFA_EFF_DT'),Join_152_P_Join_Part_v.MI006_MFA_MAINT_DT.cast('integer').alias('MI006_MFA_MAINT_DT'),Join_152_P_Join_Part_v.MI006_EIY.cast('decimal(5,2)').alias('MI006_EIY'))
    
    Join_152_LnkJoin_v = Join_152_LnkJoin_v.selectExpr("RTRIM(SOC_NO) AS SOC_NO","RTRIM(P_BASE_ID) AS P_BASE_ID","RTRIM(P_RATE_ID) AS P_RATE_ID","P_EFF_RATE","B_KEY","INT_RATE","MI006_INT_RATE","MI006_STORE_RATE","MI006_DISCH_AMOUNT","MI006_KEY_POINTER","RTRIM(MI006_ACCT_SOLD_TO) AS MI006_ACCT_SOLD_TO","MI006_ACCR_INCEPT","MI006_AMT_UNDRAWN","RTRIM(MI006_AUTO_DEBIT_IND) AS MI006_AUTO_DEBIT_IND","MI006_BASE_RATE","MI006_BOIS_LST_ADV_DATE","MI006_CLASS_CODE","RTRIM(MI006_CLASSIFY_FLAG) AS MI006_CLASSIFY_FLAG","MI006_FIRST_INST_DATE","MI006_FIRST_REPAY_DATE","RTRIM(MI006_LOAN_SOLD_CAGAMAS_FLAG) AS MI006_LOAN_SOLD_CAGAMAS_FLAG","MI006_NO_INST_PAID","RTRIM(MI006_NPA_IND) AS MI006_NPA_IND","RTRIM(MI006_NPL_CLASS_STAT) AS MI006_NPL_CLASS_STAT","RTRIM(MI006_NPL_STATUS) AS MI006_NPL_STATUS","MI006_P_WF_FEE_AMT","MI006_P_WF_INT_AMT","MI006_P_WF_LT_CHRG_AMT","MI006_P_WF_PRIN_AMT","MI006_RATE_ID","RTRIM(MI006_WRITEOFF_FLAG) AS MI006_WRITEOFF_FLAG","MI006_BORM_CHARGE_OFF_FLAG","MI006_AM_ASSEC_TAG","MI006_ACCT_SOLD_TO_NAME","MI006_WRIT_OFF_INT","MI006_WRIT_OFF_PRIN","MI006_WRIT_OFF_CHARGE","MI006_BASM_DATE","MI006_BASM_BASE_ID","MI006_PRIME_RATE","MI006_BASM_DESCRIPTION","RTRIM(MI006_CAPN_METHOD) AS MI006_CAPN_METHOD","RTRIM(MI006_CCC_REGULATED_IND) AS MI006_CCC_REGULATED_IND","MI006_FUND_CODE","MI006_EXTRACTED_DATE","MI006_INT_ONLY_EXP_DATE","RTRIM(MI006_REVOLVING_CR_IND) AS MI006_REVOLVING_CR_IND","MI006_PROV_AMT","M1006_ORIGINAL_EXP_DT","MI006_FIRST_ADV_DATE","MI006_PUR_CONTRACT_NO","MI006_COMMISSION_AMT","MI006_TYPE","RTRIM(MI006_STOP_ACCRUAL) AS MI006_STOP_ACCRUAL","MI006_SHADOW_INT_ACCURAL","MI006_SHADOW_CURR_YR_INT","MI006_TIER_GROUP_ID","MI006_SETELMENT_ACCT_NO","MI006_DATE_OF_SALE","MI006_RETENTION_AMOUNT","RTRIM(MI006_RETENTION_PERIOD) AS MI006_RETENTION_PERIOD","RTRIM(MI006_SECURITY_IND) AS MI006_SECURITY_IND","RTRIM(MI006_ACCR_TYPE) AS MI006_ACCR_TYPE","MI006_ACQUISITION_FEE","RTRIM(MI006_AKPK_CODE) AS MI006_AKPK_CODE","MI006_APPROVED_AUTH","MI006_BOIS_5TH_SCHED_ISS_DT","MI006_BOIS_ACQUISITION_FEE","MI006_BOIS_APPROVED_BY","MI006_BOIS_GROSS_EFF_YIELD","MI006_BOIS_LAST_ADV_DATE","MI006_BOIS_NET_EFF_YEILD","MI006_BOIS_NPA_BAL","MI006_BOIS_PROV_AMT","MI006_BOIS_PUR_CNTRT_NO","MI006_BOIS_SETTLE_ACCT","MI006_BOIS_SHDW_CURR_YR_INT","MI006_BOIS_SHDW_INT_ACCR","MI006_BOIS_TOT_ACCR_CAP","MI006_BUS_TYPE","MI006_DUE_DATE","MI006_FIFTH_SCHD_ISS_DAT","MI006_INDEX_CODE","RTRIM(MI006_ISLAMIC_BANK) AS MI006_ISLAMIC_BANK","RTRIM(MI006_MONTHS_IN_ARR) AS MI006_MONTHS_IN_ARR","RTRIM(MI006_MULTI_TIER_FLG) AS MI006_MULTI_TIER_FLG","RTRIM(MI006_NON_ACCR_IND) AS MI006_NON_ACCR_IND","MI006_NON_WF_AMT","MI006_ORIG_BRCH_CODE","MI006_OTHER_CHARGES","MI006_PERS_BNK_ID","MI006_RTC_EXP_DATE","RTRIM(MI006_RTC_METHOD_IND) AS MI006_RTC_METHOD_IND","RTRIM(MI006_SALE_REPUR_CODE) AS MI006_SALE_REPUR_CODE","MI006_SECURITY_IND_DATE","RTRIM(MI006_STAFF_PRODUCT_IND) AS MI006_STAFF_PRODUCT_IND","MI006_STAMPING_FEE","RTRIM(MI006_TIER_METHOD) AS MI006_TIER_METHOD","RTRIM(MI006_WATCHLIST_TAG) AS MI006_WATCHLIST_TAG","RTRIM(MI006_WRITEOFF_STATUS_CODE) AS MI006_WRITEOFF_STATUS_CODE","RTRIM(MI006_INT_REPAY_FREQ) AS MI006_INT_REPAY_FREQ","MI006_BLDVNN_NXT_PRIN_REP","MI006_BLDVNN_NXT_INT_REPA","MI006_BOIS_SOLD_AMOUNT","RTRIM(MI006_BOIS_LOAN_TYPE) AS MI006_BOIS_LOAN_TYPE","RTRIM(MI006_BOIS_EX_FLAG_3) AS MI006_BOIS_EX_FLAG_3","MI006_WATCHLIST_DT","MI006_IP_PROVISION_AMOUNT","RTRIM(MI006_INT_TYPE) AS MI006_INT_TYPE","MI006_BINS_PREMIUM","MI006_BOIS_CP1_PROV_AMT","MI006_BOIS_CP3_PROV_AMT","MI006_P_WOFF_OVERDUE","MI006_TOTAL_SUB_LEDGER_AMT","RTRIM(MI006_BOIS_RECALL_IND) AS MI006_BOIS_RECALL_IND","MI006_NO_OF_INST_ARR","MI006_OUTSTANDING_PRINCI","RTRIM(MI006_PRIMRY_NPL) AS MI006_PRIMRY_NPL","MI006_BOIS_REVERSED_INT","MI006_BOIS_PROVISIONING_DT","MI006_LITIGATION_DATE","MI006_ADV_PREP_TXN_AMNT","MI006_BORM_PRINC_REDUCT_EFF","RTRIM(MI006_BOIS_RETENTION_PERIOD) AS MI006_BOIS_RETENTION_PERIOD","MI006_ACF_SL_NO","MI006_BOIS_RECALL_IND_DT","RTRIM(MI006_BOIS_LOCK_IN) AS MI006_BOIS_LOCK_IN","MI006_CP7_PROV","MI006_RECOV_AMT","RTRIM(MI006_WORKOUT) AS MI006_WORKOUT","MI006_WDV_PRIN","MI006_WDV_INT","MI006_WDV_LPI","MI006_WDV_OC","MI006_OFF_BS_AMT","RTRIM(MI006_LOCK_IN_SRT_DT) AS MI006_LOCK_IN_SRT_DT","MI006_LOCK_IN_PERIOD","MI006_DISCH_PEN_AMT1","MI006_DISCH_PEN_AMT2","MI006_DISCH_PEN_AMT3","MI006_HOUSE_MRKT_CODE","RTRIM(MI006_BOIS_CITIZEN_CODE) AS MI006_BOIS_CITIZEN_CODE","MI006_BOIS_ATTRITION_DT","RTRIM(MI006_BOIS_ATTRITION_CODE) AS MI006_BOIS_ATTRITION_CODE","MI006_BOIS_STAF_GRAC_END_DT","MI006_BOIS_PREV_CAP_BAL","MI006_SHORTFALL_DATE","RTRIM(MI006_TYPE_OF_RECALL) AS MI006_TYPE_OF_RECALL","MI006_CHCD_COLLECN_HUB_CD","RTRIM(L_BASM_BASE_ID) AS L_BASM_BASE_ID","RTRIM(L_BASM_RATE_ID) AS L_BASM_RATE_ID","RTRIM(BASM_RATE_ID) AS BASM_RATE_ID","RTRIM(COMPANY_CODE) AS COMPANY_CODE","EFF_RATE","MI006_WRITEOFF_AMOUNT","P_RATE","P_IND","RTRIM(RECALL_IND) AS RECALL_IND","RECALL_RATE","FINE_RATE","RT_INCR","MI006_MATURITY_DATE","RTRIM(MI006_CAPN_FREQ) AS MI006_CAPN_FREQ","RTRIM(MI006_BORM_REPAY_METHOD) AS MI006_BORM_REPAY_METHOD","MIS006_NEXT_ACCR_DT","MI006_ORIG_MAT_DATE","MI006_NOTICE_FLAG","MI006_NOTICE_DATE","MI006_AUTO_DBT_ACCT_NO","MI006_AUTO_DBT_STRT_DT","MI006_AUTO_DBT_END_DT","MI006_MON_PRD_STRT_DT","MI006_MON_PRD_END_DT","MI006_RR_MARGIN","MI006_AKPK_DATE","MI006_MIN_REPAY_AMT","MI006_FOUR_SCHD_ISS_DT","MI006_RULE_THREE_ISS_DT","MI006_REPO_ORDER_DT","MI006_NOT_ISS_DT","MI006_PROMPT_PAY_CNTR","MI006_BUY_BACK_CNTR","MI006_STEP_UP_PERIOD","MI006_REDRAW_AMT","RTRIM(MI006_WRITE_OFF_VAL_TAG) AS MI006_WRITE_OFF_VAL_TAG","RTRIM(MI006_WRITE_OFF_JSTFCATN) AS MI006_WRITE_OFF_JSTFCATN","MI006_WRITE_OFF_TAG_DT","RTRIM(MI006_WRITE_OFF_EXCL_TAG) AS MI006_WRITE_OFF_EXCL_TAG","RTRIM(MI006_WRITE_OFF_EXCL_RSN) AS MI006_WRITE_OFF_EXCL_RSN","MI006_WRITE_OFF_EXCL_DT","MI006_ORIG_REM_REPAYS","RTRIM(MI006_STMT_DELIVERY) AS MI006_STMT_DELIVERY","MI006_EMAIL_ADDR","MI006_CHRG_RPY_AMT","MI006_PRIN_RPY_AMT","MI006_COURT_DEP_RPY_AMT","MI006_INT_RPY_AMT","MI006_BUY_BACK_DATE","RTRIM(MI006_BUY_BACK_IND) AS MI006_BUY_BACK_IND","MI006_PREV_DAY_IFRS_EIR_RATE","MI006_PREV_DAY_IFRS_ESTM_LOAN_TERM","MI006_MAX_REPAY_AGE","MI006_MORTGAGE_FACILITY_IND","MI006_SPGA_NO","MI006_DEDUCT_CODE","MI006_DED_STRT_MON","MI006_DED_END_MON","MI006_EXC_DPD_WOFF_VAL","MI006_GPP_INT_CAP_AMT","MI006_GPP_AMORT_AMT_MTH","MI006_RNR_REM_PROFIT","MI006_RNR_MONTHLY_PROFIT","MI006_INT_THRES_CAP_PER","MI006_PP_ADD_INT_AMT","MI006_PP_PROMPT_PAY_COUNTER","MI006_PP_MON_PRD_END_DATE","MI006_REL_RED_RSN_CD","MI006_RSN_CD_MAINTNCE_DT","MI006_INST_CHG_EXP_DT","MI006_AKPK_SPC_TAG","MI006_AKPK_SPC_START_DT","MI006_AKPK_SPC_END_DT","MI006_AKPK_MATRIX","MI006_MORA_ACCR_INT","MI006_MANUAL_IMPAIR","MI006_PRE_DISB_DOC_STAT_DT","MI006_UMA_NOTC_GEN_DT","MI006_ACC_DUE_DEF_INST","MI006_EX_FLAG_1","MI006_INSTALLMENT_NO","MI006_ADV_PREPAY_FLAG","MI006_REL_RED_RSN_FL_CD","MI006_SETLEMENT_WAIVER","MI006_INST_REINSTATE","MI006_REINSTATE_DATE","MI006_REINSTATE_COUNT","MI006_MFA_FLAG","MI006_MFA_COUNTER","MI006_MFA_EFF_DT","MI006_MFA_MAINT_DT","MI006_EIY").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'SOC_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'P_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'P_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'P_EFF_RATE', 'type': 'decimal(16,4)', 'nullable': True, 'metadata': {}}, {'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'INT_RATE', 'type': 'decimal(7,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_RATE', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STORE_RATE', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_KEY_POINTER', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACCT_SOLD_TO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ACCR_INCEPT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AMT_UNDRAWN', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AUTO_DEBIT_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BASE_RATE', 'type': 'decimal(16,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LST_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CLASS_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CLASSIFY_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_FIRST_INST_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FIRST_REPAY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_SOLD_CAGAMAS_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NO_INST_PAID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NPA_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NPL_CLASS_STAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NPL_STATUS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_P_WF_FEE_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WF_INT_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WF_LT_CHRG_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WF_PRIN_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITEOFF_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BORM_CHARGE_OFF_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AM_ASSEC_TAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACCT_SOLD_TO_NAME', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRIT_OFF_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRIT_OFF_PRIN', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRIT_OFF_CHARGE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BASM_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BASM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRIME_RATE', 'type': 'decimal(9,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BASM_DESCRIPTION', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAPN_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_CCC_REGULATED_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_FUND_CODE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EXTRACTED_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_ONLY_EXP_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REVOLVING_CR_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'M1006_ORIGINAL_EXP_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FIRST_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PUR_CONTRACT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_COMMISSION_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TYPE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STOP_ACCRUAL', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SHADOW_INT_ACCURAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SHADOW_CURR_YR_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TIER_GROUP_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SETELMENT_ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DATE_OF_SALE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RETENTION_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RETENTION_PERIOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_SECURITY_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ACCR_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ACQUISITION_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_APPROVED_AUTH', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_5TH_SCHED_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_ACQUISITION_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_APPROVED_BY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_GROSS_EFF_YIELD', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LAST_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_NET_EFF_YEILD', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_NPA_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PUR_CNTRT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SETTLE_ACCT', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SHDW_CURR_YR_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SHDW_INT_ACCR', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_TOT_ACCR_CAP', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BUS_TYPE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DUE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FIFTH_SCHD_ISS_DAT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ISLAMIC_BANK', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_MONTHS_IN_ARR', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_MULTI_TIER_FLG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NON_ACCR_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NON_WF_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ORIG_BRCH_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OTHER_CHARGES', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PERS_BNK_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RTC_EXP_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RTC_METHOD_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SALE_REPUR_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SECURITY_IND_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STAFF_PRODUCT_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_STAMPING_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TIER_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_WATCHLIST_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_WRITEOFF_STATUS_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_INT_REPAY_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BLDVNN_NXT_PRIN_REP', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_NXT_INT_REPA', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SOLD_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LOAN_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_BOIS_EX_FLAG_3', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_WATCHLIST_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_IP_PROVISION_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BINS_PREMIUM', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_CP1_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_CP3_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WOFF_OVERDUE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TOTAL_SUB_LEDGER_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_RECALL_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_NO_OF_INST_ARR', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OUTSTANDING_PRINCI', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRIMRY_NPL', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOIS_REVERSED_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PROVISIONING_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LITIGATION_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADV_PREP_TXN_AMNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_PRINC_REDUCT_EFF', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_RETENTION_PERIOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_ACF_SL_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_RECALL_IND_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LOCK_IN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_CP7_PROV', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RECOV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WORKOUT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_WDV_PRIN', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WDV_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WDV_LPI', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WDV_OC', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OFF_BS_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOCK_IN_SRT_DT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_LOCK_IN_PERIOD', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_PEN_AMT1', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_PEN_AMT2', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_PEN_AMT3', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_HOUSE_MRKT_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_CITIZEN_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOIS_ATTRITION_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_ATTRITION_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BOIS_STAF_GRAC_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PREV_CAP_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SHORTFALL_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TYPE_OF_RECALL', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_CHCD_COLLECN_HUB_CD', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'L_BASM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'L_BASM_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'BASM_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'COMPANY_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'EFF_RATE', 'type': 'decimal(16,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITEOFF_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'P_RATE', 'type': 'decimal(7,4)', 'nullable': True, 'metadata': {}}, {'name': 'P_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'RECALL_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'RECALL_RATE', 'type': 'decimal(6,4)', 'nullable': True, 'metadata': {}}, {'name': 'FINE_RATE', 'type': 'decimal(6,4)', 'nullable': True, 'metadata': {}}, {'name': 'RT_INCR', 'type': 'decimal(7,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MATURITY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAPN_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BORM_REPAY_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MIS006_NEXT_ACCR_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ORIG_MAT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NOTICE_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NOTICE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AUTO_DBT_ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AUTO_DBT_STRT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AUTO_DBT_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MON_PRD_STRT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MON_PRD_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RR_MARGIN', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MIN_REPAY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FOUR_SCHD_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RULE_THREE_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REPO_ORDER_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NOT_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PROMPT_PAY_CNTR', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BUY_BACK_CNTR', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STEP_UP_PERIOD', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REDRAW_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITE_OFF_VAL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_WRITE_OFF_JSTFCATN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(30)'}}, {'name': 'MI006_WRITE_OFF_TAG_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITE_OFF_EXCL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_WRITE_OFF_EXCL_RSN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(30)'}}, {'name': 'MI006_WRITE_OFF_EXCL_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ORIG_REM_REPAYS', 'type': 'decimal(5,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STMT_DELIVERY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_EMAIL_ADDR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CHRG_RPY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRIN_RPY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_COURT_DEP_RPY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_RPY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BUY_BACK_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BUY_BACK_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_PREV_DAY_IFRS_EIR_RATE', 'type': 'decimal(9,6)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PREV_DAY_IFRS_ESTM_LOAN_TERM', 'type': 'decimal(5,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MAX_REPAY_AGE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MORTGAGE_FACILITY_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SPGA_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DEDUCT_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DED_STRT_MON', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DED_END_MON', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EXC_DPD_WOFF_VAL', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GPP_INT_CAP_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GPP_AMORT_AMT_MTH', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RNR_REM_PROFIT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RNR_MONTHLY_PROFIT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_THRES_CAP_PER', 'type': 'decimal(5,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PP_ADD_INT_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PP_PROMPT_PAY_COUNTER', 'type': 'decimal(2,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PP_MON_PRD_END_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REL_RED_RSN_CD', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RSN_CD_MAINTNCE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INST_CHG_EXP_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_SPC_TAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_SPC_START_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_SPC_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_MATRIX', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MORA_ACCR_INT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MANUAL_IMPAIR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRE_DISB_DOC_STAT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UMA_NOTC_GEN_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACC_DUE_DEF_INST', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EX_FLAG_1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INSTALLMENT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADV_PREPAY_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REL_RED_RSN_FL_CD', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SETLEMENT_WAIVER', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INST_REINSTATE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REINSTATE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REINSTATE_COUNT', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MFA_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MFA_COUNTER', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MFA_EFF_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MFA_MAINT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EIY', 'type': 'decimal(5,2)', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Join_152_LnkJoin_v PURGE").show()
    
    print("Join_152_LnkJoin_v")
    
    print(Join_152_LnkJoin_v.schema.json())
    
    print("count:{}".format(Join_152_LnkJoin_v.count()))
    
    Join_152_LnkJoin_v.show(1000,False)
    
    Join_152_LnkJoin_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Join_152_LnkJoin_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_73_LnkJoin_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Join_152_LnkJoin_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Join_152_LnkJoin_v')
    
    Transformer_73_LnkJoin_Part_v=Join_152_LnkJoin_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Transformer_73_LnkJoin_Part_v PURGE").show()
    
    print("Transformer_73_LnkJoin_Part_v")
    
    print(Transformer_73_LnkJoin_Part_v.schema.json())
    
    print("count:{}".format(Transformer_73_LnkJoin_Part_v.count()))
    
    Transformer_73_LnkJoin_Part_v.show(1000,False)
    
    Transformer_73_LnkJoin_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Transformer_73_LnkJoin_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_73(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_73_LnkJoin_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Transformer_73_LnkJoin_Part_v')
    
    Transformer_73_v = Transformer_73_LnkJoin_Part_v.withColumn('Penalty', expr("""CASE WHEN COALESCE(RECALL_IND, '') = 'D' THEN INT_RATE WHEN P_IND = 'F' THEN FINE_RATE WHEN P_IND = 'A1' THEN RECALL_RATE WHEN P_IND = 'A2' THEN (INT_RATE + RT_INCR + P_RATE) WHEN P_IND = 'S2' THEN P_RATE WHEN NOT P_EFF_RATE IS NULL THEN P_EFF_RATE ELSE P_RATE END AS result_column""").cast('decimal(7,4)').alias('Penalty'))
    
    Transformer_73_lnk_BOIS_Tgt_v = Transformer_73_v.select(expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM B_KEY))""").cast('string').alias('B_KEY'),col('MI006_INT_RATE').cast('decimal(8,4)').alias('MI006_INT_RATE'),col('MI006_STORE_RATE').cast('decimal(8,4)').alias('MI006_STORE_RATE'),col('MI006_DISCH_AMOUNT').cast('decimal(18,3)').alias('MI006_DISCH_AMOUNT'),col('MI006_KEY_POINTER').cast('string').alias('MI006_KEY_POINTER'),col('MI006_ACCT_SOLD_TO').cast('string').alias('MI006_ACCT_SOLD_TO'),col('MI006_ACCR_INCEPT').cast('decimal(18,3)').alias('MI006_ACCR_INCEPT'),col('MI006_AMT_UNDRAWN').cast('decimal(18,3)').alias('MI006_AMT_UNDRAWN'),col('MI006_AUTO_DEBIT_IND').cast('string').alias('MI006_AUTO_DEBIT_IND'),expr("""IF(ISNULL(MI006_BASE_RATE), 0, MI006_BASE_RATE)""").cast('decimal(16,3)').alias('MI006_BASE_RATE'),expr("""IF(ISNULL(MI006_BASE_RATE), 0, MI006_BASE_RATE)""").cast('decimal(16,3)').alias('MI006_BASM_RATE'),col('MI006_BOIS_LST_ADV_DATE').cast('integer').alias('MI006_BOIS_LST_ADV_DATE'),col('MI006_CLASS_CODE').cast('string').alias('MI006_CLASS_CODE'),col('MI006_CLASSIFY_FLAG').cast('string').alias('MI006_CLASSIFY_FLAG'),col('MI006_FIRST_INST_DATE').cast('integer').alias('MI006_FIRST_INST_DATE'),col('MI006_FIRST_REPAY_DATE').cast('integer').alias('MI006_FIRST_REPAY_DATE'),col('MI006_LOAN_SOLD_CAGAMAS_FLAG').cast('string').alias('MI006_LOAN_SOLD_CAGAMAS_FLAG'),col('MI006_NO_INST_PAID').cast('string').alias('MI006_NO_INST_PAID'),col('MI006_NPA_IND').cast('string').alias('MI006_NPA_IND'),col('MI006_NPL_CLASS_STAT').cast('string').alias('MI006_NPL_CLASS_STAT'),col('MI006_NPL_STATUS').cast('string').alias('MI006_NPL_STATUS'),col('MI006_P_WF_FEE_AMT').cast('decimal(18,3)').alias('MI006_P_WF_FEE_AMT'),col('MI006_P_WF_INT_AMT').cast('decimal(18,3)').alias('MI006_P_WF_INT_AMT'),col('MI006_P_WF_LT_CHRG_AMT').cast('decimal(18,3)').alias('MI006_P_WF_LT_CHRG_AMT'),col('MI006_P_WF_PRIN_AMT').cast('decimal(18,3)').alias('MI006_P_WF_PRIN_AMT'),col('MI006_RATE_ID').cast('string').alias('MI006_RATE_ID'),col('MI006_WRITEOFF_FLAG').cast('string').alias('MI006_WRITEOFF_FLAG'),expr("""IF(INT_RATE = 0, EFF_RATE, 0)""").cast('decimal(8,4)').alias('MI006_ANN_PERCENT_RATE'),col('MI006_BORM_CHARGE_OFF_FLAG').cast('string').alias('MI006_BORM_CHARGE_OFF_FLAG'),expr("""IF(INT_RATE = 0, EFF_RATE, 0)""").cast('decimal(11,7)').alias('MI006_CURR_GRS_RATE'),expr("""IF(INT_RATE = 0, EFF_RATE, 0)""").cast('decimal(11,7)').alias('MI006_ORG_GRS_RATE'),col('MI006_AM_ASSEC_TAG').cast('string').alias('MI006_AM_ASSEC_TAG'),col('MI006_ACCT_SOLD_TO_NAME').cast('string').alias('MI006_ACCT_SOLD_TO_NAME'),col('MI006_WRIT_OFF_INT').cast('decimal(18,3)').alias('MI006_WRIT_OFF_INT'),col('MI006_WRIT_OFF_PRIN').cast('decimal(18,3)').alias('MI006_WRIT_OFF_PRIN'),col('MI006_WRIT_OFF_CHARGE').cast('decimal(18,3)').alias('MI006_WRIT_OFF_CHARGE'),col('MI006_BASM_DATE').cast('integer').alias('MI006_BASM_DATE'),col('MI006_BASM_BASE_ID').cast('string').alias('MI006_BASM_BASE_ID'),col('MI006_PRIME_RATE').cast('decimal(9,5)').alias('MI006_PRIME_RATE'),expr("""IF(MI006_BASM_DESCRIPTION = 'Baserate', 'Base rate', MI006_BASM_DESCRIPTION)""").cast('string').alias('MI006_BASM_DESCRIPTION'),col('MI006_CAPN_METHOD').cast('string').alias('MI006_CAPN_METHOD'),col('MI006_CCC_REGULATED_IND').cast('string').alias('MI006_CCC_REGULATED_IND'),col('MI006_FUND_CODE').cast('integer').alias('MI006_FUND_CODE'),col('MI006_EXTRACTED_DATE').cast('integer').alias('MI006_EXTRACTED_DATE'),col('MI006_INT_ONLY_EXP_DATE').cast('integer').alias('MI006_INT_ONLY_EXP_DATE'),col('MI006_REVOLVING_CR_IND').cast('string').alias('MI006_REVOLVING_CR_IND'),col('MI006_PROV_AMT').cast('decimal(18,3)').alias('MI006_PROV_AMT'),col('M1006_ORIGINAL_EXP_DT').cast('integer').alias('M1006_ORIGINAL_EXP_DT'),col('MI006_FIRST_ADV_DATE').cast('integer').alias('MI006_FIRST_ADV_DATE'),col('MI006_PUR_CONTRACT_NO').cast('string').alias('MI006_PUR_CONTRACT_NO'),col('MI006_COMMISSION_AMT').cast('decimal(18,3)').alias('MI006_COMMISSION_AMT'),col('MI006_TYPE').cast('string').alias('MI006_TYPE'),col('MI006_STOP_ACCRUAL').cast('string').alias('MI006_STOP_ACCRUAL'),col('MI006_SHADOW_INT_ACCURAL').cast('decimal(18,3)').alias('MI006_SHADOW_INT_ACCURAL'),col('MI006_SHADOW_CURR_YR_INT').cast('decimal(18,3)').alias('MI006_SHADOW_CURR_YR_INT'),col('MI006_TIER_GROUP_ID').cast('string').alias('MI006_TIER_GROUP_ID'),col('MI006_SETELMENT_ACCT_NO').cast('string').alias('MI006_SETELMENT_ACCT_NO'),col('MI006_DATE_OF_SALE').cast('integer').alias('MI006_DATE_OF_SALE'),col('MI006_RETENTION_AMOUNT').cast('decimal(18,3)').alias('MI006_RETENTION_AMOUNT'),expr("""RIGHT(LPAD('0', 2, ' ') || RTRIM(MI006_RETENTION_PERIOD), 2)""").cast('string').alias('MI006_RETENTION_PERIOD'),col('MI006_SECURITY_IND').cast('string').alias('MI006_SECURITY_IND'),col('MI006_ACCR_TYPE').cast('string').alias('MI006_ACCR_TYPE'),col('MI006_ACQUISITION_FEE').cast('decimal(18,3)').alias('MI006_ACQUISITION_FEE'),col('MI006_AKPK_CODE').cast('string').alias('MI006_AKPK_CODE'),col('MI006_APPROVED_AUTH').cast('string').alias('MI006_APPROVED_AUTH'),col('MI006_BOIS_5TH_SCHED_ISS_DT').cast('integer').alias('MI006_BOIS_5TH_SCHED_ISS_DT'),col('MI006_BOIS_ACQUISITION_FEE').cast('decimal(18,3)').alias('MI006_BOIS_ACQUISITION_FEE'),col('MI006_BOIS_APPROVED_BY').cast('string').alias('MI006_BOIS_APPROVED_BY'),col('MI006_BOIS_GROSS_EFF_YIELD').cast('decimal(4,2)').alias('MI006_BOIS_GROSS_EFF_YIELD'),col('MI006_BOIS_LAST_ADV_DATE').cast('integer').alias('MI006_BOIS_LAST_ADV_DATE'),col('MI006_BOIS_NET_EFF_YEILD').cast('decimal(4,2)').alias('MI006_BOIS_NET_EFF_YEILD'),col('MI006_BOIS_NPA_BAL').cast('decimal(18,3)').alias('MI006_BOIS_NPA_BAL'),col('MI006_BOIS_PROV_AMT').cast('decimal(18,3)').alias('MI006_BOIS_PROV_AMT'),col('MI006_BOIS_PUR_CNTRT_NO').cast('string').alias('MI006_BOIS_PUR_CNTRT_NO'),col('MI006_BOIS_SETTLE_ACCT').cast('string').alias('MI006_BOIS_SETTLE_ACCT'),col('MI006_BOIS_SHDW_CURR_YR_INT').cast('decimal(18,3)').alias('MI006_BOIS_SHDW_CURR_YR_INT'),col('MI006_BOIS_SHDW_INT_ACCR').cast('decimal(18,3)').alias('MI006_BOIS_SHDW_INT_ACCR'),col('MI006_BOIS_TOT_ACCR_CAP').cast('decimal(18,3)').alias('MI006_BOIS_TOT_ACCR_CAP'),col('MI006_BUS_TYPE').cast('string').alias('MI006_BUS_TYPE'),col('MI006_DUE_DATE').cast('integer').alias('MI006_DUE_DATE'),col('MI006_FIFTH_SCHD_ISS_DAT').cast('integer').alias('MI006_FIFTH_SCHD_ISS_DAT'),col('MI006_INDEX_CODE').cast('string').alias('MI006_INDEX_CODE'),col('MI006_ISLAMIC_BANK').cast('string').alias('MI006_ISLAMIC_BANK'),col('MI006_MONTHS_IN_ARR').cast('string').alias('MI006_MONTHS_IN_ARR'),col('MI006_MULTI_TIER_FLG').cast('string').alias('MI006_MULTI_TIER_FLG'),col('MI006_NON_ACCR_IND').cast('string').alias('MI006_NON_ACCR_IND'),col('MI006_NON_WF_AMT').cast('decimal(18,3)').alias('MI006_NON_WF_AMT'),col('MI006_ORIG_BRCH_CODE').cast('string').alias('MI006_ORIG_BRCH_CODE'),col('MI006_OTHER_CHARGES').cast('decimal(18,3)').alias('MI006_OTHER_CHARGES'),col('MI006_PERS_BNK_ID').cast('string').alias('MI006_PERS_BNK_ID'),col('MI006_RTC_EXP_DATE').cast('integer').alias('MI006_RTC_EXP_DATE'),col('MI006_RTC_METHOD_IND').cast('string').alias('MI006_RTC_METHOD_IND'),col('MI006_SALE_REPUR_CODE').cast('string').alias('MI006_SALE_REPUR_CODE'),col('MI006_SECURITY_IND_DATE').cast('integer').alias('MI006_SECURITY_IND_DATE'),col('MI006_STAFF_PRODUCT_IND').cast('string').alias('MI006_STAFF_PRODUCT_IND'),col('MI006_STAMPING_FEE').cast('decimal(18,3)').alias('MI006_STAMPING_FEE'),col('MI006_TIER_METHOD').cast('string').alias('MI006_TIER_METHOD'),col('MI006_WATCHLIST_TAG').cast('string').alias('MI006_WATCHLIST_TAG'),col('MI006_WRITEOFF_STATUS_CODE').cast('string').alias('MI006_WRITEOFF_STATUS_CODE'),col('MI006_INT_REPAY_FREQ').cast('string').alias('MI006_INT_REPAY_FREQ'),col('MI006_BLDVNN_NXT_PRIN_REP').cast('integer').alias('MI006_BLDVNN_NXT_PRIN_REP'),col('MI006_BLDVNN_NXT_INT_REPA').cast('integer').alias('MI006_BLDVNN_NXT_INT_REPA'),col('MI006_BOIS_SOLD_AMOUNT').cast('decimal(18,3)').alias('MI006_BOIS_SOLD_AMOUNT'),col('MI006_BOIS_LOAN_TYPE').cast('string').alias('MI006_BOIS_LOAN_TYPE'),col('MI006_BOIS_EX_FLAG_3').cast('string').alias('MI006_BOIS_EX_FLAG_3'),col('MI006_WATCHLIST_DT').cast('integer').alias('MI006_WATCHLIST_DT'),col('MI006_IP_PROVISION_AMOUNT').cast('decimal(18,3)').alias('MI006_IP_PROVISION_AMOUNT'),col('MI006_INT_TYPE').cast('string').alias('MI006_INT_TYPE'),col('MI006_BINS_PREMIUM').cast('decimal(18,3)').alias('MI006_BINS_PREMIUM'),col('MI006_BOIS_CP1_PROV_AMT').cast('decimal(18,3)').alias('MI006_BOIS_CP1_PROV_AMT'),col('MI006_BOIS_CP3_PROV_AMT').cast('decimal(18,3)').alias('MI006_BOIS_CP3_PROV_AMT'),col('MI006_P_WOFF_OVERDUE').cast('decimal(18,3)').alias('MI006_P_WOFF_OVERDUE'),col('MI006_TOTAL_SUB_LEDGER_AMT').cast('decimal(18,3)').alias('MI006_TOTAL_SUB_LEDGER_AMT'),col('MI006_BOIS_RECALL_IND').cast('string').alias('MI006_BOIS_RECALL_IND'),col('MI006_NO_OF_INST_ARR').cast('integer').alias('MI006_NO_OF_INST_ARR'),col('MI006_OUTSTANDING_PRINCI').cast('decimal(18,3)').alias('MI006_OUTSTANDING_PRINCI'),col('MI006_PRIMRY_NPL').cast('string').alias('MI006_PRIMRY_NPL'),col('MI006_BOIS_REVERSED_INT').cast('decimal(18,3)').alias('MI006_BOIS_REVERSED_INT'),col('MI006_BOIS_PROVISIONING_DT').cast('integer').alias('MI006_BOIS_PROVISIONING_DT'),col('MI006_LITIGATION_DATE').cast('integer').alias('MI006_LITIGATION_DATE'),col('MI006_ADV_PREP_TXN_AMNT').cast('decimal(18,3)').alias('MI006_ADV_PREP_TXN_AMNT'),col('MI006_BORM_PRINC_REDUCT_EFF').cast('decimal(18,3)').alias('MI006_BORM_PRINC_REDUCT_EFF'),expr("""RIGHT(LPAD(RTRIM(MI006_BOIS_RETENTION_PERIOD), 2, '0'), 2)""").cast('string').alias('MI006_BOIS_RETENTION_PERIOD'),col('MI006_ACF_SL_NO').cast('string').alias('MI006_ACF_SL_NO'),col('MI006_BOIS_RECALL_IND_DT').cast('integer').alias('MI006_BOIS_RECALL_IND_DT'),col('MI006_BOIS_LOCK_IN').cast('string').alias('MI006_BOIS_LOCK_IN'),col('MI006_CP7_PROV').cast('decimal(18,3)').alias('MI006_CP7_PROV'),col('MI006_RECOV_AMT').cast('decimal(18,3)').alias('MI006_RECOV_AMT'),col('MI006_WORKOUT').cast('string').alias('MI006_WORKOUT'),col('MI006_WDV_PRIN').cast('decimal(18,3)').alias('MI006_WDV_PRIN'),col('MI006_WDV_INT').cast('decimal(18,3)').alias('MI006_WDV_INT'),col('MI006_WDV_LPI').cast('decimal(18,3)').alias('MI006_WDV_LPI'),col('MI006_WDV_OC').cast('decimal(18,3)').alias('MI006_WDV_OC'),col('MI006_OFF_BS_AMT').cast('decimal(18,3)').alias('MI006_OFF_BS_AMT'),col('MI006_LOCK_IN_SRT_DT').cast('string').alias('MI006_LOCK_IN_SRT_DT'),col('MI006_LOCK_IN_PERIOD').cast('integer').alias('MI006_LOCK_IN_PERIOD'),col('MI006_DISCH_PEN_AMT1').cast('decimal(18,3)').alias('MI006_DISCH_PEN_AMT1'),col('MI006_DISCH_PEN_AMT2').cast('decimal(18,3)').alias('MI006_DISCH_PEN_AMT2'),col('MI006_DISCH_PEN_AMT3').cast('decimal(18,3)').alias('MI006_DISCH_PEN_AMT3'),expr("""RIGHT(CONCAT(LPAD('0', 4, ' '), RTRIM(MI006_HOUSE_MRKT_CODE)), 4)""").cast('string').alias('MI006_HOUSE_MRKT_CODE'),col('MI006_BOIS_CITIZEN_CODE').cast('string').alias('MI006_BOIS_CITIZEN_CODE'),col('MI006_BOIS_ATTRITION_DT').cast('integer').alias('MI006_BOIS_ATTRITION_DT'),col('MI006_BOIS_ATTRITION_CODE').cast('string').alias('MI006_BOIS_ATTRITION_CODE'),col('MI006_BOIS_STAF_GRAC_END_DT').cast('integer').alias('MI006_BOIS_STAF_GRAC_END_DT'),col('MI006_BOIS_PREV_CAP_BAL').cast('decimal(18,3)').alias('MI006_BOIS_PREV_CAP_BAL'),col('MI006_SHORTFALL_DATE').cast('integer').alias('MI006_SHORTFALL_DATE'),col('MI006_TYPE_OF_RECALL').cast('string').alias('MI006_TYPE_OF_RECALL'),expr("""RPAD('0', 5, ' ') || TRIM(COALESCE(MI006_CHCD_COLLECN_HUB_CD, '')) AS result""").cast('string').alias('MI006_CHCD_COLLECN_HUB_CD'),col('COMPANY_CODE').cast('string').alias('COMPANY_CODE'),col('MI006_WRITEOFF_AMOUNT').cast('decimal(18,3)').alias('MI006_WRITEOFF_AMOUNT'),col('Penalty').cast('decimal(7,4)').alias('MI006_PENALTY_RATE'),col('MI006_MATURITY_DATE').cast('integer').alias('MI006_MATURITY_DATE'),col('MI006_CAPN_FREQ').cast('string').alias('MI006_CAPN_FREQ'),col('MI006_BORM_REPAY_METHOD').cast('string').alias('MI006_BORM_REPAY_METHOD'),col('MIS006_NEXT_ACCR_DT').cast('integer').alias('MIS006_NEXT_ACCR_DT'),col('MI006_ORIG_MAT_DATE').cast('integer').alias('MI006_ORIG_MAT_DATE'),col('MI006_NOTICE_FLAG').cast('string').alias('MI006_NOTICE_FLAG'),col('MI006_NOTICE_DATE').cast('integer').alias('MI006_NOTICE_DATE'),col('MI006_AUTO_DBT_ACCT_NO').cast('string').alias('MI006_AUTO_DBT_ACCT_NO'),col('MI006_AUTO_DBT_STRT_DT').cast('integer').alias('MI006_AUTO_DBT_STRT_DT'),col('MI006_AUTO_DBT_END_DT').cast('integer').alias('MI006_AUTO_DBT_END_DT'),col('MI006_MON_PRD_STRT_DT').cast('integer').alias('MI006_MON_PRD_STRT_DT'),col('MI006_MON_PRD_END_DT').cast('integer').alias('MI006_MON_PRD_END_DT'),col('MI006_RR_MARGIN').cast('decimal(4,2)').alias('MI006_RR_MARGIN'),col('MI006_AKPK_DATE').cast('integer').alias('MI006_AKPK_DATE'),col('MI006_MIN_REPAY_AMT').cast('decimal(18,3)').alias('MI006_MIN_REPAY_AMT'),col('MI006_FOUR_SCHD_ISS_DT').cast('integer').alias('MI006_FOUR_SCHD_ISS_DT'),col('MI006_RULE_THREE_ISS_DT').cast('integer').alias('MI006_RULE_THREE_ISS_DT'),col('MI006_REPO_ORDER_DT').cast('integer').alias('MI006_REPO_ORDER_DT'),col('MI006_NOT_ISS_DT').cast('integer').alias('MI006_NOT_ISS_DT'),col('MI006_PROMPT_PAY_CNTR').cast('integer').alias('MI006_PROMPT_PAY_CNTR'),col('MI006_BUY_BACK_CNTR').cast('integer').alias('MI006_BUY_BACK_CNTR'),col('MI006_STEP_UP_PERIOD').cast('integer').alias('MI006_STEP_UP_PERIOD'),col('MI006_REDRAW_AMT').cast('decimal(18,3)').alias('MI006_REDRAW_AMT'),col('MI006_WRITE_OFF_VAL_TAG').cast('string').alias('MI006_WRITE_OFF_VAL_TAG'),col('MI006_WRITE_OFF_JSTFCATN').cast('string').alias('MI006_WRITE_OFF_JSTFCATN'),col('MI006_WRITE_OFF_TAG_DT').cast('integer').alias('MI006_WRITE_OFF_TAG_DT'),col('MI006_WRITE_OFF_EXCL_TAG').cast('string').alias('MI006_WRITE_OFF_EXCL_TAG'),col('MI006_WRITE_OFF_EXCL_RSN').cast('string').alias('MI006_WRITE_OFF_EXCL_RSN'),col('MI006_WRITE_OFF_EXCL_DT').cast('integer').alias('MI006_WRITE_OFF_EXCL_DT'),col('MI006_ORIG_REM_REPAYS').cast('decimal(5,0)').alias('MI006_ORIG_REM_REPAYS'),col('MI006_STMT_DELIVERY').cast('string').alias('MI006_STMT_DELIVERY'),col('MI006_EMAIL_ADDR').cast('string').alias('MI006_EMAIL_ADDR'),col('MI006_CHRG_RPY_AMT').cast('decimal(18,3)').alias('MI006_CHRG_RPY_AMT'),col('MI006_PRIN_RPY_AMT').cast('decimal(18,3)').alias('MI006_PRIN_RPY_AMT'),col('MI006_COURT_DEP_RPY_AMT').cast('decimal(18,3)').alias('MI006_COURT_DEP_RPY_AMT'),col('MI006_INT_RPY_AMT').cast('decimal(18,3)').alias('MI006_INT_RPY_AMT'),col('MI006_BUY_BACK_DATE').cast('integer').alias('MI006_BUY_BACK_DATE'),col('MI006_BUY_BACK_IND').cast('string').alias('MI006_BUY_BACK_IND'),col('MI006_PREV_DAY_IFRS_EIR_RATE').cast('decimal(9,6)').alias('MI006_PREV_DAY_IFRS_EIR_RATE'),col('MI006_PREV_DAY_IFRS_ESTM_LOAN_TERM').cast('decimal(5,0)').alias('MI006_PREV_DAY_IFRS_ESTM_LOAN_TERM'),col('MI006_MAX_REPAY_AGE').cast('string').alias('MI006_MAX_REPAY_AGE'),col('MI006_MORTGAGE_FACILITY_IND').cast('string').alias('MI006_MORTGAGE_FACILITY_IND'),col('MI006_SPGA_NO').cast('string').alias('MI006_SPGA_NO'),col('MI006_DEDUCT_CODE').cast('string').alias('MI006_DEDUCT_CODE'),col('MI006_DED_STRT_MON').cast('string').alias('MI006_DED_STRT_MON'),col('MI006_DED_END_MON').cast('string').alias('MI006_DED_END_MON'),col('MI006_EXC_DPD_WOFF_VAL').cast('string').alias('MI006_EXC_DPD_WOFF_VAL'),col('MI006_GPP_INT_CAP_AMT').cast('decimal(17,3)').alias('MI006_GPP_INT_CAP_AMT'),col('MI006_GPP_AMORT_AMT_MTH').cast('decimal(17,3)').alias('MI006_GPP_AMORT_AMT_MTH'),col('MI006_RNR_REM_PROFIT').cast('decimal(17,3)').alias('MI006_RNR_REM_PROFIT'),col('MI006_RNR_MONTHLY_PROFIT').cast('decimal(17,3)').alias('MI006_RNR_MONTHLY_PROFIT'),col('MI006_INT_THRES_CAP_PER').cast('decimal(5,2)').alias('MI006_INT_THRES_CAP_PER'),col('MI006_PP_ADD_INT_AMT').cast('decimal(17,3)').alias('MI006_PP_ADD_INT_AMT'),col('MI006_PP_PROMPT_PAY_COUNTER').cast('decimal(2,0)').alias('MI006_PP_PROMPT_PAY_COUNTER'),col('MI006_PP_MON_PRD_END_DATE').cast('integer').alias('MI006_PP_MON_PRD_END_DATE'),col('MI006_REL_RED_RSN_CD').cast('string').alias('MI006_REL_RED_RSN_CD'),col('MI006_RSN_CD_MAINTNCE_DT').cast('integer').alias('MI006_RSN_CD_MAINTNCE_DT'),col('MI006_INST_CHG_EXP_DT').cast('integer').alias('MI006_INST_CHG_EXP_DT'),col('MI006_AKPK_SPC_TAG').cast('string').alias('MI006_AKPK_SPC_TAG'),col('MI006_AKPK_SPC_START_DT').cast('integer').alias('MI006_AKPK_SPC_START_DT'),col('MI006_AKPK_SPC_END_DT').cast('integer').alias('MI006_AKPK_SPC_END_DT'),col('MI006_AKPK_MATRIX').cast('string').alias('MI006_AKPK_MATRIX'),col('MI006_MORA_ACCR_INT').cast('decimal(17,3)').alias('MI006_MORA_ACCR_INT'),col('MI006_MANUAL_IMPAIR').cast('string').alias('MI006_MANUAL_IMPAIR'),col('MI006_PRE_DISB_DOC_STAT_DT').cast('integer').alias('MI006_PRE_DISB_DOC_STAT_DT'),col('MI006_UMA_NOTC_GEN_DT').cast('integer').alias('MI006_UMA_NOTC_GEN_DT'),col('MI006_ACC_DUE_DEF_INST').cast('string').alias('MI006_ACC_DUE_DEF_INST'),col('MI006_EX_FLAG_1').cast('string').alias('MI006_EX_FLAG_1'),col('MI006_INSTALLMENT_NO').cast('string').alias('MI006_INSTALLMENT_NO'),col('MI006_ADV_PREPAY_FLAG').cast('string').alias('MI006_ADV_PREPAY_FLAG'),col('MI006_REL_RED_RSN_FL_CD').cast('string').alias('MI006_REL_RED_RSN_FL_CD'),col('MI006_SETLEMENT_WAIVER').cast('string').alias('MI006_SETLEMENT_WAIVER'),col('MI006_INST_REINSTATE').cast('string').alias('MI006_INST_REINSTATE'),col('MI006_REINSTATE_DATE').cast('integer').alias('MI006_REINSTATE_DATE'),col('MI006_REINSTATE_COUNT').cast('string').alias('MI006_REINSTATE_COUNT'),col('MI006_MFA_FLAG').cast('string').alias('MI006_MFA_FLAG'),col('MI006_MFA_COUNTER').cast('string').alias('MI006_MFA_COUNTER'),col('MI006_MFA_EFF_DT').cast('integer').alias('MI006_MFA_EFF_DT'),col('MI006_MFA_MAINT_DT').cast('integer').alias('MI006_MFA_MAINT_DT'),col('MI006_EIY').cast('decimal(5,2)').alias('MI006_EIY'))
    
    Transformer_73_lnk_BOIS_Tgt_v = Transformer_73_lnk_BOIS_Tgt_v.selectExpr("B_KEY","MI006_INT_RATE","MI006_STORE_RATE","MI006_DISCH_AMOUNT","MI006_KEY_POINTER","RTRIM(MI006_ACCT_SOLD_TO) AS MI006_ACCT_SOLD_TO","MI006_ACCR_INCEPT","MI006_AMT_UNDRAWN","RTRIM(MI006_AUTO_DEBIT_IND) AS MI006_AUTO_DEBIT_IND","MI006_BASE_RATE","MI006_BASM_RATE","MI006_BOIS_LST_ADV_DATE","MI006_CLASS_CODE","RTRIM(MI006_CLASSIFY_FLAG) AS MI006_CLASSIFY_FLAG","MI006_FIRST_INST_DATE","MI006_FIRST_REPAY_DATE","RTRIM(MI006_LOAN_SOLD_CAGAMAS_FLAG) AS MI006_LOAN_SOLD_CAGAMAS_FLAG","MI006_NO_INST_PAID","RTRIM(MI006_NPA_IND) AS MI006_NPA_IND","RTRIM(MI006_NPL_CLASS_STAT) AS MI006_NPL_CLASS_STAT","RTRIM(MI006_NPL_STATUS) AS MI006_NPL_STATUS","MI006_P_WF_FEE_AMT","MI006_P_WF_INT_AMT","MI006_P_WF_LT_CHRG_AMT","MI006_P_WF_PRIN_AMT","MI006_RATE_ID","RTRIM(MI006_WRITEOFF_FLAG) AS MI006_WRITEOFF_FLAG","MI006_ANN_PERCENT_RATE","MI006_BORM_CHARGE_OFF_FLAG","MI006_CURR_GRS_RATE","MI006_ORG_GRS_RATE","MI006_AM_ASSEC_TAG","MI006_ACCT_SOLD_TO_NAME","MI006_WRIT_OFF_INT","MI006_WRIT_OFF_PRIN","MI006_WRIT_OFF_CHARGE","MI006_BASM_DATE","MI006_BASM_BASE_ID","MI006_PRIME_RATE","MI006_BASM_DESCRIPTION","RTRIM(MI006_CAPN_METHOD) AS MI006_CAPN_METHOD","RTRIM(MI006_CCC_REGULATED_IND) AS MI006_CCC_REGULATED_IND","MI006_FUND_CODE","MI006_EXTRACTED_DATE","MI006_INT_ONLY_EXP_DATE","RTRIM(MI006_REVOLVING_CR_IND) AS MI006_REVOLVING_CR_IND","MI006_PROV_AMT","M1006_ORIGINAL_EXP_DT","MI006_FIRST_ADV_DATE","MI006_PUR_CONTRACT_NO","MI006_COMMISSION_AMT","MI006_TYPE","RTRIM(MI006_STOP_ACCRUAL) AS MI006_STOP_ACCRUAL","MI006_SHADOW_INT_ACCURAL","MI006_SHADOW_CURR_YR_INT","MI006_TIER_GROUP_ID","MI006_SETELMENT_ACCT_NO","MI006_DATE_OF_SALE","MI006_RETENTION_AMOUNT","RTRIM(MI006_RETENTION_PERIOD) AS MI006_RETENTION_PERIOD","RTRIM(MI006_SECURITY_IND) AS MI006_SECURITY_IND","RTRIM(MI006_ACCR_TYPE) AS MI006_ACCR_TYPE","MI006_ACQUISITION_FEE","RTRIM(MI006_AKPK_CODE) AS MI006_AKPK_CODE","MI006_APPROVED_AUTH","MI006_BOIS_5TH_SCHED_ISS_DT","MI006_BOIS_ACQUISITION_FEE","MI006_BOIS_APPROVED_BY","MI006_BOIS_GROSS_EFF_YIELD","MI006_BOIS_LAST_ADV_DATE","MI006_BOIS_NET_EFF_YEILD","MI006_BOIS_NPA_BAL","MI006_BOIS_PROV_AMT","MI006_BOIS_PUR_CNTRT_NO","MI006_BOIS_SETTLE_ACCT","MI006_BOIS_SHDW_CURR_YR_INT","MI006_BOIS_SHDW_INT_ACCR","MI006_BOIS_TOT_ACCR_CAP","MI006_BUS_TYPE","MI006_DUE_DATE","MI006_FIFTH_SCHD_ISS_DAT","MI006_INDEX_CODE","RTRIM(MI006_ISLAMIC_BANK) AS MI006_ISLAMIC_BANK","RTRIM(MI006_MONTHS_IN_ARR) AS MI006_MONTHS_IN_ARR","RTRIM(MI006_MULTI_TIER_FLG) AS MI006_MULTI_TIER_FLG","RTRIM(MI006_NON_ACCR_IND) AS MI006_NON_ACCR_IND","MI006_NON_WF_AMT","MI006_ORIG_BRCH_CODE","MI006_OTHER_CHARGES","MI006_PERS_BNK_ID","MI006_RTC_EXP_DATE","RTRIM(MI006_RTC_METHOD_IND) AS MI006_RTC_METHOD_IND","RTRIM(MI006_SALE_REPUR_CODE) AS MI006_SALE_REPUR_CODE","MI006_SECURITY_IND_DATE","RTRIM(MI006_STAFF_PRODUCT_IND) AS MI006_STAFF_PRODUCT_IND","MI006_STAMPING_FEE","RTRIM(MI006_TIER_METHOD) AS MI006_TIER_METHOD","RTRIM(MI006_WATCHLIST_TAG) AS MI006_WATCHLIST_TAG","RTRIM(MI006_WRITEOFF_STATUS_CODE) AS MI006_WRITEOFF_STATUS_CODE","RTRIM(MI006_INT_REPAY_FREQ) AS MI006_INT_REPAY_FREQ","MI006_BLDVNN_NXT_PRIN_REP","MI006_BLDVNN_NXT_INT_REPA","MI006_BOIS_SOLD_AMOUNT","RTRIM(MI006_BOIS_LOAN_TYPE) AS MI006_BOIS_LOAN_TYPE","RTRIM(MI006_BOIS_EX_FLAG_3) AS MI006_BOIS_EX_FLAG_3","MI006_WATCHLIST_DT","MI006_IP_PROVISION_AMOUNT","RTRIM(MI006_INT_TYPE) AS MI006_INT_TYPE","MI006_BINS_PREMIUM","MI006_BOIS_CP1_PROV_AMT","MI006_BOIS_CP3_PROV_AMT","MI006_P_WOFF_OVERDUE","MI006_TOTAL_SUB_LEDGER_AMT","RTRIM(MI006_BOIS_RECALL_IND) AS MI006_BOIS_RECALL_IND","MI006_NO_OF_INST_ARR","MI006_OUTSTANDING_PRINCI","RTRIM(MI006_PRIMRY_NPL) AS MI006_PRIMRY_NPL","MI006_BOIS_REVERSED_INT","MI006_BOIS_PROVISIONING_DT","MI006_LITIGATION_DATE","MI006_ADV_PREP_TXN_AMNT","MI006_BORM_PRINC_REDUCT_EFF","RTRIM(MI006_BOIS_RETENTION_PERIOD) AS MI006_BOIS_RETENTION_PERIOD","MI006_ACF_SL_NO","MI006_BOIS_RECALL_IND_DT","RTRIM(MI006_BOIS_LOCK_IN) AS MI006_BOIS_LOCK_IN","MI006_CP7_PROV","MI006_RECOV_AMT","RTRIM(MI006_WORKOUT) AS MI006_WORKOUT","MI006_WDV_PRIN","MI006_WDV_INT","MI006_WDV_LPI","MI006_WDV_OC","MI006_OFF_BS_AMT","RTRIM(MI006_LOCK_IN_SRT_DT) AS MI006_LOCK_IN_SRT_DT","MI006_LOCK_IN_PERIOD","MI006_DISCH_PEN_AMT1","MI006_DISCH_PEN_AMT2","MI006_DISCH_PEN_AMT3","MI006_HOUSE_MRKT_CODE","RTRIM(MI006_BOIS_CITIZEN_CODE) AS MI006_BOIS_CITIZEN_CODE","MI006_BOIS_ATTRITION_DT","RTRIM(MI006_BOIS_ATTRITION_CODE) AS MI006_BOIS_ATTRITION_CODE","MI006_BOIS_STAF_GRAC_END_DT","MI006_BOIS_PREV_CAP_BAL","MI006_SHORTFALL_DATE","RTRIM(MI006_TYPE_OF_RECALL) AS MI006_TYPE_OF_RECALL","MI006_CHCD_COLLECN_HUB_CD","RTRIM(COMPANY_CODE) AS COMPANY_CODE","MI006_WRITEOFF_AMOUNT","MI006_PENALTY_RATE","MI006_MATURITY_DATE","RTRIM(MI006_CAPN_FREQ) AS MI006_CAPN_FREQ","RTRIM(MI006_BORM_REPAY_METHOD) AS MI006_BORM_REPAY_METHOD","MIS006_NEXT_ACCR_DT","MI006_ORIG_MAT_DATE","MI006_NOTICE_FLAG","MI006_NOTICE_DATE","MI006_AUTO_DBT_ACCT_NO","MI006_AUTO_DBT_STRT_DT","MI006_AUTO_DBT_END_DT","MI006_MON_PRD_STRT_DT","MI006_MON_PRD_END_DT","MI006_RR_MARGIN","MI006_AKPK_DATE","MI006_MIN_REPAY_AMT","MI006_FOUR_SCHD_ISS_DT","MI006_RULE_THREE_ISS_DT","MI006_REPO_ORDER_DT","MI006_NOT_ISS_DT","MI006_PROMPT_PAY_CNTR","MI006_BUY_BACK_CNTR","MI006_STEP_UP_PERIOD","MI006_REDRAW_AMT","RTRIM(MI006_WRITE_OFF_VAL_TAG) AS MI006_WRITE_OFF_VAL_TAG","RTRIM(MI006_WRITE_OFF_JSTFCATN) AS MI006_WRITE_OFF_JSTFCATN","MI006_WRITE_OFF_TAG_DT","RTRIM(MI006_WRITE_OFF_EXCL_TAG) AS MI006_WRITE_OFF_EXCL_TAG","RTRIM(MI006_WRITE_OFF_EXCL_RSN) AS MI006_WRITE_OFF_EXCL_RSN","MI006_WRITE_OFF_EXCL_DT","MI006_ORIG_REM_REPAYS","RTRIM(MI006_STMT_DELIVERY) AS MI006_STMT_DELIVERY","MI006_EMAIL_ADDR","MI006_CHRG_RPY_AMT","MI006_PRIN_RPY_AMT","MI006_COURT_DEP_RPY_AMT","MI006_INT_RPY_AMT","MI006_BUY_BACK_DATE","RTRIM(MI006_BUY_BACK_IND) AS MI006_BUY_BACK_IND","MI006_PREV_DAY_IFRS_EIR_RATE","MI006_PREV_DAY_IFRS_ESTM_LOAN_TERM","MI006_MAX_REPAY_AGE","MI006_MORTGAGE_FACILITY_IND","MI006_SPGA_NO","MI006_DEDUCT_CODE","MI006_DED_STRT_MON","MI006_DED_END_MON","MI006_EXC_DPD_WOFF_VAL","MI006_GPP_INT_CAP_AMT","MI006_GPP_AMORT_AMT_MTH","MI006_RNR_REM_PROFIT","MI006_RNR_MONTHLY_PROFIT","MI006_INT_THRES_CAP_PER","MI006_PP_ADD_INT_AMT","MI006_PP_PROMPT_PAY_COUNTER","MI006_PP_MON_PRD_END_DATE","MI006_REL_RED_RSN_CD","MI006_RSN_CD_MAINTNCE_DT","MI006_INST_CHG_EXP_DT","MI006_AKPK_SPC_TAG","MI006_AKPK_SPC_START_DT","MI006_AKPK_SPC_END_DT","MI006_AKPK_MATRIX","MI006_MORA_ACCR_INT","MI006_MANUAL_IMPAIR","MI006_PRE_DISB_DOC_STAT_DT","MI006_UMA_NOTC_GEN_DT","MI006_ACC_DUE_DEF_INST","MI006_EX_FLAG_1","MI006_INSTALLMENT_NO","MI006_ADV_PREPAY_FLAG","MI006_REL_RED_RSN_FL_CD","MI006_SETLEMENT_WAIVER","MI006_INST_REINSTATE","MI006_REINSTATE_DATE","MI006_REINSTATE_COUNT","MI006_MFA_FLAG","MI006_MFA_COUNTER","MI006_MFA_EFF_DT","MI006_MFA_MAINT_DT","MI006_EIY").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_RATE', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STORE_RATE', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_KEY_POINTER', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACCT_SOLD_TO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ACCR_INCEPT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AMT_UNDRAWN', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AUTO_DEBIT_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BASE_RATE', 'type': 'decimal(16,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BASM_RATE', 'type': 'decimal(16,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LST_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CLASS_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CLASSIFY_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_FIRST_INST_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FIRST_REPAY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_SOLD_CAGAMAS_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NO_INST_PAID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NPA_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NPL_CLASS_STAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NPL_STATUS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_P_WF_FEE_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WF_INT_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WF_LT_CHRG_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WF_PRIN_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RATE_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITEOFF_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ANN_PERCENT_RATE', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_CHARGE_OFF_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CURR_GRS_RATE', 'type': 'decimal(11,7)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ORG_GRS_RATE', 'type': 'decimal(11,7)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AM_ASSEC_TAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACCT_SOLD_TO_NAME', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRIT_OFF_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRIT_OFF_PRIN', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRIT_OFF_CHARGE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BASM_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BASM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRIME_RATE', 'type': 'decimal(9,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BASM_DESCRIPTION', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAPN_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_CCC_REGULATED_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_FUND_CODE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EXTRACTED_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_ONLY_EXP_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REVOLVING_CR_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'M1006_ORIGINAL_EXP_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FIRST_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PUR_CONTRACT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_COMMISSION_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TYPE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STOP_ACCRUAL', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SHADOW_INT_ACCURAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SHADOW_CURR_YR_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TIER_GROUP_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SETELMENT_ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DATE_OF_SALE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RETENTION_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RETENTION_PERIOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_SECURITY_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ACCR_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ACQUISITION_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_APPROVED_AUTH', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_5TH_SCHED_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_ACQUISITION_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_APPROVED_BY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_GROSS_EFF_YIELD', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LAST_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_NET_EFF_YEILD', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_NPA_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PUR_CNTRT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SETTLE_ACCT', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SHDW_CURR_YR_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SHDW_INT_ACCR', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_TOT_ACCR_CAP', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BUS_TYPE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DUE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FIFTH_SCHD_ISS_DAT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ISLAMIC_BANK', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_MONTHS_IN_ARR', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_MULTI_TIER_FLG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NON_ACCR_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NON_WF_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ORIG_BRCH_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OTHER_CHARGES', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PERS_BNK_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RTC_EXP_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RTC_METHOD_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SALE_REPUR_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SECURITY_IND_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STAFF_PRODUCT_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_STAMPING_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TIER_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_WATCHLIST_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_WRITEOFF_STATUS_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_INT_REPAY_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BLDVNN_NXT_PRIN_REP', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_NXT_INT_REPA', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_SOLD_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LOAN_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_BOIS_EX_FLAG_3', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_WATCHLIST_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_IP_PROVISION_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BINS_PREMIUM', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_CP1_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_CP3_PROV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WOFF_OVERDUE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TOTAL_SUB_LEDGER_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_RECALL_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_NO_OF_INST_ARR', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OUTSTANDING_PRINCI', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRIMRY_NPL', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOIS_REVERSED_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PROVISIONING_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LITIGATION_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADV_PREP_TXN_AMNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_PRINC_REDUCT_EFF', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_RETENTION_PERIOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_ACF_SL_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_RECALL_IND_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LOCK_IN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_CP7_PROV', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RECOV_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WORKOUT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_WDV_PRIN', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WDV_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WDV_LPI', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WDV_OC', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OFF_BS_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOCK_IN_SRT_DT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_LOCK_IN_PERIOD', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_PEN_AMT1', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_PEN_AMT2', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_PEN_AMT3', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_HOUSE_MRKT_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_CITIZEN_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOIS_ATTRITION_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_ATTRITION_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BOIS_STAF_GRAC_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PREV_CAP_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SHORTFALL_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TYPE_OF_RECALL', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_CHCD_COLLECN_HUB_CD', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'COMPANY_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_WRITEOFF_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PENALTY_RATE', 'type': 'decimal(7,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MATURITY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAPN_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BORM_REPAY_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MIS006_NEXT_ACCR_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ORIG_MAT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NOTICE_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NOTICE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AUTO_DBT_ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AUTO_DBT_STRT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AUTO_DBT_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MON_PRD_STRT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MON_PRD_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RR_MARGIN', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MIN_REPAY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FOUR_SCHD_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RULE_THREE_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REPO_ORDER_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NOT_ISS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PROMPT_PAY_CNTR', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BUY_BACK_CNTR', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STEP_UP_PERIOD', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REDRAW_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITE_OFF_VAL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_WRITE_OFF_JSTFCATN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(30)'}}, {'name': 'MI006_WRITE_OFF_TAG_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITE_OFF_EXCL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_WRITE_OFF_EXCL_RSN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(30)'}}, {'name': 'MI006_WRITE_OFF_EXCL_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ORIG_REM_REPAYS', 'type': 'decimal(5,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STMT_DELIVERY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_EMAIL_ADDR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CHRG_RPY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRIN_RPY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_COURT_DEP_RPY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_RPY_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BUY_BACK_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BUY_BACK_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_PREV_DAY_IFRS_EIR_RATE', 'type': 'decimal(9,6)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PREV_DAY_IFRS_ESTM_LOAN_TERM', 'type': 'decimal(5,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MAX_REPAY_AGE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MORTGAGE_FACILITY_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SPGA_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DEDUCT_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DED_STRT_MON', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DED_END_MON', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EXC_DPD_WOFF_VAL', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GPP_INT_CAP_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GPP_AMORT_AMT_MTH', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RNR_REM_PROFIT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RNR_MONTHLY_PROFIT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_THRES_CAP_PER', 'type': 'decimal(5,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PP_ADD_INT_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PP_PROMPT_PAY_COUNTER', 'type': 'decimal(2,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PP_MON_PRD_END_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REL_RED_RSN_CD', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RSN_CD_MAINTNCE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INST_CHG_EXP_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_SPC_TAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_SPC_START_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_SPC_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AKPK_MATRIX', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MORA_ACCR_INT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MANUAL_IMPAIR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRE_DISB_DOC_STAT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UMA_NOTC_GEN_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACC_DUE_DEF_INST', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EX_FLAG_1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INSTALLMENT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADV_PREPAY_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REL_RED_RSN_FL_CD', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SETLEMENT_WAIVER', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INST_REINSTATE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REINSTATE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REINSTATE_COUNT', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MFA_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MFA_COUNTER', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MFA_EFF_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MFA_MAINT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EIY', 'type': 'decimal(5,2)', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Transformer_73_lnk_BOIS_Tgt_v PURGE").show()
    
    print("Transformer_73_lnk_BOIS_Tgt_v")
    
    print(Transformer_73_lnk_BOIS_Tgt_v.schema.json())
    
    print("count:{}".format(Transformer_73_lnk_BOIS_Tgt_v.count()))
    
    Transformer_73_lnk_BOIS_Tgt_v.show(1000,False)
    
    Transformer_73_lnk_BOIS_Tgt_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Transformer_73_lnk_BOIS_Tgt_v")
    

@task.pyspark(conn_id="spark-local")
def TGT_BASM_BOIS_lnk_BOIS_Tgt_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_73_lnk_BOIS_Tgt_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__Transformer_73_lnk_BOIS_Tgt_v')
    
    TGT_BASM_BOIS_lnk_BOIS_Tgt_Part_v=Transformer_73_lnk_BOIS_Tgt_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__TGT_BASM_BOIS_lnk_BOIS_Tgt_Part_v PURGE").show()
    
    print("TGT_BASM_BOIS_lnk_BOIS_Tgt_Part_v")
    
    print(TGT_BASM_BOIS_lnk_BOIS_Tgt_Part_v.schema.json())
    
    print("count:{}".format(TGT_BASM_BOIS_lnk_BOIS_Tgt_Part_v.count()))
    
    TGT_BASM_BOIS_lnk_BOIS_Tgt_Part_v.show(1000,False)
    
    TGT_BASM_BOIS_lnk_BOIS_Tgt_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__TGT_BASM_BOIS_lnk_BOIS_Tgt_Part_v")
    

@task.pyspark(conn_id="spark-local")
def TGT_BASM_BOIS(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    TGT_BASM_BOIS_lnk_BOIS_Tgt_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_BOIS_Extr_POC__TGT_BASM_BOIS_lnk_BOIS_Tgt_Part_v')
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    locations = Template('{{dbdir.pPROCESSING_DIR}}MIS006_BOIS.ds').render(job_params)
    
    log.info("write dataset files to "+locations)
    
    TGT_BASM_BOIS_lnk_BOIS_Tgt_Part_v.write.mode("overwrite").format("parquet").save(locations)
    

####################################[Main]###################################
import airflow
with DAG(
    dag_id="job_DBdirect_MIS006_BOIS_Extr_POC",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None,
    tags=['datastage'],
) as dag:
    
    job_DBdirect_MIS006_BOIS_Extr_POC_task = job_DBdirect_MIS006_BOIS_Extr_POC()
    
    Job_VIEW_task = Job_VIEW()
    
    NETZ_SRC_BASM_task = NETZ_SRC_BASM()
    
    NETZ_SRC_TBL_NM_task = NETZ_SRC_TBL_NM()
    
    V0A105_task = V0A105()
    
    V25A0_task = V25A0()
    
    srt_keys_srt_keys_Part_task = srt_keys_srt_keys_Part()
    
    TRN_CONVERT_lnk_Source_Part_task = TRN_CONVERT_lnk_Source_Part()
    
    srt_keys_task = srt_keys()
    
    TRN_CONVERT_task = TRN_CONVERT()
    
    srt_KeyChange_srt_KeyChange_Part_task = srt_KeyChange_srt_KeyChange_Part()
    
    Join_57_Left_Part_task = Join_57_Left_Part()
    
    srt_KeyChange_task = srt_KeyChange()
    
    fil_KeyChange_fil_KeyChange_Part_task = fil_KeyChange_fil_KeyChange_Part()
    
    fil_KeyChange_task = fil_KeyChange()
    
    Copy_149_DSLink148_Part_task = Copy_149_DSLink148_Part()
    
    Copy_149_task = Copy_149()
    
    Join_57_Right_Part_task = Join_57_Right_Part()
    
    Join_152_Penalty_Part_task = Join_152_Penalty_Part()
    
    Join_57_task = Join_57()
    
    Join_152_P_Join_Part_task = Join_152_P_Join_Part()
    
    Join_152_task = Join_152()
    
    Transformer_73_LnkJoin_Part_task = Transformer_73_LnkJoin_Part()
    
    Transformer_73_task = Transformer_73()
    
    TGT_BASM_BOIS_lnk_BOIS_Tgt_Part_task = TGT_BASM_BOIS_lnk_BOIS_Tgt_Part()
    
    TGT_BASM_BOIS_task = TGT_BASM_BOIS()
    
    
    job_DBdirect_MIS006_BOIS_Extr_POC_task >> Job_VIEW_task
    
    Job_VIEW_task >> NETZ_SRC_BASM_task
    
    Job_VIEW_task >> NETZ_SRC_TBL_NM_task
    
    Job_VIEW_task >> V0A105_task
    
    Job_VIEW_task >> V25A0_task
    
    NETZ_SRC_BASM_task >> srt_keys_srt_keys_Part_task
    
    NETZ_SRC_TBL_NM_task >> TRN_CONVERT_lnk_Source_Part_task
    
    srt_keys_srt_keys_Part_task >> srt_keys_task
    
    TRN_CONVERT_lnk_Source_Part_task >> TRN_CONVERT_task
    
    srt_keys_task >> srt_KeyChange_srt_KeyChange_Part_task
    
    TRN_CONVERT_task >> Join_57_Left_Part_task
    
    srt_KeyChange_srt_KeyChange_Part_task >> srt_KeyChange_task
    
    Join_57_Left_Part_task >> Join_57_task
    
    srt_KeyChange_task >> fil_KeyChange_fil_KeyChange_Part_task
    
    fil_KeyChange_fil_KeyChange_Part_task >> fil_KeyChange_task
    
    fil_KeyChange_task >> Copy_149_DSLink148_Part_task
    
    Copy_149_DSLink148_Part_task >> Copy_149_task
    
    Copy_149_task >> Join_57_Right_Part_task
    
    Copy_149_task >> Join_152_Penalty_Part_task
    
    Join_57_Right_Part_task >> Join_57_task
    
    Join_152_Penalty_Part_task >> Join_152_task
    
    Join_57_task >> Join_152_P_Join_Part_task
    
    Join_152_P_Join_Part_task >> Join_152_task
    
    Join_152_task >> Transformer_73_LnkJoin_Part_task
    
    Transformer_73_LnkJoin_Part_task >> Transformer_73_task
    
    Transformer_73_task >> TGT_BASM_BOIS_lnk_BOIS_Tgt_Part_task
    
    TGT_BASM_BOIS_lnk_BOIS_Tgt_Part_task >> TGT_BASM_BOIS_task
    



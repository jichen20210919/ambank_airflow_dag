
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-02 21:51:00
# @Author  : cloudera
# @File    : job_DBdirect_MIS006_BORM_Extr_POC.py
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
from pyspark.sql import functions as F
from pyspark.sql.types import *
import json
import logging
import pendulum
import textwrap

@task
def job_DBdirect_MIS006_BORM_Extr_POC(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def Job_VIEW(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def V0A13(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task.pyspark(conn_id="spark-local")
def netz_ODS_DTEP_INSP(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    catalog=Variable.get("ICEBERG_CATALOG_NAME", default_var="iceberg")
    
    #spark.sql(f"use {catalog}.default").show()
    
    
    
    
    
    sql=Template("""SELECT DISTINCT
    
    SUBSTR(BORM.KEY_1, 1, 19) AS B_KEY,
    
    CASE WHEN OCID.ACCT_NO IS NOT NULL THEN 'Y' ELSE NULL END AS IND,
    
    OCID.CLRNG_DATE - {{Curr_Date}} AS FD,
    
    OCID.CLRNG_DATE - OCID.TRAN_DATE AS DC,
    
    OCID.TRAN_DATE,
    
    X.DTEP_CODES,
    
    X.NXT_CODES,
    
    X.CLEARING_DAYS_1,
    
    DAY(DATE_ADD(TO_DATE('1900-01-01'), CAST({{Curr_Date}} AS INT))) AS DT,
    
    OCID.LATE_CHQ_FLAG
    
    FROM
    
    {{dbdir.pODS_SCHM}}.BORM AS BORM
    
    LEFT OUTER JOIN 
    
    (
    
        SELECT INST_NO,
    
        ACCT_NO, BRANCH_NO, CLRNG_DATE, TRAN_DATE, LATE_CHQ_FLAG, CLR_TYPE,
    
        DATE_FORMAT(DATE_ADD(TO_DATE('1899-12-31'), TRAN_DATE), 'yyyyMM') AS DATE_YYYYMM, 
    
        INSTR_AMT
    
        FROM (
    
            SELECT *,
    
            ROW_NUMBER() OVER(PARTITION BY ACCT_NO ORDER BY CLRNG_DATE DESC, TRAN_DATE DESC, JRNL_NO DESC) AS RowNum
    
            FROM
    
                {{dbdir.pODS_SCHM}}.OCID
    
            WHERE INSTR_STA <> '06' 
    
              AND OCID.CLRNG_DATE >= {{Curr_Date}}
    
        ) k
    
        WHERE RowNum = 1
    
    ) AS OCID ON OCID.ACCT_NO = SUBSTR(BORM.KEY_1, 4, 16) AND OCID.INST_NO = SUBSTR(BORM.KEY_1, 1, 3)
    
    LEFT OUTER JOIN (
    
        SELECT A.DATE_YYYYMM,
    
        A.DTEP_CODES,
    
        B.DTEP_CODES AS NXT_CODES,
    
        INSP.CLEARING_DAYS_1,
    
        INSP.SOC_NO_1,
    
        INSP.CL_TYPE_1,
    
        CASE 
    
            WHEN CAST(SUBSTR(A.DATE_YYYYMM, 5, 2) AS INT) < 12
    
            THEN CASE 
    
                WHEN (CAST(B.DATE_YYYYMM AS INT) - CAST(A.DATE_YYYYMM AS INT)) = 1
    
                THEN '1'
    
                ELSE '0' 
    
            END 
    
            ELSE CASE 
    
                WHEN (CAST(B.DTEP_YEAR AS INT) - CAST(A.DTEP_YEAR AS INT)) = 1 
    
                    AND (CAST(B.DATE_YYYYMM AS INT) - CAST(A.DATE_YYYYMM AS INT)) = 89
    
                THEN CAST(B.DTEP_YEAR AS STRING)
    
                ELSE '0'
    
            END 
    
        END AS Next_Month_code
    
        FROM {{dbdir.pODS_SCHM}}.DTEP AS A 
    
        INNER JOIN {{dbdir.pODS_SCHM}}.INSP AS INSP ON A.INST_NO = INSP.SOC_NO_1
    
        INNER JOIN 
    
        (
    
            SELECT INST_NO, CAL_TYP, SYST, DTEP_ID, DATE_YYYYMM, DTEP_CODES, DTEP_YEAR 
    
            FROM {{dbdir.pODS_SCHM}}.dtep
    
            WHERE DTEP.INST_NO = '001' 
    
              AND DTEP.CAL_TYP = 'G'   
    
              AND DTEP.SYST = 'INV'    
    
              AND DTEP_ID = '10' 
    
        ) AS B ON A.INST_NO = B.INST_NO 
    
            AND A.CAL_TYP = B.CAL_TYP
    
            AND A.SYST = B.SYST
    
            AND A.DTEP_ID = B.DTEP_ID    
    ) AS X ON X.DATE_YYYYMM = CAST(DATE_FORMAT(DATE_ADD(TO_DATE('1899-12-31'), OCID.TRAN_DATE), 'yyyyMM') AS INT) 
        AND X.Next_Month_code <> '0'
        AND X.SOC_NO_1 = SUBSTR(BORM.KEY_1, 1, 3)
    
        AND CAST(OCID.CLR_TYPE AS INT) <> CAST(X.CL_TYPE_1 AS INT)""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    netz_ODS_DTEP_INSP_v = spark.sql(sql)
    
    
    
    
    
    #spark.sql(f"use spark_catalog.default").show()
    
    netz_ODS_DTEP_INSP_joi_DTEP_INSP_v=netz_ODS_DTEP_INSP_v.select(netz_ODS_DTEP_INSP_v[0].cast('string').alias('B_KEY'),netz_ODS_DTEP_INSP_v[1].cast('string').alias('IND'),netz_ODS_DTEP_INSP_v[2].cast('integer').alias('FD'),netz_ODS_DTEP_INSP_v[3].cast('integer').alias('DC'),netz_ODS_DTEP_INSP_v[4].cast('integer').alias('TRAN_DATE'),netz_ODS_DTEP_INSP_v[5].cast('string').alias('DTEP_CODES'),netz_ODS_DTEP_INSP_v[6].cast('string').alias('NXT_CODES'),netz_ODS_DTEP_INSP_v[7].cast('string').alias('CLEARING_DAYS_1'),netz_ODS_DTEP_INSP_v[8].cast('integer').alias('DT'),netz_ODS_DTEP_INSP_v[9].cast('string').alias('LATE_CHQ_FLAG'))
    
    netz_ODS_DTEP_INSP_joi_DTEP_INSP_v = netz_ODS_DTEP_INSP_joi_DTEP_INSP_v.selectExpr("B_KEY","IND","FD","DC","TRAN_DATE","DTEP_CODES","NXT_CODES","RTRIM(CLEARING_DAYS_1) AS CLEARING_DAYS_1","DT","RTRIM(LATE_CHQ_FLAG) AS LATE_CHQ_FLAG").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'FD', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'DC', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'TRAN_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'DTEP_CODES', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'NXT_CODES', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'CLEARING_DAYS_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'LATE_CHQ_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__netz_ODS_DTEP_INSP_joi_DTEP_INSP_v PURGE").show()
    
    print("netz_ODS_DTEP_INSP_joi_DTEP_INSP_v")
    
    print(netz_ODS_DTEP_INSP_joi_DTEP_INSP_v.schema.json())
    
    print("count:{}".format(netz_ODS_DTEP_INSP_joi_DTEP_INSP_v.count()))
    
    netz_ODS_DTEP_INSP_joi_DTEP_INSP_v.show(1000,False)
    
    netz_ODS_DTEP_INSP_joi_DTEP_INSP_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__netz_ODS_DTEP_INSP_joi_DTEP_INSP_v")
    

@task.pyspark(conn_id="spark-local")
def NETZ_SRC_BORM(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    catalog=Variable.get("ICEBERG_CATALOG_NAME", default_var="iceberg")
    
    #spark.sql(f"use {catalog}.default").show()
    
    
    spark.sql(f"DROP TABLE IF EXISTS ADMIN.BORM_SUM PURGE").show()

    sql=Template("""CREATE TABLE ADMIN.BORM_SUM USING PARQUET AS
    
    SELECT 
    
        KEY_1, 
    
        ACCT_STAT, 
    
        COUNT(*) as WA_REC_COUNT, 
    
        COALESCE(SUM(Exclud_days), 0) As Exclud_days
    
    FROM
    
    (
    
        SELECT 
    
            BORM.KEY_1,
    
            BOPG.ACCT_STAT,
    
            CASE 
    
                WHEN BOPG.ACCT_STAT = '08' 
    
                     AND BORM.LST_ARR_DATE < BOPG.START_DATE 
    
                     AND BORM.LST_ARR_DATE < BOPG.END_DATE 
    
                     THEN (BOPG.END_DATE - BOPG.START_DATE) + 1
    
                WHEN BOPG.ACCT_STAT = '08' 
    
                     AND BORM.LST_ARR_DATE > BOPG.START_DATE 
    
                     AND BORM.LST_ARR_DATE < BOPG.END_DATE 
    
                     THEN (BOPG.END_DATE - BORM.LST_ARR_DATE)
    
            END As Exclud_days
    
        FROM
    
            {{dbdir.pODS_SCHM}}.BORM
    
        LEFT OUTER JOIN
    
            {{dbdir.pODS_SCHM}}.BOPG
    
            ON BORM.KEY_1 = CONCAT(BOPG.SOC_NO, BOPG.ACCT_NO)
    
    ) m
    
    GROUP BY KEY_1, ACCT_STAT
    
    ;""").render(job_params)
    
    log.info(f"execute before sql statement {sql}")
    
    spark.sql(sql).show()
    
    
    
    
    
    sql=Template("""SELECT
    
    SUBSTRING(BORM.KEY_1, 1, 19) AS B_KEY,
    
    SUBSTRING(BORM.KEY_1, 1, 3) AS MI006_SOC_NO,
    
    SUBSTRING(BORM.KEY_1, 4, 16) AS MI006_MEMB_CUST_AC,
    
    BORM.BR_NO AS MI006_BR_NO,
    
    BORM.STAT AS MI006_STAT,
    
    BORM.ACT_TYPE AS MI006_ACT_TYPE,
    
    BORM.APPLIC_ISSUE_DATE AS MI006_APPLIC_ISSUE_DATE,
    
    BORM.APPLIC_AMOUNT AS MI006_APPLIC_AMOUNT,
    
    BORM.REPAY_FREQ AS MI006_REPAY_FREQ,
    
    (BORM.APP_AMT + BORM.ADD_LOAN_APP) AS MI006_APP_AMT,
    
    BORM.LOAN_BAL AS MI006_LOAN_BAL,
    
    BORM.ADV_VAL AS MI006_ADV_VAL,
    
    BORM.ADD_LOAN_ADV AS MI006_ADD_LOAN_ADV,
    
    BORM.THEO_LOAN_BAL AS MI006_THEO_LOAN_BAL,
    
    BORM.LOAN_REPAY AS MI006_LOAN_REPAY,
    
    BORM.APPRV_DATE AS MI006_APPRV_DATE,
    
    BORM.LST_FIN_DATE AS MI006_LST_FIN_DATE,
    
    BORM.LST_MNT_DATE AS MI006_LST_MNT_DATE,
    
    BORM.ADV_DATE AS MI006_ADV_DATE,
    
    BORM.LST_ARR_DATE AS MI006_LAST_ARR_DATE,
    
    BORM.ADD_ADV_DATE AS MI006_ADD_ADV_DATE,
    
    BORM.CAT AS MI006_CAT,
    
    CAST(BORM.LOAN_TRM AS INT) AS MI006_LOAN_TRM,
    
    CAST(BORM.REM_REPAYS AS INT) AS MI006_REM_REPAYS,
    
    BORM.MORTGAGE_INSU AS MI006_MORTGAGE_INSU,
    
    BORM.RESIDUAL_IND AS MI006_RESIDUAL_IND,
    
    BORM.FIXED_RT_DATE AS MI006_FIXED_RT_DATE,
    
    BORM.RT_INCR AS MI006_RT_INCR,
    
    BORM.CUSTOMER_NO AS MI006_CUSTOMER_NO,
    
    BORM.STMT_FREQUENCY AS MI006_STMT_FREQUENCY,
    
    BORM.TERM_BASIS AS MI006_TERM_BASIS,
    
    BORM.INT_STRT_DATE AS MI006_CAFM_START_RTE_DTE,
    
    BORM.CHR_EXEM_PROFILE AS MI006_CHR_EXEM_PROFILE,
    
    BORM.REPAY_SCHED AS MI006_REPAY_SCHED,
    
    CAST(BORM.PURPOSE_CODE_A AS STRING) AS MI006_PURPOSE_CODE_A,
    
    CAST(BORM.PURPOSE_CODE_B AS STRING) AS MI006_PURPOSE_CODE_B,
    
    CAST(BORM.REPAY_DAY AS INT) AS MI006_REPAY_DAY,
    
    BORM.DUE_STRT_DATE AS MI006_DUE_STRT_DATE,
    
    CAST(BORM.SOURCE_CDE AS INT) AS MI006_SOURCE_CDE,
    
    CAST(BORM.LOAN_OFFICER AS INT) AS MI006_LOAN_OFFICER,
    
    BORM.FUND_TYPE AS MI006_FUND_TYPE,
    
    BORM.FUND_SOURCE AS MI006_FUND_SOURCE,
    
    CAST((BORM.INT_ACCR + BORM.BPI_ACCR + BORM.INT_ADJUSTMENT) AS DECIMAL(18, 5)) AS MI006_INT_ACCR,
    
    BORM.CR_INT_ACCR AS MI006_CR_INT_ACCR,
    
    BORM.HOL_STRT_DT AS MI006_HOL_STRT_DT,
    
    CAST(BORM.REP_HOL_MTHS AS INT) AS MI006_REP_HOL_MTHS,
    
    BORM.LINKED_DEP_ACCT AS MI006_LINKED_DEP_ACCOUNT,
    
    CASE WHEN TRIM(BORM.PAY_DAY) = '' THEN 0 ELSE CAST(BORM.PAY_DAY AS INT) END AS MI006_CAPN_DAY,
    
    BORM.COLLECTIBILITY AS MI006_COLLECTIBILITY,
    
    BORM.APPR_INDUSTRY_SECT AS MI006_APPR_INDUSTRY_SECT,
    
    CAST(ROUND(BORM.INTEREST, 3) AS DECIMAL(18, 3)) AS MI006_INTEREST,
    
    BORM.UNPD_PRIN_BAL AS MI006_UNPD_PRIN_BAL,
    
    BORM.OVERDUE_BAL AS MI006_OVERDUE_BAL,
    
    BORM.TOT_PROJ_COST AS MI006_TOT_PROJ_COST,
    
    BORM.UNEARNED_INT AS MI006_UNEARNED_INT,
    
    (BORM.TOT_PROJ_COST - BORM.UNEARNED_INT) AS MI006_TOT_PRJ_CST_UNERND_INT,
    
    BORM.INT_STRT_DATE AS MI006_INT_START_DATE,
    
    BORM.BANKRUPTCY AS MI006_BANKRUPTCY,
    
    CAST(BORM.WRITTEN_OFF_INT_AC AS DECIMAL(18, 3)) AS MI006_WRITTEN_OFF_INT_AC,
    
    BORM.ADV_DATE AS MI006_LAST_ADV_DATE,
    
    BORM.REPORT_MASK AS MI006_REPORT_MASK,
    
    BORM.LOAN_LOC_STAT_CODE AS MI006_LOAN_LOC_STAT_CODE,
    
    CAST(BORM.REPAY_TYPE AS STRING) AS MI006_REPAY_TYPE,
    
    BORM.PEND_DUES AS MI006_PEND_DUES,
    
    BORM.FINE_CDE AS MI006_FINE_CDE,
    
    BORM.COLLECTIBILITY AS MI006_BAD_DEBT_IND,
    
    BORM.RT_INCR_MOV_AD AS MI006_RT_INCR_MOV_AD,
    
    BORM.INS_IND AS MI006_INS_IND,
    
    BORM.SPECIAL_REPAY_FLG AS MI006_SPECIAL_REPAY_FLAG,
    
    CAST(BORM.ARR_ACCR_AMT AS INT) AS MI006_ARR_ACCR_AMT,
    
    BORM.OUT_OF_ARREARS_DT AS MI006_OUT_OF_ARREARS_DT,
    
    BORM.UNPAID_SERV_BAL AS MI006_UNPAID_SERV_BAL,
    
    BORM.STAMPED_AMT AS MI006_STAMPED_AMT,
    
    CAST(BORM.TRF_ACCT_NO AS STRING) AS MI006_TRF_ACCT_NO,
    
    BORM.SUSPENDED_INT AS MI006_SUSPENDED_INT,
    
    BORM.DISCH_DATE AS MI006_DISCH_DATE,
    
    CAST(BORM.PEND_LOAN_TERM AS INT) AS MI006_PEND_LOAN_TERM,
    
    BORM.PRINCIPAL_INST_AMT AS MI006_PRINCIPAL_INST_AMT,
    
    BORM.DUE_AMT AS MI006_DUE_AMT,
    
    BORM.SYS AS MI006_BORM_SYS,
    
    CAST(BORM.LST_REPHASE_YR AS STRING) AS MI006_LST_REPHASE_YR,
    
    BORM.SYNDICATE_TYPE AS MI006_BORM_SYNDICATE_TYPE,
    
    BORM.GROUP_CODE AS MI006_BORM_GROUP_CODE,
    
    BORM.SWEEP_ACCT_FLAG AS MI006_SWEEP_ACCT_FLAG,
    
    BORM.BR_NO AS MI006_GLDM_BRANCH,
    
    BORM.CURRENCY_IND AS MI006_GLDM_CURRENCY,
    
    SUBSTRING(BORM.KEY_1, 19, 1) AS MI006_ACCT_CHK_DGT,
    
    CAST(
    
        CASE
    
            WHEN (BORM.ACT_TYPE = '0401' AND BORM.CAT = '0001') OR (BORM.CAT IN ('0005', '0021')) THEN 'Y'
    
            WHEN (BORM.ACT_TYPE = '0404' AND BORM.CAT = '0001') OR (BORM.CAT = '0024') THEN 'Y'
    
            WHEN (BORM.ACT_TYPE = '1401' AND BORM.CAT = '0001') OR (BORM.CAT = '0005') THEN 'Y'
    
            WHEN (BORM.ACT_TYPE = '1404' AND BORM.CAT = '0001') THEN 'Y'
    
            ELSE 'N'
    
        END AS STRING
    
    ) AS MI006_ACT_GOODS,
    
    CAST(
    
        CASE WHEN (BORM.APP_AMT + BORM.ADD_LOAN_APP) < 0 THEN '-' ELSE '+' END AS STRING
    
    ) AS MI006_APPR_AMT_SIGN,
    
    (BORM.LOAN_BAL - BORM.THEO_LOAN_BAL) AS MI006_AR_AMT,
    
    ROUND(BORM.ARR_INT_ACCR, 3) AS MI006_ARR_INT_ACCR,
    
    CASE WHEN BORM.ARR_INT_ACCR < 0 THEN '-' ELSE '+' END AS MI006_ARR_INT_ACCR_SIGN,
    
    BORM.ACCEPTANCE_DATE AS MI006_BORM_ACCEPT_DATE,
    
    BORM.BASE_ID AS MI006_BORM_BASE_ID,
    
    BORM.BORR_TYPE AS MI006_BORM_BORR_TYPE,
    
    CAST(BORM.CAP_UNPD_INT AS DECIMAL(18, 3)) AS MI006_BORM_CAP_UNPD_INT,
    
    BORM.COLLECTIBILITY AS MI006_BORM_COLLECTI_STATUS,
    
    BORM.EFF_RATE AS MI006_BORM_EFF_RATE,
    
    BORM.INS_IND AS MI006_BORM_INS_IND,
    
    BORM.REDRAW_IND AS MI006_BORM_REDRAW_IND,
    
    BORM.RT_INCR_MOV_AD AS MI006_BORM_RT_INCR_MOV_AD,
    
    BORM.SETTLEMENT_DATE AS MI006_BORM_SETTLEMENT_DATE,
    
    BORM.SPECIAL_REPAY_FLG AS MI006_BORM_SPL_REP_FLAG,
    
    BORM.UNPD_CHRG_BAL AS MI006_BORM_UNPD_CHRG_BAL,
    
    CAST(BORM.CAP_THEO_UNPD_INT AS DECIMAL(18, 3)) AS MI006_CAP_THEO_UNPD_INT,
    
    CAST(BORM.CAP_UNPD_INT AS DECIMAL(18, 3)) AS MI006_CAP_UNPD_INT,
    
    BORM.CI_AVG_BAL AS MI006_CR_AVG_BAL,
    
    BORM.CURRENCY_IND AS MI006_CURRENCY,
    
    CASE
    
        WHEN BORM.APP_AMT = 0 THEN 0
    
        ELSE CAST(ROUND((BORM.ADV_VAL / BORM.APP_AMT) * 100, 4) AS DECIMAL(8, 4))
    
    END AS MI006_DISB_PERC,
    
    (BORM.LOAN_BAL - BORM.THEO_LOAN_BAL) AS MI006_DUE_NOT_PAID,
    
    BORM.DUE_AMT AS MI006_FINAL_REPAYMENT,
    
    CASE WHEN (BORM.STAT > 08 AND BORM.LOAN_BAL = 0) THEN 'Y' ELSE 'N' END AS MI006_FULL_RCVR_FLAG,
    
    CAST((BORM.UNPD_PRIN_BAL + BORM.CAP_UNPD_INT) AS DECIMAL(18, 3)) AS MI006_INST_REMAIN,
    
    BORM.PREV_DUE_DATE AS MI006_LAST_DUE_DATE,
    
    CASE WHEN BORM.LOAN_BAL < 0 THEN '-' ELSE '+' END AS MI006_LOAN_BAL_SIGN,
    
    CAST(BORM.PURPOSE_CODE_A AS STRING) AS MI006_LOAN_PURPOSE,
    
    BORM.ACT_TYPE AS MI006_LONP_ACCT_TYPE,
    
    CASE WHEN (BORM.STAT = 22 OR BORM.STAT = 40) THEN 'M' ELSE '' END AS MI006_MAT_EXTN_FLAG,
    
    CAST(
    
        CASE
    
            WHEN BORM.LST_ARR_DATE = 0 THEN 0
    
            WHEN BORM.THEO_LOAN_BAL < BORM.LOAN_BAL THEN
    
                CASE
    
                    WHEN (BORM_08.WA_REC_COUNT > 0 OR BORM_00.WA_REC_COUNT > 0) THEN
    
                        CASE
    
                            WHEN BOPG.ACCT_STAT = '00' THEN
    
                                CASE
    
                                    WHEN (BORM.LST_ARR_DATE < BOPG.START_DATE)
    
                                        AND ( '{{dbdir.pBUSINESS_DATE}}' >= DATE_ADD( '1900-01-01', BOPG.START_DATE))
    
                                        AND ( '{{dbdir.pBUSINESS_DATE}}' <= DATE_ADD( '1900-01-01', BOPG.END_DATE))
    
                                        AND (BOIS.POST_IND = 'Y')
    
                                    THEN (BOPG.START_DATE - BORM.LST_ARR_DATE) - COALESCE(BORM_08.Exclud_days, 0)
    
                                    ELSE
    
                                        CASE
    
                                            WHEN ( '{{dbdir.pBUSINESS_DATE}}' < DATE_ADD( '1900-01-01', BOPG.START_DATE))
    
                                                OR ('{{dbdir.pBUSINESS_DATE}}' >= DATE_ADD('1900-01-01', BOPG.END_DATE))
    
                                            THEN DATE_DIFF('{{dbdir.pBUSINESS_DATE}}' , DATE_ADD('1900-01-01', BORM.LST_ARR_DATE)) - COALESCE(BORM_08.Exclud_days, 0)
    
                                            WHEN (BORM.LST_ARR_DATE < BOPG.START_DATE)
    
                                                AND ('{{dbdir.pBUSINESS_DATE}}' >= DATE_ADD('1900-01-01', BOPG.START_DATE))
    
                                                AND ('{{dbdir.pBUSINESS_DATE}}' < DATE_ADD('1900-01-01', BOPG.END_DATE))
    
                                                AND (BOIS.POST_IND = 'N')
    
                                            THEN (BOPG.START_DATE - BORM.LST_ARR_DATE) - COALESCE(BORM_08.Exclud_days, 0)
    
                                            ELSE DATE_DIFF('{{dbdir.pBUSINESS_DATE}}' , DATE_ADD('1900-01-01', BORM.LST_ARR_DATE))
    
                                        END
    
                                END
    
                            ELSE DATE_DIFF('{{dbdir.pBUSINESS_DATE}}' , DATE_ADD('1900-01-01', BORM.LST_ARR_DATE)) - COALESCE(BORM_08.Exclud_days, 0)
    
                        END
    
                    ELSE DATE_DIFF('{{dbdir.pBUSINESS_DATE}}' , DATE_ADD('1900-01-01', BORM.LST_ARR_DATE))
    
                END
    
            ELSE 0
    
        END AS STRING
    
    ) AS MI006_NO_DAYS_ARREAR,
    
    CAST(BORM.NO_OF_MESSAGES AS STRING) AS MI006_NO_OF_MESSAGES,
    
    BORM.OVERDUE_INT_BAL AS MI006_OVERDUE_INT_BAL,
    
    BORM.PENALTY_RATE AS MI006_PENALTY_RATE,
    
    BORM.PP_RATE AS MI006_PP_RATE,
    
    BORM.THEO_UNPD_CHRG_BAL AS MI006_THEO_UNPD_CHRG_BAL,
    
    (BORM.APP_AMT - BORM.ADV_VAL) AS MI006_UNDISBURSED_AMT,
    
    CAST(BORM.UNPD_ARRS_INT_BAL AS DECIMAL(18, 3)) AS MI006_UPPD_ARR_INT,
    
    CASE WHEN BORM.UNPD_PRIN_BAL < 0 THEN '-' ELSE '+' END AS MI006_UNPD_PRIN_BAL_SIGN,
    
    CASE WHEN BORM.THEO_UNPD_CHRG_BAL < 0 THEN '-' ELSE '+' END AS MI006_UNPD_CHRG_BAL_SIGN,
    
    CAST(BORM.WRITTEN_OFF_INT_AC AS DECIMAL(18, 3)) AS MI006_WRITEOFF_AMOUNT,
    
    BORM.WRITTEN_OFF_DATE AS MI006_WRITTENOFF_DATE,
    
    CASE WHEN (BORM.STAT IN (10, 20, 40, 22, '')) THEN 'N' ELSE 'Y' END AS MI006_ACCT_ACTIVE,
    
    SUBSTRING(BORM.NO_OF_OWNER, 1, 2) AS MI006_NO_OF_OWNER,
    
    CASE WHEN (BORM.STAT NOT IN (22, 40)) THEN 'N' ELSE 'Y' END AS MI006_CLOSED_IND,
    
    (BORM.LOAN_BAL - BORM.THEO_LOAN_BAL) AS MI006_CYCLE_ARREARS_VALUE,
    
    SUBSTRING(BORM.COLLECTIBILITY, 2, 1) AS MI006_DEFAULT_FLAG,
    
    CAST((CAST({{Curr_Date}} AS INT) - BORM.APPLIC_ISSUE_DATE) AS STRING) AS MI006_LOAN_AGE,
    
    CAST((BORM.LOAN_TRM - BORM.REM_REPAYS) AS STRING) AS MI006_MONTHS_ON_BOOKS,
    
    BORM.REPAY_METHOD AS MI006_LAST_REPAY_METHOD,
    
    CASE WHEN BORM.STAT = 23 THEN BORM.LOAN_BAL ELSE 0 END AS MI006_SUSPENSE_AMOUNT,
    
    BORM.OVERDUE_PRIN_BAL AS MI006_OVERDUE_PRIN_BAL,
    
    (BORM.THEO_LOAN_BAL - BORM.LOAN_BAL) AS MI006_AM_INT_MTH_ARR,
    
    BORM.UNEARNED_INT AS MI006_BLDVNN_TOTAL_INT,
    
    CASE
    
        WHEN (BORM.LOAN_BAL - BORM.THEO_LOAN_BAL) > 0 THEN (BORM.LOAN_BAL - BORM.THEO_LOAN_BAL)
    
        ELSE 0
    
    END AS MI006_AM_PARPMT_BAL,
    
    CAST(BORM.LOAN_TRM AS INT) AS MI006_NO_INSTALLMENT,
    
    BORM.RATE_MATURITY_DATE AS MI006_RATE_MATURITY_DATE,
    
    BORM.COLLECT_MAN_CHG_DT AS MI006_COLLECT_MAN_CHG_DT,
    
    CASE WHEN BORM.STAT >= 06 THEN BORM.ADV_DATE ELSE 0 END AS MI006_FULL_DRAWN_DT,
    
    CASE WHEN TRIM(BORM.DEFER_FLAG) = '' THEN 0 ELSE CAST(BORM.DEFER_FLAG AS INT) END AS MI006_PSSO_NEXT_PAY_DATE,
    
    BORM.DEFER_FLAG AS MI006_DEFER_FLAG,
    
    BORM.REPAY_FREQ AS MI006_PRIN_REPAY_FREQ,
    
    BORM.TERM_BASIS AS MI006_PRIN_PAY_UNIT,
    
    CAST(BORM.LOAN_TRM AS STRING) AS MI006_ORG_TENOR,
    
    CAST(BORM.LOAN_TRM AS STRING) AS MI006_REV_TENOR,
    
    BORM.GL_CLASS_CODE AS MI006_GL_CLASS_CODE,
    
    CASE WHEN TRIM(BORM.GL_CLASS_CODE) != '' THEN SUBSTRING(BORM.GL_CLASS_CODE, 15, 8) ELSE '' END AS MI006_GLPP_COMP1,
    
    CASE WHEN BORM.GL_CLASS_CODE != '' THEN SUBSTRING(BORM.GL_CLASS_CODE, 23, 3) ELSE '' END AS MI006_GLPP_COMP2,
    
    BORM.HOL_STRT_DT AS MI006_DEFFERMENT_EXPIRY_DATE,
    
    BORM.APPL_ID AS MI006_BORM_APPL_ID,
    
    BORM.UPFRONT_CAPN_FREQ AS MI006_CAPN_FREQ,
    
    BORM.INT_TAX_YTD AS MI006_BORM_INT_TAX_YTD,
    
    SUBSTRING(BORM.STOP_IND, 1, 1) AS MI006_BOIS_STOPP_IND,
    
    BORM.OVERDUE_UNPD_INT AS MI006_BORM_OVERDUE_UNPD_INT,
    
    BORM.THEO_UNPD_CHRG_BAL AS MI006_ASSESED_FEE,
    
    BORM.CI_AVG_BAL AS MI006_BORM_CI_AVG_BAL,
    
    (BORM.INT_INCR + BORM.BPI_INCR) AS MI006_BORM_INT_INCR,
    
    BORM.REPAY_METHOD AS MI006_BORM_REPAY_METHOD,
    
    BORM.MAX_INT_RATE AS MI006_MAX_INT_RATE,
    
    BORM.HOLD_AMOUNT AS MI006_BORM_HOLD_AMT,
    
    CASE WHEN BORM.STAT = 23 THEN 'Y' ELSE 'N' END AS MI006_SHORTFALL_IND,
    
    BORM.WRITTEN_OFF_DATE AS MI006_P_WOFF_DATE,
    
    CAST(BORM.WRITTEN_OFF_INT_AC AS DECIMAL(18, 3)) AS MI006_SUB_LEDGER_ACCURAL_AMT,
    
    BORM.UNPD_CHRG_BAL AS MI006_SUB_LEDGER_MISC_FEES_AMT,
    
    BORM.CAS_STAT_CODE AS MI006_CAS_STAT_CODE,
    
    BORM.UNPD_CHRG_BAL AS MI006_BOTM_FEES_AMOUNT,
    
    BORM.SECURITY_METHOD AS MI006_SECURITY_METHOD,
    
    BORM.UNPD_PRIN_BAL AS MI006_BORM_CUSM_VIEW_AMT,
    
    CAST(BORM.PREVIOUS_OFFICER AS INT) AS MI006_BORM_PREV_OFFICER,
    
    BORM.PAY_DAY AS MI006_BORM_PAY_DAY,
    
    BORM.RENEWAL_DATE AS MI006_BORM_RENEWAL_DATE,
    
    CAST(BORM.REPAY_COUNT AS INT) AS MI006_BORM_REPAY_COUNT,
    
    BORM.FIRST_PAY_IND AS MI006_BORM_FIRST_PAY_IND,
    
    CAST(BORM.ARR_NOT_FLAG AS STRING) AS MI006_BORM_ARR_NOT_FLAG,
    
    BORM.AGREEMENT_NUM AS MI006_BORM_AGREEMENT_NUM,
    
    BORM.RESIDUAL_BAL AS MI006_BORM_RESIDUAL_BAL,
    
    BORM.UNCL_NO_INT_VAL AS MI006_UNCLEARED_AMT,
    
    BORM.THEO_UNPD_PRIN_BAL AS MI006_THEO_UNPD_PRIN_BAL,
    
    BORM.EOM_BASIC_BAL AS MI006_BORM_EOM_BASIC_BAL
    
    FROM {{dbdir.pODS_SCHM}}.BORM BORM
    
    LEFT JOIN (
    
        SELECT * FROM ADMIN.BORM_SUM WHERE ACCT_STAT = '08'
    
    ) BORM_08 ON BORM_08.KEY_1 = BORM.KEY_1
    
    LEFT JOIN (
    
        SELECT * FROM ADMIN.BORM_SUM WHERE ACCT_STAT = '00'
    
    ) BORM_00 ON BORM_00.KEY_1 = BORM.KEY_1
    
    LEFT JOIN {{dbdir.pODS_SCHM}}.BOPG ON BORM.KEY_1 = CONCAT(BOPG.SOC_NO, BOPG.ACCT_NO)
    
        AND BOPG.ACCT_STAT = '00'
    
    LEFT JOIN {{dbdir.pODS_SCHM}}.BOIS ON BOIS.KEY_1 = BORM.KEY_1""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    NETZ_SRC_BORM_v = spark.sql(sql)
    
    
    
    
    
    #spark.sql(f"use spark_catalog.default").show()
    
    NETZ_SRC_BORM_Left_v=NETZ_SRC_BORM_v.select(NETZ_SRC_BORM_v[0].cast('string').alias('B_KEY'),NETZ_SRC_BORM_v[1].cast('string').alias('MI006_SOC_NO'),NETZ_SRC_BORM_v[2].cast('string').alias('MI006_MEMB_CUST_AC'),NETZ_SRC_BORM_v[3].cast('string').alias('MI006_BR_NO'),NETZ_SRC_BORM_v[4].cast('string').alias('MI006_STAT'),NETZ_SRC_BORM_v[5].cast('string').alias('MI006_ACT_TYPE'),NETZ_SRC_BORM_v[6].cast('integer').alias('MI006_APPLIC_ISSUE_DATE'),NETZ_SRC_BORM_v[7].cast('decimal(18,3)').alias('MI006_APPLIC_AMOUNT'),NETZ_SRC_BORM_v[8].cast('string').alias('MI006_REPAY_FREQ'),NETZ_SRC_BORM_v[9].cast('decimal(18,3)').alias('MI006_APP_AMT'),NETZ_SRC_BORM_v[10].cast('decimal(18,3)').alias('MI006_LOAN_BAL'),NETZ_SRC_BORM_v[11].cast('decimal(18,3)').alias('MI006_ADV_VAL'),NETZ_SRC_BORM_v[12].cast('decimal(18,3)').alias('MI006_ADD_LOAN_ADV'),NETZ_SRC_BORM_v[13].cast('decimal(18,3)').alias('MI006_THEO_LOAN_BAL'),NETZ_SRC_BORM_v[14].cast('decimal(18,3)').alias('MI006_LOAN_REPAY'),NETZ_SRC_BORM_v[15].cast('integer').alias('MI006_APPRV_DATE'),NETZ_SRC_BORM_v[16].cast('integer').alias('MI006_LST_FIN_DATE'),NETZ_SRC_BORM_v[17].cast('integer').alias('MI006_LST_MNT_DATE'),NETZ_SRC_BORM_v[18].cast('integer').alias('MI006_ADV_DATE'),NETZ_SRC_BORM_v[19].cast('integer').alias('MI006_LAST_ARR_DATE'),NETZ_SRC_BORM_v[20].cast('integer').alias('MI006_ADD_ADV_DATE'),NETZ_SRC_BORM_v[21].cast('string').alias('MI006_CAT'),NETZ_SRC_BORM_v[22].cast('integer').alias('MI006_LOAN_TRM'),NETZ_SRC_BORM_v[23].cast('integer').alias('MI006_REM_REPAYS'),NETZ_SRC_BORM_v[24].cast('string').alias('MI006_MORTGAGE_INSU'),NETZ_SRC_BORM_v[25].cast('string').alias('MI006_RESIDUAL_IND'),NETZ_SRC_BORM_v[26].cast('integer').alias('MI006_FIXED_RT_DATE'),NETZ_SRC_BORM_v[27].cast('decimal(8,5)').alias('MI006_RT_INCR'),NETZ_SRC_BORM_v[28].cast('string').alias('MI006_CUSTOMER_NO'),NETZ_SRC_BORM_v[29].cast('string').alias('MI006_STMT_FREQUENCY'),NETZ_SRC_BORM_v[30].cast('string').alias('MI006_TERM_BASIS'),NETZ_SRC_BORM_v[31].cast('integer').alias('MI006_CAFM_START_RTE_DTE'),NETZ_SRC_BORM_v[32].cast('string').alias('MI006_CHR_EXEM_PROFILE'),NETZ_SRC_BORM_v[33].cast('string').alias('MI006_REPAY_SCHED'),NETZ_SRC_BORM_v[34].cast('string').alias('MI006_PURPOSE_CODE_A'),NETZ_SRC_BORM_v[35].cast('string').alias('MI006_PURPOSE_CODE_B'),NETZ_SRC_BORM_v[36].cast('integer').alias('MI006_REPAY_DAY'),NETZ_SRC_BORM_v[37].cast('integer').alias('MI006_DUE_STRT_DATE'),NETZ_SRC_BORM_v[38].cast('integer').alias('MI006_SOURCE_CDE'),NETZ_SRC_BORM_v[39].cast('integer').alias('MI006_LOAN_OFFICER'),NETZ_SRC_BORM_v[40].cast('string').alias('MI006_FUND_TYPE'),NETZ_SRC_BORM_v[41].cast('string').alias('MI006_FUND_SOURCE'),NETZ_SRC_BORM_v[42].cast('decimal(18,5)').alias('MI006_INT_ACCR'),NETZ_SRC_BORM_v[43].cast('decimal(18,5)').alias('MI006_CR_INT_ACCR'),NETZ_SRC_BORM_v[44].cast('integer').alias('MI006_HOL_STRT_DT'),NETZ_SRC_BORM_v[45].cast('integer').alias('MI006_REP_HOL_MTHS'),NETZ_SRC_BORM_v[46].cast('string').alias('MI006_LINKED_DEP_ACCOUNT'),NETZ_SRC_BORM_v[47].cast('integer').alias('MI006_CAPN_DAY'),NETZ_SRC_BORM_v[48].cast('string').alias('MI006_COLLECTIBILITY'),NETZ_SRC_BORM_v[49].cast('string').alias('MI006_APPR_INDUSTRY_SECT'),NETZ_SRC_BORM_v[50].cast('decimal(18,3)').alias('MI006_INTEREST'),NETZ_SRC_BORM_v[51].cast('decimal(18,3)').alias('MI006_UNPD_PRIN_BAL'),NETZ_SRC_BORM_v[52].cast('decimal(18,3)').alias('MI006_OVERDUE_BAL'),NETZ_SRC_BORM_v[53].cast('decimal(18,3)').alias('MI006_TOT_PROJ_COST'),NETZ_SRC_BORM_v[54].cast('decimal(18,3)').alias('MI006_UNEARNED_INT'),NETZ_SRC_BORM_v[55].cast('decimal(18,3)').alias('MI006_TOT_PRJ_CST_UNERND_INT'),NETZ_SRC_BORM_v[56].cast('integer').alias('MI006_INT_START_DATE'),NETZ_SRC_BORM_v[57].cast('string').alias('MI006_BANKRUPTCY'),NETZ_SRC_BORM_v[58].cast('decimal(18,3)').alias('MI006_WRITTEN_OFF_INT_AC'),NETZ_SRC_BORM_v[59].cast('integer').alias('MI006_LAST_ADV_DATE'),NETZ_SRC_BORM_v[60].cast('string').alias('MI006_REPORT_MASK'),NETZ_SRC_BORM_v[61].cast('string').alias('MI006_LOAN_LOC_STAT_CODE'),NETZ_SRC_BORM_v[62].cast('string').alias('MI006_REPAY_TYPE'),NETZ_SRC_BORM_v[63].cast('decimal(18,3)').alias('MI006_PEND_DUES'),NETZ_SRC_BORM_v[64].cast('string').alias('MI006_FINE_CDE'),NETZ_SRC_BORM_v[65].cast('string').alias('MI006_BAD_DEBT_IND'),NETZ_SRC_BORM_v[66].cast('decimal(11,4)').alias('MI006_RT_INCR_MOV_AD'),NETZ_SRC_BORM_v[67].cast('string').alias('MI006_INS_IND'),NETZ_SRC_BORM_v[68].cast('string').alias('MI006_SPECIAL_REPAY_FLAG'),NETZ_SRC_BORM_v[69].cast('integer').alias('MI006_ARR_ACCR_AMT'),NETZ_SRC_BORM_v[70].cast('integer').alias('MI006_OUT_OF_ARREARS_DT'),NETZ_SRC_BORM_v[71].cast('decimal(18,3)').alias('MI006_UNPAID_SERV_BAL'),NETZ_SRC_BORM_v[72].cast('decimal(18,3)').alias('MI006_STAMPED_AMT'),NETZ_SRC_BORM_v[73].cast('string').alias('MI006_TRF_ACCT_NO'),NETZ_SRC_BORM_v[74].cast('decimal(18,3)').alias('MI006_SUSPENDED_INT'),NETZ_SRC_BORM_v[75].cast('integer').alias('MI006_DISCH_DATE'),NETZ_SRC_BORM_v[76].cast('integer').alias('MI006_PEND_LOAN_TERM'),NETZ_SRC_BORM_v[77].cast('decimal(18,3)').alias('MI006_PRINCIPAL_INST_AMT'),NETZ_SRC_BORM_v[78].cast('decimal(18,3)').alias('MI006_DUE_AMT'),NETZ_SRC_BORM_v[79].cast('string').alias('MI006_BORM_SYS'),NETZ_SRC_BORM_v[80].cast('string').alias('MI006_LST_REPHASE_YR'),NETZ_SRC_BORM_v[81].cast('string').alias('MI006_BORM_SYNDICATE_TYPE'),NETZ_SRC_BORM_v[82].cast('string').alias('MI006_BORM_GROUP_CODE'),NETZ_SRC_BORM_v[83].cast('string').alias('MI006_SWEEP_ACCT_FLAG'),NETZ_SRC_BORM_v[84].cast('string').alias('MI006_GLDM_BRANCH'),NETZ_SRC_BORM_v[85].cast('string').alias('MI006_GLDM_CURRENCY'),NETZ_SRC_BORM_v[86].cast('string').alias('MI006_ACCT_CHK_DGT'),NETZ_SRC_BORM_v[87].cast('string').alias('MI006_ACT_GOODS'),NETZ_SRC_BORM_v[88].cast('string').alias('MI006_APPR_AMT_SIGN'),NETZ_SRC_BORM_v[89].cast('decimal(18,3)').alias('MI006_AR_AMT'),NETZ_SRC_BORM_v[90].cast('decimal(18,3)').alias('MI006_ARR_INT_ACCR'),NETZ_SRC_BORM_v[91].cast('string').alias('MI006_ARR_INT_ACCR_SIGN'),NETZ_SRC_BORM_v[92].cast('integer').alias('MI006_BORM_ACCEPT_DATE'),NETZ_SRC_BORM_v[93].cast('string').alias('MI006_BORM_BASE_ID'),NETZ_SRC_BORM_v[94].cast('string').alias('MI006_BORM_BORR_TYPE'),NETZ_SRC_BORM_v[95].cast('decimal(18,3)').alias('MI006_BORM_CAP_UNPD_INT'),NETZ_SRC_BORM_v[96].cast('string').alias('MI006_BORM_COLLECTI_STATUS'),NETZ_SRC_BORM_v[97].cast('decimal(8,5)').alias('MI006_BORM_EFF_RATE'),NETZ_SRC_BORM_v[98].cast('string').alias('MI006_BORM_INS_IND'),NETZ_SRC_BORM_v[99].cast('string').alias('MI006_BORM_REDRAW_IND'),NETZ_SRC_BORM_v[100].cast('decimal(8,4)').alias('MI006_BORM_RT_INCR_MOV_AD'),NETZ_SRC_BORM_v[101].cast('integer').alias('MI006_BORM_SETTLEMENT_DATE'),NETZ_SRC_BORM_v[102].cast('string').alias('MI006_BORM_SPL_REP_FLAG'),NETZ_SRC_BORM_v[103].cast('decimal(18,3)').alias('MI006_BORM_UNPD_CHRG_BAL'),NETZ_SRC_BORM_v[104].cast('decimal(18,3)').alias('MI006_CAP_THEO_UNPD_INT'),NETZ_SRC_BORM_v[105].cast('decimal(18,3)').alias('MI006_CAP_UNPD_INT'),NETZ_SRC_BORM_v[106].cast('decimal(18,3)').alias('MI006_CR_AVG_BAL'),NETZ_SRC_BORM_v[107].cast('string').alias('MI006_CURRENCY'),NETZ_SRC_BORM_v[108].cast('decimal(8,4)').alias('MI006_DISB_PERC'),NETZ_SRC_BORM_v[109].cast('decimal(18,3)').alias('MI006_DUE_NOT_PAID'),NETZ_SRC_BORM_v[110].cast('decimal(18,3)').alias('MI006_FINAL_REPAYMENT'),NETZ_SRC_BORM_v[111].cast('string').alias('MI006_FULL_RCVR_FLAG'),NETZ_SRC_BORM_v[112].cast('decimal(18,3)').alias('MI006_INST_REMAIN'),NETZ_SRC_BORM_v[113].cast('integer').alias('MI006_LAST_DUE_DATE'),NETZ_SRC_BORM_v[114].cast('string').alias('MI006_LOAN_BAL_SIGN'),NETZ_SRC_BORM_v[115].cast('string').alias('MI006_LOAN_PURPOSE'),NETZ_SRC_BORM_v[116].cast('string').alias('MI006_LONP_ACCT_TYPE'),NETZ_SRC_BORM_v[117].cast('string').alias('MI006_MAT_EXTN_FLAG'),NETZ_SRC_BORM_v[118].cast('string').alias('MI006_NO_DAYS_ARREAR'),NETZ_SRC_BORM_v[119].cast('string').alias('MI006_NO_OF_MESSAGES'),NETZ_SRC_BORM_v[120].cast('decimal(18,3)').alias('MI006_OVERDUE_INT_BAL'),NETZ_SRC_BORM_v[121].cast('decimal(7,4)').alias('MI006_PENALTY_RATE'),NETZ_SRC_BORM_v[122].cast('decimal(8,5)').alias('MI006_PP_RATE'),NETZ_SRC_BORM_v[123].cast('decimal(18,3)').alias('MI006_THEO_UNPD_CHRG_BAL'),NETZ_SRC_BORM_v[124].cast('decimal(18,3)').alias('MI006_UNDISBURSED_AMT'),NETZ_SRC_BORM_v[125].cast('decimal(18,3)').alias('MI006_UPPD_ARR_INT'),NETZ_SRC_BORM_v[126].cast('string').alias('MI006_UNPD_PRIN_BAL_SIGN'),NETZ_SRC_BORM_v[127].cast('string').alias('MI006_UNPD_CHRG_BAL_SIGN'),NETZ_SRC_BORM_v[128].cast('decimal(18,3)').alias('MI006_WRITEOFF_AMOUNT'),NETZ_SRC_BORM_v[129].cast('integer').alias('MI006_WRITTENOFF_DATE'),NETZ_SRC_BORM_v[130].cast('string').alias('MI006_ACCT_ACTIVE'),NETZ_SRC_BORM_v[131].cast('string').alias('MI006_NO_OF_OWNER'),NETZ_SRC_BORM_v[132].cast('string').alias('MI006_CLOSED_IND'),NETZ_SRC_BORM_v[133].cast('decimal(18,3)').alias('MI006_CYCLE_ARREARS_VALUE'),NETZ_SRC_BORM_v[134].cast('string').alias('MI006_DEFAULT_FLAG'),NETZ_SRC_BORM_v[135].cast('string').alias('MI006_LOAN_AGE'),NETZ_SRC_BORM_v[136].cast('string').alias('MI006_MONTHS_ON_BOOKS'),NETZ_SRC_BORM_v[137].cast('string').alias('MI006_LAST_REPAY_METHOD'),NETZ_SRC_BORM_v[138].cast('decimal(18,3)').alias('MI006_SUSPENSE_AMOUNT'),NETZ_SRC_BORM_v[139].cast('decimal(18,3)').alias('MI006_OVERDUE_PRIN_BAL'),NETZ_SRC_BORM_v[140].cast('decimal(18,3)').alias('MI006_AM_INT_MTH_ARR'),NETZ_SRC_BORM_v[141].cast('decimal(18,3)').alias('MI006_BLDVNN_TOTAL_INT'),NETZ_SRC_BORM_v[142].cast('decimal(18,3)').alias('MI006_AM_PARPMT_BAL'),NETZ_SRC_BORM_v[143].cast('integer').alias('MI006_NO_INSTALLMENT'),NETZ_SRC_BORM_v[144].cast('integer').alias('MI006_RATE_MATURITY_DATE'),NETZ_SRC_BORM_v[145].cast('integer').alias('MI006_COLLECT_MAN_CHG_DT'),NETZ_SRC_BORM_v[146].cast('integer').alias('MI006_FULL_DRAWN_DT'),NETZ_SRC_BORM_v[147].cast('integer').alias('MI006_PSSO_NEXT_PAY_DATE'),NETZ_SRC_BORM_v[148].cast('string').alias('MI006_DEFER_FLAG'),NETZ_SRC_BORM_v[149].cast('string').alias('MI006_PRIN_REPAY_FREQ'),NETZ_SRC_BORM_v[150].cast('string').alias('MI006_PRIN_PAY_UNIT'),NETZ_SRC_BORM_v[151].cast('string').alias('MI006_ORG_TENOR'),NETZ_SRC_BORM_v[152].cast('string').alias('MI006_REV_TENOR'),NETZ_SRC_BORM_v[153].cast('string').alias('MI006_GL_CLASS_CODE'),NETZ_SRC_BORM_v[154].cast('string').alias('MI006_GLPP_COMP1'),NETZ_SRC_BORM_v[155].cast('string').alias('MI006_GLPP_COMP2'),NETZ_SRC_BORM_v[156].cast('integer').alias('MI006_DEFFERMENT_EXPIRY_DATE'),NETZ_SRC_BORM_v[157].cast('string').alias('MI006_BORM_APPL_ID'),NETZ_SRC_BORM_v[158].cast('string').alias('MI006_CAPN_FREQ'),NETZ_SRC_BORM_v[159].cast('decimal(18,3)').alias('MI006_BORM_INT_TAX_YTD'),NETZ_SRC_BORM_v[160].cast('string').alias('MI006_BOIS_STOPP_IND'),NETZ_SRC_BORM_v[161].cast('decimal(18,3)').alias('MI006_BORM_OVERDUE_UNPD_INT'),NETZ_SRC_BORM_v[162].cast('decimal(18,3)').alias('MI006_ASSESED_FEE'),NETZ_SRC_BORM_v[163].cast('decimal(18,3)').alias('MI006_BORM_CI_AVG_BAL'),NETZ_SRC_BORM_v[164].cast('decimal(18,5)').alias('MI006_BORM_INT_INCR'),NETZ_SRC_BORM_v[165].cast('string').alias('MI006_BORM_REPAY_METHOD'),NETZ_SRC_BORM_v[166].cast('decimal(8,4)').alias('MI006_MAX_INT_RATE'),NETZ_SRC_BORM_v[167].cast('decimal(18,3)').alias('MI006_BORM_HOLD_AMT'),NETZ_SRC_BORM_v[168].cast('string').alias('MI006_SHORTFALL_IND'),NETZ_SRC_BORM_v[169].cast('integer').alias('MI006_P_WOFF_DATE'),NETZ_SRC_BORM_v[170].cast('decimal(18,3)').alias('MI006_SUB_LEDGER_ACCURAL_AMT'),NETZ_SRC_BORM_v[171].cast('decimal(18,3)').alias('MI006_SUB_LEDGER_MISC_FEES_AMT'),NETZ_SRC_BORM_v[172].cast('string').alias('MI006_CAS_STAT_CODE'),NETZ_SRC_BORM_v[173].cast('decimal(18,3)').alias('MI006_BOTM_FEES_AMOUNT'),NETZ_SRC_BORM_v[174].cast('string').alias('MI006_SECURITY_METHOD'),NETZ_SRC_BORM_v[175].cast('decimal(18,3)').alias('MI006_BORM_CUSM_VIEW_AMT'),NETZ_SRC_BORM_v[176].cast('integer').alias('MI006_BORM_PREV_OFFICER'),NETZ_SRC_BORM_v[177].cast('string').alias('MI006_BORM_PAY_DAY'),NETZ_SRC_BORM_v[178].cast('integer').alias('MI006_BORM_RENEWAL_DATE'),NETZ_SRC_BORM_v[179].cast('integer').alias('MI006_BORM_REPAY_COUNT'),NETZ_SRC_BORM_v[180].cast('string').alias('MI006_BORM_FIRST_PAY_IND'),NETZ_SRC_BORM_v[181].cast('string').alias('MI006_BORM_ARR_NOT_FLAG'),NETZ_SRC_BORM_v[182].cast('string').alias('MI006_BORM_AGREEMENT_NUM'),NETZ_SRC_BORM_v[183].cast('decimal(18,3)').alias('MI006_BORM_RESIDUAL_BAL'),NETZ_SRC_BORM_v[184].cast('decimal(18,3)').alias('MI006_UNCLEARED_AMT'),NETZ_SRC_BORM_v[185].cast('decimal(18,3)').alias('MI006_THEO_UNPD_PRIN_BAL'),NETZ_SRC_BORM_v[186].cast('decimal(18,3)').alias('MI006_BORM_EOM_BASIC_BAL'))
    
    NETZ_SRC_BORM_Left_v = NETZ_SRC_BORM_Left_v.selectExpr("B_KEY","MI006_SOC_NO","MI006_MEMB_CUST_AC","RTRIM(MI006_BR_NO) AS MI006_BR_NO","RTRIM(MI006_STAT) AS MI006_STAT","RTRIM(MI006_ACT_TYPE) AS MI006_ACT_TYPE","MI006_APPLIC_ISSUE_DATE","MI006_APPLIC_AMOUNT","RTRIM(MI006_REPAY_FREQ) AS MI006_REPAY_FREQ","MI006_APP_AMT","MI006_LOAN_BAL","MI006_ADV_VAL","MI006_ADD_LOAN_ADV","MI006_THEO_LOAN_BAL","MI006_LOAN_REPAY","MI006_APPRV_DATE","MI006_LST_FIN_DATE","MI006_LST_MNT_DATE","MI006_ADV_DATE","MI006_LAST_ARR_DATE","MI006_ADD_ADV_DATE","RTRIM(MI006_CAT) AS MI006_CAT","MI006_LOAN_TRM","MI006_REM_REPAYS","RTRIM(MI006_MORTGAGE_INSU) AS MI006_MORTGAGE_INSU","RTRIM(MI006_RESIDUAL_IND) AS MI006_RESIDUAL_IND","MI006_FIXED_RT_DATE","MI006_RT_INCR","RTRIM(MI006_CUSTOMER_NO) AS MI006_CUSTOMER_NO","RTRIM(MI006_STMT_FREQUENCY) AS MI006_STMT_FREQUENCY","RTRIM(MI006_TERM_BASIS) AS MI006_TERM_BASIS","MI006_CAFM_START_RTE_DTE","RTRIM(MI006_CHR_EXEM_PROFILE) AS MI006_CHR_EXEM_PROFILE","RTRIM(MI006_REPAY_SCHED) AS MI006_REPAY_SCHED","MI006_PURPOSE_CODE_A","MI006_PURPOSE_CODE_B","MI006_REPAY_DAY","MI006_DUE_STRT_DATE","MI006_SOURCE_CDE","MI006_LOAN_OFFICER","RTRIM(MI006_FUND_TYPE) AS MI006_FUND_TYPE","RTRIM(MI006_FUND_SOURCE) AS MI006_FUND_SOURCE","MI006_INT_ACCR","MI006_CR_INT_ACCR","MI006_HOL_STRT_DT","MI006_REP_HOL_MTHS","RTRIM(MI006_LINKED_DEP_ACCOUNT) AS MI006_LINKED_DEP_ACCOUNT","MI006_CAPN_DAY","RTRIM(MI006_COLLECTIBILITY) AS MI006_COLLECTIBILITY","RTRIM(MI006_APPR_INDUSTRY_SECT) AS MI006_APPR_INDUSTRY_SECT","MI006_INTEREST","MI006_UNPD_PRIN_BAL","MI006_OVERDUE_BAL","MI006_TOT_PROJ_COST","MI006_UNEARNED_INT","MI006_TOT_PRJ_CST_UNERND_INT","MI006_INT_START_DATE","RTRIM(MI006_BANKRUPTCY) AS MI006_BANKRUPTCY","MI006_WRITTEN_OFF_INT_AC","MI006_LAST_ADV_DATE","RTRIM(MI006_REPORT_MASK) AS MI006_REPORT_MASK","RTRIM(MI006_LOAN_LOC_STAT_CODE) AS MI006_LOAN_LOC_STAT_CODE","MI006_REPAY_TYPE","MI006_PEND_DUES","RTRIM(MI006_FINE_CDE) AS MI006_FINE_CDE","RTRIM(MI006_BAD_DEBT_IND) AS MI006_BAD_DEBT_IND","MI006_RT_INCR_MOV_AD","RTRIM(MI006_INS_IND) AS MI006_INS_IND","RTRIM(MI006_SPECIAL_REPAY_FLAG) AS MI006_SPECIAL_REPAY_FLAG","MI006_ARR_ACCR_AMT","MI006_OUT_OF_ARREARS_DT","MI006_UNPAID_SERV_BAL","MI006_STAMPED_AMT","MI006_TRF_ACCT_NO","MI006_SUSPENDED_INT","MI006_DISCH_DATE","MI006_PEND_LOAN_TERM","MI006_PRINCIPAL_INST_AMT","MI006_DUE_AMT","RTRIM(MI006_BORM_SYS) AS MI006_BORM_SYS","MI006_LST_REPHASE_YR","RTRIM(MI006_BORM_SYNDICATE_TYPE) AS MI006_BORM_SYNDICATE_TYPE","RTRIM(MI006_BORM_GROUP_CODE) AS MI006_BORM_GROUP_CODE","RTRIM(MI006_SWEEP_ACCT_FLAG) AS MI006_SWEEP_ACCT_FLAG","RTRIM(MI006_GLDM_BRANCH) AS MI006_GLDM_BRANCH","RTRIM(MI006_GLDM_CURRENCY) AS MI006_GLDM_CURRENCY","MI006_ACCT_CHK_DGT","MI006_ACT_GOODS","MI006_APPR_AMT_SIGN","MI006_AR_AMT","MI006_ARR_INT_ACCR","MI006_ARR_INT_ACCR_SIGN","MI006_BORM_ACCEPT_DATE","RTRIM(MI006_BORM_BASE_ID) AS MI006_BORM_BASE_ID","RTRIM(MI006_BORM_BORR_TYPE) AS MI006_BORM_BORR_TYPE","MI006_BORM_CAP_UNPD_INT","RTRIM(MI006_BORM_COLLECTI_STATUS) AS MI006_BORM_COLLECTI_STATUS","MI006_BORM_EFF_RATE","RTRIM(MI006_BORM_INS_IND) AS MI006_BORM_INS_IND","RTRIM(MI006_BORM_REDRAW_IND) AS MI006_BORM_REDRAW_IND","MI006_BORM_RT_INCR_MOV_AD","MI006_BORM_SETTLEMENT_DATE","RTRIM(MI006_BORM_SPL_REP_FLAG) AS MI006_BORM_SPL_REP_FLAG","MI006_BORM_UNPD_CHRG_BAL","MI006_CAP_THEO_UNPD_INT","MI006_CAP_UNPD_INT","MI006_CR_AVG_BAL","RTRIM(MI006_CURRENCY) AS MI006_CURRENCY","MI006_DISB_PERC","MI006_DUE_NOT_PAID","MI006_FINAL_REPAYMENT","MI006_FULL_RCVR_FLAG","MI006_INST_REMAIN","MI006_LAST_DUE_DATE","MI006_LOAN_BAL_SIGN","MI006_LOAN_PURPOSE","RTRIM(MI006_LONP_ACCT_TYPE) AS MI006_LONP_ACCT_TYPE","MI006_MAT_EXTN_FLAG","MI006_NO_DAYS_ARREAR","MI006_NO_OF_MESSAGES","MI006_OVERDUE_INT_BAL","MI006_PENALTY_RATE","MI006_PP_RATE","MI006_THEO_UNPD_CHRG_BAL","MI006_UNDISBURSED_AMT","MI006_UPPD_ARR_INT","MI006_UNPD_PRIN_BAL_SIGN","MI006_UNPD_CHRG_BAL_SIGN","MI006_WRITEOFF_AMOUNT","MI006_WRITTENOFF_DATE","MI006_ACCT_ACTIVE","MI006_NO_OF_OWNER","MI006_CLOSED_IND","MI006_CYCLE_ARREARS_VALUE","MI006_DEFAULT_FLAG","MI006_LOAN_AGE","MI006_MONTHS_ON_BOOKS","RTRIM(MI006_LAST_REPAY_METHOD) AS MI006_LAST_REPAY_METHOD","MI006_SUSPENSE_AMOUNT","MI006_OVERDUE_PRIN_BAL","MI006_AM_INT_MTH_ARR","MI006_BLDVNN_TOTAL_INT","MI006_AM_PARPMT_BAL","MI006_NO_INSTALLMENT","MI006_RATE_MATURITY_DATE","MI006_COLLECT_MAN_CHG_DT","MI006_FULL_DRAWN_DT","MI006_PSSO_NEXT_PAY_DATE","RTRIM(MI006_DEFER_FLAG) AS MI006_DEFER_FLAG","RTRIM(MI006_PRIN_REPAY_FREQ) AS MI006_PRIN_REPAY_FREQ","RTRIM(MI006_PRIN_PAY_UNIT) AS MI006_PRIN_PAY_UNIT","MI006_ORG_TENOR","MI006_REV_TENOR","MI006_GL_CLASS_CODE","MI006_GLPP_COMP1","MI006_GLPP_COMP2","MI006_DEFFERMENT_EXPIRY_DATE","RTRIM(MI006_BORM_APPL_ID) AS MI006_BORM_APPL_ID","RTRIM(MI006_CAPN_FREQ) AS MI006_CAPN_FREQ","MI006_BORM_INT_TAX_YTD","MI006_BOIS_STOPP_IND","MI006_BORM_OVERDUE_UNPD_INT","MI006_ASSESED_FEE","MI006_BORM_CI_AVG_BAL","MI006_BORM_INT_INCR","RTRIM(MI006_BORM_REPAY_METHOD) AS MI006_BORM_REPAY_METHOD","MI006_MAX_INT_RATE","MI006_BORM_HOLD_AMT","MI006_SHORTFALL_IND","MI006_P_WOFF_DATE","MI006_SUB_LEDGER_ACCURAL_AMT","MI006_SUB_LEDGER_MISC_FEES_AMT","RTRIM(MI006_CAS_STAT_CODE) AS MI006_CAS_STAT_CODE","MI006_BOTM_FEES_AMOUNT","RTRIM(MI006_SECURITY_METHOD) AS MI006_SECURITY_METHOD","MI006_BORM_CUSM_VIEW_AMT","MI006_BORM_PREV_OFFICER","RTRIM(MI006_BORM_PAY_DAY) AS MI006_BORM_PAY_DAY","MI006_BORM_RENEWAL_DATE","MI006_BORM_REPAY_COUNT","RTRIM(MI006_BORM_FIRST_PAY_IND) AS MI006_BORM_FIRST_PAY_IND","MI006_BORM_ARR_NOT_FLAG","MI006_BORM_AGREEMENT_NUM","MI006_BORM_RESIDUAL_BAL","MI006_UNCLEARED_AMT","MI006_THEO_UNPD_PRIN_BAL","MI006_BORM_EOM_BASIC_BAL").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SOC_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MEMB_CUST_AC', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BR_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_STAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_ACT_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_APPLIC_ISSUE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_APPLIC_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REPAY_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_APP_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADV_VAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADD_LOAN_ADV', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_THEO_LOAN_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_REPAY', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_APPRV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LST_FIN_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LST_MNT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LAST_ARR_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADD_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_LOAN_TRM', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REM_REPAYS', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MORTGAGE_INSU', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_RESIDUAL_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_FIXED_RT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RT_INCR', 'type': 'decimal(8,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CUSTOMER_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(20)'}}, {'name': 'MI006_STMT_FREQUENCY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_TERM_BASIS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_CAFM_START_RTE_DTE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CHR_EXEM_PROFILE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_REPAY_SCHED', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_PURPOSE_CODE_A', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PURPOSE_CODE_B', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REPAY_DAY', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DUE_STRT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SOURCE_CDE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_OFFICER', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FUND_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_FUND_SOURCE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_INT_ACCR', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CR_INT_ACCR', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_HOL_STRT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REP_HOL_MTHS', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LINKED_DEP_ACCOUNT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_CAPN_DAY', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_COLLECTIBILITY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_APPR_INDUSTRY_SECT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_INTEREST', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNPD_PRIN_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OVERDUE_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TOT_PROJ_COST', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNEARNED_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TOT_PRJ_CST_UNERND_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_START_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BANKRUPTCY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_WRITTEN_OFF_INT_AC', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LAST_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REPORT_MASK', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_LOAN_LOC_STAT_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_REPAY_TYPE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PEND_DUES', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FINE_CDE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BAD_DEBT_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_RT_INCR_MOV_AD', 'type': 'decimal(11,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INS_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SPECIAL_REPAY_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ARR_ACCR_AMT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OUT_OF_ARREARS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNPAID_SERV_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STAMPED_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TRF_ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SUSPENDED_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PEND_LOAN_TERM', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRINCIPAL_INST_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DUE_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_SYS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_LST_REPHASE_YR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_SYNDICATE_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BORM_GROUP_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_SWEEP_ACCT_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_GLDM_BRANCH', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_GLDM_CURRENCY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_ACCT_CHK_DGT', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACT_GOODS', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_APPR_AMT_SIGN', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AR_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ARR_INT_ACCR', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ARR_INT_ACCR_SIGN', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_ACCEPT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_BORM_BORR_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_BORM_CAP_UNPD_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_COLLECTI_STATUS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BORM_EFF_RATE', 'type': 'decimal(8,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_INS_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BORM_REDRAW_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BORM_RT_INCR_MOV_AD', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_SETTLEMENT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_SPL_REP_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BORM_UNPD_CHRG_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAP_THEO_UNPD_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAP_UNPD_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CR_AVG_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CURRENCY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_DISB_PERC', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DUE_NOT_PAID', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FINAL_REPAYMENT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FULL_RCVR_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INST_REMAIN', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LAST_DUE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_BAL_SIGN', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_PURPOSE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LONP_ACCT_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_MAT_EXTN_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_DAYS_ARREAR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_OF_MESSAGES', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OVERDUE_INT_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PENALTY_RATE', 'type': 'decimal(7,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PP_RATE', 'type': 'decimal(8,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_THEO_UNPD_CHRG_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNDISBURSED_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UPPD_ARR_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNPD_PRIN_BAL_SIGN', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNPD_CHRG_BAL_SIGN', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITEOFF_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITTENOFF_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACCT_ACTIVE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_OF_OWNER', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CLOSED_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CYCLE_ARREARS_VALUE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DEFAULT_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_AGE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MONTHS_ON_BOOKS', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LAST_REPAY_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SUSPENSE_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OVERDUE_PRIN_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AM_INT_MTH_ARR', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_TOTAL_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AM_PARPMT_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_INSTALLMENT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RATE_MATURITY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_COLLECT_MAN_CHG_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FULL_DRAWN_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PSSO_NEXT_PAY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DEFER_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_PRIN_REPAY_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(10)'}}, {'name': 'MI006_PRIN_PAY_UNIT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(7)'}}, {'name': 'MI006_ORG_TENOR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REV_TENOR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GL_CLASS_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GLPP_COMP1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GLPP_COMP2', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DEFFERMENT_EXPIRY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_APPL_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(20)'}}, {'name': 'MI006_CAPN_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BORM_INT_TAX_YTD', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_STOPP_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_OVERDUE_UNPD_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ASSESED_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_CI_AVG_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_INT_INCR', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_REPAY_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_MAX_INT_RATE', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_HOLD_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SHORTFALL_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WOFF_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SUB_LEDGER_ACCURAL_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SUB_LEDGER_MISC_FEES_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAS_STAT_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOTM_FEES_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SECURITY_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BORM_CUSM_VIEW_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_PREV_OFFICER', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_PAY_DAY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BORM_RENEWAL_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_REPAY_COUNT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_FIRST_PAY_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BORM_ARR_NOT_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_AGREEMENT_NUM', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_RESIDUAL_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNCLEARED_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_THEO_UNPD_PRIN_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_EOM_BASIC_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__NETZ_SRC_BORM_Left_v PURGE").show()
    
    print("NETZ_SRC_BORM_Left_v")
    
    print(NETZ_SRC_BORM_Left_v.schema.json())
    
    print("count:{}".format(NETZ_SRC_BORM_Left_v.count()))
    
    NETZ_SRC_BORM_Left_v.show(1000,False)
    
    NETZ_SRC_BORM_Left_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__NETZ_SRC_BORM_Left_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_46_joi_DTEP_INSP_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    netz_ODS_DTEP_INSP_joi_DTEP_INSP_v=spark.table('datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__netz_ODS_DTEP_INSP_joi_DTEP_INSP_v')
    
    Transformer_46_joi_DTEP_INSP_Part_v=netz_ODS_DTEP_INSP_joi_DTEP_INSP_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__Transformer_46_joi_DTEP_INSP_Part_v PURGE").show()
    
    print("Transformer_46_joi_DTEP_INSP_Part_v")
    
    print(Transformer_46_joi_DTEP_INSP_Part_v.schema.json())
    
    print("count:{}".format(Transformer_46_joi_DTEP_INSP_Part_v.count()))
    
    Transformer_46_joi_DTEP_INSP_Part_v.show(1000,False)
    
    Transformer_46_joi_DTEP_INSP_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__Transformer_46_joi_DTEP_INSP_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Join_40_Left_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    NETZ_SRC_BORM_Left_v=spark.table('datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__NETZ_SRC_BORM_Left_v')
    
    Join_40_Left_Part_v=NETZ_SRC_BORM_Left_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__Join_40_Left_Part_v PURGE").show()
    
    print("Join_40_Left_Part_v")
    
    print(Join_40_Left_Part_v.schema.json())
    
    print("count:{}".format(Join_40_Left_Part_v.count()))
    
    Join_40_Left_Part_v.show(1000,False)
    
    Join_40_Left_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__Join_40_Left_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_46(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_46_joi_DTEP_INSP_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__Transformer_46_joi_DTEP_INSP_Part_v')
	
	
    max_iter = 30
    Transformer_46_v_0 = Transformer_46_joi_DTEP_INSP_Part_v.withColumn('A', expr("""IF(DC > CLEARING_DAYS_1, (IF(ISNOTNULL(DTEP_CODES), (CONCAT_WS('', DTEP_CODES, NXT_CODES)), 'X')), 'X')""").cast('string').alias('A')).withColumn('B', expr("""IF(LATE_CHQ_FLAG = 'N' OR LATE_CHQ_FLAG = ' ', 'Y', 'N')""").cast('string').alias('B')).withColumn('C', lit('CLEARING_DAYS_1').cast('integer').alias('C')).withColumn('Curr_Date', expr("""DATE_DIFF('2025-08-28','1900-01-01')""").cast('int').alias('Curr_Date'))

    df_loop = Transformer_46_v_0.withColumn("iterations", F.sequence(F.lit(0), F.lit(max_iter)))
    # Variable D: Extracting the 2-digit code for every possible iteration
    df_loop = df_loop.withColumn("D", 
        F.expr(f"transform(iterations, i -> IF(A = 'X', '0', substring(A, cast((DT + i) * 2 + 1 as int), 2)))")
    )

    # Variable N: Identifying indices where D is '01' or '05'
    # In DataStage, N increments. In Spark, we find the positions of these matches.
    df_loop = df_loop.withColumn("N", 
        F.expr("filter(iterations, i -> D[i] IN ('01', '05'))")
    )

    # --- Final Loop Variables (F, Z, X) ---
    # F: Date when N=1 (first match)
    # Z: Date when N=2 (second match)
    # X: Final value of N (the count of matches)
    df_final = df_loop.withColumn("F", 
        F.when(F.col("A") != "X", F.expr("TRAN_DATE + N[0]"))
        .otherwise(F.lit(0))
    ).withColumn("Z", 
        F.when(F.col("A") != "X", F.expr("TRAN_DATE + N[1]"))
        .otherwise(F.lit(0))
    ).withColumn("X", F.size(F.col("N")))

    # Optional: Cleanup internal arrays to keep it clean like a Transformer output
    Transformer_46_v = df_final.drop("iterations", "D", "N")

    Transformer_46_DSLink48_v = Transformer_46_v.select(col('B_KEY').cast('string').alias('B_KEY'),expr("""RIGHT(CONCAT('00', CASE WHEN DC > CLEARING_DAYS_1 THEN CASE WHEN F = Curr_Date THEN CASE WHEN B = 'Y' THEN C - 1 ELSE C END WHEN F > Curr_Date THEN CASE WHEN B = 'Y' THEN C ELSE C + 1 END WHEN F < Curr_Date THEN CASE WHEN B = 'Y' THEN CASE WHEN (F + 1) = Curr_Date THEN C - 2 ELSE 0 END ELSE CASE WHEN Z = Curr_Date THEN C - 1 WHEN (Z + 1) = Curr_Date THEN C - 2 ELSE 0 END END ELSE 0 END ELSE CASE WHEN NOT DC IS NULL THEN FD ELSE 0 END END), 2)""").cast('string').alias('FLOATDAYS'))
    
    Transformer_46_DSLink48_v = Transformer_46_DSLink48_v.selectExpr("B_KEY","RTRIM(FLOATDAYS) AS FLOATDAYS").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'FLOATDAYS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__Transformer_46_DSLink48_v PURGE").show()
    
    print("Transformer_46_DSLink48_v")
    
    print(Transformer_46_DSLink48_v.schema.json())
    
    print("count:{}".format(Transformer_46_DSLink48_v.count()))
    
    Transformer_46_DSLink48_v.show(1000,False)
    
    Transformer_46_DSLink48_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__Transformer_46_DSLink48_v")
    

@task.pyspark(conn_id="spark-local")
def Join_40_DSLink48_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_46_DSLink48_v=spark.table('datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__Transformer_46_DSLink48_v')
    
    Join_40_DSLink48_Part_v=Transformer_46_DSLink48_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__Join_40_DSLink48_Part_v PURGE").show()
    
    print("Join_40_DSLink48_Part_v")
    
    print(Join_40_DSLink48_Part_v.schema.json())
    
    print("count:{}".format(Join_40_DSLink48_Part_v.count()))
    
    Join_40_DSLink48_Part_v.show(1000,False)
    
    Join_40_DSLink48_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__Join_40_DSLink48_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Join_40(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Join_40_Left_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__Join_40_Left_Part_v')
    
    Join_40_DSLink48_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__Join_40_DSLink48_Part_v')
    
    Join_40_v=Join_40_Left_Part_v.join(Join_40_DSLink48_Part_v,['B_KEY'],'left')
    
    Join_40_lnk_Source_v = Join_40_v.select(Join_40_Left_Part_v.B_KEY.cast('string').alias('B_KEY'),Join_40_Left_Part_v.MI006_SOC_NO.cast('string').alias('MI006_SOC_NO'),Join_40_Left_Part_v.MI006_MEMB_CUST_AC.cast('string').alias('MI006_MEMB_CUST_AC'),Join_40_Left_Part_v.MI006_BR_NO.cast('string').alias('MI006_BR_NO'),Join_40_Left_Part_v.MI006_STAT.cast('string').alias('MI006_STAT'),Join_40_Left_Part_v.MI006_ACT_TYPE.cast('string').alias('MI006_ACT_TYPE'),Join_40_Left_Part_v.MI006_APPLIC_ISSUE_DATE.cast('integer').alias('MI006_APPLIC_ISSUE_DATE'),Join_40_Left_Part_v.MI006_APPLIC_AMOUNT.cast('decimal(18,3)').alias('MI006_APPLIC_AMOUNT'),Join_40_Left_Part_v.MI006_REPAY_FREQ.cast('string').alias('MI006_REPAY_FREQ'),Join_40_Left_Part_v.MI006_APP_AMT.cast('decimal(18,3)').alias('MI006_APP_AMT'),Join_40_Left_Part_v.MI006_LOAN_BAL.cast('decimal(18,3)').alias('MI006_LOAN_BAL'),Join_40_Left_Part_v.MI006_ADV_VAL.cast('decimal(18,3)').alias('MI006_ADV_VAL'),Join_40_Left_Part_v.MI006_ADD_LOAN_ADV.cast('decimal(18,3)').alias('MI006_ADD_LOAN_ADV'),Join_40_Left_Part_v.MI006_THEO_LOAN_BAL.cast('decimal(18,3)').alias('MI006_THEO_LOAN_BAL'),Join_40_Left_Part_v.MI006_LOAN_REPAY.cast('decimal(18,3)').alias('MI006_LOAN_REPAY'),Join_40_Left_Part_v.MI006_APPRV_DATE.cast('integer').alias('MI006_APPRV_DATE'),Join_40_Left_Part_v.MI006_LST_FIN_DATE.cast('integer').alias('MI006_LST_FIN_DATE'),Join_40_Left_Part_v.MI006_LST_MNT_DATE.cast('integer').alias('MI006_LST_MNT_DATE'),Join_40_Left_Part_v.MI006_ADV_DATE.cast('integer').alias('MI006_ADV_DATE'),Join_40_Left_Part_v.MI006_LAST_ARR_DATE.cast('integer').alias('MI006_LAST_ARR_DATE'),Join_40_Left_Part_v.MI006_ADD_ADV_DATE.cast('integer').alias('MI006_ADD_ADV_DATE'),Join_40_Left_Part_v.MI006_CAT.cast('string').alias('MI006_CAT'),Join_40_Left_Part_v.MI006_LOAN_TRM.cast('integer').alias('MI006_LOAN_TRM'),Join_40_Left_Part_v.MI006_REM_REPAYS.cast('integer').alias('MI006_REM_REPAYS'),Join_40_Left_Part_v.MI006_MORTGAGE_INSU.cast('string').alias('MI006_MORTGAGE_INSU'),Join_40_Left_Part_v.MI006_RESIDUAL_IND.cast('string').alias('MI006_RESIDUAL_IND'),Join_40_Left_Part_v.MI006_FIXED_RT_DATE.cast('integer').alias('MI006_FIXED_RT_DATE'),Join_40_Left_Part_v.MI006_RT_INCR.cast('decimal(8,5)').alias('MI006_RT_INCR'),Join_40_Left_Part_v.MI006_CUSTOMER_NO.cast('string').alias('MI006_CUSTOMER_NO'),Join_40_Left_Part_v.MI006_STMT_FREQUENCY.cast('string').alias('MI006_STMT_FREQUENCY'),Join_40_Left_Part_v.MI006_TERM_BASIS.cast('string').alias('MI006_TERM_BASIS'),Join_40_Left_Part_v.MI006_CAFM_START_RTE_DTE.cast('integer').alias('MI006_CAFM_START_RTE_DTE'),Join_40_Left_Part_v.MI006_CHR_EXEM_PROFILE.cast('string').alias('MI006_CHR_EXEM_PROFILE'),Join_40_Left_Part_v.MI006_REPAY_SCHED.cast('string').alias('MI006_REPAY_SCHED'),Join_40_Left_Part_v.MI006_PURPOSE_CODE_A.cast('string').alias('MI006_PURPOSE_CODE_A'),Join_40_Left_Part_v.MI006_PURPOSE_CODE_B.cast('string').alias('MI006_PURPOSE_CODE_B'),Join_40_Left_Part_v.MI006_REPAY_DAY.cast('integer').alias('MI006_REPAY_DAY'),Join_40_Left_Part_v.MI006_DUE_STRT_DATE.cast('integer').alias('MI006_DUE_STRT_DATE'),Join_40_Left_Part_v.MI006_SOURCE_CDE.cast('integer').alias('MI006_SOURCE_CDE'),Join_40_Left_Part_v.MI006_LOAN_OFFICER.cast('integer').alias('MI006_LOAN_OFFICER'),Join_40_Left_Part_v.MI006_FUND_TYPE.cast('string').alias('MI006_FUND_TYPE'),Join_40_Left_Part_v.MI006_FUND_SOURCE.cast('string').alias('MI006_FUND_SOURCE'),Join_40_Left_Part_v.MI006_INT_ACCR.cast('decimal(18,5)').alias('MI006_INT_ACCR'),Join_40_Left_Part_v.MI006_CR_INT_ACCR.cast('decimal(18,5)').alias('MI006_CR_INT_ACCR'),Join_40_Left_Part_v.MI006_HOL_STRT_DT.cast('integer').alias('MI006_HOL_STRT_DT'),Join_40_Left_Part_v.MI006_REP_HOL_MTHS.cast('integer').alias('MI006_REP_HOL_MTHS'),Join_40_Left_Part_v.MI006_LINKED_DEP_ACCOUNT.cast('string').alias('MI006_LINKED_DEP_ACCOUNT'),Join_40_Left_Part_v.MI006_CAPN_DAY.cast('integer').alias('MI006_CAPN_DAY'),Join_40_Left_Part_v.MI006_COLLECTIBILITY.cast('string').alias('MI006_COLLECTIBILITY'),Join_40_Left_Part_v.MI006_APPR_INDUSTRY_SECT.cast('string').alias('MI006_APPR_INDUSTRY_SECT'),Join_40_Left_Part_v.MI006_INTEREST.cast('decimal(18,3)').alias('MI006_INTEREST'),Join_40_Left_Part_v.MI006_UNPD_PRIN_BAL.cast('decimal(18,3)').alias('MI006_UNPD_PRIN_BAL'),Join_40_Left_Part_v.MI006_OVERDUE_BAL.cast('decimal(18,3)').alias('MI006_OVERDUE_BAL'),Join_40_Left_Part_v.MI006_TOT_PROJ_COST.cast('decimal(18,3)').alias('MI006_TOT_PROJ_COST'),Join_40_Left_Part_v.MI006_UNEARNED_INT.cast('decimal(18,3)').alias('MI006_UNEARNED_INT'),Join_40_Left_Part_v.MI006_TOT_PRJ_CST_UNERND_INT.cast('decimal(18,3)').alias('MI006_TOT_PRJ_CST_UNERND_INT'),Join_40_Left_Part_v.MI006_INT_START_DATE.cast('integer').alias('MI006_INT_START_DATE'),Join_40_Left_Part_v.MI006_BANKRUPTCY.cast('string').alias('MI006_BANKRUPTCY'),Join_40_Left_Part_v.MI006_WRITTEN_OFF_INT_AC.cast('decimal(18,3)').alias('MI006_WRITTEN_OFF_INT_AC'),Join_40_Left_Part_v.MI006_LAST_ADV_DATE.cast('integer').alias('MI006_LAST_ADV_DATE'),Join_40_Left_Part_v.MI006_REPORT_MASK.cast('string').alias('MI006_REPORT_MASK'),Join_40_Left_Part_v.MI006_LOAN_LOC_STAT_CODE.cast('string').alias('MI006_LOAN_LOC_STAT_CODE'),Join_40_Left_Part_v.MI006_REPAY_TYPE.cast('string').alias('MI006_REPAY_TYPE'),Join_40_Left_Part_v.MI006_PEND_DUES.cast('decimal(18,3)').alias('MI006_PEND_DUES'),Join_40_Left_Part_v.MI006_FINE_CDE.cast('string').alias('MI006_FINE_CDE'),Join_40_Left_Part_v.MI006_BAD_DEBT_IND.cast('string').alias('MI006_BAD_DEBT_IND'),Join_40_Left_Part_v.MI006_RT_INCR_MOV_AD.cast('decimal(11,4)').alias('MI006_RT_INCR_MOV_AD'),Join_40_Left_Part_v.MI006_INS_IND.cast('string').alias('MI006_INS_IND'),Join_40_Left_Part_v.MI006_SPECIAL_REPAY_FLAG.cast('string').alias('MI006_SPECIAL_REPAY_FLAG'),Join_40_Left_Part_v.MI006_ARR_ACCR_AMT.cast('integer').alias('MI006_ARR_ACCR_AMT'),Join_40_Left_Part_v.MI006_OUT_OF_ARREARS_DT.cast('integer').alias('MI006_OUT_OF_ARREARS_DT'),Join_40_Left_Part_v.MI006_UNPAID_SERV_BAL.cast('decimal(18,3)').alias('MI006_UNPAID_SERV_BAL'),Join_40_Left_Part_v.MI006_STAMPED_AMT.cast('decimal(18,3)').alias('MI006_STAMPED_AMT'),Join_40_Left_Part_v.MI006_TRF_ACCT_NO.cast('string').alias('MI006_TRF_ACCT_NO'),Join_40_Left_Part_v.MI006_SUSPENDED_INT.cast('decimal(18,3)').alias('MI006_SUSPENDED_INT'),Join_40_Left_Part_v.MI006_DISCH_DATE.cast('integer').alias('MI006_DISCH_DATE'),Join_40_Left_Part_v.MI006_PEND_LOAN_TERM.cast('integer').alias('MI006_PEND_LOAN_TERM'),Join_40_Left_Part_v.MI006_PRINCIPAL_INST_AMT.cast('decimal(18,3)').alias('MI006_PRINCIPAL_INST_AMT'),Join_40_Left_Part_v.MI006_DUE_AMT.cast('decimal(18,3)').alias('MI006_DUE_AMT'),Join_40_Left_Part_v.MI006_BORM_SYS.cast('string').alias('MI006_BORM_SYS'),Join_40_Left_Part_v.MI006_LST_REPHASE_YR.cast('string').alias('MI006_LST_REPHASE_YR'),Join_40_Left_Part_v.MI006_BORM_SYNDICATE_TYPE.cast('string').alias('MI006_BORM_SYNDICATE_TYPE'),Join_40_Left_Part_v.MI006_BORM_GROUP_CODE.cast('string').alias('MI006_BORM_GROUP_CODE'),Join_40_Left_Part_v.MI006_SWEEP_ACCT_FLAG.cast('string').alias('MI006_SWEEP_ACCT_FLAG'),Join_40_Left_Part_v.MI006_GLDM_BRANCH.cast('string').alias('MI006_GLDM_BRANCH'),Join_40_Left_Part_v.MI006_GLDM_CURRENCY.cast('string').alias('MI006_GLDM_CURRENCY'),Join_40_Left_Part_v.MI006_ACCT_CHK_DGT.cast('string').alias('MI006_ACCT_CHK_DGT'),Join_40_Left_Part_v.MI006_ACT_GOODS.cast('string').alias('MI006_ACT_GOODS'),Join_40_Left_Part_v.MI006_APPR_AMT_SIGN.cast('string').alias('MI006_APPR_AMT_SIGN'),Join_40_Left_Part_v.MI006_AR_AMT.cast('decimal(18,3)').alias('MI006_AR_AMT'),Join_40_Left_Part_v.MI006_ARR_INT_ACCR.cast('decimal(18,3)').alias('MI006_ARR_INT_ACCR'),Join_40_Left_Part_v.MI006_ARR_INT_ACCR_SIGN.cast('string').alias('MI006_ARR_INT_ACCR_SIGN'),Join_40_Left_Part_v.MI006_BORM_ACCEPT_DATE.cast('integer').alias('MI006_BORM_ACCEPT_DATE'),Join_40_Left_Part_v.MI006_BORM_BASE_ID.cast('string').alias('MI006_BORM_BASE_ID'),Join_40_Left_Part_v.MI006_BORM_BORR_TYPE.cast('string').alias('MI006_BORM_BORR_TYPE'),Join_40_Left_Part_v.MI006_BORM_CAP_UNPD_INT.cast('decimal(18,3)').alias('MI006_BORM_CAP_UNPD_INT'),Join_40_Left_Part_v.MI006_BORM_COLLECTI_STATUS.cast('string').alias('MI006_BORM_COLLECTI_STATUS'),Join_40_Left_Part_v.MI006_BORM_EFF_RATE.cast('decimal(8,5)').alias('MI006_BORM_EFF_RATE'),Join_40_Left_Part_v.MI006_BORM_INS_IND.cast('string').alias('MI006_BORM_INS_IND'),Join_40_Left_Part_v.MI006_BORM_REDRAW_IND.cast('string').alias('MI006_BORM_REDRAW_IND'),Join_40_Left_Part_v.MI006_BORM_RT_INCR_MOV_AD.cast('decimal(8,4)').alias('MI006_BORM_RT_INCR_MOV_AD'),Join_40_Left_Part_v.MI006_BORM_SETTLEMENT_DATE.cast('integer').alias('MI006_BORM_SETTLEMENT_DATE'),Join_40_Left_Part_v.MI006_BORM_SPL_REP_FLAG.cast('string').alias('MI006_BORM_SPL_REP_FLAG'),Join_40_Left_Part_v.MI006_BORM_UNPD_CHRG_BAL.cast('decimal(18,3)').alias('MI006_BORM_UNPD_CHRG_BAL'),Join_40_Left_Part_v.MI006_CAP_THEO_UNPD_INT.cast('decimal(18,3)').alias('MI006_CAP_THEO_UNPD_INT'),Join_40_Left_Part_v.MI006_CAP_UNPD_INT.cast('decimal(18,3)').alias('MI006_CAP_UNPD_INT'),Join_40_Left_Part_v.MI006_CR_AVG_BAL.cast('decimal(18,3)').alias('MI006_CR_AVG_BAL'),Join_40_Left_Part_v.MI006_CURRENCY.cast('string').alias('MI006_CURRENCY'),Join_40_Left_Part_v.MI006_DISB_PERC.cast('decimal(8,4)').alias('MI006_DISB_PERC'),Join_40_Left_Part_v.MI006_DUE_NOT_PAID.cast('decimal(18,3)').alias('MI006_DUE_NOT_PAID'),Join_40_Left_Part_v.MI006_FINAL_REPAYMENT.cast('decimal(18,3)').alias('MI006_FINAL_REPAYMENT'),Join_40_Left_Part_v.MI006_FULL_RCVR_FLAG.cast('string').alias('MI006_FULL_RCVR_FLAG'),Join_40_Left_Part_v.MI006_INST_REMAIN.cast('decimal(18,3)').alias('MI006_INST_REMAIN'),Join_40_Left_Part_v.MI006_LAST_DUE_DATE.cast('integer').alias('MI006_LAST_DUE_DATE'),Join_40_Left_Part_v.MI006_LOAN_BAL_SIGN.cast('string').alias('MI006_LOAN_BAL_SIGN'),Join_40_Left_Part_v.MI006_LOAN_PURPOSE.cast('string').alias('MI006_LOAN_PURPOSE'),Join_40_Left_Part_v.MI006_LONP_ACCT_TYPE.cast('string').alias('MI006_LONP_ACCT_TYPE'),Join_40_Left_Part_v.MI006_MAT_EXTN_FLAG.cast('string').alias('MI006_MAT_EXTN_FLAG'),Join_40_Left_Part_v.MI006_NO_DAYS_ARREAR.cast('string').alias('MI006_NO_DAYS_ARREAR'),Join_40_Left_Part_v.MI006_NO_OF_MESSAGES.cast('string').alias('MI006_NO_OF_MESSAGES'),Join_40_Left_Part_v.MI006_OVERDUE_INT_BAL.cast('decimal(18,3)').alias('MI006_OVERDUE_INT_BAL'),Join_40_Left_Part_v.MI006_PENALTY_RATE.cast('decimal(7,4)').alias('MI006_PENALTY_RATE'),Join_40_Left_Part_v.MI006_PP_RATE.cast('decimal(8,5)').alias('MI006_PP_RATE'),Join_40_Left_Part_v.MI006_THEO_UNPD_CHRG_BAL.cast('decimal(18,3)').alias('MI006_THEO_UNPD_CHRG_BAL'),Join_40_Left_Part_v.MI006_UNDISBURSED_AMT.cast('decimal(18,3)').alias('MI006_UNDISBURSED_AMT'),Join_40_Left_Part_v.MI006_UPPD_ARR_INT.cast('decimal(18,3)').alias('MI006_UPPD_ARR_INT'),Join_40_Left_Part_v.MI006_UNPD_PRIN_BAL_SIGN.cast('string').alias('MI006_UNPD_PRIN_BAL_SIGN'),Join_40_Left_Part_v.MI006_UNPD_CHRG_BAL_SIGN.cast('string').alias('MI006_UNPD_CHRG_BAL_SIGN'),Join_40_Left_Part_v.MI006_WRITEOFF_AMOUNT.cast('decimal(18,3)').alias('MI006_WRITEOFF_AMOUNT'),Join_40_Left_Part_v.MI006_WRITTENOFF_DATE.cast('integer').alias('MI006_WRITTENOFF_DATE'),Join_40_Left_Part_v.MI006_ACCT_ACTIVE.cast('string').alias('MI006_ACCT_ACTIVE'),Join_40_Left_Part_v.MI006_NO_OF_OWNER.cast('string').alias('MI006_NO_OF_OWNER'),Join_40_Left_Part_v.MI006_CLOSED_IND.cast('string').alias('MI006_CLOSED_IND'),Join_40_Left_Part_v.MI006_CYCLE_ARREARS_VALUE.cast('decimal(18,3)').alias('MI006_CYCLE_ARREARS_VALUE'),Join_40_Left_Part_v.MI006_DEFAULT_FLAG.cast('string').alias('MI006_DEFAULT_FLAG'),Join_40_Left_Part_v.MI006_LOAN_AGE.cast('string').alias('MI006_LOAN_AGE'),Join_40_Left_Part_v.MI006_MONTHS_ON_BOOKS.cast('string').alias('MI006_MONTHS_ON_BOOKS'),Join_40_Left_Part_v.MI006_LAST_REPAY_METHOD.cast('string').alias('MI006_LAST_REPAY_METHOD'),Join_40_Left_Part_v.MI006_SUSPENSE_AMOUNT.cast('decimal(18,3)').alias('MI006_SUSPENSE_AMOUNT'),Join_40_Left_Part_v.MI006_OVERDUE_PRIN_BAL.cast('decimal(18,3)').alias('MI006_OVERDUE_PRIN_BAL'),Join_40_Left_Part_v.MI006_AM_INT_MTH_ARR.cast('decimal(18,3)').alias('MI006_AM_INT_MTH_ARR'),Join_40_Left_Part_v.MI006_BLDVNN_TOTAL_INT.cast('decimal(18,3)').alias('MI006_BLDVNN_TOTAL_INT'),Join_40_Left_Part_v.MI006_AM_PARPMT_BAL.cast('decimal(18,3)').alias('MI006_AM_PARPMT_BAL'),Join_40_Left_Part_v.MI006_NO_INSTALLMENT.cast('integer').alias('MI006_NO_INSTALLMENT'),Join_40_Left_Part_v.MI006_RATE_MATURITY_DATE.cast('integer').alias('MI006_RATE_MATURITY_DATE'),Join_40_Left_Part_v.MI006_COLLECT_MAN_CHG_DT.cast('integer').alias('MI006_COLLECT_MAN_CHG_DT'),Join_40_Left_Part_v.MI006_FULL_DRAWN_DT.cast('integer').alias('MI006_FULL_DRAWN_DT'),Join_40_Left_Part_v.MI006_PSSO_NEXT_PAY_DATE.cast('integer').alias('MI006_PSSO_NEXT_PAY_DATE'),Join_40_Left_Part_v.MI006_DEFER_FLAG.cast('string').alias('MI006_DEFER_FLAG'),Join_40_Left_Part_v.MI006_PRIN_REPAY_FREQ.cast('string').alias('MI006_PRIN_REPAY_FREQ'),Join_40_Left_Part_v.MI006_PRIN_PAY_UNIT.cast('string').alias('MI006_PRIN_PAY_UNIT'),Join_40_Left_Part_v.MI006_ORG_TENOR.cast('string').alias('MI006_ORG_TENOR'),Join_40_Left_Part_v.MI006_REV_TENOR.cast('string').alias('MI006_REV_TENOR'),Join_40_Left_Part_v.MI006_GL_CLASS_CODE.cast('string').alias('MI006_GL_CLASS_CODE'),Join_40_Left_Part_v.MI006_GLPP_COMP1.cast('string').alias('MI006_GLPP_COMP1'),Join_40_Left_Part_v.MI006_GLPP_COMP2.cast('string').alias('MI006_GLPP_COMP2'),Join_40_Left_Part_v.MI006_DEFFERMENT_EXPIRY_DATE.cast('integer').alias('MI006_DEFFERMENT_EXPIRY_DATE'),Join_40_Left_Part_v.MI006_BORM_APPL_ID.cast('string').alias('MI006_BORM_APPL_ID'),Join_40_Left_Part_v.MI006_CAPN_FREQ.cast('string').alias('MI006_CAPN_FREQ'),Join_40_Left_Part_v.MI006_BORM_INT_TAX_YTD.cast('decimal(18,3)').alias('MI006_BORM_INT_TAX_YTD'),Join_40_Left_Part_v.MI006_BOIS_STOPP_IND.cast('string').alias('MI006_BOIS_STOPP_IND'),Join_40_Left_Part_v.MI006_BORM_OVERDUE_UNPD_INT.cast('decimal(18,3)').alias('MI006_BORM_OVERDUE_UNPD_INT'),Join_40_Left_Part_v.MI006_ASSESED_FEE.cast('decimal(18,3)').alias('MI006_ASSESED_FEE'),Join_40_Left_Part_v.MI006_BORM_CI_AVG_BAL.cast('decimal(18,3)').alias('MI006_BORM_CI_AVG_BAL'),Join_40_Left_Part_v.MI006_BORM_INT_INCR.cast('decimal(18,5)').alias('MI006_BORM_INT_INCR'),Join_40_Left_Part_v.MI006_BORM_REPAY_METHOD.cast('string').alias('MI006_BORM_REPAY_METHOD'),Join_40_Left_Part_v.MI006_MAX_INT_RATE.cast('decimal(8,4)').alias('MI006_MAX_INT_RATE'),Join_40_Left_Part_v.MI006_BORM_HOLD_AMT.cast('decimal(18,3)').alias('MI006_BORM_HOLD_AMT'),Join_40_Left_Part_v.MI006_SHORTFALL_IND.cast('string').alias('MI006_SHORTFALL_IND'),Join_40_Left_Part_v.MI006_P_WOFF_DATE.cast('integer').alias('MI006_P_WOFF_DATE'),Join_40_Left_Part_v.MI006_SUB_LEDGER_ACCURAL_AMT.cast('decimal(18,3)').alias('MI006_SUB_LEDGER_ACCURAL_AMT'),Join_40_Left_Part_v.MI006_SUB_LEDGER_MISC_FEES_AMT.cast('decimal(18,3)').alias('MI006_SUB_LEDGER_MISC_FEES_AMT'),Join_40_Left_Part_v.MI006_CAS_STAT_CODE.cast('string').alias('MI006_CAS_STAT_CODE'),Join_40_Left_Part_v.MI006_BOTM_FEES_AMOUNT.cast('decimal(18,3)').alias('MI006_BOTM_FEES_AMOUNT'),Join_40_Left_Part_v.MI006_SECURITY_METHOD.cast('string').alias('MI006_SECURITY_METHOD'),Join_40_Left_Part_v.MI006_BORM_CUSM_VIEW_AMT.cast('decimal(18,3)').alias('MI006_BORM_CUSM_VIEW_AMT'),Join_40_Left_Part_v.MI006_BORM_PREV_OFFICER.cast('integer').alias('MI006_BORM_PREV_OFFICER'),Join_40_Left_Part_v.MI006_BORM_PAY_DAY.cast('string').alias('MI006_BORM_PAY_DAY'),Join_40_Left_Part_v.MI006_BORM_RENEWAL_DATE.cast('integer').alias('MI006_BORM_RENEWAL_DATE'),Join_40_Left_Part_v.MI006_BORM_REPAY_COUNT.cast('integer').alias('MI006_BORM_REPAY_COUNT'),Join_40_Left_Part_v.MI006_BORM_FIRST_PAY_IND.cast('string').alias('MI006_BORM_FIRST_PAY_IND'),Join_40_Left_Part_v.MI006_BORM_ARR_NOT_FLAG.cast('string').alias('MI006_BORM_ARR_NOT_FLAG'),Join_40_Left_Part_v.MI006_BORM_AGREEMENT_NUM.cast('string').alias('MI006_BORM_AGREEMENT_NUM'),Join_40_Left_Part_v.MI006_BORM_RESIDUAL_BAL.cast('decimal(18,3)').alias('MI006_BORM_RESIDUAL_BAL'),Join_40_Left_Part_v.MI006_UNCLEARED_AMT.cast('decimal(18,3)').alias('MI006_UNCLEARED_AMT'),Join_40_Left_Part_v.MI006_THEO_UNPD_PRIN_BAL.cast('decimal(18,3)').alias('MI006_THEO_UNPD_PRIN_BAL'),Join_40_Left_Part_v.MI006_BORM_EOM_BASIC_BAL.cast('decimal(18,3)').alias('MI006_BORM_EOM_BASIC_BAL'),Join_40_DSLink48_Part_v.FLOATDAYS.cast('string').alias('FLOATDAYS'))
    
    Join_40_lnk_Source_v = Join_40_lnk_Source_v.selectExpr("B_KEY","MI006_SOC_NO","MI006_MEMB_CUST_AC","RTRIM(MI006_BR_NO) AS MI006_BR_NO","RTRIM(MI006_STAT) AS MI006_STAT","RTRIM(MI006_ACT_TYPE) AS MI006_ACT_TYPE","MI006_APPLIC_ISSUE_DATE","MI006_APPLIC_AMOUNT","RTRIM(MI006_REPAY_FREQ) AS MI006_REPAY_FREQ","MI006_APP_AMT","MI006_LOAN_BAL","MI006_ADV_VAL","MI006_ADD_LOAN_ADV","MI006_THEO_LOAN_BAL","MI006_LOAN_REPAY","MI006_APPRV_DATE","MI006_LST_FIN_DATE","MI006_LST_MNT_DATE","MI006_ADV_DATE","MI006_LAST_ARR_DATE","MI006_ADD_ADV_DATE","RTRIM(MI006_CAT) AS MI006_CAT","MI006_LOAN_TRM","MI006_REM_REPAYS","RTRIM(MI006_MORTGAGE_INSU) AS MI006_MORTGAGE_INSU","RTRIM(MI006_RESIDUAL_IND) AS MI006_RESIDUAL_IND","MI006_FIXED_RT_DATE","MI006_RT_INCR","RTRIM(MI006_CUSTOMER_NO) AS MI006_CUSTOMER_NO","RTRIM(MI006_STMT_FREQUENCY) AS MI006_STMT_FREQUENCY","RTRIM(MI006_TERM_BASIS) AS MI006_TERM_BASIS","MI006_CAFM_START_RTE_DTE","RTRIM(MI006_CHR_EXEM_PROFILE) AS MI006_CHR_EXEM_PROFILE","RTRIM(MI006_REPAY_SCHED) AS MI006_REPAY_SCHED","MI006_PURPOSE_CODE_A","MI006_PURPOSE_CODE_B","MI006_REPAY_DAY","MI006_DUE_STRT_DATE","MI006_SOURCE_CDE","MI006_LOAN_OFFICER","RTRIM(MI006_FUND_TYPE) AS MI006_FUND_TYPE","RTRIM(MI006_FUND_SOURCE) AS MI006_FUND_SOURCE","MI006_INT_ACCR","MI006_CR_INT_ACCR","MI006_HOL_STRT_DT","MI006_REP_HOL_MTHS","RTRIM(MI006_LINKED_DEP_ACCOUNT) AS MI006_LINKED_DEP_ACCOUNT","MI006_CAPN_DAY","RTRIM(MI006_COLLECTIBILITY) AS MI006_COLLECTIBILITY","RTRIM(MI006_APPR_INDUSTRY_SECT) AS MI006_APPR_INDUSTRY_SECT","MI006_INTEREST","MI006_UNPD_PRIN_BAL","MI006_OVERDUE_BAL","MI006_TOT_PROJ_COST","MI006_UNEARNED_INT","MI006_TOT_PRJ_CST_UNERND_INT","MI006_INT_START_DATE","RTRIM(MI006_BANKRUPTCY) AS MI006_BANKRUPTCY","MI006_WRITTEN_OFF_INT_AC","MI006_LAST_ADV_DATE","RTRIM(MI006_REPORT_MASK) AS MI006_REPORT_MASK","RTRIM(MI006_LOAN_LOC_STAT_CODE) AS MI006_LOAN_LOC_STAT_CODE","MI006_REPAY_TYPE","MI006_PEND_DUES","RTRIM(MI006_FINE_CDE) AS MI006_FINE_CDE","RTRIM(MI006_BAD_DEBT_IND) AS MI006_BAD_DEBT_IND","MI006_RT_INCR_MOV_AD","RTRIM(MI006_INS_IND) AS MI006_INS_IND","RTRIM(MI006_SPECIAL_REPAY_FLAG) AS MI006_SPECIAL_REPAY_FLAG","MI006_ARR_ACCR_AMT","MI006_OUT_OF_ARREARS_DT","MI006_UNPAID_SERV_BAL","MI006_STAMPED_AMT","MI006_TRF_ACCT_NO","MI006_SUSPENDED_INT","MI006_DISCH_DATE","MI006_PEND_LOAN_TERM","MI006_PRINCIPAL_INST_AMT","MI006_DUE_AMT","RTRIM(MI006_BORM_SYS) AS MI006_BORM_SYS","MI006_LST_REPHASE_YR","RTRIM(MI006_BORM_SYNDICATE_TYPE) AS MI006_BORM_SYNDICATE_TYPE","RTRIM(MI006_BORM_GROUP_CODE) AS MI006_BORM_GROUP_CODE","RTRIM(MI006_SWEEP_ACCT_FLAG) AS MI006_SWEEP_ACCT_FLAG","RTRIM(MI006_GLDM_BRANCH) AS MI006_GLDM_BRANCH","RTRIM(MI006_GLDM_CURRENCY) AS MI006_GLDM_CURRENCY","MI006_ACCT_CHK_DGT","MI006_ACT_GOODS","MI006_APPR_AMT_SIGN","MI006_AR_AMT","MI006_ARR_INT_ACCR","MI006_ARR_INT_ACCR_SIGN","MI006_BORM_ACCEPT_DATE","RTRIM(MI006_BORM_BASE_ID) AS MI006_BORM_BASE_ID","RTRIM(MI006_BORM_BORR_TYPE) AS MI006_BORM_BORR_TYPE","MI006_BORM_CAP_UNPD_INT","RTRIM(MI006_BORM_COLLECTI_STATUS) AS MI006_BORM_COLLECTI_STATUS","MI006_BORM_EFF_RATE","RTRIM(MI006_BORM_INS_IND) AS MI006_BORM_INS_IND","RTRIM(MI006_BORM_REDRAW_IND) AS MI006_BORM_REDRAW_IND","MI006_BORM_RT_INCR_MOV_AD","MI006_BORM_SETTLEMENT_DATE","RTRIM(MI006_BORM_SPL_REP_FLAG) AS MI006_BORM_SPL_REP_FLAG","MI006_BORM_UNPD_CHRG_BAL","MI006_CAP_THEO_UNPD_INT","MI006_CAP_UNPD_INT","MI006_CR_AVG_BAL","RTRIM(MI006_CURRENCY) AS MI006_CURRENCY","MI006_DISB_PERC","MI006_DUE_NOT_PAID","MI006_FINAL_REPAYMENT","MI006_FULL_RCVR_FLAG","MI006_INST_REMAIN","MI006_LAST_DUE_DATE","MI006_LOAN_BAL_SIGN","MI006_LOAN_PURPOSE","RTRIM(MI006_LONP_ACCT_TYPE) AS MI006_LONP_ACCT_TYPE","MI006_MAT_EXTN_FLAG","MI006_NO_DAYS_ARREAR","MI006_NO_OF_MESSAGES","MI006_OVERDUE_INT_BAL","MI006_PENALTY_RATE","MI006_PP_RATE","MI006_THEO_UNPD_CHRG_BAL","MI006_UNDISBURSED_AMT","MI006_UPPD_ARR_INT","MI006_UNPD_PRIN_BAL_SIGN","MI006_UNPD_CHRG_BAL_SIGN","MI006_WRITEOFF_AMOUNT","MI006_WRITTENOFF_DATE","MI006_ACCT_ACTIVE","MI006_NO_OF_OWNER","MI006_CLOSED_IND","MI006_CYCLE_ARREARS_VALUE","MI006_DEFAULT_FLAG","MI006_LOAN_AGE","MI006_MONTHS_ON_BOOKS","RTRIM(MI006_LAST_REPAY_METHOD) AS MI006_LAST_REPAY_METHOD","MI006_SUSPENSE_AMOUNT","MI006_OVERDUE_PRIN_BAL","MI006_AM_INT_MTH_ARR","MI006_BLDVNN_TOTAL_INT","MI006_AM_PARPMT_BAL","MI006_NO_INSTALLMENT","MI006_RATE_MATURITY_DATE","MI006_COLLECT_MAN_CHG_DT","MI006_FULL_DRAWN_DT","MI006_PSSO_NEXT_PAY_DATE","RTRIM(MI006_DEFER_FLAG) AS MI006_DEFER_FLAG","RTRIM(MI006_PRIN_REPAY_FREQ) AS MI006_PRIN_REPAY_FREQ","RTRIM(MI006_PRIN_PAY_UNIT) AS MI006_PRIN_PAY_UNIT","MI006_ORG_TENOR","MI006_REV_TENOR","MI006_GL_CLASS_CODE","MI006_GLPP_COMP1","MI006_GLPP_COMP2","MI006_DEFFERMENT_EXPIRY_DATE","RTRIM(MI006_BORM_APPL_ID) AS MI006_BORM_APPL_ID","RTRIM(MI006_CAPN_FREQ) AS MI006_CAPN_FREQ","MI006_BORM_INT_TAX_YTD","MI006_BOIS_STOPP_IND","MI006_BORM_OVERDUE_UNPD_INT","MI006_ASSESED_FEE","MI006_BORM_CI_AVG_BAL","MI006_BORM_INT_INCR","RTRIM(MI006_BORM_REPAY_METHOD) AS MI006_BORM_REPAY_METHOD","MI006_MAX_INT_RATE","MI006_BORM_HOLD_AMT","MI006_SHORTFALL_IND","MI006_P_WOFF_DATE","MI006_SUB_LEDGER_ACCURAL_AMT","MI006_SUB_LEDGER_MISC_FEES_AMT","RTRIM(MI006_CAS_STAT_CODE) AS MI006_CAS_STAT_CODE","MI006_BOTM_FEES_AMOUNT","RTRIM(MI006_SECURITY_METHOD) AS MI006_SECURITY_METHOD","MI006_BORM_CUSM_VIEW_AMT","MI006_BORM_PREV_OFFICER","RTRIM(MI006_BORM_PAY_DAY) AS MI006_BORM_PAY_DAY","MI006_BORM_RENEWAL_DATE","MI006_BORM_REPAY_COUNT","RTRIM(MI006_BORM_FIRST_PAY_IND) AS MI006_BORM_FIRST_PAY_IND","MI006_BORM_ARR_NOT_FLAG","MI006_BORM_AGREEMENT_NUM","MI006_BORM_RESIDUAL_BAL","MI006_UNCLEARED_AMT","MI006_THEO_UNPD_PRIN_BAL","MI006_BORM_EOM_BASIC_BAL","RTRIM(FLOATDAYS) AS FLOATDAYS").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SOC_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MEMB_CUST_AC', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BR_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_STAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_ACT_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_APPLIC_ISSUE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_APPLIC_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REPAY_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_APP_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADV_VAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADD_LOAN_ADV', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_THEO_LOAN_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_REPAY', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_APPRV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LST_FIN_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LST_MNT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LAST_ARR_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADD_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_LOAN_TRM', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REM_REPAYS', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MORTGAGE_INSU', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_RESIDUAL_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_FIXED_RT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RT_INCR', 'type': 'decimal(8,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CUSTOMER_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(20)'}}, {'name': 'MI006_STMT_FREQUENCY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_TERM_BASIS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_CAFM_START_RTE_DTE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CHR_EXEM_PROFILE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_REPAY_SCHED', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_PURPOSE_CODE_A', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PURPOSE_CODE_B', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REPAY_DAY', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DUE_STRT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SOURCE_CDE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_OFFICER', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FUND_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_FUND_SOURCE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_INT_ACCR', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CR_INT_ACCR', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_HOL_STRT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REP_HOL_MTHS', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LINKED_DEP_ACCOUNT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_CAPN_DAY', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_COLLECTIBILITY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_APPR_INDUSTRY_SECT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_INTEREST', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNPD_PRIN_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OVERDUE_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TOT_PROJ_COST', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNEARNED_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TOT_PRJ_CST_UNERND_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_START_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BANKRUPTCY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_WRITTEN_OFF_INT_AC', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LAST_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REPORT_MASK', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_LOAN_LOC_STAT_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_REPAY_TYPE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PEND_DUES', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FINE_CDE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BAD_DEBT_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_RT_INCR_MOV_AD', 'type': 'decimal(11,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INS_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SPECIAL_REPAY_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ARR_ACCR_AMT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OUT_OF_ARREARS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNPAID_SERV_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STAMPED_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TRF_ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SUSPENDED_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PEND_LOAN_TERM', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRINCIPAL_INST_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DUE_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_SYS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_LST_REPHASE_YR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_SYNDICATE_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BORM_GROUP_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_SWEEP_ACCT_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_GLDM_BRANCH', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_GLDM_CURRENCY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_ACCT_CHK_DGT', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACT_GOODS', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_APPR_AMT_SIGN', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AR_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ARR_INT_ACCR', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ARR_INT_ACCR_SIGN', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_ACCEPT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_BORM_BORR_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_BORM_CAP_UNPD_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_COLLECTI_STATUS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BORM_EFF_RATE', 'type': 'decimal(8,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_INS_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BORM_REDRAW_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BORM_RT_INCR_MOV_AD', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_SETTLEMENT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_SPL_REP_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BORM_UNPD_CHRG_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAP_THEO_UNPD_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAP_UNPD_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CR_AVG_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CURRENCY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_DISB_PERC', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DUE_NOT_PAID', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FINAL_REPAYMENT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FULL_RCVR_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INST_REMAIN', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LAST_DUE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_BAL_SIGN', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_PURPOSE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LONP_ACCT_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_MAT_EXTN_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_DAYS_ARREAR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_OF_MESSAGES', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OVERDUE_INT_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PENALTY_RATE', 'type': 'decimal(7,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PP_RATE', 'type': 'decimal(8,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_THEO_UNPD_CHRG_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNDISBURSED_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UPPD_ARR_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNPD_PRIN_BAL_SIGN', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNPD_CHRG_BAL_SIGN', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITEOFF_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITTENOFF_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACCT_ACTIVE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_OF_OWNER', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CLOSED_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CYCLE_ARREARS_VALUE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DEFAULT_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_AGE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MONTHS_ON_BOOKS', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LAST_REPAY_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SUSPENSE_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OVERDUE_PRIN_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AM_INT_MTH_ARR', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_TOTAL_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AM_PARPMT_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_INSTALLMENT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RATE_MATURITY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_COLLECT_MAN_CHG_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FULL_DRAWN_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PSSO_NEXT_PAY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DEFER_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_PRIN_REPAY_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(10)'}}, {'name': 'MI006_PRIN_PAY_UNIT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(7)'}}, {'name': 'MI006_ORG_TENOR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REV_TENOR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GL_CLASS_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GLPP_COMP1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GLPP_COMP2', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DEFFERMENT_EXPIRY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_APPL_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(20)'}}, {'name': 'MI006_CAPN_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BORM_INT_TAX_YTD', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_STOPP_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_OVERDUE_UNPD_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ASSESED_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_CI_AVG_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_INT_INCR', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_REPAY_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_MAX_INT_RATE', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_HOLD_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SHORTFALL_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WOFF_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SUB_LEDGER_ACCURAL_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SUB_LEDGER_MISC_FEES_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAS_STAT_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOTM_FEES_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SECURITY_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BORM_CUSM_VIEW_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_PREV_OFFICER', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_PAY_DAY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BORM_RENEWAL_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_REPAY_COUNT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_FIRST_PAY_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BORM_ARR_NOT_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_AGREEMENT_NUM', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_RESIDUAL_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNCLEARED_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_THEO_UNPD_PRIN_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_EOM_BASIC_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'FLOATDAYS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__Join_40_lnk_Source_v PURGE").show()
    
    print("Join_40_lnk_Source_v")
    
    print(Join_40_lnk_Source_v.schema.json())
    
    print("count:{}".format(Join_40_lnk_Source_v.count()))
    
    Join_40_lnk_Source_v.show(1000,False)
    
    Join_40_lnk_Source_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__Join_40_lnk_Source_v")
    

@task.pyspark(conn_id="spark-local")
def TRN_CONVERT_lnk_Source_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Join_40_lnk_Source_v=spark.table('datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__Join_40_lnk_Source_v')
    
    TRN_CONVERT_lnk_Source_Part_v=Join_40_lnk_Source_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__TRN_CONVERT_lnk_Source_Part_v PURGE").show()
    
    print("TRN_CONVERT_lnk_Source_Part_v")
    
    print(TRN_CONVERT_lnk_Source_Part_v.schema.json())
    
    print("count:{}".format(TRN_CONVERT_lnk_Source_Part_v.count()))
    
    TRN_CONVERT_lnk_Source_Part_v.show(1000,False)
    
    TRN_CONVERT_lnk_Source_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__TRN_CONVERT_lnk_Source_Part_v")
    

@task.pyspark(conn_id="spark-local")
def TRN_CONVERT(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    TRN_CONVERT_lnk_Source_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__TRN_CONVERT_lnk_Source_Part_v')
    
    TRN_CONVERT_v = TRN_CONVERT_lnk_Source_Part_v
    
    TRN_CONVERT_lnk_BORM_Tgt_v = TRN_CONVERT_v.select(expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM B_KEY))""").cast('string').alias('B_KEY'),col('MI006_SOC_NO').cast('string').alias('MI006_SOC_NO'),col('MI006_MEMB_CUST_AC').cast('string').alias('MI006_MEMB_CUST_AC'),col('MI006_BR_NO').cast('string').alias('MI006_BR_NO'),col('MI006_STAT').cast('string').alias('MI006_STAT'),col('MI006_ACT_TYPE').cast('string').alias('MI006_ACT_TYPE'),col('MI006_APPLIC_ISSUE_DATE').cast('integer').alias('MI006_APPLIC_ISSUE_DATE'),col('MI006_APPLIC_AMOUNT').cast('decimal(18,3)').alias('MI006_APPLIC_AMOUNT'),col('MI006_REPAY_FREQ').cast('string').alias('MI006_REPAY_FREQ'),col('MI006_APP_AMT').cast('decimal(18,3)').alias('MI006_APP_AMT'),col('MI006_LOAN_BAL').cast('decimal(18,3)').alias('MI006_LOAN_BAL'),col('MI006_ADV_VAL').cast('decimal(18,3)').alias('MI006_ADV_VAL'),col('MI006_ADD_LOAN_ADV').cast('decimal(18,3)').alias('MI006_ADD_LOAN_ADV'),col('MI006_THEO_LOAN_BAL').cast('decimal(18,3)').alias('MI006_THEO_LOAN_BAL'),col('MI006_LOAN_REPAY').cast('decimal(18,3)').alias('MI006_LOAN_REPAY'),col('MI006_APPRV_DATE').cast('integer').alias('MI006_APPRV_DATE'),col('MI006_LST_FIN_DATE').cast('integer').alias('MI006_LST_FIN_DATE'),col('MI006_LST_MNT_DATE').cast('integer').alias('MI006_LST_MNT_DATE'),col('MI006_ADV_DATE').cast('integer').alias('MI006_ADV_DATE'),col('MI006_LAST_ARR_DATE').cast('integer').alias('MI006_LAST_ARR_DATE'),col('MI006_ADD_ADV_DATE').cast('integer').alias('MI006_ADD_ADV_DATE'),col('MI006_CAT').cast('string').alias('MI006_CAT'),col('MI006_LOAN_TRM').cast('integer').alias('MI006_LOAN_TRM'),col('MI006_REM_REPAYS').cast('integer').alias('MI006_REM_REPAYS'),col('MI006_MORTGAGE_INSU').cast('string').alias('MI006_MORTGAGE_INSU'),col('MI006_RESIDUAL_IND').cast('string').alias('MI006_RESIDUAL_IND'),col('MI006_FIXED_RT_DATE').cast('integer').alias('MI006_FIXED_RT_DATE'),col('MI006_RT_INCR').cast('decimal(8,5)').alias('MI006_RT_INCR'),expr("""LPAD(TRIM(MI006_CUSTOMER_NO), 17, '0')""").cast('string').alias('MI006_CUSTOMER_NO'),col('MI006_STMT_FREQUENCY').cast('string').alias('MI006_STMT_FREQUENCY'),col('MI006_TERM_BASIS').cast('string').alias('MI006_TERM_BASIS'),col('MI006_CAFM_START_RTE_DTE').cast('integer').alias('MI006_CAFM_START_RTE_DTE'),col('MI006_CHR_EXEM_PROFILE').cast('string').alias('MI006_CHR_EXEM_PROFILE'),col('MI006_REPAY_SCHED').cast('string').alias('MI006_REPAY_SCHED'),expr("""LPAD(TRIM(MI006_PURPOSE_CODE_A), 3, '0')""").cast('string').alias('MI006_PURPOSE_CODE_A'),expr("""RIGHT(CONCAT(LPAD('0', 3, ' '), RTRIM(MI006_PURPOSE_CODE_B)), 3)""").cast('string').alias('MI006_PURPOSE_CODE_B'),col('MI006_REPAY_DAY').cast('integer').alias('MI006_REPAY_DAY'),col('MI006_DUE_STRT_DATE').cast('integer').alias('MI006_DUE_STRT_DATE'),col('MI006_SOURCE_CDE').cast('integer').alias('MI006_SOURCE_CDE'),col('MI006_LOAN_OFFICER').cast('integer').alias('MI006_LOAN_OFFICER'),col('MI006_FUND_TYPE').cast('string').alias('MI006_FUND_TYPE'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM (IF(ISNOTNULL((MI006_FUND_SOURCE)), (MI006_FUND_SOURCE), '')))) = '', NULL, TRIM(TRAILING ' ' FROM TRIM(TRAILING '\t' FROM MI006_FUND_SOURCE)))""").cast('string').alias('MI006_FUND_SOURCE'),col('MI006_INT_ACCR').cast('decimal(18,5)').alias('MI006_INT_ACCR'),col('MI006_CR_INT_ACCR').cast('decimal(18,5)').alias('MI006_CR_INT_ACCR'),col('MI006_HOL_STRT_DT').cast('integer').alias('MI006_HOL_STRT_DT'),col('MI006_REP_HOL_MTHS').cast('integer').alias('MI006_REP_HOL_MTHS'),col('MI006_LINKED_DEP_ACCOUNT').cast('string').alias('MI006_LINKED_DEP_ACCOUNT'),col('MI006_CAPN_DAY').cast('integer').alias('MI006_CAPN_DAY'),col('MI006_COLLECTIBILITY').cast('string').alias('MI006_COLLECTIBILITY'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM (IF(ISNOTNULL((MI006_APPR_INDUSTRY_SECT)), (MI006_APPR_INDUSTRY_SECT), '')))) = '', NULL, TRIM(TRAILING ' ' FROM TRIM(TRAILING '\t' FROM MI006_APPR_INDUSTRY_SECT)))""").cast('string').alias('MI006_APPR_INDUSTRY_SECT'),col('MI006_INTEREST').cast('decimal(18,3)').alias('MI006_INTEREST'),col('MI006_UNPD_PRIN_BAL').cast('decimal(18,3)').alias('MI006_UNPD_PRIN_BAL'),col('MI006_OVERDUE_BAL').cast('decimal(18,3)').alias('MI006_OVERDUE_BAL'),col('MI006_TOT_PROJ_COST').cast('decimal(18,3)').alias('MI006_TOT_PROJ_COST'),col('MI006_UNEARNED_INT').cast('decimal(18,3)').alias('MI006_UNEARNED_INT'),col('MI006_TOT_PRJ_CST_UNERND_INT').cast('decimal(18,3)').alias('MI006_TOT_PRJ_CST_UNERND_INT'),col('MI006_INT_START_DATE').cast('integer').alias('MI006_INT_START_DATE'),col('MI006_BANKRUPTCY').cast('string').alias('MI006_BANKRUPTCY'),col('MI006_WRITTEN_OFF_INT_AC').cast('decimal(18,3)').alias('MI006_WRITTEN_OFF_INT_AC'),col('MI006_LAST_ADV_DATE').cast('integer').alias('MI006_LAST_ADV_DATE'),lit(None).cast('string').alias('MI006_REPORT_MASK'),col('MI006_LOAN_LOC_STAT_CODE').cast('string').alias('MI006_LOAN_LOC_STAT_CODE'),expr("""RIGHT(CONCAT(REPEAT('0', 5), RTRIM(MI006_REPAY_TYPE)), 5)""").cast('string').alias('MI006_REPAY_TYPE'),col('MI006_PEND_DUES').cast('decimal(18,3)').alias('MI006_PEND_DUES'),col('MI006_FINE_CDE').cast('string').alias('MI006_FINE_CDE'),col('MI006_BAD_DEBT_IND').cast('string').alias('MI006_BAD_DEBT_IND'),col('MI006_RT_INCR_MOV_AD').cast('decimal(11,4)').alias('MI006_RT_INCR_MOV_AD'),col('MI006_INS_IND').cast('string').alias('MI006_INS_IND'),col('MI006_SPECIAL_REPAY_FLAG').cast('string').alias('MI006_SPECIAL_REPAY_FLAG'),col('MI006_ARR_ACCR_AMT').cast('integer').alias('MI006_ARR_ACCR_AMT'),col('MI006_OUT_OF_ARREARS_DT').cast('integer').alias('MI006_OUT_OF_ARREARS_DT'),col('MI006_UNPAID_SERV_BAL').cast('decimal(18,3)').alias('MI006_UNPAID_SERV_BAL'),col('MI006_STAMPED_AMT').cast('decimal(18,3)').alias('MI006_STAMPED_AMT'),expr("""LPAD(RTRIM(MI006_TRF_ACCT_NO), 17, '0')""").cast('string').alias('MI006_TRF_ACCT_NO'),col('MI006_SUSPENDED_INT').cast('decimal(18,3)').alias('MI006_SUSPENDED_INT'),col('MI006_DISCH_DATE').cast('integer').alias('MI006_DISCH_DATE'),col('MI006_PEND_LOAN_TERM').cast('integer').alias('MI006_PEND_LOAN_TERM'),col('MI006_PRINCIPAL_INST_AMT').cast('decimal(18,3)').alias('MI006_PRINCIPAL_INST_AMT'),col('MI006_DUE_AMT').cast('decimal(18,3)').alias('MI006_DUE_AMT'),col('MI006_BORM_SYS').cast('string').alias('MI006_BORM_SYS'),expr("""RIGHT(LPAD(RTRIM(MI006_LST_REPHASE_YR), 4, '0'), 4)""").cast('string').alias('MI006_LST_REPHASE_YR'),col('MI006_BORM_SYNDICATE_TYPE').cast('string').alias('MI006_BORM_SYNDICATE_TYPE'),col('MI006_BORM_GROUP_CODE').cast('string').alias('MI006_BORM_GROUP_CODE'),col('MI006_SWEEP_ACCT_FLAG').cast('string').alias('MI006_SWEEP_ACCT_FLAG'),col('MI006_GLDM_BRANCH').cast('string').alias('MI006_GLDM_BRANCH'),col('MI006_GLDM_CURRENCY').cast('string').alias('MI006_GLDM_CURRENCY'),col('MI006_ACCT_CHK_DGT').cast('string').alias('MI006_ACCT_CHK_DGT'),col('MI006_ACT_GOODS').cast('string').alias('MI006_ACT_GOODS'),col('MI006_APPR_AMT_SIGN').cast('string').alias('MI006_APPR_AMT_SIGN'),col('MI006_AR_AMT').cast('decimal(18,3)').alias('MI006_AR_AMT'),col('MI006_ARR_INT_ACCR').cast('decimal(18,3)').alias('MI006_ARR_INT_ACCR'),col('MI006_ARR_INT_ACCR_SIGN').cast('string').alias('MI006_ARR_INT_ACCR_SIGN'),col('MI006_BORM_ACCEPT_DATE').cast('integer').alias('MI006_BORM_ACCEPT_DATE'),col('MI006_BORM_BASE_ID').cast('string').alias('MI006_BORM_BASE_ID'),col('MI006_BORM_BORR_TYPE').cast('string').alias('MI006_BORM_BORR_TYPE'),col('MI006_BORM_CAP_UNPD_INT').cast('decimal(18,3)').alias('MI006_BORM_CAP_UNPD_INT'),col('MI006_BORM_COLLECTI_STATUS').cast('string').alias('MI006_BORM_COLLECTI_STATUS'),col('MI006_BORM_EFF_RATE').cast('decimal(8,5)').alias('MI006_BORM_EFF_RATE'),col('MI006_BORM_INS_IND').cast('string').alias('MI006_BORM_INS_IND'),col('MI006_BORM_REDRAW_IND').cast('string').alias('MI006_BORM_REDRAW_IND'),col('MI006_BORM_RT_INCR_MOV_AD').cast('decimal(8,4)').alias('MI006_BORM_RT_INCR_MOV_AD'),col('MI006_BORM_SETTLEMENT_DATE').cast('integer').alias('MI006_BORM_SETTLEMENT_DATE'),col('MI006_BORM_SPL_REP_FLAG').cast('string').alias('MI006_BORM_SPL_REP_FLAG'),col('MI006_BORM_UNPD_CHRG_BAL').cast('decimal(18,3)').alias('MI006_BORM_UNPD_CHRG_BAL'),col('MI006_CAP_THEO_UNPD_INT').cast('decimal(18,3)').alias('MI006_CAP_THEO_UNPD_INT'),col('MI006_CAP_UNPD_INT').cast('decimal(18,3)').alias('MI006_CAP_UNPD_INT'),col('MI006_CR_AVG_BAL').cast('decimal(18,3)').alias('MI006_CR_AVG_BAL'),col('MI006_CURRENCY').cast('string').alias('MI006_CURRENCY'),col('MI006_DISB_PERC').cast('decimal(8,4)').alias('MI006_DISB_PERC'),col('MI006_DUE_NOT_PAID').cast('decimal(18,3)').alias('MI006_DUE_NOT_PAID'),col('MI006_FINAL_REPAYMENT').cast('decimal(18,3)').alias('MI006_FINAL_REPAYMENT'),col('MI006_FULL_RCVR_FLAG').cast('string').alias('MI006_FULL_RCVR_FLAG'),col('MI006_INST_REMAIN').cast('decimal(18,3)').alias('MI006_INST_REMAIN'),col('MI006_LAST_DUE_DATE').cast('integer').alias('MI006_LAST_DUE_DATE'),col('MI006_LOAN_BAL_SIGN').cast('string').alias('MI006_LOAN_BAL_SIGN'),col('MI006_LOAN_PURPOSE').cast('string').alias('MI006_LOAN_PURPOSE'),col('MI006_LONP_ACCT_TYPE').cast('string').alias('MI006_LONP_ACCT_TYPE'),col('MI006_MAT_EXTN_FLAG').cast('string').alias('MI006_MAT_EXTN_FLAG'),col('MI006_NO_DAYS_ARREAR').cast('string').alias('MI006_NO_DAYS_ARREAR'),col('MI006_NO_OF_MESSAGES').cast('string').alias('MI006_NO_OF_MESSAGES'),col('MI006_OVERDUE_INT_BAL').cast('decimal(18,3)').alias('MI006_OVERDUE_INT_BAL'),col('MI006_PP_RATE').cast('decimal(8,5)').alias('MI006_PP_RATE'),col('MI006_THEO_UNPD_CHRG_BAL').cast('decimal(18,3)').alias('MI006_THEO_UNPD_CHRG_BAL'),col('MI006_UNDISBURSED_AMT').cast('decimal(18,3)').alias('MI006_UNDISBURSED_AMT'),col('MI006_UPPD_ARR_INT').cast('decimal(18,3)').alias('MI006_UPPD_ARR_INT'),col('MI006_UNPD_PRIN_BAL_SIGN').cast('string').alias('MI006_UNPD_PRIN_BAL_SIGN'),col('MI006_UNPD_CHRG_BAL_SIGN').cast('string').alias('MI006_UNPD_CHRG_BAL_SIGN'),col('MI006_WRITTENOFF_DATE').cast('integer').alias('MI006_WRITTENOFF_DATE'),col('MI006_ACCT_ACTIVE').cast('string').alias('MI006_ACCT_ACTIVE'),col('MI006_NO_OF_OWNER').cast('string').alias('MI006_NO_OF_OWNER'),col('MI006_CLOSED_IND').cast('string').alias('MI006_CLOSED_IND'),col('MI006_CYCLE_ARREARS_VALUE').cast('decimal(18,3)').alias('MI006_CYCLE_ARREARS_VALUE'),col('MI006_DEFAULT_FLAG').cast('string').alias('MI006_DEFAULT_FLAG'),col('MI006_LOAN_AGE').cast('string').alias('MI006_LOAN_AGE'),col('MI006_MONTHS_ON_BOOKS').cast('string').alias('MI006_MONTHS_ON_BOOKS'),col('MI006_LAST_REPAY_METHOD').cast('string').alias('MI006_LAST_REPAY_METHOD'),col('MI006_SUSPENSE_AMOUNT').cast('decimal(18,3)').alias('MI006_SUSPENSE_AMOUNT'),col('MI006_OVERDUE_PRIN_BAL').cast('decimal(18,3)').alias('MI006_OVERDUE_PRIN_BAL'),col('MI006_AM_INT_MTH_ARR').cast('decimal(18,3)').alias('MI006_AM_INT_MTH_ARR'),col('MI006_BLDVNN_TOTAL_INT').cast('decimal(18,3)').alias('MI006_BLDVNN_TOTAL_INT'),col('MI006_AM_PARPMT_BAL').cast('decimal(18,3)').alias('MI006_AM_PARPMT_BAL'),col('MI006_NO_INSTALLMENT').cast('integer').alias('MI006_NO_INSTALLMENT'),col('MI006_RATE_MATURITY_DATE').cast('integer').alias('MI006_RATE_MATURITY_DATE'),col('MI006_COLLECT_MAN_CHG_DT').cast('integer').alias('MI006_COLLECT_MAN_CHG_DT'),col('MI006_FULL_DRAWN_DT').cast('integer').alias('MI006_FULL_DRAWN_DT'),col('MI006_DEFER_FLAG').cast('string').alias('MI006_DEFER_FLAG'),expr("""RPAD(RTRIM(MI006_PRIN_REPAY_FREQ), 6, '0')""").cast('string').alias('MI006_PRIN_REPAY_FREQ'),col('MI006_PRIN_PAY_UNIT').cast('string').alias('MI006_PRIN_PAY_UNIT'),expr("""LPAD(RTRIM(MI006_ORG_TENOR), 5, '0')""").cast('string').alias('MI006_ORG_TENOR'),expr("""LPAD(RTRIM(MI006_REV_TENOR), 5, '0')""").cast('string').alias('MI006_REV_TENOR'),col('MI006_GL_CLASS_CODE').cast('string').alias('MI006_GL_CLASS_CODE'),col('MI006_GLPP_COMP1').cast('string').alias('MI006_GLPP_COMP1'),col('MI006_GLPP_COMP2').cast('string').alias('MI006_GLPP_COMP2'),col('MI006_DEFFERMENT_EXPIRY_DATE').cast('integer').alias('MI006_DEFFERMENT_EXPIRY_DATE'),col('MI006_BORM_APPL_ID').cast('string').alias('MI006_BORM_APPL_ID'),col('MI006_BORM_INT_TAX_YTD').cast('decimal(18,3)').alias('MI006_BORM_INT_TAX_YTD'),col('MI006_BOIS_STOPP_IND').cast('string').alias('MI006_BOIS_STOPP_IND'),col('MI006_BORM_OVERDUE_UNPD_INT').cast('decimal(18,3)').alias('MI006_BORM_OVERDUE_UNPD_INT'),col('MI006_ASSESED_FEE').cast('decimal(18,3)').alias('MI006_ASSESED_FEE'),col('MI006_BORM_CI_AVG_BAL').cast('decimal(18,3)').alias('MI006_BORM_CI_AVG_BAL'),col('MI006_BORM_INT_INCR').cast('decimal(18,5)').alias('MI006_BORM_INT_INCR'),col('MI006_MAX_INT_RATE').cast('decimal(8,4)').alias('MI006_MAX_INT_RATE'),col('MI006_BORM_HOLD_AMT').cast('decimal(18,3)').alias('MI006_BORM_HOLD_AMT'),col('MI006_SHORTFALL_IND').cast('string').alias('MI006_SHORTFALL_IND'),col('MI006_P_WOFF_DATE').cast('integer').alias('MI006_P_WOFF_DATE'),col('MI006_SUB_LEDGER_ACCURAL_AMT').cast('decimal(18,3)').alias('MI006_SUB_LEDGER_ACCURAL_AMT'),col('MI006_SUB_LEDGER_MISC_FEES_AMT').cast('decimal(18,3)').alias('MI006_SUB_LEDGER_MISC_FEES_AMT'),col('MI006_CAS_STAT_CODE').cast('string').alias('MI006_CAS_STAT_CODE'),col('MI006_BOTM_FEES_AMOUNT').cast('decimal(18,3)').alias('MI006_BOTM_FEES_AMOUNT'),col('MI006_SECURITY_METHOD').cast('string').alias('MI006_SECURITY_METHOD'),lit(0.000).cast('decimal(18,3)').alias('MI006_BORM_CUSM_VIEW_AMT'),col('MI006_BORM_PREV_OFFICER').cast('integer').alias('MI006_BORM_PREV_OFFICER'),col('MI006_BORM_PAY_DAY').cast('string').alias('MI006_BORM_PAY_DAY'),col('MI006_BORM_RENEWAL_DATE').cast('integer').alias('MI006_BORM_RENEWAL_DATE'),col('MI006_BORM_REPAY_COUNT').cast('integer').alias('MI006_BORM_REPAY_COUNT'),col('MI006_BORM_FIRST_PAY_IND').cast('string').alias('MI006_BORM_FIRST_PAY_IND'),expr("""RIGHT(CONCAT(LPAD('0', 3, '0'), TRIM(BOTH ' ' FROM MI006_BORM_ARR_NOT_FLAG)), 3) AS result_column""").cast('string').alias('MI006_BORM_ARR_NOT_FLAG'),col('MI006_BORM_AGREEMENT_NUM').cast('string').alias('MI006_BORM_AGREEMENT_NUM'),col('MI006_BORM_RESIDUAL_BAL').cast('decimal(18,3)').alias('MI006_BORM_RESIDUAL_BAL'),col('MI006_UNCLEARED_AMT').cast('decimal(18,3)').alias('MI006_UNCLEARED_AMT'),col('MI006_THEO_UNPD_PRIN_BAL').cast('decimal(18,3)').alias('MI006_THEO_UNPD_PRIN_BAL'),col('MI006_BORM_EOM_BASIC_BAL').cast('decimal(18,3)').alias('MI006_BORM_EOM_BASIC_BAL'),col('FLOATDAYS').cast('string').alias('MI007_NO_OF_FLOAT_DAYS'))
    
    TRN_CONVERT_lnk_BORM_Tgt_v = TRN_CONVERT_lnk_BORM_Tgt_v.selectExpr("B_KEY","MI006_SOC_NO","MI006_MEMB_CUST_AC","RTRIM(MI006_BR_NO) AS MI006_BR_NO","RTRIM(MI006_STAT) AS MI006_STAT","RTRIM(MI006_ACT_TYPE) AS MI006_ACT_TYPE","MI006_APPLIC_ISSUE_DATE","MI006_APPLIC_AMOUNT","RTRIM(MI006_REPAY_FREQ) AS MI006_REPAY_FREQ","MI006_APP_AMT","MI006_LOAN_BAL","MI006_ADV_VAL","MI006_ADD_LOAN_ADV","MI006_THEO_LOAN_BAL","MI006_LOAN_REPAY","MI006_APPRV_DATE","MI006_LST_FIN_DATE","MI006_LST_MNT_DATE","MI006_ADV_DATE","MI006_LAST_ARR_DATE","MI006_ADD_ADV_DATE","RTRIM(MI006_CAT) AS MI006_CAT","MI006_LOAN_TRM","MI006_REM_REPAYS","RTRIM(MI006_MORTGAGE_INSU) AS MI006_MORTGAGE_INSU","RTRIM(MI006_RESIDUAL_IND) AS MI006_RESIDUAL_IND","MI006_FIXED_RT_DATE","MI006_RT_INCR","RTRIM(MI006_CUSTOMER_NO) AS MI006_CUSTOMER_NO","RTRIM(MI006_STMT_FREQUENCY) AS MI006_STMT_FREQUENCY","RTRIM(MI006_TERM_BASIS) AS MI006_TERM_BASIS","MI006_CAFM_START_RTE_DTE","RTRIM(MI006_CHR_EXEM_PROFILE) AS MI006_CHR_EXEM_PROFILE","RTRIM(MI006_REPAY_SCHED) AS MI006_REPAY_SCHED","MI006_PURPOSE_CODE_A","MI006_PURPOSE_CODE_B","MI006_REPAY_DAY","MI006_DUE_STRT_DATE","MI006_SOURCE_CDE","MI006_LOAN_OFFICER","RTRIM(MI006_FUND_TYPE) AS MI006_FUND_TYPE","RTRIM(MI006_FUND_SOURCE) AS MI006_FUND_SOURCE","MI006_INT_ACCR","MI006_CR_INT_ACCR","MI006_HOL_STRT_DT","MI006_REP_HOL_MTHS","RTRIM(MI006_LINKED_DEP_ACCOUNT) AS MI006_LINKED_DEP_ACCOUNT","MI006_CAPN_DAY","RTRIM(MI006_COLLECTIBILITY) AS MI006_COLLECTIBILITY","RTRIM(MI006_APPR_INDUSTRY_SECT) AS MI006_APPR_INDUSTRY_SECT","MI006_INTEREST","MI006_UNPD_PRIN_BAL","MI006_OVERDUE_BAL","MI006_TOT_PROJ_COST","MI006_UNEARNED_INT","MI006_TOT_PRJ_CST_UNERND_INT","MI006_INT_START_DATE","RTRIM(MI006_BANKRUPTCY) AS MI006_BANKRUPTCY","MI006_WRITTEN_OFF_INT_AC","MI006_LAST_ADV_DATE","RTRIM(MI006_REPORT_MASK) AS MI006_REPORT_MASK","RTRIM(MI006_LOAN_LOC_STAT_CODE) AS MI006_LOAN_LOC_STAT_CODE","MI006_REPAY_TYPE","MI006_PEND_DUES","RTRIM(MI006_FINE_CDE) AS MI006_FINE_CDE","RTRIM(MI006_BAD_DEBT_IND) AS MI006_BAD_DEBT_IND","MI006_RT_INCR_MOV_AD","RTRIM(MI006_INS_IND) AS MI006_INS_IND","RTRIM(MI006_SPECIAL_REPAY_FLAG) AS MI006_SPECIAL_REPAY_FLAG","MI006_ARR_ACCR_AMT","MI006_OUT_OF_ARREARS_DT","MI006_UNPAID_SERV_BAL","MI006_STAMPED_AMT","MI006_TRF_ACCT_NO","MI006_SUSPENDED_INT","MI006_DISCH_DATE","MI006_PEND_LOAN_TERM","MI006_PRINCIPAL_INST_AMT","MI006_DUE_AMT","RTRIM(MI006_BORM_SYS) AS MI006_BORM_SYS","MI006_LST_REPHASE_YR","RTRIM(MI006_BORM_SYNDICATE_TYPE) AS MI006_BORM_SYNDICATE_TYPE","RTRIM(MI006_BORM_GROUP_CODE) AS MI006_BORM_GROUP_CODE","RTRIM(MI006_SWEEP_ACCT_FLAG) AS MI006_SWEEP_ACCT_FLAG","RTRIM(MI006_GLDM_BRANCH) AS MI006_GLDM_BRANCH","RTRIM(MI006_GLDM_CURRENCY) AS MI006_GLDM_CURRENCY","MI006_ACCT_CHK_DGT","MI006_ACT_GOODS","MI006_APPR_AMT_SIGN","MI006_AR_AMT","MI006_ARR_INT_ACCR","MI006_ARR_INT_ACCR_SIGN","MI006_BORM_ACCEPT_DATE","RTRIM(MI006_BORM_BASE_ID) AS MI006_BORM_BASE_ID","RTRIM(MI006_BORM_BORR_TYPE) AS MI006_BORM_BORR_TYPE","MI006_BORM_CAP_UNPD_INT","RTRIM(MI006_BORM_COLLECTI_STATUS) AS MI006_BORM_COLLECTI_STATUS","MI006_BORM_EFF_RATE","RTRIM(MI006_BORM_INS_IND) AS MI006_BORM_INS_IND","RTRIM(MI006_BORM_REDRAW_IND) AS MI006_BORM_REDRAW_IND","MI006_BORM_RT_INCR_MOV_AD","MI006_BORM_SETTLEMENT_DATE","RTRIM(MI006_BORM_SPL_REP_FLAG) AS MI006_BORM_SPL_REP_FLAG","MI006_BORM_UNPD_CHRG_BAL","MI006_CAP_THEO_UNPD_INT","MI006_CAP_UNPD_INT","MI006_CR_AVG_BAL","RTRIM(MI006_CURRENCY) AS MI006_CURRENCY","MI006_DISB_PERC","MI006_DUE_NOT_PAID","MI006_FINAL_REPAYMENT","MI006_FULL_RCVR_FLAG","MI006_INST_REMAIN","MI006_LAST_DUE_DATE","MI006_LOAN_BAL_SIGN","MI006_LOAN_PURPOSE","RTRIM(MI006_LONP_ACCT_TYPE) AS MI006_LONP_ACCT_TYPE","MI006_MAT_EXTN_FLAG","MI006_NO_DAYS_ARREAR","MI006_NO_OF_MESSAGES","MI006_OVERDUE_INT_BAL","MI006_PP_RATE","MI006_THEO_UNPD_CHRG_BAL","MI006_UNDISBURSED_AMT","MI006_UPPD_ARR_INT","MI006_UNPD_PRIN_BAL_SIGN","MI006_UNPD_CHRG_BAL_SIGN","MI006_WRITTENOFF_DATE","MI006_ACCT_ACTIVE","MI006_NO_OF_OWNER","MI006_CLOSED_IND","MI006_CYCLE_ARREARS_VALUE","MI006_DEFAULT_FLAG","MI006_LOAN_AGE","MI006_MONTHS_ON_BOOKS","RTRIM(MI006_LAST_REPAY_METHOD) AS MI006_LAST_REPAY_METHOD","MI006_SUSPENSE_AMOUNT","MI006_OVERDUE_PRIN_BAL","MI006_AM_INT_MTH_ARR","MI006_BLDVNN_TOTAL_INT","MI006_AM_PARPMT_BAL","MI006_NO_INSTALLMENT","MI006_RATE_MATURITY_DATE","MI006_COLLECT_MAN_CHG_DT","MI006_FULL_DRAWN_DT","RTRIM(MI006_DEFER_FLAG) AS MI006_DEFER_FLAG","RTRIM(MI006_PRIN_REPAY_FREQ) AS MI006_PRIN_REPAY_FREQ","RTRIM(MI006_PRIN_PAY_UNIT) AS MI006_PRIN_PAY_UNIT","MI006_ORG_TENOR","MI006_REV_TENOR","MI006_GL_CLASS_CODE","MI006_GLPP_COMP1","MI006_GLPP_COMP2","MI006_DEFFERMENT_EXPIRY_DATE","RTRIM(MI006_BORM_APPL_ID) AS MI006_BORM_APPL_ID","MI006_BORM_INT_TAX_YTD","MI006_BOIS_STOPP_IND","MI006_BORM_OVERDUE_UNPD_INT","MI006_ASSESED_FEE","MI006_BORM_CI_AVG_BAL","MI006_BORM_INT_INCR","MI006_MAX_INT_RATE","MI006_BORM_HOLD_AMT","MI006_SHORTFALL_IND","MI006_P_WOFF_DATE","MI006_SUB_LEDGER_ACCURAL_AMT","MI006_SUB_LEDGER_MISC_FEES_AMT","RTRIM(MI006_CAS_STAT_CODE) AS MI006_CAS_STAT_CODE","MI006_BOTM_FEES_AMOUNT","RTRIM(MI006_SECURITY_METHOD) AS MI006_SECURITY_METHOD","MI006_BORM_CUSM_VIEW_AMT","MI006_BORM_PREV_OFFICER","RTRIM(MI006_BORM_PAY_DAY) AS MI006_BORM_PAY_DAY","MI006_BORM_RENEWAL_DATE","MI006_BORM_REPAY_COUNT","RTRIM(MI006_BORM_FIRST_PAY_IND) AS MI006_BORM_FIRST_PAY_IND","MI006_BORM_ARR_NOT_FLAG","MI006_BORM_AGREEMENT_NUM","MI006_BORM_RESIDUAL_BAL","MI006_UNCLEARED_AMT","MI006_THEO_UNPD_PRIN_BAL","MI006_BORM_EOM_BASIC_BAL","RTRIM(MI007_NO_OF_FLOAT_DAYS) AS MI007_NO_OF_FLOAT_DAYS").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SOC_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MEMB_CUST_AC', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BR_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_STAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_ACT_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_APPLIC_ISSUE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_APPLIC_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REPAY_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_APP_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADV_VAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADD_LOAN_ADV', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_THEO_LOAN_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_REPAY', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_APPRV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LST_FIN_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LST_MNT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LAST_ARR_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADD_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_LOAN_TRM', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REM_REPAYS', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MORTGAGE_INSU', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_RESIDUAL_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_FIXED_RT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RT_INCR', 'type': 'decimal(8,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CUSTOMER_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(20)'}}, {'name': 'MI006_STMT_FREQUENCY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_TERM_BASIS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_CAFM_START_RTE_DTE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CHR_EXEM_PROFILE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_REPAY_SCHED', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_PURPOSE_CODE_A', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PURPOSE_CODE_B', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REPAY_DAY', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DUE_STRT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SOURCE_CDE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_OFFICER', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FUND_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_FUND_SOURCE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_INT_ACCR', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CR_INT_ACCR', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_HOL_STRT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REP_HOL_MTHS', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LINKED_DEP_ACCOUNT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_CAPN_DAY', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_COLLECTIBILITY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_APPR_INDUSTRY_SECT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_INTEREST', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNPD_PRIN_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OVERDUE_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TOT_PROJ_COST', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNEARNED_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TOT_PRJ_CST_UNERND_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INT_START_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BANKRUPTCY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_WRITTEN_OFF_INT_AC', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LAST_ADV_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REPORT_MASK', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_LOAN_LOC_STAT_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_REPAY_TYPE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PEND_DUES', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FINE_CDE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BAD_DEBT_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_RT_INCR_MOV_AD', 'type': 'decimal(11,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INS_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SPECIAL_REPAY_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ARR_ACCR_AMT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OUT_OF_ARREARS_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNPAID_SERV_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STAMPED_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TRF_ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SUSPENDED_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DISCH_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PEND_LOAN_TERM', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRINCIPAL_INST_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DUE_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_SYS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_LST_REPHASE_YR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_SYNDICATE_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BORM_GROUP_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_SWEEP_ACCT_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_GLDM_BRANCH', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_GLDM_CURRENCY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_ACCT_CHK_DGT', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACT_GOODS', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_APPR_AMT_SIGN', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AR_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ARR_INT_ACCR', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ARR_INT_ACCR_SIGN', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_ACCEPT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_BASE_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_BORM_BORR_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_BORM_CAP_UNPD_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_COLLECTI_STATUS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BORM_EFF_RATE', 'type': 'decimal(8,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_INS_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BORM_REDRAW_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BORM_RT_INCR_MOV_AD', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_SETTLEMENT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_SPL_REP_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BORM_UNPD_CHRG_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAP_THEO_UNPD_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAP_UNPD_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CR_AVG_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CURRENCY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_DISB_PERC', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DUE_NOT_PAID', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FINAL_REPAYMENT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FULL_RCVR_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INST_REMAIN', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LAST_DUE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_BAL_SIGN', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_PURPOSE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LONP_ACCT_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_MAT_EXTN_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_DAYS_ARREAR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_OF_MESSAGES', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OVERDUE_INT_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PP_RATE', 'type': 'decimal(8,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_THEO_UNPD_CHRG_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNDISBURSED_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UPPD_ARR_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNPD_PRIN_BAL_SIGN', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNPD_CHRG_BAL_SIGN', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WRITTENOFF_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACCT_ACTIVE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_OF_OWNER', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CLOSED_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CYCLE_ARREARS_VALUE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DEFAULT_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_AGE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MONTHS_ON_BOOKS', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LAST_REPAY_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_SUSPENSE_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OVERDUE_PRIN_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AM_INT_MTH_ARR', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVNN_TOTAL_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AM_PARPMT_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_INSTALLMENT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RATE_MATURITY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_COLLECT_MAN_CHG_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FULL_DRAWN_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DEFER_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_PRIN_REPAY_FREQ', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(10)'}}, {'name': 'MI006_PRIN_PAY_UNIT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(7)'}}, {'name': 'MI006_ORG_TENOR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REV_TENOR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GL_CLASS_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GLPP_COMP1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GLPP_COMP2', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DEFFERMENT_EXPIRY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_APPL_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(20)'}}, {'name': 'MI006_BORM_INT_TAX_YTD', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_STOPP_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_OVERDUE_UNPD_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ASSESED_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_CI_AVG_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_INT_INCR', 'type': 'decimal(18,5)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MAX_INT_RATE', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_HOLD_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SHORTFALL_IND', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_P_WOFF_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SUB_LEDGER_ACCURAL_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SUB_LEDGER_MISC_FEES_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CAS_STAT_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOTM_FEES_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SECURITY_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BORM_CUSM_VIEW_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_PREV_OFFICER', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_PAY_DAY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BORM_RENEWAL_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_REPAY_COUNT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_FIRST_PAY_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BORM_ARR_NOT_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_AGREEMENT_NUM', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_RESIDUAL_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_UNCLEARED_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_THEO_UNPD_PRIN_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_EOM_BASIC_BAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI007_NO_OF_FLOAT_DAYS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}]}))
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__TRN_CONVERT_lnk_BORM_Tgt_v PURGE").show()
    
    print("TRN_CONVERT_lnk_BORM_Tgt_v")
    
    print(TRN_CONVERT_lnk_BORM_Tgt_v.schema.json())
    
    print("count:{}".format(TRN_CONVERT_lnk_BORM_Tgt_v.count()))
    
    TRN_CONVERT_lnk_BORM_Tgt_v.show(1000,False)
    
    TRN_CONVERT_lnk_BORM_Tgt_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__TRN_CONVERT_lnk_BORM_Tgt_v")
    

@task.pyspark(conn_id="spark-local")
def DS_TGT_BORM_lnk_BORM_Tgt_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    TRN_CONVERT_lnk_BORM_Tgt_v=spark.table('datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__TRN_CONVERT_lnk_BORM_Tgt_v')
    
    DS_TGT_BORM_lnk_BORM_Tgt_Part_v=TRN_CONVERT_lnk_BORM_Tgt_v
    
    spark.sql("DROP TABLE IF EXISTS datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__DS_TGT_BORM_lnk_BORM_Tgt_Part_v PURGE").show()
    
    print("DS_TGT_BORM_lnk_BORM_Tgt_Part_v")
    
    print(DS_TGT_BORM_lnk_BORM_Tgt_Part_v.schema.json())
    
    print("count:{}".format(DS_TGT_BORM_lnk_BORM_Tgt_Part_v.count()))
    
    DS_TGT_BORM_lnk_BORM_Tgt_Part_v.show(1000,False)
    
    DS_TGT_BORM_lnk_BORM_Tgt_Part_v.write.mode("overwrite").saveAsTable("datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__DS_TGT_BORM_lnk_BORM_Tgt_Part_v")
    

@task.pyspark(conn_id="spark-local")
def DS_TGT_BORM(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    DS_TGT_BORM_lnk_BORM_Tgt_Part_v=spark.table('datastage_temp_job_DBdirect_MIS006_BORM_Extr_POC__DS_TGT_BORM_lnk_BORM_Tgt_Part_v')
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    locations = Template('{{dbdir.pPROCESSING_DIR}}MIS006_BORM.ds').render(job_params)
    
    log.info("write dataset files to "+locations)
    
    DS_TGT_BORM_lnk_BORM_Tgt_Part_v.write.mode("overwrite").format("parquet").save(locations)
    

####################################[Main]###################################
import airflow
with DAG(
    dag_id="job_DBdirect_MIS006_BORM_Extr_POC",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None,
    tags=['datastage'],
) as dag:
    
    job_DBdirect_MIS006_BORM_Extr_POC_task = job_DBdirect_MIS006_BORM_Extr_POC()
    
    Job_VIEW_task = Job_VIEW()
    
    V0A13_task = V0A13()
    
    netz_ODS_DTEP_INSP_task = netz_ODS_DTEP_INSP()
    
    NETZ_SRC_BORM_task = NETZ_SRC_BORM()
    
    Transformer_46_joi_DTEP_INSP_Part_task = Transformer_46_joi_DTEP_INSP_Part()
    
    Join_40_Left_Part_task = Join_40_Left_Part()
    
    Transformer_46_task = Transformer_46()
    
    Join_40_DSLink48_Part_task = Join_40_DSLink48_Part()
    
    Join_40_task = Join_40()
    
    TRN_CONVERT_lnk_Source_Part_task = TRN_CONVERT_lnk_Source_Part()
    
    TRN_CONVERT_task = TRN_CONVERT()
    
    DS_TGT_BORM_lnk_BORM_Tgt_Part_task = DS_TGT_BORM_lnk_BORM_Tgt_Part()
    
    DS_TGT_BORM_task = DS_TGT_BORM()
    
    
    job_DBdirect_MIS006_BORM_Extr_POC_task >> Job_VIEW_task
    
    Job_VIEW_task >> V0A13_task
    
    Job_VIEW_task >> netz_ODS_DTEP_INSP_task
    
    Job_VIEW_task >> NETZ_SRC_BORM_task
    
    netz_ODS_DTEP_INSP_task >> Transformer_46_joi_DTEP_INSP_Part_task
    
    NETZ_SRC_BORM_task >> Join_40_Left_Part_task
    
    Transformer_46_joi_DTEP_INSP_Part_task >> Transformer_46_task
    
    Join_40_Left_Part_task >> Join_40_task
    
    Transformer_46_task >> Join_40_DSLink48_Part_task
    
    Join_40_DSLink48_Part_task >> Join_40_task
    
    Join_40_task >> TRN_CONVERT_lnk_Source_Part_task
    
    TRN_CONVERT_lnk_Source_Part_task >> TRN_CONVERT_task
    
    TRN_CONVERT_task >> DS_TGT_BORM_lnk_BORM_Tgt_Part_task
    
    DS_TGT_BORM_lnk_BORM_Tgt_Part_task >> DS_TGT_BORM_task
    



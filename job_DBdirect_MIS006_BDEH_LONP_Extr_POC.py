
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-02 17:38:52
# @Author  : cloudera
# @File    : job_DBdirect_MIS006_BDEH_LONP_Extr_POC.py
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
def job_DBdirect_MIS006_BDEH_LONP_Extr_POC(**kw_args) -> str:
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
    
    
    
    
    
    sql=Template("""SELECT * FROM (
    
      SELECT 
    
        BORM.KEY_1,
    
        COALESCE(BDEH.PROJECTED_FEE, 0) AS PROJECTED_FEE,  -- new field Bancs Mapping v7.4.2[prod acr]
    
        BOIS.SHIFT_DUE_DT,  -- new field Bancs Mapping v7.4.2[prod acr]
    
        BOIS.WORKOUT_DT,  -- Additional field in MIS006 as per IA AHCR1461
    
        -- start of select block 1
    
        BOIS.TPT_CLAIM_RECEIVED,
    
        BOIS.TPT_CLAIM_REFUND,
    
        BOIS.CCRIS_FCLTY_TYP,
    
        BOIS.MORATORIUM_PERIOD,
    
        BOIS.LAST_PAYMENT_DT,
    
        BOIS.ISLAMIC_CONCEPT,
    
        BOIS.GPP_EARNED,
    
        BOIS.ACC_CASH_REBATE,
    
        CAST(BOIS.ORIG_REPAY_TERM AS INT) AS ORIG_REPAY_TERM,
    
        BOIS.LTTR_OFFR_DT,
    
        BOIS.BD_AGREE_DATE,
    
        BOIS.HIRERS_NAME,
    
        BOIS.HIRER_ID_TYPE,
    
        BOIS.HIRER_ID_NO,
    
        BOIS.WDV_UNEARNED_INT,
    
        BOIS.PROMPT_PAY_REB_PER,
    
        BOIS.ISL_FUNDING_TYPE,
    
        BOIS.PAYMENT_CNTR,
    
        BOIS.UPFR_CAP_BASIS,
    
        BOIS.CAP_RATE,
    
        BOIS.ACCT_SOLD_TO,  -- existing field in PROd but in AMHP2 change in derivation
    
        BORM.NISBAH_RATE,
    
        -- avoiding divide by zero exception --
    
        CASE 
    
          WHEN LONP.MUSYARAKAH_FLAG = 'Y' 
    
               AND (BOIS.PURCHASE_PRICE + BOIS.FREIGHT_CHARGES + BOIS.REGN_CHARGES + 
    
                    BOIS.OTHER_CHARGES + BOIS.INS_PRMUM + BOIS.STMP_DUTY) != 0 
    
          THEN 
    
            CASE 
    
              WHEN (((BORM.PRINC_REDUCT_EFF + BORM.ADV_VAL - BORM.UNPD_PRIN_BAL) / 
    
                     (BOIS.PURCHASE_PRICE + BOIS.FREIGHT_CHARGES + BOIS.REGN_CHARGES + 
    
                      BOIS.OTHER_CHARGES + BOIS.INS_PRMUM + BOIS.STMP_DUTY)) * 100) > 0
    
              THEN ((BORM.PRINC_REDUCT_EFF + BORM.ADV_VAL - BORM.UNPD_PRIN_BAL) / 
    
                    (BOIS.PURCHASE_PRICE + BOIS.FREIGHT_CHARGES + BOIS.REGN_CHARGES + 
    
                     BOIS.OTHER_CHARGES + BOIS.INS_PRMUM + BOIS.STMP_DUTY)) * 100
    
              ELSE 0
    
            END
    
          ELSE 0 
    
        END AS MI006_CUSTOMER_CCR,
    
        CASE 
    
          WHEN LONP.MUSYARAKAH_FLAG = 'Y' 
    
          THEN 100 - MI006_CUSTOMER_CCR 
    
          ELSE 0 
    
        END AS MI006_BANK_CCR,
    
        -- end of select block 1
    
      
    
        -- select block 2
    
        ROW_NUMBER() OVER (PARTITION BY BORM.KEY_1 ORDER BY RRMD.SEQ_NO DESC) AS MAX_RRMD_SEQ_NO,
    
        BDEH.EFFECT_DATE,
    
        BDEH.EXPIRY_DATE,
    
        BDEH.UNEARNED_INT_REB,
    
        BDEH.ACCRUED_INT,
    
        BDEH.ARREARS_PENALTY,
    
        BDEH.DISCH_AMOUNT,
    
        BDEH.WAIVER_AMT,
    
        BDEH.LEGAL_FEE,
    
        BDEH.ADMIN_FEE,
    
        BDEH.MISC_FEE,
    
        BDEH.SINKING_FUND,
    
        BDEH.PREPAID_AMT,
    
        ATRP.GRACE_PERIOD,
    
        BOEX.PROP_SEC_EXMP_PER,
    
        RRMD.RR_OLD_ACC_DET,
    
        RRMD.RR_NPL_STATUS,
    
        RRMD.RR_WAIVER_AMT,
    
        RMCD.RETAIN_FLAG,
    
    
    
        -- start of prod Acr --
    
        -- Start of AHCR1486
    
        CAST(BOIS.RR_TAG AS STRING) AS RR_TAG,
    
        BOIS.RR_DT,
    
        CAST(BOIS.REC_TAG AS STRING) AS REC_TAG,
    
        BOIS.REC_DATE,
    
        CAST(BORM.THRES_FLAG AS STRING) AS THRES_FLAG,
    
        BORM.NO_REPHASE_TOTAL,
    
        -- End of IA AHCR1486
    
    
    
        -- Start of IA AHCR1492
    
        CAST(BOIS.RR_APPL_TAG AS STRING) AS RR_APPL_TAG,
    
        BOIS.RR_APPL_EXP_DT,
    
        -- End of IA AHCR1492
    
        -- end of prod Acr --
    
        
    
        BDEH.REASON
    
      
    
      FROM {{dbdir.pODS_SCHM}}.BORM 
    
      LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.BOIS ON BOIS.KEY_1 = BORM.KEY_1
    
      LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.BOEX ON BOEX.KEY_1 = BORM.KEY_1
    
      LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.CUSVD5 ON SUBSTR(CUSVD5.KEY_1, 1, 19) = BORM.KEY_1
    
      LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.BDEH ON BDEH.INST_NO = SUBSTR(BORM.KEY_1, 1, 3) 
    
        AND BDEH.ACCOUNT_NO = SUBSTR(BORM.KEY_1, 4, 16) 
    
        AND BDEH.STATUS = 0
    
      LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.LONP ON LONP.INST_NO = SUBSTR(BORM.KEY_1, 1, 3) 
    
        AND LONP.SYST = 'BOR' 
    
        AND LONP.ACCT_TYPE = BORM.ACT_TYPE 
    
        AND LONP.INT_CAT = BORM.CAT
    
      LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.RRMD ON RRMD.INST_NO = SUBSTR(BORM.KEY_1, 1, 3) 
    
        AND RRMD.ACCT_NO = SUBSTR(BORM.KEY_1, 4, 16)
    
      LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.RMCD ON RMCD.INST_NO = 999 
    
        AND RMCD.CIF_NO = BORM.CUSTOMER_NO  
    
        AND RMCD.ACCT_NO = SUBSTRING(BORM.KEY_1, 4, 16)
    
      LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.ATRP ON ATRP.INST_NO = SUBSTR(BORM.KEY_1, 1, 3) 
    
        AND ATRP.ATTR_CODE = BOIS.ATTRITION_CODE 
    
        AND ATRP.ACCT_TYPE = BORM.ACT_TYPE 
    
        AND ATRP.INT_CAT = BORM.CAT 
    
        AND ATRP.COMPANY_CODE = CUSVD5.EMPLOYER_CODE
    
    ) A 
    
    WHERE A.MAX_RRMD_SEQ_NO = 1
    
    -- LIMIT 100  -- Uncomment if needed for testing""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    NETZ_SRC_TBL_NM_v = spark.sql(sql)
    
    
    
    
    
    #spark.sql(f"use spark_catalog.default").show()
    
    NETZ_SRC_TBL_NM_lnk_Source_v=NETZ_SRC_TBL_NM_v.select(NETZ_SRC_TBL_NM_v[0].cast('string').alias('KEY_1'),NETZ_SRC_TBL_NM_v[1].cast('decimal(17,3)').alias('PROJECTED_FEE'),NETZ_SRC_TBL_NM_v[2].cast('string').alias('SHIFT_DUE_DT'),NETZ_SRC_TBL_NM_v[3].cast('integer').alias('WORKOUT_DT'),NETZ_SRC_TBL_NM_v[4].cast('decimal(17,3)').alias('TPT_CLAIM_RECEIVED'),NETZ_SRC_TBL_NM_v[5].cast('decimal(17,3)').alias('TPT_CLAIM_REFUND'),NETZ_SRC_TBL_NM_v[6].cast('decimal(5,0)').alias('CCRIS_FCLTY_TYP'),NETZ_SRC_TBL_NM_v[7].cast('decimal(2,0)').alias('MORATORIUM_PERIOD'),NETZ_SRC_TBL_NM_v[8].cast('integer').alias('LAST_PAYMENT_DT'),NETZ_SRC_TBL_NM_v[9].cast('string').alias('ISLAMIC_CONCEPT'),NETZ_SRC_TBL_NM_v[10].cast('decimal(17,3)').alias('GPP_EARNED'),NETZ_SRC_TBL_NM_v[11].cast('decimal(17,3)').alias('ACC_CASH_REBATE'),NETZ_SRC_TBL_NM_v[12].cast('integer').alias('ORIG_REPAY_TERM'),NETZ_SRC_TBL_NM_v[13].cast('integer').alias('LTTR_OFFR_DT'),NETZ_SRC_TBL_NM_v[14].cast('integer').alias('BD_AGREE_DATE'),NETZ_SRC_TBL_NM_v[15].cast('string').alias('HIRERS_NAME'),NETZ_SRC_TBL_NM_v[16].cast('string').alias('HIRER_ID_TYPE'),NETZ_SRC_TBL_NM_v[17].cast('string').alias('HIRER_ID_NO'),NETZ_SRC_TBL_NM_v[18].cast('decimal(17,3)').alias('WDV_UNEARNED_INT'),NETZ_SRC_TBL_NM_v[19].cast('decimal(4,2)').alias('PROMPT_PAY_REB_PER'),NETZ_SRC_TBL_NM_v[20].cast('string').alias('ISL_FUNDING_TYPE'),NETZ_SRC_TBL_NM_v[21].cast('string').alias('PAYMENT_CNTR'),NETZ_SRC_TBL_NM_v[22].cast('string').alias('UPFR_CAP_BASIS'),NETZ_SRC_TBL_NM_v[23].cast('decimal(7,4)').alias('CAP_RATE'),NETZ_SRC_TBL_NM_v[24].cast('string').alias('ACCT_SOLD_TO'),NETZ_SRC_TBL_NM_v[25].cast('decimal(4,2)').alias('NISBAH_RATE'),NETZ_SRC_TBL_NM_v[26].cast('decimal(6,2)').alias('MI006_CUSTOMER_CCR'),NETZ_SRC_TBL_NM_v[27].cast('decimal(6,2)').alias('MI006_BANK_CCR'),NETZ_SRC_TBL_NM_v[28].cast('integer').alias('MAX_RRMD_SEQ_NO'),NETZ_SRC_TBL_NM_v[29].cast('integer').alias('EFFECT_DATE'),NETZ_SRC_TBL_NM_v[30].cast('integer').alias('EXPIRY_DATE'),NETZ_SRC_TBL_NM_v[31].cast('decimal(17,3)').alias('UNEARNED_INT_REB'),NETZ_SRC_TBL_NM_v[32].cast('decimal(17,3)').alias('ACCRUED_INT'),NETZ_SRC_TBL_NM_v[33].cast('decimal(17,3)').alias('ARREARS_PENALTY'),NETZ_SRC_TBL_NM_v[34].cast('decimal(17,3)').alias('DISCH_AMOUNT'),NETZ_SRC_TBL_NM_v[35].cast('decimal(17,3)').alias('WAIVER_AMT'),NETZ_SRC_TBL_NM_v[36].cast('decimal(17,3)').alias('LEGAL_FEE'),NETZ_SRC_TBL_NM_v[37].cast('decimal(17,3)').alias('ADMIN_FEE'),NETZ_SRC_TBL_NM_v[38].cast('decimal(17,3)').alias('MISC_FEE'),NETZ_SRC_TBL_NM_v[39].cast('decimal(17,3)').alias('SINKING_FUND'),NETZ_SRC_TBL_NM_v[40].cast('decimal(17,3)').alias('PREPAID_AMT'),NETZ_SRC_TBL_NM_v[41].cast('decimal(3,0)').alias('GRACE_PERIOD'),NETZ_SRC_TBL_NM_v[42].cast('decimal(5,2)').alias('PROP_SEC_EXMP_PER'),NETZ_SRC_TBL_NM_v[43].cast('string').alias('RR_OLD_ACC_DET'),NETZ_SRC_TBL_NM_v[44].cast('string').alias('RR_NPL_STATUS'),NETZ_SRC_TBL_NM_v[45].cast('decimal(17,3)').alias('RR_WAIVER_AMT'),NETZ_SRC_TBL_NM_v[46].cast('string').alias('RETAIN_FLAG'),NETZ_SRC_TBL_NM_v[47].cast('string').alias('RR_TAG'),NETZ_SRC_TBL_NM_v[48].cast('integer').alias('RR_DT'),NETZ_SRC_TBL_NM_v[49].cast('string').alias('REC_TAG'),NETZ_SRC_TBL_NM_v[50].cast('integer').alias('REC_DATE'),NETZ_SRC_TBL_NM_v[51].cast('string').alias('THRES_FLAG'),NETZ_SRC_TBL_NM_v[52].cast('decimal(4,0)').alias('NO_REPHASE_TOTAL'),NETZ_SRC_TBL_NM_v[53].cast('string').alias('RR_APPL_TAG'),NETZ_SRC_TBL_NM_v[54].cast('integer').alias('RR_APPL_EXP_DT'),NETZ_SRC_TBL_NM_v[55].cast('string').alias('REASON'))
    
    NETZ_SRC_TBL_NM_lnk_Source_v = NETZ_SRC_TBL_NM_lnk_Source_v.selectExpr("RTRIM(KEY_1) AS KEY_1","PROJECTED_FEE","RTRIM(SHIFT_DUE_DT) AS SHIFT_DUE_DT","WORKOUT_DT","TPT_CLAIM_RECEIVED","TPT_CLAIM_REFUND","CCRIS_FCLTY_TYP","MORATORIUM_PERIOD","LAST_PAYMENT_DT","RTRIM(ISLAMIC_CONCEPT) AS ISLAMIC_CONCEPT","GPP_EARNED","ACC_CASH_REBATE","ORIG_REPAY_TERM","LTTR_OFFR_DT","BD_AGREE_DATE","RTRIM(HIRERS_NAME) AS HIRERS_NAME","RTRIM(HIRER_ID_TYPE) AS HIRER_ID_TYPE","RTRIM(HIRER_ID_NO) AS HIRER_ID_NO","WDV_UNEARNED_INT","PROMPT_PAY_REB_PER","RTRIM(ISL_FUNDING_TYPE) AS ISL_FUNDING_TYPE","RTRIM(PAYMENT_CNTR) AS PAYMENT_CNTR","RTRIM(UPFR_CAP_BASIS) AS UPFR_CAP_BASIS","CAP_RATE","RTRIM(ACCT_SOLD_TO) AS ACCT_SOLD_TO","NISBAH_RATE","MI006_CUSTOMER_CCR","MI006_BANK_CCR","MAX_RRMD_SEQ_NO","EFFECT_DATE","EXPIRY_DATE","UNEARNED_INT_REB","ACCRUED_INT","ARREARS_PENALTY","DISCH_AMOUNT","WAIVER_AMT","LEGAL_FEE","ADMIN_FEE","MISC_FEE","SINKING_FUND","PREPAID_AMT","GRACE_PERIOD","PROP_SEC_EXMP_PER","RTRIM(RR_OLD_ACC_DET) AS RR_OLD_ACC_DET","RTRIM(RR_NPL_STATUS) AS RR_NPL_STATUS","RR_WAIVER_AMT","RTRIM(RETAIN_FLAG) AS RETAIN_FLAG","RR_TAG","RR_DT","REC_TAG","REC_DATE","THRES_FLAG","NO_REPHASE_TOTAL","RR_APPL_TAG","RR_APPL_EXP_DT","RTRIM(REASON) AS REASON").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'KEY_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(19)'}}, {'name': 'PROJECTED_FEE', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'SHIFT_DUE_DT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'WORKOUT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'TPT_CLAIM_RECEIVED', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'TPT_CLAIM_REFUND', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'CCRIS_FCLTY_TYP', 'type': 'decimal(5,0)', 'nullable': True, 'metadata': {}}, {'name': 'MORATORIUM_PERIOD', 'type': 'decimal(2,0)', 'nullable': True, 'metadata': {}}, {'name': 'LAST_PAYMENT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'ISLAMIC_CONCEPT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'GPP_EARNED', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'ACC_CASH_REBATE', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'ORIG_REPAY_TERM', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'LTTR_OFFR_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'BD_AGREE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'HIRERS_NAME', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(60)'}}, {'name': 'HIRER_ID_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'HIRER_ID_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(24)'}}, {'name': 'WDV_UNEARNED_INT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'PROMPT_PAY_REB_PER', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}, {'name': 'ISL_FUNDING_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'PAYMENT_CNTR', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'UPFR_CAP_BASIS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'CAP_RATE', 'type': 'decimal(7,4)', 'nullable': True, 'metadata': {}}, {'name': 'ACCT_SOLD_TO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'NISBAH_RATE', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CUSTOMER_CCR', 'type': 'decimal(6,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BANK_CCR', 'type': 'decimal(6,2)', 'nullable': True, 'metadata': {}}, {'name': 'MAX_RRMD_SEQ_NO', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'EFFECT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'EXPIRY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'UNEARNED_INT_REB', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'ACCRUED_INT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'ARREARS_PENALTY', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'DISCH_AMOUNT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'WAIVER_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'LEGAL_FEE', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'ADMIN_FEE', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MISC_FEE', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'SINKING_FUND', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'PREPAID_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'GRACE_PERIOD', 'type': 'decimal(3,0)', 'nullable': True, 'metadata': {}}, {'name': 'PROP_SEC_EXMP_PER', 'type': 'decimal(5,2)', 'nullable': True, 'metadata': {}}, {'name': 'RR_OLD_ACC_DET', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(17)'}}, {'name': 'RR_NPL_STATUS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'RR_WAIVER_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'RETAIN_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'RR_TAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'RR_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REC_TAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'REC_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'THRES_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'NO_REPHASE_TOTAL', 'type': 'decimal(4,0)', 'nullable': True, 'metadata': {}}, {'name': 'RR_APPL_TAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'RR_APPL_EXP_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REASON', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}]}))
    
    spark.sql("DROP TABLE IF EXISTS NETZ_SRC_TBL_NM_lnk_Source_v").show()
    
    print("NETZ_SRC_TBL_NM_lnk_Source_v")
    
    print(NETZ_SRC_TBL_NM_lnk_Source_v.schema.json())
    
    print("count:{}".format(NETZ_SRC_TBL_NM_lnk_Source_v.count()))
    
    NETZ_SRC_TBL_NM_lnk_Source_v.show(1000,False)
    
    NETZ_SRC_TBL_NM_lnk_Source_v.write.mode("overwrite").saveAsTable("NETZ_SRC_TBL_NM_lnk_Source_v")
    

@task
def V0A105(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task.pyspark(conn_id="spark-local")
def TRN_CONVERT_lnk_Source_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    NETZ_SRC_TBL_NM_lnk_Source_v=spark.table('NETZ_SRC_TBL_NM_lnk_Source_v')
    
    TRN_CONVERT_lnk_Source_Part_v=NETZ_SRC_TBL_NM_lnk_Source_v
    
    spark.sql("DROP TABLE IF EXISTS TRN_CONVERT_lnk_Source_Part_v").show()
    
    print("TRN_CONVERT_lnk_Source_Part_v")
    
    print(TRN_CONVERT_lnk_Source_Part_v.schema.json())
    
    print("count:{}".format(TRN_CONVERT_lnk_Source_Part_v.count()))
    
    TRN_CONVERT_lnk_Source_Part_v.show(1000,False)
    
    TRN_CONVERT_lnk_Source_Part_v.write.mode("overwrite").saveAsTable("TRN_CONVERT_lnk_Source_Part_v")
    

@task.pyspark(conn_id="spark-local")
def TRN_CONVERT(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    TRN_CONVERT_lnk_Source_Part_v=spark.table('TRN_CONVERT_lnk_Source_Part_v')
    
    TRN_CONVERT_v = TRN_CONVERT_lnk_Source_Part_v
    
    TRN_CONVERT_lnk_BDEH_Tgt_v = TRN_CONVERT_v.select(expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM KEY_1))""").cast('string').alias('B_KEY'),col('TPT_CLAIM_RECEIVED').cast('decimal(18,3)').alias('MI006_BOIS_TPT_CLAIM_RECEIVED'),col('TPT_CLAIM_REFUND').cast('decimal(18,3)').alias('MI006_BOIS_TPT_CLAIM_REFUND'),col('CCRIS_FCLTY_TYP').cast('integer').alias('MI006_BOIS_CCRIS_FCLTY_TYP'),col('MORATORIUM_PERIOD').cast('integer').alias('MI006_BOIS_MORATORIUM_PERIOD'),col('LAST_PAYMENT_DT').cast('integer').alias('MI006_BOIS_LAST_PAY_DATE'),expr("""CASE WHEN TRIM(COALESCE(ISLAMIC_CONCEPT, '')) = '' THEN '00' ELSE LPAD(TRIM(COALESCE(ISLAMIC_CONCEPT, '')), 2, '0') END""").cast('string').alias('MI006_BOIS_ISLAMIC_CONCEPT'),col('GPP_EARNED').cast('decimal(18,3)').alias('MI006_BOIS_GPP_EARNED'),col('ACC_CASH_REBATE').cast('decimal(18,3)').alias('MI006_BOIS_ACCUM_PAY_REBATE'),col('ORIG_REPAY_TERM').cast('integer').alias('MI006_BOIS_ORIGINAL_TERM'),col('LTTR_OFFR_DT').cast('integer').alias('MI006_BOIS_LTTR_OFFR_DT'),col('BD_AGREE_DATE').cast('integer').alias('MI006_BOIS_AGREEMENT_DT'),col('HIRERS_NAME').cast('string').alias('MI006_BOIS_HIRER_NAME'),col('HIRER_ID_TYPE').cast('string').alias('MI006_BOIS_HIRER_ID_TYPE'),col('HIRER_ID_NO').cast('string').alias('MI006_BOIS_HIRER_ID_NUM'),col('WDV_UNEARNED_INT').cast('decimal(18,3)').alias('MI006_BOIS_WDV_UNEARNED_INT'),col('PROMPT_PAY_REB_PER').cast('decimal(5,2)').alias('MI006_BOIS_PAY_REBATE_RATE'),expr("""CASE WHEN TRIM(COALESCE(ISL_FUNDING_TYPE, '')) = '' THEN '0' ELSE RIGHT(CONCAT('0', TRIM(ISL_FUNDING_TYPE)), 1) END""").cast('string').alias('MI006_ISL_FUNDING_TYPE'),expr("""CASE WHEN TRIM(COALESCE(PAYMENT_CNTR, '')) = '' THEN '00' ELSE RIGHT(LPAD(TRIM(PAYMENT_CNTR), 2, '0'), 2) END""").cast('string').alias('MI006_BOIS_PAYMENT_COUNTER'),col('UPFR_CAP_BASIS').cast('string').alias('MI006_BOIS_UPFRONT_CAP_BASIS'),col('CAP_RATE').cast('decimal(7,4)').alias('MI006_BOIS_CAP_RATE'),col('MI006_CUSTOMER_CCR').cast('decimal(6,2)').alias('MI006_CUSTOMER_CCR'),col('MI006_BANK_CCR').cast('decimal(6,2)').alias('MI006_BANK_CCR'),col('NISBAH_RATE').cast('decimal(4,2)').alias('MI006_BORM_NISBAH_RATE'),expr("""IF(ACCT_SOLD_TO = '1' OR ACCT_SOLD_TO = '2' OR ACCT_SOLD_TO = '3' OR ACCT_SOLD_TO = '4', 'Y', REPEAT(' ', 1))""").cast('string').alias('MI006_ACCT_SOLD_TO_E'),expr("""IF(ACCT_SOLD_TO = '1', 'CAGAMAS', IF(ACCT_SOLD_TO = '2', 'AmMortgage', IF(ACCT_SOLD_TO = '3', 'Johor State', IF(ACCT_SOLD_TO = '4', 'AIQON', REPEAT(' ', 1)))))""").cast('string').alias('MI006_AM_ASSEC_TAG_E'),expr("""IF(ACCT_SOLD_TO = '1', 'CAGAMAS', IF(ACCT_SOLD_TO = '2', 'AmMortgage', IF(ACCT_SOLD_TO = '3', 'Johor State', IF(ACCT_SOLD_TO = '4', 'AIQON', REPEAT(' ', 1)))))""").cast('string').alias('MI006_ACCT_SOLD_TO_NAME_E'),col('EFFECT_DATE').cast('integer').alias('MI006_BDEH_EFFECT_DATE'),col('EXPIRY_DATE').cast('integer').alias('MI006_BDEH_EXPIRY_DATE'),col('UNEARNED_INT_REB').cast('decimal(18,3)').alias('MI006_BDEH_UNEARNED_INT_REB'),col('ACCRUED_INT').cast('decimal(18,3)').alias('MI006_BDEH_ACCRUED_INT'),col('ARREARS_PENALTY').cast('decimal(18,3)').alias('MI006_BDEH_ARREARS_PENALTY'),col('DISCH_AMOUNT').cast('decimal(18,3)').alias('MI006_BDEH_DISCH_AMOUNT'),col('WAIVER_AMT').cast('decimal(18,3)').alias('MI006_BDEH_WAIVER_AMT'),col('LEGAL_FEE').cast('decimal(18,3)').alias('MI006_BDEH_LEGAL_FEE'),col('ADMIN_FEE').cast('decimal(18,3)').alias('MI006_BDEH_ADMIN_FEE'),col('MISC_FEE').cast('decimal(18,3)').alias('MI006_BDEH_MISC_FEE'),col('SINKING_FUND').cast('decimal(18,3)').alias('MI006_BDEH_SINKING_FUND'),col('PREPAID_AMT').cast('decimal(18,3)').alias('MI006_BDEH_PREPAID_AMT'),col('PROP_SEC_EXMP_PER').cast('decimal(6,3)').alias('MI006_BOEX_PROP_SEC_EXMP_PER'),expr("""CASE WHEN TRIM(COALESCE(RR_OLD_ACC_DET, '')) = '' THEN LPAD('0', 17, '0') ELSE RIGHT(CONCAT(LPAD('0', 17, '0'), TRIM(RR_OLD_ACC_DET)), 17) END""").cast('string').alias('MI006_RRMD_RR_OLD_ACC_DET'),expr("""CASE WHEN TRIM(COALESCE(RR_NPL_STATUS, '')) = '' THEN '00' ELSE RIGHT(CONCAT(LPAD('0', 2, '0'), TRIM(RR_NPL_STATUS)), 2) END""").cast('string').alias('MI006_RRMD_RR_NPL_STATUS'),col('RR_WAIVER_AMT').cast('decimal(18,3)').alias('MI006_RRMD_WAIVER_AMT'),col('RETAIN_FLAG').cast('string').alias('MI006_RMCD_RETAIN_FLAG'),col('GRACE_PERIOD').cast('integer').alias('MI006_ATRP_GRACE_PERIOD'),col('SHIFT_DUE_DT').cast('string').alias('MI006_SHIFT_DUE_DT_FLAG'),col('PROJECTED_FEE').cast('decimal(18,3)').alias('MI006_PROJECTED_FEES'),col('RR_TAG').cast('string').alias('MI006_BOIS_RR_TAG'),col('RR_DT').cast('integer').alias('MI006_BOIS_RR_DT'),col('REC_TAG').cast('string').alias('MI006_BOIS_REC_TAG'),col('REC_DATE').cast('integer').alias('MI006_BOIS_REC_DATE'),col('THRES_FLAG').cast('string').alias('MI006_BORM_THRES_FLAG'),col('NO_REPHASE_TOTAL').cast('decimal(4,0)').alias('MI006_BORM_NO_REPHASE_TOTAL'),col('RR_APPL_TAG').cast('string').alias('MI006_RR_APPL_TAG'),col('RR_APPL_EXP_DT').cast('integer').alias('MI006_RR_EXP_DT'),col('WORKOUT_DT').cast('integer').alias('MI006_WORKOUT_DATE'),col('REASON').cast('string').alias('MI006_BDEH_SETTLEMENT_REASON'))
    
    TRN_CONVERT_lnk_BDEH_Tgt_v = TRN_CONVERT_lnk_BDEH_Tgt_v.selectExpr("B_KEY","MI006_BOIS_TPT_CLAIM_RECEIVED","MI006_BOIS_TPT_CLAIM_REFUND","MI006_BOIS_CCRIS_FCLTY_TYP","MI006_BOIS_MORATORIUM_PERIOD","MI006_BOIS_LAST_PAY_DATE","RTRIM(MI006_BOIS_ISLAMIC_CONCEPT) AS MI006_BOIS_ISLAMIC_CONCEPT","MI006_BOIS_GPP_EARNED","MI006_BOIS_ACCUM_PAY_REBATE","MI006_BOIS_ORIGINAL_TERM","MI006_BOIS_LTTR_OFFR_DT","MI006_BOIS_AGREEMENT_DT","MI006_BOIS_HIRER_NAME","MI006_BOIS_HIRER_ID_TYPE","MI006_BOIS_HIRER_ID_NUM","MI006_BOIS_WDV_UNEARNED_INT","MI006_BOIS_PAY_REBATE_RATE","RTRIM(MI006_ISL_FUNDING_TYPE) AS MI006_ISL_FUNDING_TYPE","RTRIM(MI006_BOIS_PAYMENT_COUNTER) AS MI006_BOIS_PAYMENT_COUNTER","RTRIM(MI006_BOIS_UPFRONT_CAP_BASIS) AS MI006_BOIS_UPFRONT_CAP_BASIS","MI006_BOIS_CAP_RATE","MI006_CUSTOMER_CCR","MI006_BANK_CCR","MI006_BORM_NISBAH_RATE","RTRIM(MI006_ACCT_SOLD_TO_E) AS MI006_ACCT_SOLD_TO_E","MI006_AM_ASSEC_TAG_E","MI006_ACCT_SOLD_TO_NAME_E","MI006_BDEH_EFFECT_DATE","MI006_BDEH_EXPIRY_DATE","MI006_BDEH_UNEARNED_INT_REB","MI006_BDEH_ACCRUED_INT","MI006_BDEH_ARREARS_PENALTY","MI006_BDEH_DISCH_AMOUNT","MI006_BDEH_WAIVER_AMT","MI006_BDEH_LEGAL_FEE","MI006_BDEH_ADMIN_FEE","MI006_BDEH_MISC_FEE","MI006_BDEH_SINKING_FUND","MI006_BDEH_PREPAID_AMT","MI006_BOEX_PROP_SEC_EXMP_PER","RTRIM(MI006_RRMD_RR_OLD_ACC_DET) AS MI006_RRMD_RR_OLD_ACC_DET","RTRIM(MI006_RRMD_RR_NPL_STATUS) AS MI006_RRMD_RR_NPL_STATUS","MI006_RRMD_WAIVER_AMT","RTRIM(MI006_RMCD_RETAIN_FLAG) AS MI006_RMCD_RETAIN_FLAG","MI006_ATRP_GRACE_PERIOD","RTRIM(MI006_SHIFT_DUE_DT_FLAG) AS MI006_SHIFT_DUE_DT_FLAG","MI006_PROJECTED_FEES","MI006_BOIS_RR_TAG","MI006_BOIS_RR_DT","MI006_BOIS_REC_TAG","MI006_BOIS_REC_DATE","MI006_BORM_THRES_FLAG","MI006_BORM_NO_REPHASE_TOTAL","MI006_RR_APPL_TAG","MI006_RR_EXP_DT","MI006_WORKOUT_DATE","RTRIM(MI006_BDEH_SETTLEMENT_REASON) AS MI006_BDEH_SETTLEMENT_REASON").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_TPT_CLAIM_RECEIVED', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_TPT_CLAIM_REFUND', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_CCRIS_FCLTY_TYP', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_MORATORIUM_PERIOD', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LAST_PAY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_ISLAMIC_CONCEPT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BOIS_GPP_EARNED', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_ACCUM_PAY_REBATE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_ORIGINAL_TERM', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_LTTR_OFFR_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_AGREEMENT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_HIRER_NAME', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_HIRER_ID_TYPE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_HIRER_ID_NUM', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_WDV_UNEARNED_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_PAY_REBATE_RATE', 'type': 'decimal(5,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ISL_FUNDING_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOIS_PAYMENT_COUNTER', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BOIS_UPFRONT_CAP_BASIS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOIS_CAP_RATE', 'type': 'decimal(7,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CUSTOMER_CCR', 'type': 'decimal(6,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BANK_CCR', 'type': 'decimal(6,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_NISBAH_RATE', 'type': 'decimal(4,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACCT_SOLD_TO_E', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_AM_ASSEC_TAG_E', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACCT_SOLD_TO_NAME_E', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BDEH_EFFECT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BDEH_EXPIRY_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BDEH_UNEARNED_INT_REB', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BDEH_ACCRUED_INT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BDEH_ARREARS_PENALTY', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BDEH_DISCH_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BDEH_WAIVER_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BDEH_LEGAL_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BDEH_ADMIN_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BDEH_MISC_FEE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BDEH_SINKING_FUND', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BDEH_PREPAID_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOEX_PROP_SEC_EXMP_PER', 'type': 'decimal(6,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RRMD_RR_OLD_ACC_DET', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(17)'}}, {'name': 'MI006_RRMD_RR_NPL_STATUS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_RRMD_WAIVER_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RMCD_RETAIN_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ATRP_GRACE_PERIOD', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SHIFT_DUE_DT_FLAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_PROJECTED_FEES', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_RR_TAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_RR_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_REC_TAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOIS_REC_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_THRES_FLAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BORM_NO_REPHASE_TOTAL', 'type': 'decimal(4,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RR_APPL_TAG', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RR_EXP_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_WORKOUT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BDEH_SETTLEMENT_REASON', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}]}))
    
    spark.sql("DROP TABLE IF EXISTS TRN_CONVERT_lnk_BDEH_Tgt_v").show()
    
    print("TRN_CONVERT_lnk_BDEH_Tgt_v")
    
    print(TRN_CONVERT_lnk_BDEH_Tgt_v.schema.json())
    
    print("count:{}".format(TRN_CONVERT_lnk_BDEH_Tgt_v.count()))
    
    TRN_CONVERT_lnk_BDEH_Tgt_v.show(1000,False)
    
    TRN_CONVERT_lnk_BDEH_Tgt_v.write.mode("overwrite").saveAsTable("TRN_CONVERT_lnk_BDEH_Tgt_v")
    

@task.pyspark(conn_id="spark-local")
def TGT_BDEH_LONP_lnk_BDEH_Tgt_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    TRN_CONVERT_lnk_BDEH_Tgt_v=spark.table('TRN_CONVERT_lnk_BDEH_Tgt_v')
    
    TGT_BDEH_LONP_lnk_BDEH_Tgt_Part_v=TRN_CONVERT_lnk_BDEH_Tgt_v
    
    spark.sql("DROP TABLE IF EXISTS TGT_BDEH_LONP_lnk_BDEH_Tgt_Part_v").show()
    
    print("TGT_BDEH_LONP_lnk_BDEH_Tgt_Part_v")
    
    print(TGT_BDEH_LONP_lnk_BDEH_Tgt_Part_v.schema.json())
    
    print("count:{}".format(TGT_BDEH_LONP_lnk_BDEH_Tgt_Part_v.count()))
    
    TGT_BDEH_LONP_lnk_BDEH_Tgt_Part_v.show(1000,False)
    
    TGT_BDEH_LONP_lnk_BDEH_Tgt_Part_v.write.mode("overwrite").saveAsTable("TGT_BDEH_LONP_lnk_BDEH_Tgt_Part_v")
    

@task.pyspark(conn_id="spark-local")
def TGT_BDEH_LONP(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    TGT_BDEH_LONP_lnk_BDEH_Tgt_Part_v=spark.table('TGT_BDEH_LONP_lnk_BDEH_Tgt_Part_v')
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    locations = Template('{{dbdir.pPROCESSING_DIR}}MIS006_BDEH_LONP.ds').render(job_params)
    
    log.info("write dataset files to "+locations)
    
    spark.table("TGT_BDEH_LONP_lnk_BDEH_Tgt_Part_v").write.mode("overwrite").format("parquet").save(locations)
    

####################################[Main]###################################
import airflow
with DAG(
    dag_id="job_DBdirect_MIS006_BDEH_LONP_Extr_POC",
    start_date=airflow.utils.dates.days_ago(1),
    tags=['datastage'],
) as dag:
    
    job_DBdirect_MIS006_BDEH_LONP_Extr_POC_task = job_DBdirect_MIS006_BDEH_LONP_Extr_POC()
    
    Job_VIEW_task = Job_VIEW()
    
    NETZ_SRC_TBL_NM_task = NETZ_SRC_TBL_NM()
    
    V0A105_task = V0A105()
    
    TRN_CONVERT_lnk_Source_Part_task = TRN_CONVERT_lnk_Source_Part()
    
    TRN_CONVERT_task = TRN_CONVERT()
    
    TGT_BDEH_LONP_lnk_BDEH_Tgt_Part_task = TGT_BDEH_LONP_lnk_BDEH_Tgt_Part()
    
    TGT_BDEH_LONP_task = TGT_BDEH_LONP()
    
    
    job_DBdirect_MIS006_BDEH_LONP_Extr_POC_task >> Job_VIEW_task
    
    Job_VIEW_task >> NETZ_SRC_TBL_NM_task
    
    Job_VIEW_task >> V0A105_task
    
    NETZ_SRC_TBL_NM_task >> TRN_CONVERT_lnk_Source_Part_task
    
    TRN_CONVERT_lnk_Source_Part_task >> TRN_CONVERT_task
    
    TRN_CONVERT_task >> TGT_BDEH_LONP_lnk_BDEH_Tgt_Part_task
    
    TGT_BDEH_LONP_lnk_BDEH_Tgt_Part_task >> TGT_BDEH_LONP_task
    



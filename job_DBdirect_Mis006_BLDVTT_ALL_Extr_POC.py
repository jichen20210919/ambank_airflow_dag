
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-02 20:14:56
# @Author  : cloudera
# @File    : job_DBdirect_Mis006_BLDVTT_ALL_Extr_POC.py
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
def job_DBdirect_Mis006_BLDVTT_ALL_Extr_POC(**kw_args) -> str:
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
    
        --##select list##--
    
        --SUBSTRING(BORM.KEY_1,4,16) MI006_MEMB_CUST_AC,
    
        BORM.KEY_1 AS BORM_KEY_1,
    
        BOEX.AAT,
    
        BOEX.AAT_DATE,
    
        CUSVBB.ALT_ADD1,
    
        CUSVBB.ALT_ADD2,
    
        CUSVBB.ALT_ADD3,
    
        CUSVBB.ALT_ADD4,
    
        CUSVBB.ALT_POSTCODE,
    
        COALESCE(BLDVTT.AMOUNT_GUAR, 0) AS AMOUNT_GUAR,
    
        BOEX.APP_REF_NO,
    
        BLDVNN.BALANCE_01,
    
        BOEX.BUSINESS_SOURCE,
    
        LONP.CAPN_METHOD,
    
        BOEX.CHANNEL_SRC,
    
        BOEX.CHARGEE,
    
        BLDVLL.CIV_ACT_COD_DAT,
    
        BLDVLL.CIV_ACT_CODE,
    
        BOEX.CREDIT_SCORE,
    
        TIER.DESCRIPTION,
    
        BLDVFF.DOCUMENT_DATE,
    
        --BLDVCC.DOCUMENT_NO,
    
        BLDVCC.DOCUMENT_NO AS BLDVCC_DOCUMENT_NO,
    
        BLDVFF.DOCUMENT_NO AS BLDVFF_DOCUMENT_NO,
    
        BLDVFF.DOCUMENT_TYPE,
    
        BOEX.EFF_YIELD,
    
        BOEX.EQUIPMENT_CODE,
    
        CUSVBB.FORGN_ADD_IND,
    
        BOEX.FRS_DATE,
    
        BOEX.FRS_IP_TAG,
    
        BLDVTT.GUAR_ADD_1,
    
        BLDVTT.GUAR_ADD_2,
    
        BLDVTT.GUAR_ADD_3,
    
        BLDVTT.GUAR_EXP_DATE,
    
        BLDVTT.GUAR_ID_NO,
    
        BLDVTT.GUAR_NAME,
    
        BLDVTT.GUAR_OCC_CODE,
    
        BLDVTT.GUAR_POSTCODE,
    
        BLDVTT.GUAR_TOWN,
    
        TIER.ID,
    
        BLDVNN.INSUR_AMT_DUE_01,
    
        COALESCE(BLDVEE.INSURE_COVER, 0) AS INSURE_COVER,
    
        LONP.IO_TRM_METHOD,
    
        LONP.ISLAMIC_PRODUCT,
    
        BOEX.JTM_STM_IND,
    
        BLDVCC.KEY_1,
    
        COALESCE(BLDVCC.LATEST_VAL, 0) AS LATEST_VAL,
    
        BLDVLL.LGL_TAG,
    
        BOEX.MAIN_PURP_CODE,
    
        --CAST(SUBSTRING(BOEX.MAIN_PURP_CODE,1,2) AS CHAR(4)) MAIN_PURP_CODE,
    
        /*(CASE 
    
            WHEN TRIM(CAST(SUBSTRING(BOEX.MAIN_PURP_CODE,1,2) AS CHAR(4))) != '' THEN CAST(SUBSTRING(BOEX.MAIN_PURP_CODE,1,2) AS CHAR(4))
    
            ELSE '0'
    
        END)::INT MAIN_PURP_CODE,*/
    
        BOEX.MAIN_TYPE_CDE,
    
        BOEX.MAKE_CODE,
    
        BOEX.MARKETERS_NAME,
    
        BOEX.MARKETERS_TEAM,
    
        COALESCE(TIER.MAXIMUM_VALUE, 0) AS MAXIMUM_VALUE,
    
        BOEX.MRTA_DISB_DT,
    
        LONP.MULTITIER_ENABLED,
    
        LONP.NEG_RATE_IND,
    
        BOEX.OWN_BUSINESS_IND,
    
        BOEX.PARENT_COMPANY,
    
        BOEX.PAYMENT_RECORD,
    
        BOEX.PERS_BANKR_ID,
    
        BLDVEE.POLICY_NO,
    
        BLDVEE.POLICY_TYPE,
    
        --BLDVNN.POST_DATE,
    
        BLDVNN.POST_DATE AS BLDVNN_POST_DATE,
    
        BLDVTT.POST_DATE AS BLDVTT_POST_DATE,
    
        BOEX.PROMOTION_PKG_1,
    
        BOEX.PROMOTION_PKG_2,
    
        BOEX.PROMOTION_PKG_3,
    
        BOEX.PROMOTION_PKG_4,
    
        BLDVCC.PROP_ADD1,
    
        BLDVCC.PROP_ADD2,
    
        BLDVCC.PROP_ADD3,
    
        COALESCE(BLDVCC.PROPERTY_COST, 0) AS PROPERTY_COST,
    
        BLDVCC.PROPERTY_TYPE,
    
        COALESCE(BLDVCC.PROPERTY_VALUE, 0) AS PROPERTY_VALUE,
    
        BLDVCC.PURCHASE_DATE,
    
        COALESCE(TIER.RATE_INCR_DECR, 0) AS RATE_INCR_DECR,
    
        BORH.REASON_CD,
    
        BORH.REBATE_PERC,
    
        COALESCE(BLDVCC.REC_STAT, '0') AS REC_STAT,
    
        BOEX.REDEMP_AMT,
    
        BOEX.REDEMP_CODE,
    
        BOEX.REF_BANKER,
    
        BLDVNN.REPAYMENT_01,
    
        LONP.REVOLVING_CR_IND,
    
        BOEX.SQUATTER,
    
        LONP.STAFF_PROD_IND,
    
        BLDVNN.START_DATE_01,
    
        --BOEX.STATE_CODE,
    
        BOEX.STATE_CODE AS BOEX_STATE_CODE,
    
        CUSVBB.STATE_CODE AS CUSVBB_STATE_CODE,
    
        BOEX.STATUTORY_IND,
    
        BOEX.SUB_PRIME_IND,
    
        BOEX.SUB_PURP_CODE,
    
        BOEX.SUB_TYPE_CDE,
    
        LONP.TIER_GROUP_ID,
    
        LONP.TIER_METH,
    
        BLDVCC.TRAN_TYPE,
    
        BOEX.USE_CODE,
    
        BLDVXX.USER_DEF_CODE_001,
    
        BLDVXX.USER_DEF_CODE_002,
    
        BLDVXX.USER_DEF_CODE_003,
    
        BLDVXX.USER_DEF_CODE_004,
    
        BLDVXX.USER_DEF_CODE_005,
    
        BLDVXX.USER_DEF_CODE_006,
    
        BLDVXX.USER_DEF_CODE_007,
    
        BLDVXX.USER_DEF_CODE_008,
    
        BLDVXX.USER_DEF_CODE_009,
    
        BLDVXX.USER_DEF_CODE_010,
    
        BLDVXX.USER_DEF_CODE_011,
    
        BLDVXX.USER_DEF_CODE_012,
    
        BLDVXX.USER_DEF_CODE_013,
    
        BLDVXX.USER_DEF_CODE_014,
    
        BLDVXX.USER_DEF_CODE_015,
    
        BLDVXX.USER_DEF_CODE_016,
    
        BLDVXX.USER_DEF_CODE_017,
    
        BLDVXX.USER_DEF_CODE_018,
    
        BLDVXX.USER_DEF_CODE_019,
    
        BLDVXX.USER_DEF_CODE_020,
    
        BLDVXX.USER_DEF_CODE_021,
    
        BLDVXX.USER_DEF_CODE_022,
    
        BLDVXX.USER_DEF_CODE_023,
    
        BLDVXX.USER_DEF_CODE_024,
    
        BLDVXX.USER_DEF_CODE_025,
    
        BLDVXX.USER_DEF_CODE_026,
    
        BLDVXX.USER_DEF_CODE_027,
    
        BLDVXX.USER_DEF_CODE_028,
    
        BLDVXX.USER_DEF_CODE_029,
    
        BLDVXX.USER_DEF_CODE_030,
    
        BLDVXX.USER_DEF_CODE_031,
    
        BLDVXX.USER_DEF_CODE_032,
    
        BLDVXX.USER_DEF_CODE_033,
    
        BLDVXX.USER_DEF_CODE_034,
    
        BLDVXX.USER_DEF_CODE_035,
    
        BLDVXX.USER_DEF_CODE_036,
    
        BLDVXX.USER_DEF_CODE_037,
    
        BLDVXX.USER_DEF_CODE_038,
    
        BLDVXX.USER_DEF_CODE_039,
    
        BLDVXX.USER_DEF_CODE_040,
    
        BLDVXX.USER_DEF_CODE_041,
    
        BLDVXX.USER_DEF_CODE_042,
    
        BLDVXX.USER_DEF_CODE_043,
    
        BLDVXX.USER_DEF_CODE_044,
    
        BLDVXX.USER_DEF_CODE_045,
    
        BLDVXX.USER_DEF_CODE_046,
    
        BLDVXX.USER_DEF_CODE_047,
    
        BLDVXX.USER_DEF_CODE_048,
    
        BLDVXX.USER_DEF_CODE_049,
    
        BLDVXX.USER_DEF_CODE_050,
    
        BLDVXX.USER_DEF_CODE_051,
    
        BLDVXX.USER_DEF_CODE_052,
    
        BLDVXX.USER_DEF_CODE_053,
    
        BLDVXX.USER_DEF_CODE_054,
    
        BLDVXX.USER_DEF_CODE_055,
    
        BLDVXX.USER_DEF_CODE_056,
    
        BLDVXX.USER_DEF_CODE_057,
    
        BLDVXX.USER_DEF_CODE_058,
    
        BLDVXX.USER_DEF_CODE_059,
    
        BLDVXX.USER_DEF_CODE_060,
    
        BLDVXX.USER_DEF_CODE_061,
    
        BLDVXX.USER_DEF_CODE_062,
    
        BLDVXX.USER_DEF_CODE_063,
    
        BLDVXX.USER_DEF_CODE_064,
    
        BLDVXX.USER_DEF_CODE_065,
    
        BLDVXX.USER_DEF_CODE_066,
    
        BLDVXX.USER_DEF_CODE_067,
    
        BLDVXX.USER_DEF_CODE_068,
    
        BLDVXX.USER_DEF_CODE_069,
    
        BLDVXX.USER_DEF_CODE_070,
    
        BLDVXX.USER_DEF_CODE_071,
    
        BLDVXX.USER_DEF_CODE_072,
    
        BLDVXX.USER_DEF_CODE_073,
    
        BLDVXX.USER_DEF_CODE_074,
    
        BLDVXX.USER_DEF_CODE_075,
    
        BLDVXX.USER_DEF_CODE_076,
    
        BLDVXX.USER_DEF_CODE_077,
    
        BLDVXX.USER_DEF_CODE_078,
    
        BLDVXX.USER_DEF_CODE_079,
    
        BLDVXX.USER_DEF_CODE_080,
    
        BLDVXX.USER_DEF_CODE_081,
    
        BLDVXX.USER_DEF_CODE_082,
    
        BLDVXX.USER_DEF_CODE_083,
    
        BLDVXX.USER_DEF_CODE_084,
    
        BLDVXX.USER_DEF_CODE_085,
    
        BLDVXX.USER_DEF_CODE_086,
    
        BLDVXX.USER_DEF_CODE_087,
    
        BLDVXX.USER_DEF_CODE_088,
    
        BLDVXX.USER_DEF_CODE_089,
    
        BLDVXX.USER_DEF_CODE_090,
    
        BLDVXX.USER_DEF_CODE_091,
    
        BLDVXX.USER_DEF_CODE_092,
    
        BLDVXX.USER_DEF_CODE_093,
    
        BLDVXX.USER_DEF_CODE_094,
    
        BLDVXX.USER_DEF_CODE_095,
    
        BLDVXX.USER_DEF_CODE_096,
    
        BLDVXX.USER_DEF_CODE_097,
    
        BLDVXX.USER_DEF_CODE_098,
    
        BLDVXX.USER_DEF_CODE_099,
    
        BLDVXX.USER_DEF_CODE_100,
    
        BLDVCC.VAL_DATE,
    
        BOEX.FACILITY_CODE,
    
        SORD.AMOUNT,     --newly added
    
        BOEX.BNM_REF_NO,
    
        BOEX.BNM_EXPIRY_DT,
    
        ----ODS SPEC v9.9 START
    
        CUSVBB.NAME1,
    
        CUSVBB.NAME2,
    
        CUSVBB.NAME3,
    
        ----ODS SPEC v9.9 END
    
    
    
        ----ODS SPEC v11.11 START
    
        BOEX.SME_FIN_CAT,
    
        BOEX.DEBT_SERV_RATIO,
    
        BOEX.OLD_FI_CODE,
    
        BOEX.OLD_MAST_ACCT_NO,
    
        BOEX.OLD_SUB_ACCT_NO,
    
        ----BLDVLL.REASON_UP_CV_ACN_CD
    
        ----ODS SPEC v11.11 END
    
        BOEX.AAT_MAINT_DT AS AAT_MAINT_DT,   --ODS SPEC v14.5
    
        BOEX.AAT_COUNT AS AAT_COUNT,   --ODS SPEC v14.5
    
        BOEX.INSTL_WITH_COMMN,          --ODS SPEC v15.2
    
        BOEX.CLIM_CHG_CLASS,           --ODS SPEC v18.4
    
        BOEX.GREEN_FINANCE,            --ODS SPEC v18.4
    
        BOEX.PRE_DISB_DOC_ST,          --ODS SPEC v18.8
    
        BOEX.CCPT_CLASS               --ODS SPEC v19.8
    
    FROM 
    
        {{dbdir.pODS_SCHM}}.BORM AS BORM
    
        LEFT JOIN {{dbdir.pODS_SCHM}}.BLDVCC AS BLDVCC
    
            ON SUBSTRING(BORM.KEY_1, 1, 3) = SUBSTRING(BLDVCC.KEY_1, 1, 3)
    
            AND SUBSTRING(BORM.KEY_1, 4, 16) = SUBSTRING(BLDVCC.KEY_1, 4, 16)
    
            --TRIM(SUBSTRING(BLDVCC.KEY_1,20,4)) = ' '
    
        
    
        LEFT JOIN {{dbdir.pODS_SCHM}}.BLDVEE AS BLDVEE 
    
            ON SUBSTRING(BLDVEE.KEY_1, 1, 19) = BORM.KEY_1  
    
            --AND TRIM(SUBSTRING(BLDVEE.KEY_1,20,4)) = '' LIMIT 600
    
        
    
        LEFT JOIN {{dbdir.pODS_SCHM}}.BLDVFF AS BLDVFF
    
            ON SUBSTRING(BLDVFF.KEY_1, 1, 3) = SUBSTRING(BORM.KEY_1, 1, 3)
    
            AND SUBSTRING(BLDVFF.KEY_1, 4, 16) = SUBSTRING(BORM.KEY_1, 4, 16)
    
            --TRIM(SUBSTRING(BLDVFF.KEY_1,20,4)) = '' LIMIT 500
    
        
    
        --Get only the records where the BLDVNN.KEY_1,20,4 =1
    
        LEFT JOIN (
    
            SELECT 
    
                B.BL_KEY,
    
                B.START_DATE_01,
    
                B.BALANCE_01,
    
                B.INSUR_AMT_DUE_01,
    
                B.POST_DATE,
    
                B.REPAYMENT_01
    
            FROM (
    
                SELECT 
    
                    SUBSTRING(BLDVNN.KEY_1, 1, 19) AS BL_KEY,
    
                    RANK() OVER (PARTITION BY SUBSTRING(BLDVNN.KEY_1, 1, 19) ORDER BY HEX(SUBSTRING(BLDVNN.KEY_1, 20, 4))) AS RNK,
    
                    BLDVNN.START_DATE_01,
    
                    BLDVNN.BALANCE_01,
    
                    BLDVNN.INSUR_AMT_DUE_01,
    
                    BLDVNN.POST_DATE,
    
                    BLDVNN.REPAYMENT_01
    
                FROM {{dbdir.pODS_SCHM}}.BLDVNN
    
            ) B
    
            WHERE B.RNK = 1
    
        ) BLDVNN 
    
            ON BLDVNN.BL_KEY = BORM.KEY_1 
    
            --AND TRIM(SUBSTRING(BLDVNN.KEY_1,20,4)) = ''  --this contains compressed characters
    
        
    
        LEFT JOIN {{dbdir.pODS_SCHM}}.BLDVLL AS BLDVLL
    
            ON SUBSTRING(BLDVLL.KEY_1, 1, 19) = BORM.KEY_1 
    
        
    
        LEFT JOIN {{dbdir.pODS_SCHM}}.BLDVTT AS BLDVTT
    
            ON SUBSTRING(BLDVTT.KEY_1, 1, 19) = BORM.KEY_1  
    
        
    
        LEFT JOIN {{dbdir.pODS_SCHM}}.BLDVXX AS BLDVXX
    
            ON BLDVXX.SOCIETY = SUBSTRING(BORM.KEY_1, 1, 3)
    
            AND BLDVXX.MEMB_NO = SUBSTRING(BORM.KEY_1, 4, 16)
    
            --AND TRIM(BLDVXX.REC_NO) = ''
    
        
    
        LEFT JOIN {{dbdir.pODS_SCHM}}.BOEX AS BOEX
    
            ON BOEX.KEY_1 = BORM.KEY_1  
    
        
    
        LEFT JOIN {{dbdir.pODS_SCHM}}.BOIS AS BOIS
    
            ON BOIS.KEY_1 = BORM.KEY_1 
    
        
    
        LEFT JOIN {{dbdir.pODS_SCHM}}.BORH AS BORH
    
            ON BORH.SOC_NO = SUBSTRING(BORM.KEY_1, 1, 3)
    
            AND BORH.MEMB_CUST_AC = SUBSTRING(BORM.KEY_1, 4, 16)
    
        
    
        LEFT JOIN {{dbdir.pODS_SCHM}}.LONP AS LONP
    
            ON LONP.INST_NO = SUBSTRING(BORM.KEY_1, 1, 3)
    
            AND LONP.SYST = 'BOR'
    
            AND LONP.ACCT_TYPE = BORM.ACT_TYPE
    
            AND LONP.INT_CAT = BORM.CAT
    
        
    
        LEFT JOIN {{dbdir.pODS_SCHM}}.TIER AS TIER
    
            ON TIER.SOC_NO = SUBSTRING(BORM.KEY_1, 1, 3)
    
            AND TIER.GROUP_ID = LONP.TIER_GROUP_ID
    
        
    
        LEFT JOIN (
    
            SELECT * FROM (
    
                SELECT 
    
                    CUST_NO,
    
                    INST_NO,
    
                    EFFE_DATE,
    
                    EXPI_DATE,
    
                    ROW_NUMBER() OVER (PARTITION BY CUST_NO ORDER BY HEX(RECNO) DESC) AS O_KEY,
    
                    ALT_ADD1,
    
                    ALT_ADD2,
    
                    ALT_ADD3,
    
                    ALT_ADD4,
    
                    ALT_POSTCODE,
    
                    FORGN_ADD_IND,
    
                    STATE_CODE,
    
                    DELI,
    
                    ----ODS SPEC v9.9 START
    
                    NAME1,
    
                    NAME2,
    
                    NAME3
    
                    ----ODS SPEC v9.9 END
    
                FROM {{dbdir.pODS_SCHM}}.CUSVBB 
    
            ) A 
    
            WHERE A.O_KEY = 1
    
        ) CUSVBB
    
            ON CUSVBB.INST_NO = '999'
    
            AND CUSVBB.CUST_NO = SUBSTRING(BORM.KEY_1, 4, 16)
    
            AND CUSVBB.DELI = 0
    
            AND CUSVBB.EFFE_DATE <= CAST({{Curr_Date}} AS INT)
    
            AND CUSVBB.EXPI_DATE >= CAST({{Curr_Date}} AS INT)
    
            --AND HEX(CUSVBB.RECNO) = HEX('00000001')
    
        
    
        LEFT JOIN (
    
            SELECT * FROM (
    
                SELECT 
    
                    SORD.TO_ACCT_NO,
    
                    SORD.TO_INST_NO,
    
                    SORD.TO_SYS,
    
                    SORD.AMOUNT,
    
                    ROW_NUMBER() OVER (PARTITION BY SORD.TO_ACCT_NO ORDER BY PROS_YYYYMMDD, REC_YYYYMMDD, REC_HHMMSSHS) AS SEQ
    
                FROM {{dbdir.pODS_SCHM}}.SORD AS SORD
    
                --WHERE SORD.TO_ACCT_NO IN('0000448100522681','0008881001575648','                ')
    
            ) XX 
    
            WHERE SEQ = 1
    
        ) SORD 
    
            ON SORD.TO_INST_NO = SUBSTRING(BORM.KEY_1, 1, 3)
    
            AND SORD.TO_ACCT_NO = SUBSTRING(BORM.KEY_1, 4, 16)
    
            AND SORD.TO_SYS = 'LON'
    
    --WHERE SUBSTRING(BORM.KEY_1,4,16) IN(00088820000467592,00088820000308235)
    
    --WHERE SUBSTRING(BORM.KEY_1,4,16) IN(00088820000671052)
    
    --WHERE BORM_KEY_1 LIKE '%2498001012683%' OR BORM_KEY_1 LIKE '%2498001009010%'""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    NETZ_SRC_TBL_NM_v = spark.sql(sql)
    
    
    
    
    
    #spark.sql(f"use spark_catalog.default").show()
    
    NETZ_SRC_TBL_NM_ln_All_Dir_Src_v=NETZ_SRC_TBL_NM_v.select(NETZ_SRC_TBL_NM_v[0].cast('string').alias('BORM_KEY_1'),NETZ_SRC_TBL_NM_v[1].cast('string').alias('TRAN_TYPE'),NETZ_SRC_TBL_NM_v[2].cast('string').alias('KEY_1'),NETZ_SRC_TBL_NM_v[3].cast('integer').alias('BLDVNN_POST_DATE'),NETZ_SRC_TBL_NM_v[4].cast('integer').alias('BLDVTT_POST_DATE'),NETZ_SRC_TBL_NM_v[5].cast('decimal(17,3)').alias('PROPERTY_COST'),NETZ_SRC_TBL_NM_v[6].cast('decimal(17,3)').alias('PROPERTY_VALUE'),NETZ_SRC_TBL_NM_v[7].cast('decimal(17,3)').alias('LATEST_VAL'),NETZ_SRC_TBL_NM_v[8].cast('integer').alias('VAL_DATE'),NETZ_SRC_TBL_NM_v[9].cast('integer').alias('PURCHASE_DATE'),NETZ_SRC_TBL_NM_v[10].cast('string').alias('PROP_ADD1'),NETZ_SRC_TBL_NM_v[11].cast('string').alias('PROP_ADD2'),NETZ_SRC_TBL_NM_v[12].cast('string').alias('PROP_ADD3'),NETZ_SRC_TBL_NM_v[13].cast('string').alias('BLDVCC_DOCUMENT_NO'),NETZ_SRC_TBL_NM_v[14].cast('string').alias('BLDVFF_DOCUMENT_NO'),NETZ_SRC_TBL_NM_v[15].cast('string').alias('PROPERTY_TYPE'),NETZ_SRC_TBL_NM_v[16].cast('string').alias('REC_STAT'),NETZ_SRC_TBL_NM_v[17].cast('decimal(17,3)').alias('INSURE_COVER'),NETZ_SRC_TBL_NM_v[18].cast('string').alias('POLICY_TYPE'),NETZ_SRC_TBL_NM_v[19].cast('string').alias('POLICY_NO'),NETZ_SRC_TBL_NM_v[20].cast('string').alias('DOCUMENT_TYPE'),NETZ_SRC_TBL_NM_v[21].cast('integer').alias('DOCUMENT_DATE'),NETZ_SRC_TBL_NM_v[22].cast('integer').alias('START_DATE_01'),NETZ_SRC_TBL_NM_v[23].cast('decimal(17,3)').alias('INSUR_AMT_DUE_01'),NETZ_SRC_TBL_NM_v[24].cast('decimal(17,3)').alias('REPAYMENT_01'),NETZ_SRC_TBL_NM_v[25].cast('decimal(17,3)').alias('BALANCE_01'),NETZ_SRC_TBL_NM_v[26].cast('string').alias('CIV_ACT_CODE'),NETZ_SRC_TBL_NM_v[27].cast('integer').alias('CIV_ACT_COD_DAT'),NETZ_SRC_TBL_NM_v[28].cast('string').alias('LGL_TAG'),NETZ_SRC_TBL_NM_v[29].cast('string').alias('GUAR_TOWN'),NETZ_SRC_TBL_NM_v[30].cast('string').alias('GUAR_POSTCODE'),NETZ_SRC_TBL_NM_v[31].cast('string').alias('GUAR_OCC_CODE'),NETZ_SRC_TBL_NM_v[32].cast('string').alias('GUAR_ID_NO'),NETZ_SRC_TBL_NM_v[33].cast('decimal(17,3)').alias('AMOUNT_GUAR'),NETZ_SRC_TBL_NM_v[34].cast('integer').alias('GUAR_EXP_DATE'),NETZ_SRC_TBL_NM_v[35].cast('string').alias('GUAR_NAME'),NETZ_SRC_TBL_NM_v[36].cast('string').alias('GUAR_ADD_1'),NETZ_SRC_TBL_NM_v[37].cast('string').alias('GUAR_ADD_2'),NETZ_SRC_TBL_NM_v[38].cast('string').alias('GUAR_ADD_3'),NETZ_SRC_TBL_NM_v[39].cast('string').alias('USER_DEF_CODE_001'),NETZ_SRC_TBL_NM_v[40].cast('string').alias('USER_DEF_CODE_002'),NETZ_SRC_TBL_NM_v[41].cast('string').alias('USER_DEF_CODE_003'),NETZ_SRC_TBL_NM_v[42].cast('string').alias('USER_DEF_CODE_004'),NETZ_SRC_TBL_NM_v[43].cast('string').alias('USER_DEF_CODE_005'),NETZ_SRC_TBL_NM_v[44].cast('string').alias('USER_DEF_CODE_006'),NETZ_SRC_TBL_NM_v[45].cast('string').alias('USER_DEF_CODE_007'),NETZ_SRC_TBL_NM_v[46].cast('string').alias('USER_DEF_CODE_008'),NETZ_SRC_TBL_NM_v[47].cast('string').alias('USER_DEF_CODE_009'),NETZ_SRC_TBL_NM_v[48].cast('string').alias('USER_DEF_CODE_010'),NETZ_SRC_TBL_NM_v[49].cast('string').alias('USER_DEF_CODE_011'),NETZ_SRC_TBL_NM_v[50].cast('string').alias('USER_DEF_CODE_012'),NETZ_SRC_TBL_NM_v[51].cast('string').alias('USER_DEF_CODE_013'),NETZ_SRC_TBL_NM_v[52].cast('string').alias('USER_DEF_CODE_014'),NETZ_SRC_TBL_NM_v[53].cast('string').alias('USER_DEF_CODE_015'),NETZ_SRC_TBL_NM_v[54].cast('string').alias('USER_DEF_CODE_016'),NETZ_SRC_TBL_NM_v[55].cast('string').alias('USER_DEF_CODE_017'),NETZ_SRC_TBL_NM_v[56].cast('string').alias('USER_DEF_CODE_018'),NETZ_SRC_TBL_NM_v[57].cast('string').alias('USER_DEF_CODE_019'),NETZ_SRC_TBL_NM_v[58].cast('string').alias('USER_DEF_CODE_020'),NETZ_SRC_TBL_NM_v[59].cast('string').alias('USER_DEF_CODE_021'),NETZ_SRC_TBL_NM_v[60].cast('string').alias('USER_DEF_CODE_022'),NETZ_SRC_TBL_NM_v[61].cast('string').alias('USER_DEF_CODE_023'),NETZ_SRC_TBL_NM_v[62].cast('string').alias('USER_DEF_CODE_024'),NETZ_SRC_TBL_NM_v[63].cast('string').alias('USER_DEF_CODE_025'),NETZ_SRC_TBL_NM_v[64].cast('string').alias('USER_DEF_CODE_026'),NETZ_SRC_TBL_NM_v[65].cast('string').alias('USER_DEF_CODE_027'),NETZ_SRC_TBL_NM_v[66].cast('string').alias('USER_DEF_CODE_028'),NETZ_SRC_TBL_NM_v[67].cast('string').alias('USER_DEF_CODE_029'),NETZ_SRC_TBL_NM_v[68].cast('string').alias('USER_DEF_CODE_030'),NETZ_SRC_TBL_NM_v[69].cast('string').alias('USER_DEF_CODE_031'),NETZ_SRC_TBL_NM_v[70].cast('string').alias('USER_DEF_CODE_032'),NETZ_SRC_TBL_NM_v[71].cast('string').alias('USER_DEF_CODE_033'),NETZ_SRC_TBL_NM_v[72].cast('string').alias('USER_DEF_CODE_034'),NETZ_SRC_TBL_NM_v[73].cast('string').alias('USER_DEF_CODE_035'),NETZ_SRC_TBL_NM_v[74].cast('string').alias('USER_DEF_CODE_036'),NETZ_SRC_TBL_NM_v[75].cast('string').alias('USER_DEF_CODE_037'),NETZ_SRC_TBL_NM_v[76].cast('string').alias('USER_DEF_CODE_038'),NETZ_SRC_TBL_NM_v[77].cast('string').alias('USER_DEF_CODE_039'),NETZ_SRC_TBL_NM_v[78].cast('string').alias('USER_DEF_CODE_040'),NETZ_SRC_TBL_NM_v[79].cast('string').alias('USER_DEF_CODE_041'),NETZ_SRC_TBL_NM_v[80].cast('string').alias('USER_DEF_CODE_042'),NETZ_SRC_TBL_NM_v[81].cast('string').alias('USER_DEF_CODE_043'),NETZ_SRC_TBL_NM_v[82].cast('string').alias('USER_DEF_CODE_044'),NETZ_SRC_TBL_NM_v[83].cast('string').alias('USER_DEF_CODE_045'),NETZ_SRC_TBL_NM_v[84].cast('string').alias('USER_DEF_CODE_046'),NETZ_SRC_TBL_NM_v[85].cast('string').alias('USER_DEF_CODE_047'),NETZ_SRC_TBL_NM_v[86].cast('string').alias('USER_DEF_CODE_048'),NETZ_SRC_TBL_NM_v[87].cast('string').alias('USER_DEF_CODE_049'),NETZ_SRC_TBL_NM_v[88].cast('string').alias('USER_DEF_CODE_050'),NETZ_SRC_TBL_NM_v[89].cast('string').alias('USER_DEF_CODE_051'),NETZ_SRC_TBL_NM_v[90].cast('string').alias('USER_DEF_CODE_052'),NETZ_SRC_TBL_NM_v[91].cast('string').alias('USER_DEF_CODE_053'),NETZ_SRC_TBL_NM_v[92].cast('string').alias('USER_DEF_CODE_054'),NETZ_SRC_TBL_NM_v[93].cast('string').alias('USER_DEF_CODE_055'),NETZ_SRC_TBL_NM_v[94].cast('string').alias('USER_DEF_CODE_056'),NETZ_SRC_TBL_NM_v[95].cast('string').alias('USER_DEF_CODE_057'),NETZ_SRC_TBL_NM_v[96].cast('string').alias('USER_DEF_CODE_058'),NETZ_SRC_TBL_NM_v[97].cast('string').alias('USER_DEF_CODE_059'),NETZ_SRC_TBL_NM_v[98].cast('string').alias('USER_DEF_CODE_060'),NETZ_SRC_TBL_NM_v[99].cast('string').alias('USER_DEF_CODE_061'),NETZ_SRC_TBL_NM_v[100].cast('string').alias('USER_DEF_CODE_062'),NETZ_SRC_TBL_NM_v[101].cast('string').alias('USER_DEF_CODE_063'),NETZ_SRC_TBL_NM_v[102].cast('string').alias('USER_DEF_CODE_064'),NETZ_SRC_TBL_NM_v[103].cast('string').alias('USER_DEF_CODE_065'),NETZ_SRC_TBL_NM_v[104].cast('string').alias('USER_DEF_CODE_066'),NETZ_SRC_TBL_NM_v[105].cast('string').alias('USER_DEF_CODE_067'),NETZ_SRC_TBL_NM_v[106].cast('string').alias('USER_DEF_CODE_068'),NETZ_SRC_TBL_NM_v[107].cast('string').alias('USER_DEF_CODE_069'),NETZ_SRC_TBL_NM_v[108].cast('string').alias('USER_DEF_CODE_070'),NETZ_SRC_TBL_NM_v[109].cast('string').alias('USER_DEF_CODE_071'),NETZ_SRC_TBL_NM_v[110].cast('string').alias('USER_DEF_CODE_072'),NETZ_SRC_TBL_NM_v[111].cast('string').alias('USER_DEF_CODE_073'),NETZ_SRC_TBL_NM_v[112].cast('string').alias('USER_DEF_CODE_074'),NETZ_SRC_TBL_NM_v[113].cast('string').alias('USER_DEF_CODE_075'),NETZ_SRC_TBL_NM_v[114].cast('string').alias('USER_DEF_CODE_076'),NETZ_SRC_TBL_NM_v[115].cast('string').alias('USER_DEF_CODE_077'),NETZ_SRC_TBL_NM_v[116].cast('string').alias('USER_DEF_CODE_078'),NETZ_SRC_TBL_NM_v[117].cast('string').alias('USER_DEF_CODE_079'),NETZ_SRC_TBL_NM_v[118].cast('string').alias('USER_DEF_CODE_080'),NETZ_SRC_TBL_NM_v[119].cast('string').alias('USER_DEF_CODE_081'),NETZ_SRC_TBL_NM_v[120].cast('string').alias('USER_DEF_CODE_082'),NETZ_SRC_TBL_NM_v[121].cast('string').alias('USER_DEF_CODE_083'),NETZ_SRC_TBL_NM_v[122].cast('string').alias('USER_DEF_CODE_084'),NETZ_SRC_TBL_NM_v[123].cast('string').alias('USER_DEF_CODE_085'),NETZ_SRC_TBL_NM_v[124].cast('string').alias('USER_DEF_CODE_086'),NETZ_SRC_TBL_NM_v[125].cast('string').alias('USER_DEF_CODE_087'),NETZ_SRC_TBL_NM_v[126].cast('string').alias('USER_DEF_CODE_088'),NETZ_SRC_TBL_NM_v[127].cast('string').alias('USER_DEF_CODE_089'),NETZ_SRC_TBL_NM_v[128].cast('string').alias('USER_DEF_CODE_090'),NETZ_SRC_TBL_NM_v[129].cast('string').alias('USER_DEF_CODE_091'),NETZ_SRC_TBL_NM_v[130].cast('string').alias('USER_DEF_CODE_092'),NETZ_SRC_TBL_NM_v[131].cast('string').alias('USER_DEF_CODE_093'),NETZ_SRC_TBL_NM_v[132].cast('string').alias('USER_DEF_CODE_094'),NETZ_SRC_TBL_NM_v[133].cast('string').alias('USER_DEF_CODE_095'),NETZ_SRC_TBL_NM_v[134].cast('string').alias('USER_DEF_CODE_096'),NETZ_SRC_TBL_NM_v[135].cast('string').alias('USER_DEF_CODE_097'),NETZ_SRC_TBL_NM_v[136].cast('string').alias('USER_DEF_CODE_098'),NETZ_SRC_TBL_NM_v[137].cast('string').alias('USER_DEF_CODE_099'),NETZ_SRC_TBL_NM_v[138].cast('string').alias('USER_DEF_CODE_100'),NETZ_SRC_TBL_NM_v[139].cast('string').alias('EQUIPMENT_CODE'),NETZ_SRC_TBL_NM_v[140].cast('string').alias('MAIN_PURP_CODE'),NETZ_SRC_TBL_NM_v[141].cast('string').alias('SUB_PURP_CODE'),NETZ_SRC_TBL_NM_v[142].cast('string').alias('BOEX_STATE_CODE'),NETZ_SRC_TBL_NM_v[143].cast('string').alias('CUSVBB_STATE_CODE'),NETZ_SRC_TBL_NM_v[144].cast('string').alias('MAKE_CODE'),NETZ_SRC_TBL_NM_v[145].cast('string').alias('USE_CODE'),NETZ_SRC_TBL_NM_v[146].cast('string').alias('MAIN_TYPE_CDE'),NETZ_SRC_TBL_NM_v[147].cast('string').alias('SUB_TYPE_CDE'),NETZ_SRC_TBL_NM_v[148].cast('string').alias('APP_REF_NO'),NETZ_SRC_TBL_NM_v[149].cast('string').alias('PERS_BANKR_ID'),NETZ_SRC_TBL_NM_v[150].cast('string').alias('CHANNEL_SRC'),NETZ_SRC_TBL_NM_v[151].cast('string').alias('SUB_PRIME_IND'),NETZ_SRC_TBL_NM_v[152].cast('string').alias('CREDIT_SCORE'),NETZ_SRC_TBL_NM_v[153].cast('string').alias('PAYMENT_RECORD'),NETZ_SRC_TBL_NM_v[154].cast('string').alias('OWN_BUSINESS_IND'),NETZ_SRC_TBL_NM_v[155].cast('string').alias('STATUTORY_IND'),NETZ_SRC_TBL_NM_v[156].cast('string').alias('JTM_STM_IND'),NETZ_SRC_TBL_NM_v[157].cast('string').alias('SQUATTER'),NETZ_SRC_TBL_NM_v[158].cast('string').alias('MARKETERS_NAME'),NETZ_SRC_TBL_NM_v[159].cast('string').alias('MARKETERS_TEAM'),NETZ_SRC_TBL_NM_v[160].cast('string').alias('PARENT_COMPANY'),NETZ_SRC_TBL_NM_v[161].cast('string').alias('BUSINESS_SOURCE'),NETZ_SRC_TBL_NM_v[162].cast('string').alias('PROMOTION_PKG_1'),NETZ_SRC_TBL_NM_v[163].cast('string').alias('PROMOTION_PKG_2'),NETZ_SRC_TBL_NM_v[164].cast('string').alias('PROMOTION_PKG_3'),NETZ_SRC_TBL_NM_v[165].cast('string').alias('PROMOTION_PKG_4'),NETZ_SRC_TBL_NM_v[166].cast('string').alias('AAT'),NETZ_SRC_TBL_NM_v[167].cast('integer').alias('AAT_DATE'),NETZ_SRC_TBL_NM_v[168].cast('decimal(17,3)').alias('REDEMP_AMT'),NETZ_SRC_TBL_NM_v[169].cast('string').alias('CHARGEE'),NETZ_SRC_TBL_NM_v[170].cast('string').alias('REDEMP_CODE'),NETZ_SRC_TBL_NM_v[171].cast('string').alias('REF_BANKER'),NETZ_SRC_TBL_NM_v[172].cast('string').alias('FRS_IP_TAG'),NETZ_SRC_TBL_NM_v[173].cast('integer').alias('FRS_DATE'),NETZ_SRC_TBL_NM_v[174].cast('decimal(5,2)').alias('EFF_YIELD'),NETZ_SRC_TBL_NM_v[175].cast('integer').alias('MRTA_DISB_DT'),NETZ_SRC_TBL_NM_v[176].cast('string').alias('TIER_GROUP_ID'),NETZ_SRC_TBL_NM_v[177].cast('string').alias('REBATE_PERC'),NETZ_SRC_TBL_NM_v[178].cast('string').alias('REASON_CD'),NETZ_SRC_TBL_NM_v[179].cast('string').alias('ID'),NETZ_SRC_TBL_NM_v[180].cast('decimal(17,3)').alias('MAXIMUM_VALUE'),NETZ_SRC_TBL_NM_v[181].cast('decimal(13,9)').alias('RATE_INCR_DECR'),NETZ_SRC_TBL_NM_v[182].cast('string').alias('DESCRIPTION'),NETZ_SRC_TBL_NM_v[183].cast('string').alias('NEG_RATE_IND'),NETZ_SRC_TBL_NM_v[184].cast('string').alias('TIER_METH'),NETZ_SRC_TBL_NM_v[185].cast('string').alias('CAPN_METHOD'),NETZ_SRC_TBL_NM_v[186].cast('string').alias('MULTITIER_ENABLED'),NETZ_SRC_TBL_NM_v[187].cast('string').alias('IO_TRM_METHOD'),NETZ_SRC_TBL_NM_v[188].cast('string').alias('ISLAMIC_PRODUCT'),NETZ_SRC_TBL_NM_v[189].cast('string').alias('REVOLVING_CR_IND'),NETZ_SRC_TBL_NM_v[190].cast('string').alias('STAFF_PROD_IND'),NETZ_SRC_TBL_NM_v[191].cast('string').alias('ALT_ADD1'),NETZ_SRC_TBL_NM_v[192].cast('string').alias('ALT_ADD2'),NETZ_SRC_TBL_NM_v[193].cast('string').alias('ALT_ADD3'),NETZ_SRC_TBL_NM_v[194].cast('string').alias('ALT_ADD4'),NETZ_SRC_TBL_NM_v[195].cast('string').alias('ALT_POSTCODE'),NETZ_SRC_TBL_NM_v[196].cast('string').alias('FORGN_ADD_IND'),NETZ_SRC_TBL_NM_v[197].cast('string').alias('FACILITY_CODE'),NETZ_SRC_TBL_NM_v[198].cast('decimal(17,3)').alias('AMOUNT'),NETZ_SRC_TBL_NM_v[199].cast('string').alias('BNM_REF_NO'),NETZ_SRC_TBL_NM_v[200].cast('integer').alias('BNM_EXPIRY_DT'),NETZ_SRC_TBL_NM_v[201].cast('string').alias('NAME1'),NETZ_SRC_TBL_NM_v[202].cast('string').alias('NAME2'),NETZ_SRC_TBL_NM_v[203].cast('string').alias('NAME3'),NETZ_SRC_TBL_NM_v[204].cast('decimal(2,0)').alias('SME_FIN_CAT'),NETZ_SRC_TBL_NM_v[205].cast('decimal(5,2)').alias('DEBT_SERV_RATIO'),NETZ_SRC_TBL_NM_v[206].cast('string').alias('OLD_FI_CODE'),NETZ_SRC_TBL_NM_v[207].cast('string').alias('OLD_MAST_ACCT_NO'),NETZ_SRC_TBL_NM_v[208].cast('string').alias('OLD_SUB_ACCT_NO'),NETZ_SRC_TBL_NM_v[209].cast('integer').alias('AAT_MAINT_DT'),NETZ_SRC_TBL_NM_v[210].cast('decimal(2,0)').alias('AAT_COUNT'),NETZ_SRC_TBL_NM_v[211].cast('decimal(17,3)').alias('INSTL_WITH_COMMN'),NETZ_SRC_TBL_NM_v[212].cast('string').alias('CLIM_CHG_CLASS'),NETZ_SRC_TBL_NM_v[213].cast('string').alias('GREEN_FINANCE'),NETZ_SRC_TBL_NM_v[214].cast('string').alias('PRE_DISB_DOC_ST'),NETZ_SRC_TBL_NM_v[215].cast('string').alias('CCPT_CLASS'))
    
    NETZ_SRC_TBL_NM_ln_All_Dir_Src_v = NETZ_SRC_TBL_NM_ln_All_Dir_Src_v.selectExpr("RTRIM(BORM_KEY_1) AS BORM_KEY_1","RTRIM(TRAN_TYPE) AS TRAN_TYPE","RTRIM(KEY_1) AS KEY_1","BLDVNN_POST_DATE","BLDVTT_POST_DATE","PROPERTY_COST","PROPERTY_VALUE","LATEST_VAL","VAL_DATE","PURCHASE_DATE","RTRIM(PROP_ADD1) AS PROP_ADD1","RTRIM(PROP_ADD2) AS PROP_ADD2","RTRIM(PROP_ADD3) AS PROP_ADD3","RTRIM(BLDVCC_DOCUMENT_NO) AS BLDVCC_DOCUMENT_NO","RTRIM(BLDVFF_DOCUMENT_NO) AS BLDVFF_DOCUMENT_NO","RTRIM(PROPERTY_TYPE) AS PROPERTY_TYPE","RTRIM(REC_STAT) AS REC_STAT","INSURE_COVER","RTRIM(POLICY_TYPE) AS POLICY_TYPE","RTRIM(POLICY_NO) AS POLICY_NO","RTRIM(DOCUMENT_TYPE) AS DOCUMENT_TYPE","DOCUMENT_DATE","START_DATE_01","INSUR_AMT_DUE_01","REPAYMENT_01","BALANCE_01","RTRIM(CIV_ACT_CODE) AS CIV_ACT_CODE","CIV_ACT_COD_DAT","RTRIM(LGL_TAG) AS LGL_TAG","RTRIM(GUAR_TOWN) AS GUAR_TOWN","RTRIM(GUAR_POSTCODE) AS GUAR_POSTCODE","RTRIM(GUAR_OCC_CODE) AS GUAR_OCC_CODE","RTRIM(GUAR_ID_NO) AS GUAR_ID_NO","AMOUNT_GUAR","GUAR_EXP_DATE","RTRIM(GUAR_NAME) AS GUAR_NAME","RTRIM(GUAR_ADD_1) AS GUAR_ADD_1","RTRIM(GUAR_ADD_2) AS GUAR_ADD_2","RTRIM(GUAR_ADD_3) AS GUAR_ADD_3","RTRIM(USER_DEF_CODE_001) AS USER_DEF_CODE_001","RTRIM(USER_DEF_CODE_002) AS USER_DEF_CODE_002","RTRIM(USER_DEF_CODE_003) AS USER_DEF_CODE_003","RTRIM(USER_DEF_CODE_004) AS USER_DEF_CODE_004","RTRIM(USER_DEF_CODE_005) AS USER_DEF_CODE_005","RTRIM(USER_DEF_CODE_006) AS USER_DEF_CODE_006","RTRIM(USER_DEF_CODE_007) AS USER_DEF_CODE_007","RTRIM(USER_DEF_CODE_008) AS USER_DEF_CODE_008","RTRIM(USER_DEF_CODE_009) AS USER_DEF_CODE_009","RTRIM(USER_DEF_CODE_010) AS USER_DEF_CODE_010","RTRIM(USER_DEF_CODE_011) AS USER_DEF_CODE_011","RTRIM(USER_DEF_CODE_012) AS USER_DEF_CODE_012","RTRIM(USER_DEF_CODE_013) AS USER_DEF_CODE_013","RTRIM(USER_DEF_CODE_014) AS USER_DEF_CODE_014","RTRIM(USER_DEF_CODE_015) AS USER_DEF_CODE_015","RTRIM(USER_DEF_CODE_016) AS USER_DEF_CODE_016","RTRIM(USER_DEF_CODE_017) AS USER_DEF_CODE_017","RTRIM(USER_DEF_CODE_018) AS USER_DEF_CODE_018","RTRIM(USER_DEF_CODE_019) AS USER_DEF_CODE_019","RTRIM(USER_DEF_CODE_020) AS USER_DEF_CODE_020","RTRIM(USER_DEF_CODE_021) AS USER_DEF_CODE_021","RTRIM(USER_DEF_CODE_022) AS USER_DEF_CODE_022","RTRIM(USER_DEF_CODE_023) AS USER_DEF_CODE_023","RTRIM(USER_DEF_CODE_024) AS USER_DEF_CODE_024","RTRIM(USER_DEF_CODE_025) AS USER_DEF_CODE_025","RTRIM(USER_DEF_CODE_026) AS USER_DEF_CODE_026","RTRIM(USER_DEF_CODE_027) AS USER_DEF_CODE_027","RTRIM(USER_DEF_CODE_028) AS USER_DEF_CODE_028","RTRIM(USER_DEF_CODE_029) AS USER_DEF_CODE_029","RTRIM(USER_DEF_CODE_030) AS USER_DEF_CODE_030","RTRIM(USER_DEF_CODE_031) AS USER_DEF_CODE_031","RTRIM(USER_DEF_CODE_032) AS USER_DEF_CODE_032","RTRIM(USER_DEF_CODE_033) AS USER_DEF_CODE_033","RTRIM(USER_DEF_CODE_034) AS USER_DEF_CODE_034","RTRIM(USER_DEF_CODE_035) AS USER_DEF_CODE_035","RTRIM(USER_DEF_CODE_036) AS USER_DEF_CODE_036","RTRIM(USER_DEF_CODE_037) AS USER_DEF_CODE_037","RTRIM(USER_DEF_CODE_038) AS USER_DEF_CODE_038","RTRIM(USER_DEF_CODE_039) AS USER_DEF_CODE_039","RTRIM(USER_DEF_CODE_040) AS USER_DEF_CODE_040","RTRIM(USER_DEF_CODE_041) AS USER_DEF_CODE_041","RTRIM(USER_DEF_CODE_042) AS USER_DEF_CODE_042","RTRIM(USER_DEF_CODE_043) AS USER_DEF_CODE_043","RTRIM(USER_DEF_CODE_044) AS USER_DEF_CODE_044","RTRIM(USER_DEF_CODE_045) AS USER_DEF_CODE_045","RTRIM(USER_DEF_CODE_046) AS USER_DEF_CODE_046","RTRIM(USER_DEF_CODE_047) AS USER_DEF_CODE_047","RTRIM(USER_DEF_CODE_048) AS USER_DEF_CODE_048","RTRIM(USER_DEF_CODE_049) AS USER_DEF_CODE_049","RTRIM(USER_DEF_CODE_050) AS USER_DEF_CODE_050","RTRIM(USER_DEF_CODE_051) AS USER_DEF_CODE_051","RTRIM(USER_DEF_CODE_052) AS USER_DEF_CODE_052","RTRIM(USER_DEF_CODE_053) AS USER_DEF_CODE_053","RTRIM(USER_DEF_CODE_054) AS USER_DEF_CODE_054","RTRIM(USER_DEF_CODE_055) AS USER_DEF_CODE_055","RTRIM(USER_DEF_CODE_056) AS USER_DEF_CODE_056","RTRIM(USER_DEF_CODE_057) AS USER_DEF_CODE_057","RTRIM(USER_DEF_CODE_058) AS USER_DEF_CODE_058","RTRIM(USER_DEF_CODE_059) AS USER_DEF_CODE_059","RTRIM(USER_DEF_CODE_060) AS USER_DEF_CODE_060","RTRIM(USER_DEF_CODE_061) AS USER_DEF_CODE_061","RTRIM(USER_DEF_CODE_062) AS USER_DEF_CODE_062","RTRIM(USER_DEF_CODE_063) AS USER_DEF_CODE_063","RTRIM(USER_DEF_CODE_064) AS USER_DEF_CODE_064","RTRIM(USER_DEF_CODE_065) AS USER_DEF_CODE_065","RTRIM(USER_DEF_CODE_066) AS USER_DEF_CODE_066","RTRIM(USER_DEF_CODE_067) AS USER_DEF_CODE_067","RTRIM(USER_DEF_CODE_068) AS USER_DEF_CODE_068","RTRIM(USER_DEF_CODE_069) AS USER_DEF_CODE_069","RTRIM(USER_DEF_CODE_070) AS USER_DEF_CODE_070","RTRIM(USER_DEF_CODE_071) AS USER_DEF_CODE_071","RTRIM(USER_DEF_CODE_072) AS USER_DEF_CODE_072","RTRIM(USER_DEF_CODE_073) AS USER_DEF_CODE_073","RTRIM(USER_DEF_CODE_074) AS USER_DEF_CODE_074","RTRIM(USER_DEF_CODE_075) AS USER_DEF_CODE_075","RTRIM(USER_DEF_CODE_076) AS USER_DEF_CODE_076","RTRIM(USER_DEF_CODE_077) AS USER_DEF_CODE_077","RTRIM(USER_DEF_CODE_078) AS USER_DEF_CODE_078","RTRIM(USER_DEF_CODE_079) AS USER_DEF_CODE_079","RTRIM(USER_DEF_CODE_080) AS USER_DEF_CODE_080","RTRIM(USER_DEF_CODE_081) AS USER_DEF_CODE_081","RTRIM(USER_DEF_CODE_082) AS USER_DEF_CODE_082","RTRIM(USER_DEF_CODE_083) AS USER_DEF_CODE_083","RTRIM(USER_DEF_CODE_084) AS USER_DEF_CODE_084","RTRIM(USER_DEF_CODE_085) AS USER_DEF_CODE_085","RTRIM(USER_DEF_CODE_086) AS USER_DEF_CODE_086","RTRIM(USER_DEF_CODE_087) AS USER_DEF_CODE_087","RTRIM(USER_DEF_CODE_088) AS USER_DEF_CODE_088","RTRIM(USER_DEF_CODE_089) AS USER_DEF_CODE_089","RTRIM(USER_DEF_CODE_090) AS USER_DEF_CODE_090","RTRIM(USER_DEF_CODE_091) AS USER_DEF_CODE_091","RTRIM(USER_DEF_CODE_092) AS USER_DEF_CODE_092","RTRIM(USER_DEF_CODE_093) AS USER_DEF_CODE_093","RTRIM(USER_DEF_CODE_094) AS USER_DEF_CODE_094","RTRIM(USER_DEF_CODE_095) AS USER_DEF_CODE_095","RTRIM(USER_DEF_CODE_096) AS USER_DEF_CODE_096","RTRIM(USER_DEF_CODE_097) AS USER_DEF_CODE_097","RTRIM(USER_DEF_CODE_098) AS USER_DEF_CODE_098","RTRIM(USER_DEF_CODE_099) AS USER_DEF_CODE_099","RTRIM(USER_DEF_CODE_100) AS USER_DEF_CODE_100","RTRIM(EQUIPMENT_CODE) AS EQUIPMENT_CODE","RTRIM(MAIN_PURP_CODE) AS MAIN_PURP_CODE","RTRIM(SUB_PURP_CODE) AS SUB_PURP_CODE","RTRIM(BOEX_STATE_CODE) AS BOEX_STATE_CODE","RTRIM(CUSVBB_STATE_CODE) AS CUSVBB_STATE_CODE","RTRIM(MAKE_CODE) AS MAKE_CODE","RTRIM(USE_CODE) AS USE_CODE","RTRIM(MAIN_TYPE_CDE) AS MAIN_TYPE_CDE","RTRIM(SUB_TYPE_CDE) AS SUB_TYPE_CDE","RTRIM(APP_REF_NO) AS APP_REF_NO","RTRIM(PERS_BANKR_ID) AS PERS_BANKR_ID","RTRIM(CHANNEL_SRC) AS CHANNEL_SRC","RTRIM(SUB_PRIME_IND) AS SUB_PRIME_IND","RTRIM(CREDIT_SCORE) AS CREDIT_SCORE","RTRIM(PAYMENT_RECORD) AS PAYMENT_RECORD","RTRIM(OWN_BUSINESS_IND) AS OWN_BUSINESS_IND","RTRIM(STATUTORY_IND) AS STATUTORY_IND","RTRIM(JTM_STM_IND) AS JTM_STM_IND","RTRIM(SQUATTER) AS SQUATTER","MARKETERS_NAME","MARKETERS_TEAM","PARENT_COMPANY","RTRIM(BUSINESS_SOURCE) AS BUSINESS_SOURCE","RTRIM(PROMOTION_PKG_1) AS PROMOTION_PKG_1","RTRIM(PROMOTION_PKG_2) AS PROMOTION_PKG_2","RTRIM(PROMOTION_PKG_3) AS PROMOTION_PKG_3","RTRIM(PROMOTION_PKG_4) AS PROMOTION_PKG_4","RTRIM(AAT) AS AAT","AAT_DATE","REDEMP_AMT","RTRIM(CHARGEE) AS CHARGEE","RTRIM(REDEMP_CODE) AS REDEMP_CODE","RTRIM(REF_BANKER) AS REF_BANKER","RTRIM(FRS_IP_TAG) AS FRS_IP_TAG","FRS_DATE","EFF_YIELD","MRTA_DISB_DT","RTRIM(TIER_GROUP_ID) AS TIER_GROUP_ID","RTRIM(REBATE_PERC) AS REBATE_PERC","RTRIM(REASON_CD) AS REASON_CD","RTRIM(ID) AS ID","MAXIMUM_VALUE","RATE_INCR_DECR","RTRIM(DESCRIPTION) AS DESCRIPTION","RTRIM(NEG_RATE_IND) AS NEG_RATE_IND","RTRIM(TIER_METH) AS TIER_METH","RTRIM(CAPN_METHOD) AS CAPN_METHOD","RTRIM(MULTITIER_ENABLED) AS MULTITIER_ENABLED","RTRIM(IO_TRM_METHOD) AS IO_TRM_METHOD","RTRIM(ISLAMIC_PRODUCT) AS ISLAMIC_PRODUCT","RTRIM(REVOLVING_CR_IND) AS REVOLVING_CR_IND","RTRIM(STAFF_PROD_IND) AS STAFF_PROD_IND","ALT_ADD1","ALT_ADD2","ALT_ADD3","ALT_ADD4","RTRIM(ALT_POSTCODE) AS ALT_POSTCODE","RTRIM(FORGN_ADD_IND) AS FORGN_ADD_IND","RTRIM(FACILITY_CODE) AS FACILITY_CODE","AMOUNT","RTRIM(BNM_REF_NO) AS BNM_REF_NO","BNM_EXPIRY_DT","NAME1","NAME2","NAME3","SME_FIN_CAT","DEBT_SERV_RATIO","RTRIM(OLD_FI_CODE) AS OLD_FI_CODE","RTRIM(OLD_MAST_ACCT_NO) AS OLD_MAST_ACCT_NO","RTRIM(OLD_SUB_ACCT_NO) AS OLD_SUB_ACCT_NO","AAT_MAINT_DT","AAT_COUNT","INSTL_WITH_COMMN","RTRIM(CLIM_CHG_CLASS) AS CLIM_CHG_CLASS","RTRIM(GREEN_FINANCE) AS GREEN_FINANCE","PRE_DISB_DOC_ST","RTRIM(CCPT_CLASS) AS CCPT_CLASS").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'BORM_KEY_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(19)'}}, {'name': 'TRAN_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'KEY_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(23)'}}, {'name': 'BLDVNN_POST_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'BLDVTT_POST_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'PROPERTY_COST', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'PROPERTY_VALUE', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'LATEST_VAL', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'VAL_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'PURCHASE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'PROP_ADD1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(40)'}}, {'name': 'PROP_ADD2', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(40)'}}, {'name': 'PROP_ADD3', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(40)'}}, {'name': 'BLDVCC_DOCUMENT_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(14)'}}, {'name': 'BLDVFF_DOCUMENT_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(14)'}}, {'name': 'PROPERTY_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'REC_STAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'INSURE_COVER', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'POLICY_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'POLICY_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(15)'}}, {'name': 'DOCUMENT_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'DOCUMENT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'START_DATE_01', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'INSUR_AMT_DUE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'REPAYMENT_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'BALANCE_01', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'CIV_ACT_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'CIV_ACT_COD_DAT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'LGL_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'GUAR_TOWN', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(20)'}}, {'name': 'GUAR_POSTCODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(10)'}}, {'name': 'GUAR_OCC_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'GUAR_ID_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(24)'}}, {'name': 'AMOUNT_GUAR', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'GUAR_EXP_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'GUAR_NAME', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(40)'}}, {'name': 'GUAR_ADD_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(40)'}}, {'name': 'GUAR_ADD_2', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(40)'}}, {'name': 'GUAR_ADD_3', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(40)'}}, {'name': 'USER_DEF_CODE_001', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_002', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_003', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_004', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_005', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_006', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_007', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_008', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_009', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_010', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_011', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_012', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_013', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_014', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_015', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_016', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_017', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_018', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_019', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_020', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_021', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_022', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_023', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_024', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_025', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_026', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_027', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_028', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_029', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_030', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_031', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_032', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_033', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_034', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_035', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_036', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_037', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_038', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_039', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_040', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_041', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_042', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_043', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_044', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_045', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_046', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_047', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_048', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_049', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_050', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_051', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_052', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_053', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_054', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_055', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_056', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_057', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_058', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_059', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_060', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_061', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_062', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_063', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_064', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_065', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_066', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_067', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_068', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_069', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_070', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_071', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_072', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_073', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_074', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_075', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_076', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_077', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_078', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_079', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_080', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_081', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_082', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_083', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_084', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_085', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_086', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_087', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_088', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_089', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_090', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_091', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_092', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_093', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_094', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_095', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_096', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_097', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_098', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_099', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'USER_DEF_CODE_100', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'EQUIPMENT_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MAIN_PURP_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'SUB_PURP_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'BOEX_STATE_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'CUSVBB_STATE_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MAKE_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'USE_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MAIN_TYPE_CDE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'SUB_TYPE_CDE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'APP_REF_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(30)'}}, {'name': 'PERS_BANKR_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(10)'}}, {'name': 'CHANNEL_SRC', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'SUB_PRIME_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'CREDIT_SCORE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'PAYMENT_RECORD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'OWN_BUSINESS_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'STATUTORY_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'JTM_STM_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'SQUATTER', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MARKETERS_NAME', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MARKETERS_TEAM', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'PARENT_COMPANY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'BUSINESS_SOURCE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'PROMOTION_PKG_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'PROMOTION_PKG_2', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'PROMOTION_PKG_3', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'PROMOTION_PKG_4', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'AAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'AAT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'REDEMP_AMT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'CHARGEE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(20)'}}, {'name': 'REDEMP_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'REF_BANKER', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'FRS_IP_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'FRS_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'EFF_YIELD', 'type': 'decimal(5,2)', 'nullable': True, 'metadata': {}}, {'name': 'MRTA_DISB_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'TIER_GROUP_ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'REBATE_PERC', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'REASON_CD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'ID', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MAXIMUM_VALUE', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'RATE_INCR_DECR', 'type': 'decimal(13,9)', 'nullable': True, 'metadata': {}}, {'name': 'DESCRIPTION', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(20)'}}, {'name': 'NEG_RATE_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'TIER_METH', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'CAPN_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MULTITIER_ENABLED', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'IO_TRM_METHOD', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'ISLAMIC_PRODUCT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'REVOLVING_CR_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'STAFF_PROD_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'ALT_ADD1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'ALT_ADD2', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'ALT_ADD3', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'ALT_ADD4', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'ALT_POSTCODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(10)'}}, {'name': 'FORGN_ADD_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'FACILITY_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'AMOUNT', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'BNM_REF_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(30)'}}, {'name': 'BNM_EXPIRY_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'NAME1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'NAME2', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'NAME3', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'SME_FIN_CAT', 'type': 'decimal(2,0)', 'nullable': True, 'metadata': {}}, {'name': 'DEBT_SERV_RATIO', 'type': 'decimal(5,2)', 'nullable': True, 'metadata': {}}, {'name': 'OLD_FI_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(30)'}}, {'name': 'OLD_MAST_ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(30)'}}, {'name': 'OLD_SUB_ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(30)'}}, {'name': 'AAT_MAINT_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'AAT_COUNT', 'type': 'decimal(2,0)', 'nullable': True, 'metadata': {}}, {'name': 'INSTL_WITH_COMMN', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'CLIM_CHG_CLASS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'GREEN_FINANCE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'PRE_DISB_DOC_ST', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'CCPT_CLASS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}]}))
    
    spark.sql("DROP TABLE IF EXISTS NETZ_SRC_TBL_NM_ln_All_Dir_Src_v").show()
    
    print("NETZ_SRC_TBL_NM_ln_All_Dir_Src_v")
    
    print(NETZ_SRC_TBL_NM_ln_All_Dir_Src_v.schema.json())
    
    print("count:{}".format(NETZ_SRC_TBL_NM_ln_All_Dir_Src_v.count()))
    
    NETZ_SRC_TBL_NM_ln_All_Dir_Src_v.show(1000,False)
    
    NETZ_SRC_TBL_NM_ln_All_Dir_Src_v.write.mode("overwrite").saveAsTable("NETZ_SRC_TBL_NM_ln_All_Dir_Src_v")
    

@task
def V0A59(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task.pyspark(conn_id="spark-local")
def xfm_ln_All_Dir_Src_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    NETZ_SRC_TBL_NM_ln_All_Dir_Src_v=spark.table('NETZ_SRC_TBL_NM_ln_All_Dir_Src_v')
    
    xfm_ln_All_Dir_Src_Part_v=NETZ_SRC_TBL_NM_ln_All_Dir_Src_v
    
    spark.sql("DROP TABLE IF EXISTS xfm_ln_All_Dir_Src_Part_v").show()
    
    print("xfm_ln_All_Dir_Src_Part_v")
    
    print(xfm_ln_All_Dir_Src_Part_v.schema.json())
    
    print("count:{}".format(xfm_ln_All_Dir_Src_Part_v.count()))
    
    xfm_ln_All_Dir_Src_Part_v.show(1000,False)
    
    xfm_ln_All_Dir_Src_Part_v.write.mode("overwrite").saveAsTable("xfm_ln_All_Dir_Src_Part_v")
    

@task.pyspark(conn_id="spark-local")
def xfm(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    xfm_ln_All_Dir_Src_Part_v=spark.table('xfm_ln_All_Dir_Src_Part_v')
    
    xfm_v = xfm_ln_All_Dir_Src_Part_v.withColumn('A', expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM MAIN_PURP_CODE))""").cast('string').alias('A')).withColumn('B', expr("""IF(DS_ISVALID('int32', A), DS_STRINGTODECIMAL(A), 0)""").cast('integer').alias('B')).withColumn('MnTYP', expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM (IF(ISNOTNULL((MAIN_TYPE_CDE)), (MAIN_TYPE_CDE), 0))))""").cast('string').alias('MnTYP')).withColumn('vMnTYP', expr("""IF(DS_ISVALID('int32', MnTYP), DS_STRINGTODECIMAL(MnTYP), 0)""").cast('integer').alias('vMnTYP'))
    
    xfm_ln_ALL_Direct_Tgt_v = xfm_v.select(expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM BORM_KEY_1))""").cast('string').alias('B_KEY'),col('CREDIT_SCORE').cast('string').alias('MI006_CR_RATING'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM PERS_BANKR_ID)) <> '', PERS_BANKR_ID, NULL)""").cast('string').alias('MI006_AAID'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM APP_REF_NO)) <> '', APP_REF_NO, NULL)""").cast('string').alias('MI006_APP_REF_NO'),col('INSURE_COVER').cast('decimal(18,3)').alias('MI006_BLDVEE_INSURE_COVER'),col('POLICY_TYPE').cast('string').alias('MI006_BLDVEE_POLICY_TYPE'),col('POLICY_NO').cast('string').alias('MI006_BLDVEE_POLICY_NO'),col('GUAR_ADD_1').cast('string').alias('MI006_BLDVTT_GUAR_ADD1'),col('GUAR_ADD_2').cast('string').alias('MI006_BLDVTT_GUAR_ADD2'),col('GUAR_ADD_3').cast('string').alias('MI006_BLDVTT_GUAR_ADD3'),col('GUAR_OCC_CODE').cast('string').alias('MI006_BLDVTT_GUAR_OCC_CODE'),col('GUAR_POSTCODE').cast('string').alias('MI006_BLDVTT_GUAR_POST_CD'),col('GUAR_TOWN').cast('string').alias('MI006_BLDVTT_GUAR_TOWN'),expr("""IF(DS_ISVALID('int32', TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM MAKE_CODE))), DS_STRINGTODECIMAL(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM MAKE_CODE))), 0)""").cast('integer').alias('MI006_BNM_MAKE'),expr("""RIGHT(LEFT(RPAD((IF(ISNOTNULL((A)), (A), 0)), 4, '0'), 4), 2)""").cast('integer').alias('MI006_BNM_MN_PRPS'),expr("""RIGHT(CONCAT_WS('', REPEAT('0', 2), (IF(ISNOTNULL((vMnTYP)), (vMnTYP), 0))), 2)""").cast('string').alias('MI006_BNM_MN_TYP'),expr("""(IF(ISNOTNULL((MAIN_TYPE_CDE)), (MAIN_TYPE_CDE), ('00')))""").cast('string').alias('MI006_BNM_PRTCLR'),col('SUB_PURP_CODE').cast('string').alias('MI006_BNM_SUB_PRPS'),expr("""IF(DS_ISVALID('int32', TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM SUB_TYPE_CDE))), DS_STRINGTODECIMAL(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM SUB_TYPE_CDE))), 0)""").cast('integer').alias('MI006_BNM_SUB_TYP'),col('FRS_DATE').cast('integer').alias('MI006_BOEX_FRS_DATE'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM FRS_IP_TAG)) <> '', FRS_IP_TAG, NULL)""").cast('string').alias('MI006_BOEX_FRS_IP_TAG'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM JTM_STM_IND)) <> '', JTM_STM_IND, NULL)""").cast('string').alias('MI006_BOEX_JTM_STM_IND'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM PAYMENT_RECORD)) <> '', PAYMENT_RECORD, NULL)""").cast('string').alias('MI006_BOEX_PYMY_REC'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM REDEMP_CODE)) <> '', REDEMP_CODE, NULL)""").cast('string').alias('MI006_BOEX_REDEMP_CODE'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM REF_BANKER)) <> '', REF_BANKER, NULL)""").cast('string').alias('MI006_BOEX_REF_BANKER'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM SQUATTER)) <> '', SQUATTER, NULL)""").cast('string').alias('MI006_BOEX_SQUATTER'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM SUB_PRIME_IND)) <> '', SUB_PRIME_IND, NULL)""").cast('string').alias('MI006_BOEX_SUB_PRIME_IND'),col('USE_CODE').cast('string').alias('MI006_BOEX_USE_CODE'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM CHANNEL_SRC)) <> '', CHANNEL_SRC, NULL)""").cast('string').alias('MI006_BOEX_CHANNEL_SRC'),col('EQUIPMENT_CODE').cast('string').alias('MI006_EQUIPMENT_CODE'),col('BLDVTT_POST_DATE').cast('integer').alias('MI006_GAURANTOR_CREAT_DATE'),col('GUAR_ID_NO').cast('string').alias('MI006_GAURANTOR_ID_NO'),col('GUAR_NAME').cast('string').alias('MI006_GTE_NAME'),col('AMOUNT_GUAR').cast('decimal(18,3)').alias('MI006_GUA_AMOUNT'),col('MAKE_CODE').cast('string').alias('MI006_MAKE_CODE'),col('MAIN_TYPE_CDE').cast('string').alias('MI006_PARTICULAR'),col('PROPERTY_TYPE').cast('string').alias('MI006_PROPERTY_TYPE'),col('PROPERTY_COST').cast('decimal(18,3)').alias('MI006_PROPERTY_COST'),col('PROPERTY_VALUE').cast('decimal(18,3)').alias('MI006_PROP_VALUE'),col('VAL_DATE').cast('integer').alias('MI006_VAL_DATE'),col('PURCHASE_DATE').cast('integer').alias('MI006_PURCHASE_DT'),col('DESCRIPTION').cast('string').alias('MI006_TIER_DESC'),col('ID').cast('string').alias('MI006_TIER_ID'),col('MAXIMUM_VALUE').cast('decimal(18,3)').alias('MI006_TIER_MAX_VAL'),col('RATE_INCR_DECR').cast('decimal(14,9)').alias('MI006_TIER_RATE_INCR_DECR'),expr("""RIGHT(CONCAT(LPAD('0', 5, '0'), B), 5)""").cast('string').alias('MI006_BNM_SECTOR_CODE'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM OWN_BUSINESS_IND)) <> '', OWN_BUSINESS_IND, NULL)""").cast('string').alias('MI006_BOEX_OWN_BUSS_IND'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM STATUTORY_IND)) <> '', STATUTORY_IND, NULL)""").cast('string').alias('MI006_BOEX_STATUTORY_IND'),col('TRAN_TYPE').cast('string').alias('MI006_BLDVCC_TRAN_TYPE'),expr("""(IF(ISNOTNULL((KEY_1)), (KEY_1), (REPEAT(' ', 1))))""").cast('string').alias('MI006_BLDVCC_KEY_1'),col('BLDVCC_DOCUMENT_NO').cast('string').alias('MI006_BLDVCC_DOC_NO'),col('BOEX_STATE_CODE').cast('string').alias('MI006_LOAN_LOC_STATE_CODE'),col('DOCUMENT_TYPE').cast('string').alias('MI006_BLDVFF_DOCUMENT_TYPE'),col('BLDVFF_DOCUMENT_NO').cast('string').alias('MI006_BLDVFF_DOCUMENT_NO'),col('AMOUNT_GUAR').cast('decimal(18,3)').alias('MI006_BLDVTT_AMOUNT_GUAR'),col('GUAR_EXP_DATE').cast('integer').alias('MI006_BLDVTT_GUAR_EXP_DATE'),col('DOCUMENT_DATE').cast('integer').alias('MI006_BLDVFF_DOCUMENT_DATE'),col('REC_STAT').cast('string').alias('MI006_BLDVCC_REC_STAT'),col('PROP_ADD1').cast('string').alias('MI006_BLDVCC_PROP_ADD1'),col('PROP_ADD2').cast('string').alias('MI006_BLDVCC_PROP_ADD2'),col('PROP_ADD3').cast('string').alias('MI006_BLDVCC_PROP_ADD3'),col('PROPERTY_COST').cast('decimal(18,3)').alias('MI006_BLDVCC_PROPERTY_COST'),col('LATEST_VAL').cast('decimal(18,3)').alias('MI006_BLDVCC_LATEST_VAL'),col('VAL_DATE').cast('integer').alias('MI006_BLDVCC_VAL_DATE'),lit(None).cast('string').alias('MI006_BOEX_OWN_BUSINESS_IND'),col('PROPERTY_TYPE').cast('integer').alias('MI006_BLDVCC_PROPERTY_TYPE'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_001)) <> '', USER_DEF_CODE_001, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_001'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_002)) <> '', USER_DEF_CODE_002, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_002'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_003)) <> '', USER_DEF_CODE_003, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_003'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_004)) <> '', USER_DEF_CODE_004, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_004'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_005)) <> '', USER_DEF_CODE_005, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_005'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_006)) <> '', USER_DEF_CODE_006, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_006'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_007)) <> '', USER_DEF_CODE_007, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_007'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_008)) <> '', USER_DEF_CODE_008, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_008'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_009)) <> '', USER_DEF_CODE_009, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_009'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_010)) <> '', USER_DEF_CODE_010, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_010'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_011)) <> '', USER_DEF_CODE_011, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_011'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_012)) <> '', USER_DEF_CODE_012, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_012'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_013)) <> '', USER_DEF_CODE_013, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_013'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_014)) <> '', USER_DEF_CODE_014, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_014'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_015)) <> '', USER_DEF_CODE_015, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_015'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_016)) <> '', USER_DEF_CODE_016, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_016'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_017)) <> '', USER_DEF_CODE_017, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_017'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_018)) <> '', USER_DEF_CODE_018, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_018'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_019)) <> '', USER_DEF_CODE_019, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_019'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_020)) <> '', USER_DEF_CODE_020, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_020'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_021)) <> '', USER_DEF_CODE_021, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_021'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_022)) <> '', USER_DEF_CODE_022, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_022'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_023)) <> '', USER_DEF_CODE_023, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_023'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_024)) <> '', USER_DEF_CODE_024, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_024'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_025)) <> '', USER_DEF_CODE_025, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_025'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_026)) <> '', USER_DEF_CODE_026, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_026'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_027)) <> '', USER_DEF_CODE_027, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_027'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_028)) <> '', USER_DEF_CODE_028, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_028'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_029)) <> '', USER_DEF_CODE_029, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_029'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_030)) <> '', USER_DEF_CODE_030, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_030'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_031)) <> '', USER_DEF_CODE_031, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_031'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_032)) <> '', USER_DEF_CODE_032, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_032'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_033)) <> '', USER_DEF_CODE_033, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_033'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_034)) <> '', USER_DEF_CODE_034, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_034'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_035)) <> '', USER_DEF_CODE_035, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_035'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_036)) <> '', USER_DEF_CODE_036, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_036'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_037)) <> '', USER_DEF_CODE_037, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_037'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_038)) <> '', USER_DEF_CODE_038, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_038'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_039)) <> '', USER_DEF_CODE_039, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_039'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_040)) <> '', USER_DEF_CODE_040, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_040'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_041)) <> '', USER_DEF_CODE_041, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_041'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_042)) <> '', USER_DEF_CODE_042, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_042'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_043)) <> '', USER_DEF_CODE_043, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_043'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_044)) <> '', USER_DEF_CODE_044, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_044'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_045)) <> '', USER_DEF_CODE_045, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_045'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_046)) <> '', USER_DEF_CODE_046, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_046'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_047)) <> '', USER_DEF_CODE_047, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_047'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_048)) <> '', USER_DEF_CODE_048, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_048'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_049)) <> '', USER_DEF_CODE_049, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_049'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_050)) <> '', USER_DEF_CODE_050, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_050'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_051)) <> '', USER_DEF_CODE_051, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_051'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_052)) <> '', USER_DEF_CODE_052, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_052'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_053)) <> '', USER_DEF_CODE_053, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_053'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_054)) <> '', USER_DEF_CODE_054, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_054'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_055)) <> '', USER_DEF_CODE_055, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_055'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_056)) <> '', USER_DEF_CODE_056, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_056'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_057)) <> '', USER_DEF_CODE_057, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_057'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_058)) <> '', USER_DEF_CODE_058, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_058'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_059)) <> '', USER_DEF_CODE_059, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_059'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_060)) <> '', USER_DEF_CODE_060, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_060'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_061)) <> '', USER_DEF_CODE_061, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_061'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_062)) <> '', USER_DEF_CODE_062, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_062'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_063)) <> '', USER_DEF_CODE_063, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_063'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_064)) <> '', USER_DEF_CODE_064, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_064'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_065)) <> '', USER_DEF_CODE_065, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_065'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_066)) <> '', USER_DEF_CODE_066, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_066'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_067)) <> '', USER_DEF_CODE_067, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_067'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_068)) <> '', USER_DEF_CODE_068, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_068'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_069)) <> '', USER_DEF_CODE_069, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_069'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_070)) <> '', USER_DEF_CODE_070, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_070'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_071)) <> '', USER_DEF_CODE_071, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_071'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_072)) <> '', USER_DEF_CODE_072, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_072'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_073)) <> '', USER_DEF_CODE_073, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_073'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_074)) <> '', USER_DEF_CODE_074, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_074'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_075)) <> '', USER_DEF_CODE_075, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_075'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_076)) <> '', USER_DEF_CODE_076, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_076'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_077)) <> '', USER_DEF_CODE_077, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_077'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_078)) <> '', USER_DEF_CODE_078, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_078'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_079)) <> '', USER_DEF_CODE_079, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_079'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_080)) <> '', USER_DEF_CODE_080, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_080'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_081)) <> '', USER_DEF_CODE_081, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_081'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_082)) <> '', USER_DEF_CODE_082, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_082'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_083)) <> '', USER_DEF_CODE_083, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_083'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_084)) <> '', USER_DEF_CODE_084, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_084'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_085)) <> '', USER_DEF_CODE_085, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_085'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_086)) <> '', USER_DEF_CODE_086, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_086'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_087)) <> '', USER_DEF_CODE_087, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_087'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_088)) <> '', USER_DEF_CODE_088, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_088'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_089)) <> '', USER_DEF_CODE_089, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_089'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_090)) <> '', USER_DEF_CODE_090, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_090'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_091)) <> '', USER_DEF_CODE_091, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_091'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_092)) <> '', USER_DEF_CODE_092, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_092'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_093)) <> '', USER_DEF_CODE_093, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_093'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_094)) <> '', USER_DEF_CODE_094, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_094'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_095)) <> '', USER_DEF_CODE_095, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_095'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_096)) <> '', USER_DEF_CODE_096, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_096'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_097)) <> '', USER_DEF_CODE_097, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_097'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_098)) <> '', USER_DEF_CODE_098, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_098'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_099)) <> '', USER_DEF_CODE_099, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_099'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM USER_DEF_CODE_100)) <> '', USER_DEF_CODE_100, NULL)""").cast('string').alias('MI006_USER_DEF_CODE_100'),col('ALT_ADD1').cast('string').alias('MI006_ADDR_1'),col('ALT_ADD2').cast('string').alias('MI006_ADDR_2'),col('ALT_ADD3').cast('string').alias('MI006_ADDR_3'),col('ALT_ADD4').cast('string').alias('MI006_ADDR_4'),col('ALT_POSTCODE').cast('string').alias('MI006_POST_CODE'),col('CUSVBB_STATE_CODE').cast('string').alias('MI006_STATE_CODE'),col('FORGN_ADD_IND').cast('string').alias('MI006_FOREIGN_ADDR_IND'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM MARKETERS_NAME)) <> '', MARKETERS_NAME, NULL)""").cast('string').alias('MI006_MARKETERS_NAME'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM MARKETERS_TEAM)) <> '', MARKETERS_TEAM, NULL)""").cast('string').alias('MI006_MARKETERS_TEAM'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM PARENT_COMPANY)) <> '', PARENT_COMPANY, NULL)""").cast('string').alias('MI006_PARENT_COMPANY'),col('EFF_YIELD').cast('decimal(6,2)').alias('MI006_EFF_YIELD'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM PROMOTION_PKG_1)) <> '', PROMOTION_PKG_1, NULL)""").cast('string').alias('MI006_PROMOTION_PKG_1'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM PROMOTION_PKG_2)) <> '', PROMOTION_PKG_2, NULL)""").cast('string').alias('MI006_PROMOTION_PKG_2'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM PROMOTION_PKG_3)) <> '', PROMOTION_PKG_3, NULL)""").cast('string').alias('MI006_PROMOTION_PKG_3'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM PROMOTION_PKG_4)) <> '', PROMOTION_PKG_4, NULL)""").cast('string').alias('MI006_PROMOTION_PKG_4'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM BUSINESS_SOURCE)) <> '', BUSINESS_SOURCE, NULL)""").cast('string').alias('MI006_BUSINESS_SOURCE'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM CHARGEE)) <> '', CHARGEE, NULL)""").cast('string').alias('MI006_CHARGEE'),col('REDEMP_AMT').cast('decimal(18,3)').alias('MI006_REDEMP_AMT'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM AAT)) <> '', AAT, NULL)""").cast('string').alias('MI006_AAT'),col('AAT_DATE').cast('integer').alias('MI006_AAT_DATE'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM MAIN_TYPE_CDE)) = '', NULL, MAIN_TYPE_CDE)""").cast('string').alias('MI006_BOEX_MAIN_TYPE_CDE'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM SUB_TYPE_CDE)) <> '', SUB_TYPE_CDE, NULL)""").cast('string').alias('MI006_BOEX_SUB_TYPE_CDE'),expr("""IF(ISNOTNULL(MRTA_DISB_DT), IF(MRTA_DISB_DT = 0, 'N', 'Y'), NULL)""").cast('string').alias('MI006_MRTA_IND'),col('FACILITY_CODE').cast('string').alias('MI006_FAC_CODE'),lit(0).cast('decimal(18,3)').alias('MI006_STNDG_INST_AMT'),expr("""IF(TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM BNM_REF_NO)) <> '', BNM_REF_NO, NULL)""").cast('string').alias('MIS006_BOEX_BNM_REF_NO'),col('BNM_EXPIRY_DT').cast('integer').alias('MIS006_BOEX_BNM_EXPIRY_DT'),col('NAME1').cast('string').alias('MI006_ACCT_NAME_1'),col('NAME2').cast('string').alias('MI006_ACCT_NAME_2'),col('NAME3').cast('string').alias('MI006_ACCT_NAME_3'),col('SME_FIN_CAT').cast('decimal(2,0)').alias('MI006_SME_FIN_CAT'),col('DEBT_SERV_RATIO').cast('decimal(5,2)').alias('MI006_DEBT_SERV_RATIO'),col('OLD_FI_CODE').cast('string').alias('MI006_OLD_FI_CODE'),col('OLD_MAST_ACCT_NO').cast('string').alias('MI006_OLD_MAST_ACCT_NO'),col('OLD_SUB_ACCT_NO').cast('string').alias('MI006_OLD_SUB_ACCT_NO'),col('AAT_MAINT_DT').cast('integer').alias('MI006_AAT_MAINT_DATE'),col('AAT_COUNT').cast('decimal(2,0)').alias('MI006_AAT_COUNT'),col('INSTL_WITH_COMMN').cast('decimal(17,3)').alias('MI006_INSTL_WITH_COMMN'),col('CLIM_CHG_CLASS').cast('string').alias('MI006_CLIM_CHG_CLASS'),col('GREEN_FINANCE').cast('string').alias('MI006_GREEN_FINANCE'),col('PRE_DISB_DOC_ST').cast('string').alias('MI006_PRE_DISB_DOC_STAT'),col('CCPT_CLASS').cast('string').alias('MI006_CCPT_CLASS'))
    
    xfm_ln_ALL_Direct_Tgt_v = xfm_ln_ALL_Direct_Tgt_v.selectExpr("B_KEY","MI006_CR_RATING","MI006_AAID","MI006_APP_REF_NO","MI006_BLDVEE_INSURE_COVER","RTRIM(MI006_BLDVEE_POLICY_TYPE) AS MI006_BLDVEE_POLICY_TYPE","MI006_BLDVEE_POLICY_NO","MI006_BLDVTT_GUAR_ADD1","MI006_BLDVTT_GUAR_ADD2","MI006_BLDVTT_GUAR_ADD3","RTRIM(MI006_BLDVTT_GUAR_OCC_CODE) AS MI006_BLDVTT_GUAR_OCC_CODE","MI006_BLDVTT_GUAR_POST_CD","MI006_BLDVTT_GUAR_TOWN","MI006_BNM_MAKE","MI006_BNM_MN_PRPS","MI006_BNM_MN_TYP","MI006_BNM_PRTCLR","RTRIM(MI006_BNM_SUB_PRPS) AS MI006_BNM_SUB_PRPS","MI006_BNM_SUB_TYP","MI006_BOEX_FRS_DATE","RTRIM(MI006_BOEX_FRS_IP_TAG) AS MI006_BOEX_FRS_IP_TAG","RTRIM(MI006_BOEX_JTM_STM_IND) AS MI006_BOEX_JTM_STM_IND","RTRIM(MI006_BOEX_PYMY_REC) AS MI006_BOEX_PYMY_REC","MI006_BOEX_REDEMP_CODE","MI006_BOEX_REF_BANKER","RTRIM(MI006_BOEX_SQUATTER) AS MI006_BOEX_SQUATTER","RTRIM(MI006_BOEX_SUB_PRIME_IND) AS MI006_BOEX_SUB_PRIME_IND","RTRIM(MI006_BOEX_USE_CODE) AS MI006_BOEX_USE_CODE","RTRIM(MI006_BOEX_CHANNEL_SRC) AS MI006_BOEX_CHANNEL_SRC","RTRIM(MI006_EQUIPMENT_CODE) AS MI006_EQUIPMENT_CODE","MI006_GAURANTOR_CREAT_DATE","MI006_GAURANTOR_ID_NO","MI006_GTE_NAME","MI006_GUA_AMOUNT","RTRIM(MI006_MAKE_CODE) AS MI006_MAKE_CODE","RTRIM(MI006_PARTICULAR) AS MI006_PARTICULAR","RTRIM(MI006_PROPERTY_TYPE) AS MI006_PROPERTY_TYPE","MI006_PROPERTY_COST","MI006_PROP_VALUE","MI006_VAL_DATE","MI006_PURCHASE_DT","MI006_TIER_DESC","MI006_TIER_ID","MI006_TIER_MAX_VAL","MI006_TIER_RATE_INCR_DECR","MI006_BNM_SECTOR_CODE","RTRIM(MI006_BOEX_OWN_BUSS_IND) AS MI006_BOEX_OWN_BUSS_IND","RTRIM(MI006_BOEX_STATUTORY_IND) AS MI006_BOEX_STATUTORY_IND","RTRIM(MI006_BLDVCC_TRAN_TYPE) AS MI006_BLDVCC_TRAN_TYPE","MI006_BLDVCC_KEY_1","MI006_BLDVCC_DOC_NO","RTRIM(MI006_LOAN_LOC_STATE_CODE) AS MI006_LOAN_LOC_STATE_CODE","RTRIM(MI006_BLDVFF_DOCUMENT_TYPE) AS MI006_BLDVFF_DOCUMENT_TYPE","MI006_BLDVFF_DOCUMENT_NO","MI006_BLDVTT_AMOUNT_GUAR","MI006_BLDVTT_GUAR_EXP_DATE","MI006_BLDVFF_DOCUMENT_DATE","RTRIM(MI006_BLDVCC_REC_STAT) AS MI006_BLDVCC_REC_STAT","MI006_BLDVCC_PROP_ADD1","MI006_BLDVCC_PROP_ADD2","MI006_BLDVCC_PROP_ADD3","MI006_BLDVCC_PROPERTY_COST","MI006_BLDVCC_LATEST_VAL","MI006_BLDVCC_VAL_DATE","RTRIM(MI006_BOEX_OWN_BUSINESS_IND) AS MI006_BOEX_OWN_BUSINESS_IND","MI006_BLDVCC_PROPERTY_TYPE","RTRIM(MI006_USER_DEF_CODE_001) AS MI006_USER_DEF_CODE_001","RTRIM(MI006_USER_DEF_CODE_002) AS MI006_USER_DEF_CODE_002","RTRIM(MI006_USER_DEF_CODE_003) AS MI006_USER_DEF_CODE_003","RTRIM(MI006_USER_DEF_CODE_004) AS MI006_USER_DEF_CODE_004","RTRIM(MI006_USER_DEF_CODE_005) AS MI006_USER_DEF_CODE_005","RTRIM(MI006_USER_DEF_CODE_006) AS MI006_USER_DEF_CODE_006","RTRIM(MI006_USER_DEF_CODE_007) AS MI006_USER_DEF_CODE_007","RTRIM(MI006_USER_DEF_CODE_008) AS MI006_USER_DEF_CODE_008","RTRIM(MI006_USER_DEF_CODE_009) AS MI006_USER_DEF_CODE_009","RTRIM(MI006_USER_DEF_CODE_010) AS MI006_USER_DEF_CODE_010","RTRIM(MI006_USER_DEF_CODE_011) AS MI006_USER_DEF_CODE_011","RTRIM(MI006_USER_DEF_CODE_012) AS MI006_USER_DEF_CODE_012","RTRIM(MI006_USER_DEF_CODE_013) AS MI006_USER_DEF_CODE_013","RTRIM(MI006_USER_DEF_CODE_014) AS MI006_USER_DEF_CODE_014","RTRIM(MI006_USER_DEF_CODE_015) AS MI006_USER_DEF_CODE_015","RTRIM(MI006_USER_DEF_CODE_016) AS MI006_USER_DEF_CODE_016","RTRIM(MI006_USER_DEF_CODE_017) AS MI006_USER_DEF_CODE_017","RTRIM(MI006_USER_DEF_CODE_018) AS MI006_USER_DEF_CODE_018","RTRIM(MI006_USER_DEF_CODE_019) AS MI006_USER_DEF_CODE_019","RTRIM(MI006_USER_DEF_CODE_020) AS MI006_USER_DEF_CODE_020","RTRIM(MI006_USER_DEF_CODE_021) AS MI006_USER_DEF_CODE_021","RTRIM(MI006_USER_DEF_CODE_022) AS MI006_USER_DEF_CODE_022","RTRIM(MI006_USER_DEF_CODE_023) AS MI006_USER_DEF_CODE_023","RTRIM(MI006_USER_DEF_CODE_024) AS MI006_USER_DEF_CODE_024","RTRIM(MI006_USER_DEF_CODE_025) AS MI006_USER_DEF_CODE_025","RTRIM(MI006_USER_DEF_CODE_026) AS MI006_USER_DEF_CODE_026","RTRIM(MI006_USER_DEF_CODE_027) AS MI006_USER_DEF_CODE_027","RTRIM(MI006_USER_DEF_CODE_028) AS MI006_USER_DEF_CODE_028","RTRIM(MI006_USER_DEF_CODE_029) AS MI006_USER_DEF_CODE_029","RTRIM(MI006_USER_DEF_CODE_030) AS MI006_USER_DEF_CODE_030","RTRIM(MI006_USER_DEF_CODE_031) AS MI006_USER_DEF_CODE_031","RTRIM(MI006_USER_DEF_CODE_032) AS MI006_USER_DEF_CODE_032","RTRIM(MI006_USER_DEF_CODE_033) AS MI006_USER_DEF_CODE_033","RTRIM(MI006_USER_DEF_CODE_034) AS MI006_USER_DEF_CODE_034","RTRIM(MI006_USER_DEF_CODE_035) AS MI006_USER_DEF_CODE_035","RTRIM(MI006_USER_DEF_CODE_036) AS MI006_USER_DEF_CODE_036","RTRIM(MI006_USER_DEF_CODE_037) AS MI006_USER_DEF_CODE_037","RTRIM(MI006_USER_DEF_CODE_038) AS MI006_USER_DEF_CODE_038","RTRIM(MI006_USER_DEF_CODE_039) AS MI006_USER_DEF_CODE_039","RTRIM(MI006_USER_DEF_CODE_040) AS MI006_USER_DEF_CODE_040","RTRIM(MI006_USER_DEF_CODE_041) AS MI006_USER_DEF_CODE_041","RTRIM(MI006_USER_DEF_CODE_042) AS MI006_USER_DEF_CODE_042","RTRIM(MI006_USER_DEF_CODE_043) AS MI006_USER_DEF_CODE_043","RTRIM(MI006_USER_DEF_CODE_044) AS MI006_USER_DEF_CODE_044","RTRIM(MI006_USER_DEF_CODE_045) AS MI006_USER_DEF_CODE_045","RTRIM(MI006_USER_DEF_CODE_046) AS MI006_USER_DEF_CODE_046","RTRIM(MI006_USER_DEF_CODE_047) AS MI006_USER_DEF_CODE_047","RTRIM(MI006_USER_DEF_CODE_048) AS MI006_USER_DEF_CODE_048","RTRIM(MI006_USER_DEF_CODE_049) AS MI006_USER_DEF_CODE_049","RTRIM(MI006_USER_DEF_CODE_050) AS MI006_USER_DEF_CODE_050","RTRIM(MI006_USER_DEF_CODE_051) AS MI006_USER_DEF_CODE_051","RTRIM(MI006_USER_DEF_CODE_052) AS MI006_USER_DEF_CODE_052","RTRIM(MI006_USER_DEF_CODE_053) AS MI006_USER_DEF_CODE_053","RTRIM(MI006_USER_DEF_CODE_054) AS MI006_USER_DEF_CODE_054","RTRIM(MI006_USER_DEF_CODE_055) AS MI006_USER_DEF_CODE_055","RTRIM(MI006_USER_DEF_CODE_056) AS MI006_USER_DEF_CODE_056","RTRIM(MI006_USER_DEF_CODE_057) AS MI006_USER_DEF_CODE_057","RTRIM(MI006_USER_DEF_CODE_058) AS MI006_USER_DEF_CODE_058","RTRIM(MI006_USER_DEF_CODE_059) AS MI006_USER_DEF_CODE_059","RTRIM(MI006_USER_DEF_CODE_060) AS MI006_USER_DEF_CODE_060","RTRIM(MI006_USER_DEF_CODE_061) AS MI006_USER_DEF_CODE_061","RTRIM(MI006_USER_DEF_CODE_062) AS MI006_USER_DEF_CODE_062","RTRIM(MI006_USER_DEF_CODE_063) AS MI006_USER_DEF_CODE_063","RTRIM(MI006_USER_DEF_CODE_064) AS MI006_USER_DEF_CODE_064","RTRIM(MI006_USER_DEF_CODE_065) AS MI006_USER_DEF_CODE_065","RTRIM(MI006_USER_DEF_CODE_066) AS MI006_USER_DEF_CODE_066","RTRIM(MI006_USER_DEF_CODE_067) AS MI006_USER_DEF_CODE_067","RTRIM(MI006_USER_DEF_CODE_068) AS MI006_USER_DEF_CODE_068","RTRIM(MI006_USER_DEF_CODE_069) AS MI006_USER_DEF_CODE_069","RTRIM(MI006_USER_DEF_CODE_070) AS MI006_USER_DEF_CODE_070","RTRIM(MI006_USER_DEF_CODE_071) AS MI006_USER_DEF_CODE_071","RTRIM(MI006_USER_DEF_CODE_072) AS MI006_USER_DEF_CODE_072","RTRIM(MI006_USER_DEF_CODE_073) AS MI006_USER_DEF_CODE_073","RTRIM(MI006_USER_DEF_CODE_074) AS MI006_USER_DEF_CODE_074","RTRIM(MI006_USER_DEF_CODE_075) AS MI006_USER_DEF_CODE_075","RTRIM(MI006_USER_DEF_CODE_076) AS MI006_USER_DEF_CODE_076","RTRIM(MI006_USER_DEF_CODE_077) AS MI006_USER_DEF_CODE_077","RTRIM(MI006_USER_DEF_CODE_078) AS MI006_USER_DEF_CODE_078","RTRIM(MI006_USER_DEF_CODE_079) AS MI006_USER_DEF_CODE_079","RTRIM(MI006_USER_DEF_CODE_080) AS MI006_USER_DEF_CODE_080","RTRIM(MI006_USER_DEF_CODE_081) AS MI006_USER_DEF_CODE_081","RTRIM(MI006_USER_DEF_CODE_082) AS MI006_USER_DEF_CODE_082","RTRIM(MI006_USER_DEF_CODE_083) AS MI006_USER_DEF_CODE_083","RTRIM(MI006_USER_DEF_CODE_084) AS MI006_USER_DEF_CODE_084","RTRIM(MI006_USER_DEF_CODE_085) AS MI006_USER_DEF_CODE_085","RTRIM(MI006_USER_DEF_CODE_086) AS MI006_USER_DEF_CODE_086","RTRIM(MI006_USER_DEF_CODE_087) AS MI006_USER_DEF_CODE_087","RTRIM(MI006_USER_DEF_CODE_088) AS MI006_USER_DEF_CODE_088","RTRIM(MI006_USER_DEF_CODE_089) AS MI006_USER_DEF_CODE_089","RTRIM(MI006_USER_DEF_CODE_090) AS MI006_USER_DEF_CODE_090","RTRIM(MI006_USER_DEF_CODE_091) AS MI006_USER_DEF_CODE_091","RTRIM(MI006_USER_DEF_CODE_092) AS MI006_USER_DEF_CODE_092","RTRIM(MI006_USER_DEF_CODE_093) AS MI006_USER_DEF_CODE_093","RTRIM(MI006_USER_DEF_CODE_094) AS MI006_USER_DEF_CODE_094","RTRIM(MI006_USER_DEF_CODE_095) AS MI006_USER_DEF_CODE_095","RTRIM(MI006_USER_DEF_CODE_096) AS MI006_USER_DEF_CODE_096","RTRIM(MI006_USER_DEF_CODE_097) AS MI006_USER_DEF_CODE_097","RTRIM(MI006_USER_DEF_CODE_098) AS MI006_USER_DEF_CODE_098","RTRIM(MI006_USER_DEF_CODE_099) AS MI006_USER_DEF_CODE_099","RTRIM(MI006_USER_DEF_CODE_100) AS MI006_USER_DEF_CODE_100","MI006_ADDR_1","MI006_ADDR_2","MI006_ADDR_3","MI006_ADDR_4","MI006_POST_CODE","RTRIM(MI006_STATE_CODE) AS MI006_STATE_CODE","RTRIM(MI006_FOREIGN_ADDR_IND) AS MI006_FOREIGN_ADDR_IND","MI006_MARKETERS_NAME","MI006_MARKETERS_TEAM","MI006_PARENT_COMPANY","MI006_EFF_YIELD","MI006_PROMOTION_PKG_1","MI006_PROMOTION_PKG_2","MI006_PROMOTION_PKG_3","MI006_PROMOTION_PKG_4","MI006_BUSINESS_SOURCE","MI006_CHARGEE","MI006_REDEMP_AMT","RTRIM(MI006_AAT) AS MI006_AAT","MI006_AAT_DATE","RTRIM(MI006_BOEX_MAIN_TYPE_CDE) AS MI006_BOEX_MAIN_TYPE_CDE","RTRIM(MI006_BOEX_SUB_TYPE_CDE) AS MI006_BOEX_SUB_TYPE_CDE","RTRIM(MI006_MRTA_IND) AS MI006_MRTA_IND","RTRIM(MI006_FAC_CODE) AS MI006_FAC_CODE","MI006_STNDG_INST_AMT","MIS006_BOEX_BNM_REF_NO","MIS006_BOEX_BNM_EXPIRY_DT","MI006_ACCT_NAME_1","MI006_ACCT_NAME_2","MI006_ACCT_NAME_3","MI006_SME_FIN_CAT","MI006_DEBT_SERV_RATIO","MI006_OLD_FI_CODE","MI006_OLD_MAST_ACCT_NO","MI006_OLD_SUB_ACCT_NO","MI006_AAT_MAINT_DATE","MI006_AAT_COUNT","MI006_INSTL_WITH_COMMN","MI006_CLIM_CHG_CLASS","MI006_GREEN_FINANCE","MI006_PRE_DISB_DOC_STAT","MI006_CCPT_CLASS").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CR_RATING', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AAID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_APP_REF_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVEE_INSURE_COVER', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVEE_POLICY_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BLDVEE_POLICY_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVTT_GUAR_ADD1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVTT_GUAR_ADD2', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVTT_GUAR_ADD3', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVTT_GUAR_OCC_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BLDVTT_GUAR_POST_CD', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVTT_GUAR_TOWN', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BNM_MAKE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BNM_MN_PRPS', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BNM_MN_TYP', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BNM_PRTCLR', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BNM_SUB_PRPS', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(4)'}}, {'name': 'MI006_BNM_SUB_TYP', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOEX_FRS_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOEX_FRS_IP_TAG', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOEX_JTM_STM_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_BOEX_PYMY_REC', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOEX_REDEMP_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOEX_REF_BANKER', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOEX_SQUATTER', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOEX_SUB_PRIME_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOEX_USE_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BOEX_CHANNEL_SRC', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_EQUIPMENT_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_GAURANTOR_CREAT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GAURANTOR_ID_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GTE_NAME', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GUA_AMOUNT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MAKE_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_PARTICULAR', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_PROPERTY_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_PROPERTY_COST', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PROP_VALUE', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_VAL_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PURCHASE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TIER_DESC', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TIER_ID', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TIER_MAX_VAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_TIER_RATE_INCR_DECR', 'type': 'decimal(14,9)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BNM_SECTOR_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOEX_OWN_BUSS_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BOEX_STATUTORY_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BLDVCC_TRAN_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BLDVCC_KEY_1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVCC_DOC_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LOAN_LOC_STATE_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BLDVFF_DOCUMENT_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BLDVFF_DOCUMENT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVTT_AMOUNT_GUAR', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVTT_GUAR_EXP_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVFF_DOCUMENT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVCC_REC_STAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BLDVCC_PROP_ADD1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVCC_PROP_ADD2', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVCC_PROP_ADD3', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVCC_PROPERTY_COST', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVCC_LATEST_VAL', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVCC_VAL_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOEX_OWN_BUSINESS_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_BLDVCC_PROPERTY_TYPE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_USER_DEF_CODE_001', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_002', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_003', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_004', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_005', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_006', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_007', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_008', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_009', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_010', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_011', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_012', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_013', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_014', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_015', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_016', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_017', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_018', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_019', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_020', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_021', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_022', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_023', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_024', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_025', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_026', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_027', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_028', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_029', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_030', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_031', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_032', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_033', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_034', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_035', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_036', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_037', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_038', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_039', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_040', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_041', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_042', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_043', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_044', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_045', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_046', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_047', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_048', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_049', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_050', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_051', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_052', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_053', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_054', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_055', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_056', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_057', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_058', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_059', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_060', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_061', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_062', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_063', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_064', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_065', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_066', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_067', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_068', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_069', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_070', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_071', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_072', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_073', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_074', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_075', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_076', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_077', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_078', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_079', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_080', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_081', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_082', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_083', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_084', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_085', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_086', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_087', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_088', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_089', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_090', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_091', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_092', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_093', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_094', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_095', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_096', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_097', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_098', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_099', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_USER_DEF_CODE_100', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_ADDR_1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADDR_2', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADDR_3', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADDR_4', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_POST_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_STATE_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(3)'}}, {'name': 'MI006_FOREIGN_ADDR_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_MARKETERS_NAME', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MARKETERS_TEAM', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PARENT_COMPANY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_YIELD', 'type': 'decimal(6,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PROMOTION_PKG_1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PROMOTION_PKG_2', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PROMOTION_PKG_3', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PROMOTION_PKG_4', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BUSINESS_SOURCE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CHARGEE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_REDEMP_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AAT', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_AAT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BOEX_MAIN_TYPE_CDE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_BOEX_SUB_TYPE_CDE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(2)'}}, {'name': 'MI006_MRTA_IND', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_FAC_CODE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(5)'}}, {'name': 'MI006_STNDG_INST_AMT', 'type': 'decimal(18,3)', 'nullable': True, 'metadata': {}}, {'name': 'MIS006_BOEX_BNM_REF_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MIS006_BOEX_BNM_EXPIRY_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACCT_NAME_1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACCT_NAME_2', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACCT_NAME_3', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_SME_FIN_CAT', 'type': 'decimal(2,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_DEBT_SERV_RATIO', 'type': 'decimal(5,2)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OLD_FI_CODE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OLD_MAST_ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_OLD_SUB_ACCT_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AAT_MAINT_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_AAT_COUNT', 'type': 'decimal(2,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INSTL_WITH_COMMN', 'type': 'decimal(17,3)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CLIM_CHG_CLASS', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_GREEN_FINANCE', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_PRE_DISB_DOC_STAT', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_CCPT_CLASS', 'type': 'string', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS xfm_ln_ALL_Direct_Tgt_v").show()
    
    print("xfm_ln_ALL_Direct_Tgt_v")
    
    print(xfm_ln_ALL_Direct_Tgt_v.schema.json())
    
    print("count:{}".format(xfm_ln_ALL_Direct_Tgt_v.count()))
    
    xfm_ln_ALL_Direct_Tgt_v.show(1000,False)
    
    xfm_ln_ALL_Direct_Tgt_v.write.mode("overwrite").saveAsTable("xfm_ln_ALL_Direct_Tgt_v")
    

@task.pyspark(conn_id="spark-local")
def TGT_Direct_All_ln_ALL_Direct_Tgt_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    xfm_ln_ALL_Direct_Tgt_v=spark.table('xfm_ln_ALL_Direct_Tgt_v')
    
    TGT_Direct_All_ln_ALL_Direct_Tgt_Part_v=xfm_ln_ALL_Direct_Tgt_v
    
    spark.sql("DROP TABLE IF EXISTS TGT_Direct_All_ln_ALL_Direct_Tgt_Part_v").show()
    
    print("TGT_Direct_All_ln_ALL_Direct_Tgt_Part_v")
    
    print(TGT_Direct_All_ln_ALL_Direct_Tgt_Part_v.schema.json())
    
    print("count:{}".format(TGT_Direct_All_ln_ALL_Direct_Tgt_Part_v.count()))
    
    TGT_Direct_All_ln_ALL_Direct_Tgt_Part_v.show(1000,False)
    
    TGT_Direct_All_ln_ALL_Direct_Tgt_Part_v.write.mode("overwrite").saveAsTable("TGT_Direct_All_ln_ALL_Direct_Tgt_Part_v")
    

@task.pyspark(conn_id="spark-local")
def TGT_Direct_All(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    TGT_Direct_All_ln_ALL_Direct_Tgt_Part_v=spark.table('TGT_Direct_All_ln_ALL_Direct_Tgt_Part_v')
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    locations = Template('{{dbdir.pPROCESSING_DIR}}MIS006_DIRECT_Col.ds').render(job_params)
    
    log.info("write dataset files to "+locations)
    
    spark.table("TGT_Direct_All_ln_ALL_Direct_Tgt_Part_v").write.mode("overwrite").format("parquet").save(locations)
    

####################################[Main]###################################
import airflow
with DAG(
    dag_id="job_DBdirect_Mis006_BLDVTT_ALL_Extr_POC",
    start_date=airflow.utils.dates.days_ago(1),
    tags=['datastage'],
) as dag:
    
    job_DBdirect_Mis006_BLDVTT_ALL_Extr_POC_task = job_DBdirect_Mis006_BLDVTT_ALL_Extr_POC()
    
    Job_VIEW_task = Job_VIEW()
    
    NETZ_SRC_TBL_NM_task = NETZ_SRC_TBL_NM()
    
    V0A59_task = V0A59()
    
    xfm_ln_All_Dir_Src_Part_task = xfm_ln_All_Dir_Src_Part()
    
    xfm_task = xfm()
    
    TGT_Direct_All_ln_ALL_Direct_Tgt_Part_task = TGT_Direct_All_ln_ALL_Direct_Tgt_Part()
    
    TGT_Direct_All_task = TGT_Direct_All()
    
    
    job_DBdirect_Mis006_BLDVTT_ALL_Extr_POC_task >> Job_VIEW_task
    
    Job_VIEW_task >> NETZ_SRC_TBL_NM_task
    
    Job_VIEW_task >> V0A59_task
    
    NETZ_SRC_TBL_NM_task >> xfm_ln_All_Dir_Src_Part_task
    
    xfm_ln_All_Dir_Src_Part_task >> xfm_task
    
    xfm_task >> TGT_Direct_All_ln_ALL_Direct_Tgt_Part_task
    
    TGT_Direct_All_ln_ALL_Direct_Tgt_Part_task >> TGT_Direct_All_task
    



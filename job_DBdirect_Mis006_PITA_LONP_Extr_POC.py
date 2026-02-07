
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-04 11:00:23
# @Author  : cloudera
# @File    : job_DBdirect_Mis006_PITA_LONP_Extr_POC.py
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
from pyspark.sql.window import Window
import json
import logging
import pendulum
import pyspark.sql.functions as F
import textwrap

@task
def job_DBdirect_Mis006_PITA_LONP_Extr_POC(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def Job_VIEW(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task.pyspark(conn_id="spark-local")
def NETZ_SRC_TBL_NM2(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    catalog=Variable.get("ICEBERG_CATALOG_NAME", default_var="iceberg")
    
    #spark.sql(f"use {catalog}.default").show()
    
    
    
    
    
    sql=Template("""SELECT
    
        KEY_1 AS BORM_KEY_1,
    
        SEQ,
    
        MI006_MEMB_CUST_AC,
    
        MI006_RATE_REPRICE_TYPE,
    
        MI006_NEXT_REPRICE_DATE,
    
        MI006_LAST_REPRICE_DATE
    
    FROM (
    
        SELECT
    
            BORM.KEY_1,
    
            -- Derivation for MD (Maturity date) --
    
            SUBSTRING(BORM.KEY_1, 4, 16) AS MI006_MEMB_CUST_AC,
    
            DATE_FORMAT(
    
                DATE_ADD(TO_DATE('1899-12-30'), 
    
                    CAST(
    
                        CASE 
    
                            WHEN BORM.DUE_STRT_DATE = 0 THEN 
    
                                CASE 
    
                                    WHEN BORM.INT_STRT_DATE = 0 THEN BORM.APPLIC_ISSUE_DATE 
    
                                    ELSE BORM.INT_STRT_DATE 
    
                                END
    
                            ELSE BORM.DUE_STRT_DATE 
    
                        END AS INT
    
                    )
    
                ), 
    
                'yyyyMMdd'
    
            ) AS DD,
    
            
    
            CASE 
    
                WHEN BORM.TERM_BASIS = 'D' THEN 
    
                    DATE_FORMAT(
    
                        DATE_ADD(
    
                            DATE_ADD(TO_DATE('1899-12-30'), 
    
                                CAST(
    
                                    CASE 
    
                                        WHEN BORM.DUE_STRT_DATE = 0 THEN 
    
                                            CASE 
    
                                                WHEN BORM.INT_STRT_DATE = 0 THEN BORM.APPLIC_ISSUE_DATE 
    
                                                ELSE BORM.INT_STRT_DATE 
    
                                            END
    
                                        ELSE BORM.DUE_STRT_DATE 
    
                                    END AS INT
    
                                )
    
                            ), 
    
                            CAST(BORM.LOAN_TRM AS INT) - 1
    
                        ), 
    
                        'yyyyMMdd'
    
                    )
    
                WHEN BORM.TERM_BASIS = 'M' THEN 
    
                    DATE_FORMAT(
    
                        ADD_MONTHS(
    
                            DATE_ADD(TO_DATE('1899-12-30'), 
    
                                CAST(
    
                                    CASE 
    
                                        WHEN BORM.DUE_STRT_DATE = 0 THEN 
    
                                            CASE 
    
                                                WHEN BORM.INT_STRT_DATE = 0 THEN BORM.APPLIC_ISSUE_DATE 
    
                                                ELSE BORM.INT_STRT_DATE 
    
                                            END
    
                                        ELSE BORM.DUE_STRT_DATE 
    
                                    END AS INT
    
                                )
    
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
    
                        WHEN RIGHT(CONCAT('00', LONP.REPAY_DAY), 2) = '00' THEN SUBSTRING(MD, 7, 2)
    
                        ELSE RIGHT(CONCAT('00', LONP.REPAY_DAY), 2)
    
                    END
    
                ELSE BORM.REPAY_DAY 
    
            END AS RD,
    
            
    
            -- Begin CR 32956 --
    
            CASE 
    
                WHEN BOIS.ORIGINAL_EXP_DATE <> 0 THEN 
    
                    CAST(DATE_FORMAT(
    
                        DATE_ADD(TO_DATE('1899-12-30'), CAST(BOIS.ORIGINAL_EXP_DATE AS INT)), 
    
                        'yyyyMMdd'
    
                    ) AS INT)
    
                ELSE 
    
                    CAST(
    
                        CASE 
    
                            WHEN CAST(RD AS INT) <= CAST(
    
                                SUBSTRING(
    
                                    DATE_FORMAT(
    
                                        LAST_DAY(
    
                                            TO_DATE(
    
                                                CONCAT(
    
                                                    SUBSTRING(
    
                                                        CASE 
    
                                                            WHEN CAST(SUBSTRING(DD, 7, 2) AS INT) < CAST(RD AS INT) THEN 
    
                                                                DATE_FORMAT(
    
                                                                    ADD_MONTHS(TO_DATE(MD, 'yyyyMMdd'), -1), 
    
                                                                    'yyyyMMdd'
    
                                                                )
    
                                                            ELSE MD 
    
                                                        END, 1, 6
    
                                                    ), 
    
                                                    '01'
    
                                                ), 
    
                                                'yyyyMMdd'
    
                                            )
    
                                        ), 
    
                                        'yyyyMMdd'
    
                                    ), 7, 2
    
                                ) AS INT
    
                            ) THEN 
    
                                CONCAT(
    
                                    SUBSTRING(
    
                                        CASE 
    
                                            WHEN CAST(SUBSTRING(DD, 7, 2) AS INT) < CAST(RD AS INT) THEN 
    
                                                DATE_FORMAT(
    
                                                    ADD_MONTHS(TO_DATE(MD, 'yyyyMMdd'), -1), 
    
                                                    'yyyyMMdd'
    
                                                )
    
                                            ELSE MD 
    
                                        END, 1, 6
    
                                    ), 
    
                                    RD
    
                                )
    
                            ELSE 
    
                                CONCAT(
    
                                    SUBSTRING(
    
                                        CASE 
    
                                            WHEN CAST(SUBSTRING(DD, 7, 2) AS INT) < CAST(RD AS INT) THEN 
    
                                                DATE_FORMAT(
    
                                                    ADD_MONTHS(TO_DATE(MD, 'yyyyMMdd'), -1), 
    
                                                    'yyyyMMdd'
    
                                                )
    
                                            ELSE MD 
    
                                        END, 1, 6
    
                                    ), 
    
                                    CAST(
    
                                        SUBSTRING(
    
                                            DATE_FORMAT(
    
                                                LAST_DAY(
    
                                                    TO_DATE(
    
                                                        CONCAT(
    
                                                            SUBSTRING(
    
                                                                CASE 
    
                                                                    WHEN CAST(SUBSTRING(DD, 7, 2) AS INT) < CAST(RD AS INT) THEN 
    
                                                                        DATE_FORMAT(
    
                                                                            ADD_MONTHS(TO_DATE(MD, 'yyyyMMdd'), -1), 
    
                                                                            'yyyyMMdd'
    
                                                                        )
    
                                                                    ELSE MD 
    
                                                                END, 1, 6
    
                                                            ), 
    
                                                            '01'
    
                                                        ), 
    
                                                        'yyyyMMdd'
    
                                                    )
    
                                                ), 
    
                                                'yyyyMMdd'
    
                                            ), 7, 2
    
                                        ) AS INT
    
                                    )
    
                                )
    
                        END AS INT
    
                    )
    
            END AS MI006_MATURITY_DATE,
    
            
    
            DATEDIFF(
    
                TO_DATE(CAST(MI006_MATURITY_DATE AS STRING), 'yyyyMMdd'), 
    
                TO_DATE('1899-12-30')
    
            ) AS BINARY_MATURITY_DATE,
    
            
    
            -- End of derivation MD --
    
            -- End CR 32956 --
    
            
    
            -- Start CR16850/AHCR1117 --
    
            -- Start of RATE_REPRICE_TYPE --
    
            CASE 
    
                WHEN (LONP.FOURTH_SCHD_NOT = 'Y' OR LONP.RULE_THREE_NOT = 'Y' OR LONP.NOTICE_OF_TERM = 'Y') THEN 'F'
    
                WHEN BOIS.RECALL_IND IN ('D', 'C', 'F', 'G') AND BINARY_MATURITY_DATE >= {{Curr_Date}} THEN 
    
                    CASE 
    
                        WHEN BOIS.ATTRITION_CODE IN ('01', '02', '03') AND BOIS.RECALL_IND = 'D' THEN 'F'
    
                        ELSE 'B'
    
                    END
    
                WHEN BINARY_MATURITY_DATE < {{Curr_Date}} THEN 'B'
    
                WHEN BOIS.RR_TAG IN ('RR', 'MP') THEN 'B'
    
                ELSE 
    
                    CASE 
    
                        WHEN PITA.INT_RATE_TYPE = 'S' THEN 'F'
    
                        ELSE 'B'
    
                    END
    
            END AS MI006_RATE_REPRICE_TYPE,
    
            
    
            -- End of RATE_REPRICE_TYPE --
    
            
    
            -- Start of NEXT_REPRICE_DATE --
    
            CASE 
    
                WHEN (LONP.FOURTH_SCHD_NOT = 'Y' OR LONP.RULE_THREE_NOT = 'Y' OR LONP.NOTICE_OF_TERM = 'Y') THEN BINARY_MATURITY_DATE
    
                WHEN BOIS.RECALL_IND IN ('D', 'C', 'F', 'G') AND BINARY_MATURITY_DATE >= {{Curr_Date}} THEN
    
                    CASE 
    
                        WHEN MI006_RATE_REPRICE_TYPE = 'F' THEN BINARY_MATURITY_DATE
    
                        ELSE 
    
                            CASE 
    
                                WHEN BINARY_MATURITY_DATE < ({{Curr_Date}} + 14) THEN BINARY_MATURITY_DATE
    
                                ELSE ({{Curr_Date}} + 14)
    
                            END
    
                    END
    
                WHEN BINARY_MATURITY_DATE < {{Curr_Date}} THEN {{Curr_Date}} + 14
    
                WHEN BOIS.RR_TAG IN ('RR', 'MP') THEN
    
                    CASE 
    
                        WHEN BINARY_MATURITY_DATE < ({{Curr_Date}} + 14) THEN BINARY_MATURITY_DATE
    
                        ELSE ({{Curr_Date}} + 14)
    
                    END
    
                ELSE 
    
                    CASE 
    
                        WHEN {{Curr_Date}} > PITA.START_DATE AND {{Curr_Date}} < 
    
                            CASE 
    
                                WHEN PITA.PERIOD_TYPE = 'M' OR TRIM(PITA.PERIOD_TYPE) = '' THEN 
    
                                    DATEDIFF(
    
                                        ADD_MONTHS(
    
                                            DATE_ADD(TO_DATE('1899-12-30'), CAST(PITA.START_DATE AS INT)), 
    
                                            CASE WHEN PITA.PERIOD > 999 THEN 999 ELSE CAST(PITA.PERIOD AS INT) END
    
                                        ), 
    
                                        TO_DATE('1899-12-30')
    
                                    )
    
                                WHEN PITA.PERIOD_TYPE = 'D' THEN PITA.START_DATE + CAST(PITA.PERIOD AS INT)
    
                                ELSE 0
    
                            END
    
                        THEN 
    
                            CASE 
    
                                WHEN PITA.PERIOD = '99999' THEN BINARY_MATURITY_DATE
    
                                ELSE 
    
                                    CASE 
    
                                        WHEN PITA.PERIOD = '99999' THEN 
    
                                            CASE 
    
                                                WHEN BINARY_MATURITY_DATE < ({{Curr_Date}} + 14) THEN BINARY_MATURITY_DATE
    
                                                ELSE ({{Curr_Date}} + 14)
    
                                            END
    
                                        ELSE 
    
                                            CASE 
    
                                                WHEN LEAD(PITA.START_DATE, 1) OVER (PARTITION BY SUBSTRING(BORM.KEY_1, 4, 16) ORDER BY PITA.REC_NO) < {{Curr_Date}} THEN 
    
                                                    LEAD(PITA.START_DATE, 1) OVER (PARTITION BY SUBSTRING(BORM.KEY_1, 4, 16) ORDER BY PITA.REC_NO)
    
                                                ELSE ({{Curr_Date}} + 14)
    
                                            END
    
                                    END
    
                            END
    
                    END
    
            END AS MI006_NEXT_REPRICE_DATE,
    
            
    
            -- End of NEXT_REPRICE_DATE --
    
            
    
            -- Start of LAST_REPRICE_DATE --
    
            CASE 
    
                WHEN (LONP.FOURTH_SCHD_NOT = 'Y' OR LONP.RULE_THREE_NOT = 'Y' OR LONP.NOTICE_OF_TERM = 'Y') THEN BORM.ADV_DATE
    
                WHEN BOIS.RECALL_IND IN ('D', 'C', 'F', 'G') AND BINARY_MATURITY_DATE >= {{Curr_Date}} THEN
    
                    CASE 
    
                        WHEN BOIS.ATTRITION_DATE > BOIS.STAF_GRAC_END_DT THEN BOIS.ATTRITION_DATE
    
                        ELSE BOIS.STAF_GRAC_END_DT
    
                    END
    
                WHEN BINARY_MATURITY_DATE < {{Curr_Date}} THEN BINARY_MATURITY_DATE
    
                WHEN BOIS.RR_TAG IN ('RR', 'MP') THEN BOIS.RR_DT
    
                ELSE 
    
                    CASE 
    
                        WHEN {{Curr_Date}} > PITA.START_DATE AND {{Curr_Date}} < 
    
                            CASE 
    
                                WHEN PITA.PERIOD_TYPE = 'M' OR TRIM(PITA.PERIOD_TYPE) = '' THEN 
    
                                    DATEDIFF(
    
                                        ADD_MONTHS(
    
                                            DATE_ADD(TO_DATE('1899-12-30'), CAST(PITA.START_DATE AS INT)), 
    
                                            CASE WHEN PITA.PERIOD > 999 THEN 999 ELSE CAST(PITA.PERIOD AS INT) END
    
                                        ), 
    
                                        TO_DATE('1899-12-30')
    
                                    )
    
                                WHEN PITA.PERIOD_TYPE = 'D' THEN PITA.START_DATE + CAST(PITA.PERIOD AS INT)
    
                                ELSE 0
    
                            END
    
                        THEN PITA.START_DATE
    
                    END
    
            END AS MI006_LAST_REPRICE_DATE,
    
            
    
            CAST(ROW_NUMBER() OVER (PARTITION BY SUBSTRING(BORM.KEY_1, 4, 16) ORDER BY SUBSTRING(BORM.KEY_1, 4, 16), PITA.REC_NO) AS INT) AS SEQ
    
            
    
            -- End of LAST_REPRICE_DATE --
    
            -- End of CR16850/AHCR1117 --
    
            
    
        FROM {{dbdir.pODS_SCHM}}.BORM
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.LONP LONP 
    
            ON LONP.INST_NO = SUBSTRING(BORM.KEY_1, 1, 3)
    
            AND LONP.SYST = 'BOR'
    
            AND LONP.ACCT_TYPE = BORM.ACT_TYPE
    
            AND LONP.INT_CAT = BORM.CAT
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.BOIS BOIS 
    
            ON BOIS.KEY_1 = BORM.KEY_1
    
        LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.PITA PITA
    
            ON PITA.INST_NO = SUBSTRING(BORM.KEY_1, 1, 3)
    
            AND PITA.ACCT_NO = SUBSTRING(BORM.KEY_1, 4, 16)
    
    ) Der
    
    -- WHERE SUBSTRING(BORM.KEY_1, 4, 16) IN ('0250100045329', '250200067386', '340100163819')""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    NETZ_SRC_TBL_NM2_v = spark.sql(sql)
    
    
    
    
    
    #spark.sql(f"use spark_catalog.default").show()
    
    NETZ_SRC_TBL_NM2_ln_Nxt_Lst_v=NETZ_SRC_TBL_NM2_v.select(NETZ_SRC_TBL_NM2_v[0].cast('string').alias('BORM_KEY_1'),NETZ_SRC_TBL_NM2_v[1].cast('integer').alias('SEQ'),NETZ_SRC_TBL_NM2_v[2].cast('string').alias('MI006_MEMB_CUST_AC'),NETZ_SRC_TBL_NM2_v[3].cast('string').alias('MI006_RATE_REPRICE_TYPE'),NETZ_SRC_TBL_NM2_v[4].cast('integer').alias('MI006_NEXT_REPRICE_DATE'),NETZ_SRC_TBL_NM2_v[5].cast('integer').alias('MI006_LAST_REPRICE_DATE'))
    
    NETZ_SRC_TBL_NM2_ln_Nxt_Lst_v = NETZ_SRC_TBL_NM2_ln_Nxt_Lst_v.selectExpr("RTRIM(BORM_KEY_1) AS BORM_KEY_1","SEQ","MI006_MEMB_CUST_AC","RTRIM(MI006_RATE_REPRICE_TYPE) AS MI006_RATE_REPRICE_TYPE","MI006_NEXT_REPRICE_DATE","MI006_LAST_REPRICE_DATE").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'BORM_KEY_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(19)'}}, {'name': 'SEQ', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MEMB_CUST_AC', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RATE_REPRICE_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NEXT_REPRICE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LAST_REPRICE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS NETZ_SRC_TBL_NM2_ln_Nxt_Lst_v").show()
    
    print("NETZ_SRC_TBL_NM2_ln_Nxt_Lst_v")
    
    print(NETZ_SRC_TBL_NM2_ln_Nxt_Lst_v.schema.json())
    
    print("count:{}".format(NETZ_SRC_TBL_NM2_ln_Nxt_Lst_v.count()))
    
    NETZ_SRC_TBL_NM2_ln_Nxt_Lst_v.show(1000,False)
    
    NETZ_SRC_TBL_NM2_ln_Nxt_Lst_v.write.mode("overwrite").saveAsTable("NETZ_SRC_TBL_NM2_ln_Nxt_Lst_v")
    

@task
def V99A7(**kw_args) -> str:
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
    
        X.BORM_KEY_1,
    
        X.SEQ,
    
        X.MI006_MEMB_CUST_AC,
    
        X.MI006_EFF_TIER_N_RATE_DT,
    
        X.MI006_EFF_TIER_N_RATE_END_DT,
    
        X.MI006_FLAT_RATE_TIER_N,
    
        CAST(X.MI006_INDEX_CD_RT_TIER_N AS DECIMAL(8,4)) AS MI006_INDEX_CD_RT_TIER_N,
    
        X.MI006_ADJ_PERC_RATE_TIER_N
    
    FROM (
    
        SELECT 
    
            BORM.KEY_1 AS BORM_KEY_1,
    
            CAST(ROW_NUMBER() OVER (PARTITION BY SUBSTR(BORM.KEY_1, 4, 16) ORDER BY SUBSTR(BORM.KEY_1, 4, 16), PITA.REC_NO) AS INT) AS SEQ,
    
            PITA.START_DATE,
    
            LAG(PITA.START_DATE, 1) OVER (PARTITION BY SUBSTR(BORM.KEY_1, 4, 16) ORDER BY PITA.REC_NO) AS Prev_START_DATE,
    
            BORM.ADV_DATE,
    
            PITA.PERIOD,
    
            PITA.PERIOD_TYPE,
    
            PITA.ACCT_NO,
    
            PITA.INST_NO,
    
            BORM.TERM_BASIS,
    
            BORM.LOAN_TRM,
    
            BORM.DUE_STRT_DATE,
    
            LONP.MULTITIER_ENABLED,
    
            SUBSTR(BORM.KEY_1, 4, 16) AS MI006_MEMB_CUST_AC,
    
            COALESCE(
    
                CAST(DATE_FORMAT(DATE_ADD(TO_DATE('1899-12-31'), CAST(PITA.START_DATE AS INT)), 'yyyyMMdd') AS INT),
    
                0
    
            ) AS MI006_EFF_TIER_N_RATE_DT,
    
            CAST(DATE_FORMAT(
    
                ADD_MONTHS(
    
                    DATE_ADD(TO_DATE('1899-12-31'), CAST(PITA.START_DATE AS INT)),
    
                    PITA.PERIOD
    
                ),
    
                'yyyyMMdd'
    
            ) AS INT) AS MI006_EFF_TIER_N_RATE_END_DT,
    
            CASE 
    
                WHEN PITA.INT_RATE_TYPE = 'S' THEN COALESCE(PITA.EFFECTIVE_RATE, 0)
    
            END AS MI006_FLAT_RATE_TIER_N,
    
            COALESCE(PITA.REC_NO, '0') AS MI006_INDEX_CD_RT_TIER_N,
    
            CASE 
    
                WHEN PITA.INT_RATE_TYPE = 'I' THEN COALESCE(PITA.EFFECTIVE_RATE, 0)
    
            END AS MI006_ADJ_PERC_RATE_TIER_N,
    
            Z.MI006_RATE_REPRICE_TYPE
    
        FROM {{dbdir.pODS_SCHM}}.BORM borm 
    
        LEFT JOIN {{dbdir.pODS_SCHM}}.PITA pita
    
            ON PITA.INST_NO = SUBSTR(BORM.KEY_1, 1, 3) 
    
            AND PITA.ACCT_NO = SUBSTR(BORM.KEY_1, 4, 16)
    
        LEFT JOIN {{dbdir.pODS_SCHM}}.LONP LONP
    
            ON LONP.INST_NO = SUBSTR(BORM.KEY_1, 1, 3)
    
            AND LONP.ACCT_TYPE = BORM.ACT_TYPE
    
            AND LONP.INT_CAT = BORM.CAT
    
        LEFT JOIN (
    
            SELECT 
    
                KEY_1,
    
                MI006_RATE_REPRICE_TYPE
    
            FROM (
    
                SELECT 
    
                    KEY_1,
    
                    ROW_NUMBER() OVER (PARTITION BY PITA.ACCT_NO ORDER BY PITA.ACCT_NO, PITA.REC_NO DESC) AS SEQ2,
    
                    CASE 
    
                        WHEN ROW_NUMBER() OVER (PARTITION BY PITA.ACCT_NO ORDER BY PITA.ACCT_NO, PITA.REC_NO DESC) = 1 
    
                        THEN PITA.INT_RATE_TYPE 
    
                        ELSE ' '
    
                    END AS MI006_RATE_REPRICE_TYPE
    
                FROM {{dbdir.pODS_SCHM}}.BORM borm 
    
                LEFT JOIN {{dbdir.pODS_SCHM}}.PITA pita 
    
                    ON PITA.INST_NO = SUBSTR(BORM.KEY_1, 1, 3) 
    
                    AND PITA.ACCT_NO = SUBSTR(BORM.KEY_1, 4, 16)
    
            ) A
    
            WHERE A.SEQ2 = 1
    
        ) Z
    
            ON BORM.KEY_1 = Z.KEY_1
    
    ) X
    
    WHERE X.SEQ <= 5""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    NETZ_SRC_TBL_NM_v = spark.sql(sql)
    
    
    
    
    
    #spark.sql(f"use spark_catalog.default").show()
    
    NETZ_SRC_TBL_NM_lnk_Source_v=NETZ_SRC_TBL_NM_v.select(NETZ_SRC_TBL_NM_v[0].cast('string').alias('BORM_KEY_1'),NETZ_SRC_TBL_NM_v[1].cast('integer').alias('SEQ'),NETZ_SRC_TBL_NM_v[2].cast('string').alias('MI006_MEMB_CUST_AC'),NETZ_SRC_TBL_NM_v[3].cast('integer').alias('MI006_EFF_TIER_N_RATE_DT'),NETZ_SRC_TBL_NM_v[4].cast('integer').alias('MI006_EFF_TIER_N_RATE_END_DT'),NETZ_SRC_TBL_NM_v[5].cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER_N'),NETZ_SRC_TBL_NM_v[6].cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER_N'),NETZ_SRC_TBL_NM_v[7].cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER_N'))
    
    NETZ_SRC_TBL_NM_lnk_Source_v = NETZ_SRC_TBL_NM_lnk_Source_v.selectExpr("RTRIM(BORM_KEY_1) AS BORM_KEY_1","SEQ","MI006_MEMB_CUST_AC","MI006_EFF_TIER_N_RATE_DT","MI006_EFF_TIER_N_RATE_END_DT","MI006_FLAT_RATE_TIER_N","MI006_INDEX_CD_RT_TIER_N","MI006_ADJ_PERC_RATE_TIER_N").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'BORM_KEY_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(19)'}}, {'name': 'SEQ', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MEMB_CUST_AC', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER_N_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER_N_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER_N', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER_N', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER_N', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS NETZ_SRC_TBL_NM_lnk_Source_v").show()
    
    print("NETZ_SRC_TBL_NM_lnk_Source_v")
    
    print(NETZ_SRC_TBL_NM_lnk_Source_v.schema.json())
    
    print("count:{}".format(NETZ_SRC_TBL_NM_lnk_Source_v.count()))
    
    NETZ_SRC_TBL_NM_lnk_Source_v.show(1000,False)
    
    NETZ_SRC_TBL_NM_lnk_Source_v.write.mode("overwrite").saveAsTable("NETZ_SRC_TBL_NM_lnk_Source_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_84_ln_Nxt_Lst_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    NETZ_SRC_TBL_NM2_ln_Nxt_Lst_v=spark.table('NETZ_SRC_TBL_NM2_ln_Nxt_Lst_v')
    
    Transformer_84_ln_Nxt_Lst_Part_v=NETZ_SRC_TBL_NM2_ln_Nxt_Lst_v
    
    spark.sql("DROP TABLE IF EXISTS Transformer_84_ln_Nxt_Lst_Part_v").show()
    
    print("Transformer_84_ln_Nxt_Lst_Part_v")
    
    print(Transformer_84_ln_Nxt_Lst_Part_v.schema.json())
    
    print("count:{}".format(Transformer_84_ln_Nxt_Lst_Part_v.count()))
    
    Transformer_84_ln_Nxt_Lst_Part_v.show(1000,False)
    
    Transformer_84_ln_Nxt_Lst_Part_v.write.mode("overwrite").saveAsTable("Transformer_84_ln_Nxt_Lst_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Vertical_Pivot_of_Tier_rates_lnk_Source_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    NETZ_SRC_TBL_NM_lnk_Source_v=spark.table('NETZ_SRC_TBL_NM_lnk_Source_v')
    
    Vertical_Pivot_of_Tier_rates_lnk_Source_Part_v=NETZ_SRC_TBL_NM_lnk_Source_v
    
    spark.sql("DROP TABLE IF EXISTS Vertical_Pivot_of_Tier_rates_lnk_Source_Part_v").show()
    
    print("Vertical_Pivot_of_Tier_rates_lnk_Source_Part_v")
    
    print(Vertical_Pivot_of_Tier_rates_lnk_Source_Part_v.schema.json())
    
    print("count:{}".format(Vertical_Pivot_of_Tier_rates_lnk_Source_Part_v.count()))
    
    Vertical_Pivot_of_Tier_rates_lnk_Source_Part_v.show(1000,False)
    
    Vertical_Pivot_of_Tier_rates_lnk_Source_Part_v.write.mode("overwrite").saveAsTable("Vertical_Pivot_of_Tier_rates_lnk_Source_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_84(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_84_ln_Nxt_Lst_Part_v=spark.table('Transformer_84_ln_Nxt_Lst_Part_v')
    # 1. Define the Window partitioned by the key
    # We need an ordering column to ensure "LastRow" is deterministic. 
    # If your DS job had no specific sort within the key, we assume natural order.
    window_spec = Window.partitionBy("BORM_KEY_1").orderBy(F.monotonically_increasing_id())

    # 2. Replicating the "LastRowInGroup" logic
    # In PySpark, instead of iterating and holding state, we can use 
    # F.last() with the window to get the final values for the group.
    df_transformed = Transformer_84_ln_Nxt_Lst_Part_v.withColumn(
        "is_last_in_group", 
        F.row_number().over(Window.partitionBy("BORM_KEY_1").orderBy(F.desc(F.monotonically_increasing_id()))) == 1
    )

    # 3. Handling Stage Variables A and B
    # The DS logic essentially propagates the first non-null date found in the group 
    # or maintains 'X' and then outputs the result at the end of the group.
    # Based on the DS Spec, it looks like it's trying to capture specific date values 
    # within the group.

    joi_LONP = df_transformed.withColumn("A_val", F.coalesce(F.col("MI006_NEXT_REPRICE_DATE").cast("string"), F.lit("X"))).withColumn("B_val", F.coalesce(F.col("MI006_LAST_REPRICE_DATE").cast("string"), F.lit("X"))).withColumn("FINAL_A", F.last("A_val").over(window_spec)).withColumn("FINAL_B", F.last("B_val").over(window_spec)).withColumn("_row_idx", F.row_number().over(Window.partitionBy("BORM_KEY_1").orderBy(F.desc(F.monotonically_increasing_id())))).filter(F.col("_row_idx") == 1)
    

    # 4. Generate output according to CTrxOutput spec
    CTrxOutput = joi_LONP.select(
        F.col("BORM_KEY_1").cast("string"),
        F.col("MI006_MEMB_CUST_AC").cast("string"),
        F.col("MI006_RATE_REPRICE_TYPE").cast("string"),
        F.col("FINAL_A").alias("MI006_NEXT_REPRICE_DATE").cast("int"),
        F.col("FINAL_B").alias("MI006_LAST_REPRICE_DATE").cast("int")
    )
    
    
    Transformer_84_joi_LONP_v = CTrxOutput.selectExpr("RTRIM(BORM_KEY_1) AS BORM_KEY_1","MI006_MEMB_CUST_AC","RTRIM(MI006_RATE_REPRICE_TYPE) AS MI006_RATE_REPRICE_TYPE","MI006_NEXT_REPRICE_DATE","MI006_LAST_REPRICE_DATE").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'BORM_KEY_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(19)'}}, {'name': 'MI006_MEMB_CUST_AC', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RATE_REPRICE_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NEXT_REPRICE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LAST_REPRICE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS Transformer_84_joi_LONP_v").show()
    
    print("Transformer_84_joi_LONP_v")
    
    print(Transformer_84_joi_LONP_v.schema.json())
    
    print("count:{}".format(Transformer_84_joi_LONP_v.count()))
    
    Transformer_84_joi_LONP_v.show(1000,False)
    
    Transformer_84_joi_LONP_v.write.mode("overwrite").saveAsTable("Transformer_84_joi_LONP_v")
    

@task.pyspark(conn_id="spark-local")
def Vertical_Pivot_of_Tier_rates(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Vertical_Pivot_of_Tier_rates_lnk_Source_Part_v=spark.table('Vertical_Pivot_of_Tier_rates_lnk_Source_Part_v')
    group_keys = ["BORM_KEY_1", "SEQ", "MI006_MEMB_CUST_AC"]
    window_spec = Window.partitionBy(group_keys).orderBy("MI006_EFF_TIER_N_RATE_DT")
    df_with_index = Vertical_Pivot_of_Tier_rates_lnk_Source_Part_v.withColumn("Pivot_index", F.row_number().over(window_spec) - 1)
    # 2. These are the metrics we are spreading across the 5 array slots.
    # We use explicit aliases to control the naming convention.
    df_pivoted = df_with_index.groupBy(group_keys).pivot("Pivot_index", [0, 1, 2, 3, 4]).agg(
        F.first("MI006_EFF_TIER_N_RATE_DT").alias("MI006_EFF_TIER_N_RATE_DT"),
        F.first("MI006_EFF_TIER_N_RATE_END_DT").alias("MI006_EFF_TIER_N_RATE_END_DT"),
        F.first("MI006_FLAT_RATE_TIER_N").alias("MI006_FLAT_RATE_TIER_N"),
        F.first("MI006_ADJ_PERC_RATE_TIER_N").alias("MI006_ADJ_PERC_RATE_TIER_N"),
        F.first("MI006_INDEX_CD_RT_TIER_N").alias("MI006_INDEX_CD_RT_TIER_N")
    )

    # 3. Rename columns to match the CCustomOutput spec.
    # Spark Pivot output format is: {PivotValue}_{AliasName}
    # DataStage spec format is: {Name} (for index 0) and {Name}_{Index} (for 1-4)
    
    final_cols = group_keys.copy()
    
    # Define the order of metrics to match your MappingAdd records
    metrics = [
        "MI006_EFF_TIER_N_RATE_DT",
        "MI006_EFF_TIER_N_RATE_END_DT",
        "MI006_FLAT_RATE_TIER_N",
        "MI006_ADJ_PERC_RATE_TIER_N",
        "MI006_INDEX_CD_RT_TIER_N"
    ]

    # Dynamically build the select list to match DataStage naming exactly
    for i in range(5):
        for m in metrics:
            spark_col_name = f"{i}_{m}"
            ds_col_name = f"{m}_{i}" if i > 0 else m
            
            # Add to selection with the correct DS name
            if i == 0:
                # Slot 0 has no suffix in your DS spec
                final_cols.append(F.col(spark_col_name).alias(m))
            else:
                # Slots 1-4 have suffixes _1, _2, etc.
                final_cols.append(F.col(spark_col_name).alias(ds_col_name))

    # Apply the selection and naming
    df_final = df_pivoted.select(final_cols)

    # # 4. Final Casting to Numeric(8,4) as per DS Precision/Scale
    # decimal_cols = [c for c in df_final.columns if "RATE_TIER_N" in c]
    # for c in decimal_cols:
    #     df_final = df_final.withColumn(c, F.col(c).cast("decimal(8,4)"))
    
    Vertical_Pivot_of_Tier_rates_i_v = df_final.select(col('BORM_KEY_1').cast('string').alias('B_KEY'),col('SEQ').cast('integer').alias('SEQ'),expr("""MI006_MEMB_CUST_AC""").cast('string').alias('MI006_MEMB_CUST_AC'),expr("""MI006_EFF_TIER_N_RATE_DT""").cast('integer').alias('MI006_EFF_TIER_N_RATE_DT'),expr("""MI006_EFF_TIER_N_RATE_END_DT""").cast('integer').alias('MI006_EFF_TIER_N_RATE_END_DT'),expr("""MI006_FLAT_RATE_TIER_N""").cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER_N'),expr("""MI006_ADJ_PERC_RATE_TIER_N""").cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER_N'),expr("""MI006_INDEX_CD_RT_TIER_N""").cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER_N'),expr("""MI006_EFF_TIER_N_RATE_DT_1""").cast('integer').alias('MI006_EFF_TIER_N_RATE_DT_1'),expr("""MI006_EFF_TIER_N_RATE_END_DT_1""").cast('integer').alias('MI006_EFF_TIER_N_RATE_END_DT_1'),expr("""MI006_FLAT_RATE_TIER_N_1""").cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER_N_1'),expr("""MI006_ADJ_PERC_RATE_TIER_N_1""").cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER_N_1'),expr("""MI006_INDEX_CD_RT_TIER_N_1""").cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER_N_1'),expr("""MI006_EFF_TIER_N_RATE_DT_2""").cast('integer').alias('MI006_EFF_TIER_N_RATE_DT_2'),expr("""MI006_EFF_TIER_N_RATE_END_DT_2""").cast('integer').alias('MI006_EFF_TIER_N_RATE_END_DT_2'),expr("""MI006_FLAT_RATE_TIER_N_2""").cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER_N_2'),expr("""MI006_ADJ_PERC_RATE_TIER_N_2""").cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER_N_2'),expr("""MI006_INDEX_CD_RT_TIER_N_2""").cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER_N_2'),expr("""MI006_EFF_TIER_N_RATE_DT_3""").cast('integer').alias('MI006_EFF_TIER_N_RATE_DT_3'),expr("""MI006_EFF_TIER_N_RATE_END_DT_3""").cast('integer').alias('MI006_EFF_TIER_N_RATE_END_DT_3'),expr("""MI006_FLAT_RATE_TIER_N_3""").cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER_N_3'),expr("""MI006_ADJ_PERC_RATE_TIER_N_3""").cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER_N_3'),expr("""MI006_INDEX_CD_RT_TIER_N_3""").cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER_N_3'),expr("""MI006_EFF_TIER_N_RATE_DT_4""").cast('integer').alias('MI006_EFF_TIER_N_RATE_DT_4'),expr("""MI006_EFF_TIER_N_RATE_END_DT_4""").cast('integer').alias('MI006_EFF_TIER_N_RATE_END_DT_4'),expr("""MI006_FLAT_RATE_TIER_N_4""").cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER_N_4'),expr("""MI006_ADJ_PERC_RATE_TIER_N_4""").cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER_N_4'),expr("""MI006_INDEX_CD_RT_TIER_N_4""").cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER_N_4'))
    
    Vertical_Pivot_of_Tier_rates_i_v = Vertical_Pivot_of_Tier_rates_i_v.selectExpr("RTRIM(B_KEY) AS B_KEY","SEQ","MI006_MEMB_CUST_AC","MI006_EFF_TIER_N_RATE_DT","MI006_EFF_TIER_N_RATE_END_DT","MI006_FLAT_RATE_TIER_N","MI006_ADJ_PERC_RATE_TIER_N","MI006_INDEX_CD_RT_TIER_N","MI006_EFF_TIER_N_RATE_DT_1","MI006_EFF_TIER_N_RATE_END_DT_1","MI006_FLAT_RATE_TIER_N_1","MI006_ADJ_PERC_RATE_TIER_N_1","MI006_INDEX_CD_RT_TIER_N_1","MI006_EFF_TIER_N_RATE_DT_2","MI006_EFF_TIER_N_RATE_END_DT_2","MI006_FLAT_RATE_TIER_N_2","MI006_ADJ_PERC_RATE_TIER_N_2","MI006_INDEX_CD_RT_TIER_N_2","MI006_EFF_TIER_N_RATE_DT_3","MI006_EFF_TIER_N_RATE_END_DT_3","MI006_FLAT_RATE_TIER_N_3","MI006_ADJ_PERC_RATE_TIER_N_3","MI006_INDEX_CD_RT_TIER_N_3","MI006_EFF_TIER_N_RATE_DT_4","MI006_EFF_TIER_N_RATE_END_DT_4","MI006_FLAT_RATE_TIER_N_4","MI006_ADJ_PERC_RATE_TIER_N_4","MI006_INDEX_CD_RT_TIER_N_4").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(19)'}}, {'name': 'SEQ', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MEMB_CUST_AC', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER_N_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER_N_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER_N', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER_N', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER_N', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER_N_RATE_DT_1', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER_N_RATE_END_DT_1', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER_N_1', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER_N_1', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER_N_1', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER_N_RATE_DT_2', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER_N_RATE_END_DT_2', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER_N_2', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER_N_2', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER_N_2', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER_N_RATE_DT_3', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER_N_RATE_END_DT_3', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER_N_3', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER_N_3', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER_N_3', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER_N_RATE_DT_4', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER_N_RATE_END_DT_4', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER_N_4', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER_N_4', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER_N_4', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS Vertical_Pivot_of_Tier_rates_i_v").show()
    
    print("Vertical_Pivot_of_Tier_rates_i_v")
    
    print(Vertical_Pivot_of_Tier_rates_i_v.schema.json())
    
    print("count:{}".format(Vertical_Pivot_of_Tier_rates_i_v.count()))
    
    Vertical_Pivot_of_Tier_rates_i_v.show(1000,False)
    
    Vertical_Pivot_of_Tier_rates_i_v.write.mode("overwrite").saveAsTable("Vertical_Pivot_of_Tier_rates_i_v")
    

@task.pyspark(conn_id="spark-local")
def joi_PITA_joi_LONP_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_84_joi_LONP_v=spark.table('Transformer_84_joi_LONP_v')
    
    joi_PITA_joi_LONP_Part_v=Transformer_84_joi_LONP_v
    
    spark.sql("DROP TABLE IF EXISTS joi_PITA_joi_LONP_Part_v").show()
    
    print("joi_PITA_joi_LONP_Part_v")
    
    print(joi_PITA_joi_LONP_Part_v.schema.json())
    
    print("count:{}".format(joi_PITA_joi_LONP_Part_v.count()))
    
    joi_PITA_joi_LONP_Part_v.show(1000,False)
    
    joi_PITA_joi_LONP_Part_v.write.mode("overwrite").saveAsTable("joi_PITA_joi_LONP_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_50_i_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Vertical_Pivot_of_Tier_rates_i_v=spark.table('Vertical_Pivot_of_Tier_rates_i_v')
    
    Transformer_50_i_Part_v=Vertical_Pivot_of_Tier_rates_i_v
    
    spark.sql("DROP TABLE IF EXISTS Transformer_50_i_Part_v").show()
    
    print("Transformer_50_i_Part_v")
    
    print(Transformer_50_i_Part_v.schema.json())
    
    print("count:{}".format(Transformer_50_i_Part_v.count()))
    
    Transformer_50_i_Part_v.show(1000,False)
    
    Transformer_50_i_Part_v.write.mode("overwrite").saveAsTable("Transformer_50_i_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_50(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_50_i_Part_v=spark.table('Transformer_50_i_Part_v')
    
    Transformer_50_v = Transformer_50_i_Part_v
    
    Transformer_50_DSLink61_v = Transformer_50_v.select(col('B_KEY').cast('string').alias('BORM_KEY_1'),col('SEQ').cast('integer').alias('SEQ'),col('MI006_EFF_TIER_N_RATE_DT').cast('integer').alias('MI006_EFF_TIER1_RATE_DT'),col('MI006_EFF_TIER_N_RATE_DT_1').cast('integer').alias('MI006_EFF_TIER2_RATE_DT'),col('MI006_EFF_TIER_N_RATE_DT_2').cast('integer').alias('MI006_EFF_TIER3_RATE_DT'),col('MI006_EFF_TIER_N_RATE_DT_3').cast('integer').alias('MI006_EFF_TIER4_RATE_DT'),col('MI006_EFF_TIER_N_RATE_DT_4').cast('integer').alias('MI006_EFF_TIER5_RATE_DT'),col('MI006_EFF_TIER_N_RATE_END_DT').cast('integer').alias('MI006_EFF_TIER1_RATE_END_DT'),col('MI006_EFF_TIER_N_RATE_END_DT_1').cast('integer').alias('MI006_EFF_TIER2_RATE_END_DT'),col('MI006_EFF_TIER_N_RATE_END_DT_2').cast('integer').alias('MI006_EFF_TIER3_RATE_END_DT'),col('MI006_EFF_TIER_N_RATE_END_DT_3').cast('integer').alias('MI006_EFF_TIER4_RATE_END_DT'),col('MI006_EFF_TIER_N_RATE_END_DT_4').cast('integer').alias('MI006_EFF_TIER5_RATE_END_DT'),col('MI006_FLAT_RATE_TIER_N').cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER1'),col('MI006_FLAT_RATE_TIER_N_1').cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER2'),col('MI006_FLAT_RATE_TIER_N_2').cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER3'),col('MI006_FLAT_RATE_TIER_N_3').cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER4'),col('MI006_FLAT_RATE_TIER_N_4').cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER5'),col('MI006_INDEX_CD_RT_TIER_N').cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER1'),col('MI006_INDEX_CD_RT_TIER_N_1').cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER2'),col('MI006_INDEX_CD_RT_TIER_N_2').cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER3'),col('MI006_INDEX_CD_RT_TIER_N_3').cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER4'),col('MI006_INDEX_CD_RT_TIER_N_4').cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER5'),col('MI006_ADJ_PERC_RATE_TIER_N').cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER1'),col('MI006_ADJ_PERC_RATE_TIER_N_1').cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER2'),col('MI006_ADJ_PERC_RATE_TIER_N_2').cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER3'),col('MI006_ADJ_PERC_RATE_TIER_N_3').cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER4'),col('MI006_ADJ_PERC_RATE_TIER_N_4').cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER5'))
    
    Transformer_50_DSLink61_v = Transformer_50_DSLink61_v.selectExpr("RTRIM(BORM_KEY_1) AS BORM_KEY_1","SEQ","MI006_EFF_TIER1_RATE_DT","MI006_EFF_TIER2_RATE_DT","MI006_EFF_TIER3_RATE_DT","MI006_EFF_TIER4_RATE_DT","MI006_EFF_TIER5_RATE_DT","MI006_EFF_TIER1_RATE_END_DT","MI006_EFF_TIER2_RATE_END_DT","MI006_EFF_TIER3_RATE_END_DT","MI006_EFF_TIER4_RATE_END_DT","MI006_EFF_TIER5_RATE_END_DT","MI006_FLAT_RATE_TIER1","MI006_FLAT_RATE_TIER2","MI006_FLAT_RATE_TIER3","MI006_FLAT_RATE_TIER4","MI006_FLAT_RATE_TIER5","MI006_INDEX_CD_RT_TIER1","MI006_INDEX_CD_RT_TIER2","MI006_INDEX_CD_RT_TIER3","MI006_INDEX_CD_RT_TIER4","MI006_INDEX_CD_RT_TIER5","MI006_ADJ_PERC_RATE_TIER1","MI006_ADJ_PERC_RATE_TIER2","MI006_ADJ_PERC_RATE_TIER3","MI006_ADJ_PERC_RATE_TIER4","MI006_ADJ_PERC_RATE_TIER5").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'BORM_KEY_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(19)'}}, {'name': 'SEQ', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER1_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER2_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER3_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER4_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER5_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER1_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER2_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER3_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER4_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER5_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER1', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER2', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER3', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER4', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER5', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER1', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER2', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER3', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER4', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER5', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER1', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER2', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER3', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER4', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER5', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS Transformer_50_DSLink61_v").show()
    
    print("Transformer_50_DSLink61_v")
    
    print(Transformer_50_DSLink61_v.schema.json())
    
    print("count:{}".format(Transformer_50_DSLink61_v.count()))
    
    Transformer_50_DSLink61_v.show(1000,False)
    
    Transformer_50_DSLink61_v.write.mode("overwrite").saveAsTable("Transformer_50_DSLink61_v")
    

@task.pyspark(conn_id="spark-local")
def Copy_59_DSLink61_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_50_DSLink61_v=spark.table('Transformer_50_DSLink61_v')
    
    Copy_59_DSLink61_Part_v=Transformer_50_DSLink61_v
    
    spark.sql("DROP TABLE IF EXISTS Copy_59_DSLink61_Part_v").show()
    
    print("Copy_59_DSLink61_Part_v")
    
    print(Copy_59_DSLink61_Part_v.schema.json())
    
    print("count:{}".format(Copy_59_DSLink61_Part_v.count()))
    
    Copy_59_DSLink61_Part_v.show(1000,False)
    
    Copy_59_DSLink61_Part_v.write.mode("overwrite").saveAsTable("Copy_59_DSLink61_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Copy_59(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Copy_59_DSLink61_Part_v=spark.table('Copy_59_DSLink61_Part_v')
    
    Copy_59_v=Copy_59_DSLink61_Part_v
    
    Copy_59_joi_PITA_v = Copy_59_v.select(col('BORM_KEY_1').cast('string').alias('BORM_KEY_1'),col('SEQ').cast('integer').alias('SEQ'),col('MI006_EFF_TIER1_RATE_DT').cast('integer').alias('MI006_EFF_TIER1_RATE_DT'),col('MI006_EFF_TIER2_RATE_DT').cast('integer').alias('MI006_EFF_TIER2_RATE_DT'),col('MI006_EFF_TIER3_RATE_DT').cast('integer').alias('MI006_EFF_TIER3_RATE_DT'),col('MI006_EFF_TIER4_RATE_DT').cast('integer').alias('MI006_EFF_TIER4_RATE_DT'),col('MI006_EFF_TIER5_RATE_DT').cast('integer').alias('MI006_EFF_TIER5_RATE_DT'),col('MI006_EFF_TIER1_RATE_END_DT').cast('integer').alias('MI006_EFF_TIER1_RATE_END_DT'),col('MI006_EFF_TIER2_RATE_END_DT').cast('integer').alias('MI006_EFF_TIER2_RATE_END_DT'),col('MI006_EFF_TIER3_RATE_END_DT').cast('integer').alias('MI006_EFF_TIER3_RATE_END_DT'),col('MI006_EFF_TIER4_RATE_END_DT').cast('integer').alias('MI006_EFF_TIER4_RATE_END_DT'),col('MI006_EFF_TIER5_RATE_END_DT').cast('integer').alias('MI006_EFF_TIER5_RATE_END_DT'),col('MI006_FLAT_RATE_TIER1').cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER1'),col('MI006_FLAT_RATE_TIER2').cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER2'),col('MI006_FLAT_RATE_TIER3').cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER3'),col('MI006_FLAT_RATE_TIER4').cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER4'),col('MI006_FLAT_RATE_TIER5').cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER5'),col('MI006_INDEX_CD_RT_TIER1').cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER1'),col('MI006_INDEX_CD_RT_TIER2').cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER2'),col('MI006_INDEX_CD_RT_TIER3').cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER3'),col('MI006_INDEX_CD_RT_TIER4').cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER4'),col('MI006_INDEX_CD_RT_TIER5').cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER5'),col('MI006_ADJ_PERC_RATE_TIER1').cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER1'),col('MI006_ADJ_PERC_RATE_TIER2').cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER2'),col('MI006_ADJ_PERC_RATE_TIER3').cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER3'),col('MI006_ADJ_PERC_RATE_TIER4').cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER4'),col('MI006_ADJ_PERC_RATE_TIER5').cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER5'))
    
    Copy_59_joi_PITA_v = Copy_59_joi_PITA_v.selectExpr("RTRIM(BORM_KEY_1) AS BORM_KEY_1","SEQ","MI006_EFF_TIER1_RATE_DT","MI006_EFF_TIER2_RATE_DT","MI006_EFF_TIER3_RATE_DT","MI006_EFF_TIER4_RATE_DT","MI006_EFF_TIER5_RATE_DT","MI006_EFF_TIER1_RATE_END_DT","MI006_EFF_TIER2_RATE_END_DT","MI006_EFF_TIER3_RATE_END_DT","MI006_EFF_TIER4_RATE_END_DT","MI006_EFF_TIER5_RATE_END_DT","MI006_FLAT_RATE_TIER1","MI006_FLAT_RATE_TIER2","MI006_FLAT_RATE_TIER3","MI006_FLAT_RATE_TIER4","MI006_FLAT_RATE_TIER5","MI006_INDEX_CD_RT_TIER1","MI006_INDEX_CD_RT_TIER2","MI006_INDEX_CD_RT_TIER3","MI006_INDEX_CD_RT_TIER4","MI006_INDEX_CD_RT_TIER5","MI006_ADJ_PERC_RATE_TIER1","MI006_ADJ_PERC_RATE_TIER2","MI006_ADJ_PERC_RATE_TIER3","MI006_ADJ_PERC_RATE_TIER4","MI006_ADJ_PERC_RATE_TIER5").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'BORM_KEY_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(19)'}}, {'name': 'SEQ', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER1_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER2_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER3_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER4_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER5_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER1_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER2_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER3_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER4_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER5_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER1', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER2', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER3', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER4', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER5', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER1', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER2', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER3', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER4', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER5', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER1', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER2', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER3', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER4', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER5', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS Copy_59_joi_PITA_v").show()
    
    print("Copy_59_joi_PITA_v")
    
    print(Copy_59_joi_PITA_v.schema.json())
    
    print("count:{}".format(Copy_59_joi_PITA_v.count()))
    
    Copy_59_joi_PITA_v.show(1000,False)
    
    Copy_59_joi_PITA_v.write.mode("overwrite").saveAsTable("Copy_59_joi_PITA_v")
    

@task.pyspark(conn_id="spark-local")
def joi_PITA_joi_PITA_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Copy_59_joi_PITA_v=spark.table('Copy_59_joi_PITA_v')
    
    joi_PITA_joi_PITA_Part_v=Copy_59_joi_PITA_v
    
    spark.sql("DROP TABLE IF EXISTS joi_PITA_joi_PITA_Part_v").show()
    
    print("joi_PITA_joi_PITA_Part_v")
    
    print(joi_PITA_joi_PITA_Part_v.schema.json())
    
    print("count:{}".format(joi_PITA_joi_PITA_Part_v.count()))
    
    joi_PITA_joi_PITA_Part_v.show(1000,False)
    
    joi_PITA_joi_PITA_Part_v.write.mode("overwrite").saveAsTable("joi_PITA_joi_PITA_Part_v")
    

@task.pyspark(conn_id="spark-local")
def joi_PITA(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    joi_PITA_joi_PITA_Part_v=spark.table('joi_PITA_joi_PITA_Part_v')
    
    joi_PITA_joi_LONP_Part_v=spark.table('joi_PITA_joi_LONP_Part_v')
    
    joi_PITA_v=joi_PITA_joi_PITA_Part_v.join(joi_PITA_joi_LONP_Part_v,['BORM_KEY_1'],'left')
    
    joi_PITA_trx_PITA_v = joi_PITA_v.select(joi_PITA_joi_PITA_Part_v.BORM_KEY_1.cast('string').alias('BORM_KEY_1'),joi_PITA_joi_PITA_Part_v.SEQ.cast('integer').alias('SEQ'),joi_PITA_joi_LONP_Part_v.MI006_MEMB_CUST_AC.cast('string').alias('MI006_MEMB_CUST_AC'),joi_PITA_joi_PITA_Part_v.MI006_EFF_TIER1_RATE_DT.cast('integer').alias('MI006_EFF_TIER1_RATE_DT'),joi_PITA_joi_PITA_Part_v.MI006_EFF_TIER2_RATE_DT.cast('integer').alias('MI006_EFF_TIER2_RATE_DT'),joi_PITA_joi_PITA_Part_v.MI006_EFF_TIER3_RATE_DT.cast('integer').alias('MI006_EFF_TIER3_RATE_DT'),joi_PITA_joi_PITA_Part_v.MI006_EFF_TIER4_RATE_DT.cast('integer').alias('MI006_EFF_TIER4_RATE_DT'),joi_PITA_joi_PITA_Part_v.MI006_EFF_TIER5_RATE_DT.cast('integer').alias('MI006_EFF_TIER5_RATE_DT'),joi_PITA_joi_PITA_Part_v.MI006_EFF_TIER1_RATE_END_DT.cast('integer').alias('MI006_EFF_TIER1_RATE_END_DT'),joi_PITA_joi_PITA_Part_v.MI006_EFF_TIER2_RATE_END_DT.cast('integer').alias('MI006_EFF_TIER2_RATE_END_DT'),joi_PITA_joi_PITA_Part_v.MI006_EFF_TIER3_RATE_END_DT.cast('integer').alias('MI006_EFF_TIER3_RATE_END_DT'),joi_PITA_joi_PITA_Part_v.MI006_EFF_TIER4_RATE_END_DT.cast('integer').alias('MI006_EFF_TIER4_RATE_END_DT'),joi_PITA_joi_PITA_Part_v.MI006_EFF_TIER5_RATE_END_DT.cast('integer').alias('MI006_EFF_TIER5_RATE_END_DT'),joi_PITA_joi_PITA_Part_v.MI006_FLAT_RATE_TIER1.cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER1'),joi_PITA_joi_PITA_Part_v.MI006_FLAT_RATE_TIER2.cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER2'),joi_PITA_joi_PITA_Part_v.MI006_FLAT_RATE_TIER3.cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER3'),joi_PITA_joi_PITA_Part_v.MI006_FLAT_RATE_TIER4.cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER4'),joi_PITA_joi_PITA_Part_v.MI006_FLAT_RATE_TIER5.cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER5'),joi_PITA_joi_PITA_Part_v.MI006_INDEX_CD_RT_TIER1.cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER1'),joi_PITA_joi_PITA_Part_v.MI006_INDEX_CD_RT_TIER2.cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER2'),joi_PITA_joi_PITA_Part_v.MI006_INDEX_CD_RT_TIER3.cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER3'),joi_PITA_joi_PITA_Part_v.MI006_INDEX_CD_RT_TIER4.cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER4'),joi_PITA_joi_PITA_Part_v.MI006_INDEX_CD_RT_TIER5.cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER5'),joi_PITA_joi_PITA_Part_v.MI006_ADJ_PERC_RATE_TIER1.cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER1'),joi_PITA_joi_PITA_Part_v.MI006_ADJ_PERC_RATE_TIER2.cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER2'),joi_PITA_joi_PITA_Part_v.MI006_ADJ_PERC_RATE_TIER3.cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER3'),joi_PITA_joi_PITA_Part_v.MI006_ADJ_PERC_RATE_TIER4.cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER4'),joi_PITA_joi_PITA_Part_v.MI006_ADJ_PERC_RATE_TIER5.cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER5'),joi_PITA_joi_LONP_Part_v.MI006_RATE_REPRICE_TYPE.cast('string').alias('MI006_RATE_REPRICE_TYPE'),joi_PITA_joi_LONP_Part_v.MI006_NEXT_REPRICE_DATE.cast('integer').alias('MI006_NEXT_REPRICE_DATE'),joi_PITA_joi_LONP_Part_v.MI006_LAST_REPRICE_DATE.cast('integer').alias('MI006_LAST_REPRICE_DATE'))
    
    joi_PITA_trx_PITA_v = joi_PITA_trx_PITA_v.selectExpr("RTRIM(BORM_KEY_1) AS BORM_KEY_1","SEQ","MI006_MEMB_CUST_AC","MI006_EFF_TIER1_RATE_DT","MI006_EFF_TIER2_RATE_DT","MI006_EFF_TIER3_RATE_DT","MI006_EFF_TIER4_RATE_DT","MI006_EFF_TIER5_RATE_DT","MI006_EFF_TIER1_RATE_END_DT","MI006_EFF_TIER2_RATE_END_DT","MI006_EFF_TIER3_RATE_END_DT","MI006_EFF_TIER4_RATE_END_DT","MI006_EFF_TIER5_RATE_END_DT","MI006_FLAT_RATE_TIER1","MI006_FLAT_RATE_TIER2","MI006_FLAT_RATE_TIER3","MI006_FLAT_RATE_TIER4","MI006_FLAT_RATE_TIER5","MI006_INDEX_CD_RT_TIER1","MI006_INDEX_CD_RT_TIER2","MI006_INDEX_CD_RT_TIER3","MI006_INDEX_CD_RT_TIER4","MI006_INDEX_CD_RT_TIER5","MI006_ADJ_PERC_RATE_TIER1","MI006_ADJ_PERC_RATE_TIER2","MI006_ADJ_PERC_RATE_TIER3","MI006_ADJ_PERC_RATE_TIER4","MI006_ADJ_PERC_RATE_TIER5","RTRIM(MI006_RATE_REPRICE_TYPE) AS MI006_RATE_REPRICE_TYPE","MI006_NEXT_REPRICE_DATE","MI006_LAST_REPRICE_DATE").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'BORM_KEY_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(19)'}}, {'name': 'SEQ', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_MEMB_CUST_AC', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER1_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER2_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER3_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER4_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER5_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER1_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER2_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER3_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER4_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER5_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER1', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER2', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER3', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER4', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER5', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER1', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER2', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER3', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER4', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER5', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER1', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER2', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER3', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER4', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER5', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RATE_REPRICE_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'MI006_NEXT_REPRICE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LAST_REPRICE_DATE', 'type': 'integer', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS joi_PITA_trx_PITA_v").show()
    
    print("joi_PITA_trx_PITA_v")
    
    print(joi_PITA_trx_PITA_v.schema.json())
    
    print("count:{}".format(joi_PITA_trx_PITA_v.count()))
    
    joi_PITA_trx_PITA_v.show(1000,False)
    
    joi_PITA_trx_PITA_v.write.mode("overwrite").saveAsTable("joi_PITA_trx_PITA_v")
    

@task.pyspark(conn_id="spark-local")
def trx_PITA_trx_PITA_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    joi_PITA_trx_PITA_v=spark.table('joi_PITA_trx_PITA_v')
    
    trx_PITA_trx_PITA_Part_v=joi_PITA_trx_PITA_v
    
    spark.sql("DROP TABLE IF EXISTS trx_PITA_trx_PITA_Part_v").show()
    
    print("trx_PITA_trx_PITA_Part_v")
    
    print(trx_PITA_trx_PITA_Part_v.schema.json())
    
    print("count:{}".format(trx_PITA_trx_PITA_Part_v.count()))
    
    trx_PITA_trx_PITA_Part_v.show(1000,False)
    
    trx_PITA_trx_PITA_Part_v.write.mode("overwrite").saveAsTable("trx_PITA_trx_PITA_Part_v")
    

@task.pyspark(conn_id="spark-local")
def trx_PITA(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    trx_PITA_trx_PITA_Part_v=spark.table('trx_PITA_trx_PITA_Part_v')
    
    trx_PITA_v = trx_PITA_trx_PITA_Part_v
    
    trx_PITA_Lnk_PITA_Tgt_v = trx_PITA_v.select(expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM BORM_KEY_1))""").cast('string').alias('B_KEY'),col('SEQ').cast('integer').alias('SEQ'),col('MI006_EFF_TIER1_RATE_DT').cast('integer').alias('MI006_EFF_TIER1_RATE_DT'),col('MI006_EFF_TIER2_RATE_DT').cast('integer').alias('MI006_EFF_TIER2_RATE_DT'),col('MI006_EFF_TIER3_RATE_DT').cast('integer').alias('MI006_EFF_TIER3_RATE_DT'),col('MI006_EFF_TIER4_RATE_DT').cast('integer').alias('MI006_EFF_TIER4_RATE_DT'),col('MI006_EFF_TIER5_RATE_DT').cast('integer').alias('MI006_EFF_TIER5_RATE_DT'),col('MI006_EFF_TIER1_RATE_END_DT').cast('integer').alias('MI006_EFF_TIER1_RATE_END_DT'),col('MI006_EFF_TIER2_RATE_END_DT').cast('integer').alias('MI006_EFF_TIER2_RATE_END_DT'),col('MI006_EFF_TIER3_RATE_END_DT').cast('integer').alias('MI006_EFF_TIER3_RATE_END_DT'),col('MI006_EFF_TIER4_RATE_END_DT').cast('integer').alias('MI006_EFF_TIER4_RATE_END_DT'),col('MI006_EFF_TIER5_RATE_END_DT').cast('integer').alias('MI006_EFF_TIER5_RATE_END_DT'),col('MI006_LAST_REPRICE_DATE').cast('decimal(10,0)').alias('MI006_LAST_REPRICE_DATE'),col('MI006_NEXT_REPRICE_DATE').cast('decimal(10,0)').alias('MI006_NEXT_REPRICE_DATE'),col('MI006_FLAT_RATE_TIER1').cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER1'),col('MI006_FLAT_RATE_TIER2').cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER2'),col('MI006_FLAT_RATE_TIER3').cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER3'),col('MI006_FLAT_RATE_TIER4').cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER4'),col('MI006_FLAT_RATE_TIER5').cast('decimal(8,4)').alias('MI006_FLAT_RATE_TIER5'),col('MI006_INDEX_CD_RT_TIER1').cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER1'),col('MI006_INDEX_CD_RT_TIER2').cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER2'),col('MI006_INDEX_CD_RT_TIER3').cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER3'),col('MI006_INDEX_CD_RT_TIER4').cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER4'),col('MI006_INDEX_CD_RT_TIER5').cast('decimal(8,4)').alias('MI006_INDEX_CD_RT_TIER5'),col('MI006_ADJ_PERC_RATE_TIER1').cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER1'),col('MI006_ADJ_PERC_RATE_TIER2').cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER2'),col('MI006_ADJ_PERC_RATE_TIER3').cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER3'),col('MI006_ADJ_PERC_RATE_TIER4').cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER4'),col('MI006_ADJ_PERC_RATE_TIER5').cast('decimal(8,4)').alias('MI006_ADJ_PERC_RATE_TIER5'),col('MI006_RATE_REPRICE_TYPE').cast('string').alias('MI006_RATE_REPRICE_TYPE'))
    
    trx_PITA_Lnk_PITA_Tgt_v = trx_PITA_Lnk_PITA_Tgt_v.selectExpr("B_KEY","SEQ","MI006_EFF_TIER1_RATE_DT","MI006_EFF_TIER2_RATE_DT","MI006_EFF_TIER3_RATE_DT","MI006_EFF_TIER4_RATE_DT","MI006_EFF_TIER5_RATE_DT","MI006_EFF_TIER1_RATE_END_DT","MI006_EFF_TIER2_RATE_END_DT","MI006_EFF_TIER3_RATE_END_DT","MI006_EFF_TIER4_RATE_END_DT","MI006_EFF_TIER5_RATE_END_DT","MI006_LAST_REPRICE_DATE","MI006_NEXT_REPRICE_DATE","MI006_FLAT_RATE_TIER1","MI006_FLAT_RATE_TIER2","MI006_FLAT_RATE_TIER3","MI006_FLAT_RATE_TIER4","MI006_FLAT_RATE_TIER5","MI006_INDEX_CD_RT_TIER1","MI006_INDEX_CD_RT_TIER2","MI006_INDEX_CD_RT_TIER3","MI006_INDEX_CD_RT_TIER4","MI006_INDEX_CD_RT_TIER5","MI006_ADJ_PERC_RATE_TIER1","MI006_ADJ_PERC_RATE_TIER2","MI006_ADJ_PERC_RATE_TIER3","MI006_ADJ_PERC_RATE_TIER4","MI006_ADJ_PERC_RATE_TIER5","RTRIM(MI006_RATE_REPRICE_TYPE) AS MI006_RATE_REPRICE_TYPE").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'SEQ', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER1_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER2_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER3_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER4_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER5_RATE_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER1_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER2_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER3_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER4_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_EFF_TIER5_RATE_END_DT', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_LAST_REPRICE_DATE', 'type': 'decimal(10,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NEXT_REPRICE_DATE', 'type': 'decimal(10,0)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER1', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER2', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER3', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER4', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_FLAT_RATE_TIER5', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER1', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER2', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER3', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER4', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INDEX_CD_RT_TIER5', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER1', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER2', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER3', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER4', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ADJ_PERC_RATE_TIER5', 'type': 'decimal(8,4)', 'nullable': True, 'metadata': {}}, {'name': 'MI006_RATE_REPRICE_TYPE', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}]}))
    
    spark.sql("DROP TABLE IF EXISTS trx_PITA_Lnk_PITA_Tgt_v").show()
    
    print("trx_PITA_Lnk_PITA_Tgt_v")
    
    print(trx_PITA_Lnk_PITA_Tgt_v.schema.json())
    
    print("count:{}".format(trx_PITA_Lnk_PITA_Tgt_v.count()))
    
    trx_PITA_Lnk_PITA_Tgt_v.show(1000,False)
    
    trx_PITA_Lnk_PITA_Tgt_v.write.mode("overwrite").saveAsTable("trx_PITA_Lnk_PITA_Tgt_v")
    

@task.pyspark(conn_id="spark-local")
def DS_TGT_PITA_Lnk_PITA_Tgt_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    trx_PITA_Lnk_PITA_Tgt_v=spark.table('trx_PITA_Lnk_PITA_Tgt_v')
    
    DS_TGT_PITA_Lnk_PITA_Tgt_Part_v=trx_PITA_Lnk_PITA_Tgt_v
    
    spark.sql("DROP TABLE IF EXISTS DS_TGT_PITA_Lnk_PITA_Tgt_Part_v").show()
    
    print("DS_TGT_PITA_Lnk_PITA_Tgt_Part_v")
    
    print(DS_TGT_PITA_Lnk_PITA_Tgt_Part_v.schema.json())
    
    print("count:{}".format(DS_TGT_PITA_Lnk_PITA_Tgt_Part_v.count()))
    
    DS_TGT_PITA_Lnk_PITA_Tgt_Part_v.show(1000,False)
    
    DS_TGT_PITA_Lnk_PITA_Tgt_Part_v.write.mode("overwrite").saveAsTable("DS_TGT_PITA_Lnk_PITA_Tgt_Part_v")
    

@task.pyspark(conn_id="spark-local")
def DS_TGT_PITA(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    DS_TGT_PITA_Lnk_PITA_Tgt_Part_v=spark.table('DS_TGT_PITA_Lnk_PITA_Tgt_Part_v')
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    locations = Template('{{dbdir.pPROCESSING_DIR}}MIS006_PITA.ds').render(job_params)
    
    log.info("write dataset files to "+locations)
    
    spark.table("DS_TGT_PITA_Lnk_PITA_Tgt_Part_v").write.mode("overwrite").format("parquet").save(locations)
    

####################################[Main]###################################
import airflow
with DAG(
    dag_id="job_DBdirect_Mis006_PITA_LONP_Extr_POC",
    start_date=airflow.utils.dates.days_ago(1),
    tags=['datastage'],
) as dag:
    
    job_DBdirect_Mis006_PITA_LONP_Extr_POC_task = job_DBdirect_Mis006_PITA_LONP_Extr_POC()
    
    Job_VIEW_task = Job_VIEW()
    
    NETZ_SRC_TBL_NM2_task = NETZ_SRC_TBL_NM2()
    
    V99A7_task = V99A7()
    
    NETZ_SRC_TBL_NM_task = NETZ_SRC_TBL_NM()
    
    Transformer_84_ln_Nxt_Lst_Part_task = Transformer_84_ln_Nxt_Lst_Part()
    
    Vertical_Pivot_of_Tier_rates_lnk_Source_Part_task = Vertical_Pivot_of_Tier_rates_lnk_Source_Part()
    
    Transformer_84_task = Transformer_84()
    
    Vertical_Pivot_of_Tier_rates_task = Vertical_Pivot_of_Tier_rates()
    
    joi_PITA_joi_LONP_Part_task = joi_PITA_joi_LONP_Part()
    
    Transformer_50_i_Part_task = Transformer_50_i_Part()
    
    Transformer_50_task = Transformer_50()
    
    Copy_59_DSLink61_Part_task = Copy_59_DSLink61_Part()
    
    Copy_59_task = Copy_59()
    
    joi_PITA_joi_PITA_Part_task = joi_PITA_joi_PITA_Part()
    
    joi_PITA_task = joi_PITA()
    
    trx_PITA_trx_PITA_Part_task = trx_PITA_trx_PITA_Part()
    
    trx_PITA_task = trx_PITA()
    
    DS_TGT_PITA_Lnk_PITA_Tgt_Part_task = DS_TGT_PITA_Lnk_PITA_Tgt_Part()
    
    DS_TGT_PITA_task = DS_TGT_PITA()
    
    
    job_DBdirect_Mis006_PITA_LONP_Extr_POC_task >> Job_VIEW_task
    
    Job_VIEW_task >> NETZ_SRC_TBL_NM2_task
    
    Job_VIEW_task >> V99A7_task
    
    Job_VIEW_task >> NETZ_SRC_TBL_NM_task
    
    NETZ_SRC_TBL_NM2_task >> Transformer_84_ln_Nxt_Lst_Part_task
    
    NETZ_SRC_TBL_NM_task >> Vertical_Pivot_of_Tier_rates_lnk_Source_Part_task
    
    Transformer_84_ln_Nxt_Lst_Part_task >> Transformer_84_task
    
    Vertical_Pivot_of_Tier_rates_lnk_Source_Part_task >> Vertical_Pivot_of_Tier_rates_task
    
    Transformer_84_task >> joi_PITA_joi_LONP_Part_task
    
    Vertical_Pivot_of_Tier_rates_task >> Transformer_50_i_Part_task
    
    joi_PITA_joi_LONP_Part_task >> joi_PITA_task
    
    Transformer_50_i_Part_task >> Transformer_50_task
    
    Transformer_50_task >> Copy_59_DSLink61_Part_task
    
    Copy_59_DSLink61_Part_task >> Copy_59_task
    
    Copy_59_task >> joi_PITA_joi_PITA_Part_task
    
    joi_PITA_joi_PITA_Part_task >> joi_PITA_task
    
    joi_PITA_task >> trx_PITA_trx_PITA_Part_task
    
    trx_PITA_trx_PITA_Part_task >> trx_PITA_task
    
    trx_PITA_task >> DS_TGT_PITA_Lnk_PITA_Tgt_Part_task
    
    DS_TGT_PITA_Lnk_PITA_Tgt_Part_task >> DS_TGT_PITA_task
    



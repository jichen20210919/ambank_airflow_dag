
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-02 22:44:39
# @Author  : cloudera
# @File    : job_DBdirect_Mis006_CUSVAA_Extr_POC.py
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
def job_DBdirect_Mis006_CUSVAA_Extr_POC(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def Job_VIEW(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def V80A0(**kw_args) -> str:
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
    
        Q.KEY_1,
    
        Q.MI006_NO_OF_GUARANTOR,
    
        Q.MI006_BLDVTT_GUA1_NAME,
    
        P.MI006_JOINT_BOR_CIF_NO,
    
        ACSN.SHORT_NAME,
    
        CUSVAA2.NAME1,
    
        Q.INTRO_BROKER_INDIC,
    
        BKAC.INTRO_BROKER_NO
    
    FROM (
    
        SELECT 
    
            KEY_1,
    
            LNK_TYP_G_CNT AS MI006_NO_OF_GUARANTOR,
    
            CASE 
    
                WHEN CUSC_CUSTOMER_NO IS NOT NULL AND LINK_TYPE IS NOT NULL 
    
                THEN CUSVAA_NAME1 
    
                ELSE NULL 
    
            END AS MI006_BLDVTT_GUA1_NAME,
    
            CUSVAA_NAME1,
    
            INTRO_BROKER_INDIC,
    
            BORM_CUSTOMER_NO
    
        FROM (
    
            SELECT 
    
                BORM.KEY_1,
    
                ROW_NUMBER() OVER (PARTITION BY BORM.KEY_1 ORDER BY HEX(CUSC.CUSTOMER_NO) ASC) AS o_key,
    
                COUNT(LINK_TYPE) OVER (PARTITION BY BORM.KEY_1) AS LNK_TYP_G_CNT,
    
                CUSC.LINK_TYPE,
    
                CUSC.CUSTOMER_NO AS CUSC_CUSTOMER_NO,
    
                CUSVAA.EXPI_DATE,
    
                CUSVAA.NAME1 AS CUSVAA_NAME1,
    
                BORM.INTRO_BROKER_INDIC,
    
                BORM.CUSTOMER_NO AS BORM_CUSTOMER_NO
    
            FROM {{dbdir.pODS_SCHM}}.BORM BORM
    
            LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.CUSC CUSC 
    
                ON CUSC.ACCT_NO = SUBSTRING(BORM.KEY_1, 4, 16) 
    
                AND CUSC.INST_NO = SUBSTRING(BORM.KEY_1, 1, 3)
    
                AND CUSC.LINK_TYPE = 'G'
    
                AND CUSC.CUSTOMER_NO > 0
    
            LEFT JOIN {{dbdir.pODS_SCHM}}.CUSVAA CUSVAA
    
                ON CUSVAA.INST_NO = '999' 
    
                AND CUSVAA.CUST_NO = CUSC.CUSTOMER_NO 
    
                AND CUSVAA.EXPI_DATE = 99999999
    
        ) A 
    
        WHERE A.o_key = 1
    
    ) Q
    
    LEFT JOIN (
    
        SELECT 
    
            KEY_1,
    
            CUSTOMER_NO AS MI006_JOINT_BOR_CIF_NO
    
        FROM (
    
            SELECT 
    
                BORM.KEY_1,
    
                ROW_NUMBER() OVER (PARTITION BY BORM.KEY_1 ORDER BY HEX(CUSC.CUSTOMER_NO) ASC) AS o_key,
    
                CUSC.LINK_TYPE,
    
                CUSC.CUSTOMER_NO
    
            FROM {{dbdir.pODS_SCHM}}.BORM BORM
    
            LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.CUSC CUSC 
    
                ON CUSC.ACCT_NO = SUBSTRING(BORM.KEY_1, 4, 16) 
    
                AND CUSC.INST_NO = SUBSTRING(BORM.KEY_1, 1, 3)
    
                AND CUSC.LINK_TYPE = 'A' 
    
                AND CUSC.CUSTOMER_NO > 0
    
        ) A 
    
        WHERE A.o_key = 1
    
    ) P ON P.KEY_1 = Q.KEY_1
    
    LEFT JOIN {{dbdir.pODS_SCHM}}.CUSVAA CUSVAA2 
    
        ON CUSVAA2.INST_NO = '999' 
    
        AND CUSVAA2.CUST_NO = Q.BORM_CUSTOMER_NO 
    
        AND CUSVAA2.EXPI_DATE = 99999999
    
    LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.ACSN ACSN 
    
        ON ACSN.INST_NO = SUBSTRING(Q.KEY_1, 1, 3) 
    
        AND ACSN.ACCT_NO = SUBSTRING(Q.KEY_1, 4, 16) 
    
        AND ACSN.SYS_ID = 'BOR'
    
    LEFT OUTER JOIN {{dbdir.pODS_SCHM}}.BKAC BKAC 
    
        ON BKAC.INST_NO = SUBSTRING(Q.KEY_1, 1, 3) 
    
        AND BKAC.ACCT_NO = SUBSTRING(Q.KEY_1, 4, 16) 
    
        AND BKAC.SYS = 'LON'""").render(job_params)
    
    log.info(f"execute sql query {sql}")
    
    NETZ_SRC_TBL_NM_v = spark.sql(sql)
    
    
    
    
    
    #spark.sql(f"use spark_catalog.default").show()
    
    NETZ_SRC_TBL_NM_x_v=NETZ_SRC_TBL_NM_v.select(NETZ_SRC_TBL_NM_v[0].cast('string').alias('KEY_1'),NETZ_SRC_TBL_NM_v[1].cast('integer').alias('MI006_NO_OF_GUARANTOR'),NETZ_SRC_TBL_NM_v[2].cast('string').alias('MI006_BLDVTT_GUA1_NAME'),NETZ_SRC_TBL_NM_v[3].cast('string').alias('MI006_JOINT_BOR_CIF_NO'),NETZ_SRC_TBL_NM_v[4].cast('string').alias('SHORT_NAME'),NETZ_SRC_TBL_NM_v[5].cast('string').alias('NAME1'),NETZ_SRC_TBL_NM_v[6].cast('string').alias('INTRO_BROKER_INDIC'),NETZ_SRC_TBL_NM_v[7].cast('string').alias('INTRO_BROKER_NO'))
    
    NETZ_SRC_TBL_NM_x_v = NETZ_SRC_TBL_NM_x_v.selectExpr("RTRIM(KEY_1) AS KEY_1","MI006_NO_OF_GUARANTOR","MI006_BLDVTT_GUA1_NAME","RTRIM(MI006_JOINT_BOR_CIF_NO) AS MI006_JOINT_BOR_CIF_NO","SHORT_NAME","NAME1","RTRIM(INTRO_BROKER_INDIC) AS INTRO_BROKER_INDIC","RTRIM(INTRO_BROKER_NO) AS INTRO_BROKER_NO").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'KEY_1', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(19)'}}, {'name': 'MI006_NO_OF_GUARANTOR', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVTT_GUA1_NAME', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_JOINT_BOR_CIF_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(16)'}}, {'name': 'SHORT_NAME', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'NAME1', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'INTRO_BROKER_INDIC', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(1)'}}, {'name': 'INTRO_BROKER_NO', 'type': 'string', 'nullable': True, 'metadata': {'__CHAR_VARCHAR_TYPE_STRING': 'char(16)'}}]}))
    
    spark.sql("DROP TABLE IF EXISTS NETZ_SRC_TBL_NM_x_v").show()
    
    print("NETZ_SRC_TBL_NM_x_v")
    
    print(NETZ_SRC_TBL_NM_x_v.schema.json())
    
    print("count:{}".format(NETZ_SRC_TBL_NM_x_v.count()))
    
    NETZ_SRC_TBL_NM_x_v.show(1000,False)
    
    NETZ_SRC_TBL_NM_x_v.write.mode("overwrite").saveAsTable("NETZ_SRC_TBL_NM_x_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_52_x_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    NETZ_SRC_TBL_NM_x_v=spark.table('NETZ_SRC_TBL_NM_x_v')
    
    Transformer_52_x_Part_v=NETZ_SRC_TBL_NM_x_v
    
    spark.sql("DROP TABLE IF EXISTS Transformer_52_x_Part_v").show()
    
    print("Transformer_52_x_Part_v")
    
    print(Transformer_52_x_Part_v.schema.json())
    
    print("count:{}".format(Transformer_52_x_Part_v.count()))
    
    Transformer_52_x_Part_v.show(1000,False)
    
    Transformer_52_x_Part_v.write.mode("overwrite").saveAsTable("Transformer_52_x_Part_v")
    

@task.pyspark(conn_id="spark-local")
def Transformer_52(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_52_x_Part_v=spark.table('Transformer_52_x_Part_v')
    
    Transformer_52_v = Transformer_52_x_Part_v
    
    Transformer_52_Lnk_Cusvaa_Tgt_v = Transformer_52_v.select(expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM KEY_1))""").cast('string').alias('B_KEY'),expr("""IF(ISNULL(SHORT_NAME), NAME1, SHORT_NAME)""").cast('string').alias('MI006_ACSN_SHORT_NAME'),col('MI006_BLDVTT_GUA1_NAME').cast('string').alias('MI006_BLDVTT_GUA1_NAME'),col('MI006_JOINT_BOR_CIF_NO').cast('string').alias('MI006_JOINT_BOR_CIF_NO'),col('MI006_NO_OF_GUARANTOR').cast('integer').alias('MI006_NO_OF_GUARANTOR'),expr("""IF(INTRO_BROKER_INDIC = 'Y', INTRO_BROKER_NO, 0)""").cast('decimal(18,0)').alias('MI006_INTRO_BROKER'))
    
    Transformer_52_Lnk_Cusvaa_Tgt_v = Transformer_52_Lnk_Cusvaa_Tgt_v.selectExpr("B_KEY","MI006_ACSN_SHORT_NAME","MI006_BLDVTT_GUA1_NAME","MI006_JOINT_BOR_CIF_NO","MI006_NO_OF_GUARANTOR","MI006_INTRO_BROKER").to(StructType.fromJson({'type': 'struct', 'fields': [{'name': 'B_KEY', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_ACSN_SHORT_NAME', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_BLDVTT_GUA1_NAME', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_JOINT_BOR_CIF_NO', 'type': 'string', 'nullable': True, 'metadata': {}}, {'name': 'MI006_NO_OF_GUARANTOR', 'type': 'integer', 'nullable': True, 'metadata': {}}, {'name': 'MI006_INTRO_BROKER', 'type': 'decimal(18,0)', 'nullable': True, 'metadata': {}}]}))
    
    spark.sql("DROP TABLE IF EXISTS Transformer_52_Lnk_Cusvaa_Tgt_v").show()
    
    print("Transformer_52_Lnk_Cusvaa_Tgt_v")
    
    print(Transformer_52_Lnk_Cusvaa_Tgt_v.schema.json())
    
    print("count:{}".format(Transformer_52_Lnk_Cusvaa_Tgt_v.count()))
    
    Transformer_52_Lnk_Cusvaa_Tgt_v.show(1000,False)
    
    Transformer_52_Lnk_Cusvaa_Tgt_v.write.mode("overwrite").saveAsTable("Transformer_52_Lnk_Cusvaa_Tgt_v")
    

@task.pyspark(conn_id="spark-local")
def TGT_CUSVAA_Lnk_Cusvaa_Tgt_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    Transformer_52_Lnk_Cusvaa_Tgt_v=spark.table('Transformer_52_Lnk_Cusvaa_Tgt_v')
    
    TGT_CUSVAA_Lnk_Cusvaa_Tgt_Part_v=Transformer_52_Lnk_Cusvaa_Tgt_v
    
    spark.sql("DROP TABLE IF EXISTS TGT_CUSVAA_Lnk_Cusvaa_Tgt_Part_v").show()
    
    print("TGT_CUSVAA_Lnk_Cusvaa_Tgt_Part_v")
    
    print(TGT_CUSVAA_Lnk_Cusvaa_Tgt_Part_v.schema.json())
    
    print("count:{}".format(TGT_CUSVAA_Lnk_Cusvaa_Tgt_Part_v.count()))
    
    TGT_CUSVAA_Lnk_Cusvaa_Tgt_Part_v.show(1000,False)
    
    TGT_CUSVAA_Lnk_Cusvaa_Tgt_Part_v.write.mode("overwrite").saveAsTable("TGT_CUSVAA_Lnk_Cusvaa_Tgt_Part_v")
    

@task.pyspark(conn_id="spark-local")
def TGT_CUSVAA(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    TGT_CUSVAA_Lnk_Cusvaa_Tgt_Part_v=spark.table('TGT_CUSVAA_Lnk_Cusvaa_Tgt_Part_v')
    
    log = logging.getLogger(__name__)
    
    job_params = Variable.get("JOB_PARAMS",deserialize_json=True)
    
    locations = Template('{{dbdir.pPROCESSING_DIR}}MIS006_CUSVAA.ds').render(job_params)
    
    log.info("write dataset files to "+locations)
    
    spark.table("TGT_CUSVAA_Lnk_Cusvaa_Tgt_Part_v").write.mode("overwrite").format("parquet").save(locations)
    

####################################[Main]###################################
import airflow
with DAG(
    dag_id="job_DBdirect_Mis006_CUSVAA_Extr_POC",
    start_date=airflow.utils.dates.days_ago(1),
    tags=['datastage'],
) as dag:
    
    job_DBdirect_Mis006_CUSVAA_Extr_POC_task = job_DBdirect_Mis006_CUSVAA_Extr_POC()
    
    Job_VIEW_task = Job_VIEW()
    
    V80A0_task = V80A0()
    
    NETZ_SRC_TBL_NM_task = NETZ_SRC_TBL_NM()
    
    Transformer_52_x_Part_task = Transformer_52_x_Part()
    
    Transformer_52_task = Transformer_52()
    
    TGT_CUSVAA_Lnk_Cusvaa_Tgt_Part_task = TGT_CUSVAA_Lnk_Cusvaa_Tgt_Part()
    
    TGT_CUSVAA_task = TGT_CUSVAA()
    
    
    job_DBdirect_Mis006_CUSVAA_Extr_POC_task >> Job_VIEW_task
    
    Job_VIEW_task >> V80A0_task
    
    Job_VIEW_task >> NETZ_SRC_TBL_NM_task
    
    NETZ_SRC_TBL_NM_task >> Transformer_52_x_Part_task
    
    Transformer_52_x_Part_task >> Transformer_52_task
    
    Transformer_52_task >> TGT_CUSVAA_Lnk_Cusvaa_Tgt_Part_task
    
    TGT_CUSVAA_Lnk_Cusvaa_Tgt_Part_task >> TGT_CUSVAA_task
    



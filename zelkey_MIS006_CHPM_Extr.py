
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-19 16:35:17
# @Author  : cloudera
# @File    : job_DBdirect_MIS006_CHPM_Extr_POC_ORIGINAL_AIRFLOW_JOB.py
# @Copyright: Cloudera.Inc




from __future__ import annotations
from abc import abstractmethod
from airflow.decorators import task, task_group
from airflow.models import DAG
from airflow.models.dag import DAG
from datetime import datetime, timedelta
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col,expr,lit
from pyspark.sql.types import *
import json
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

@task
def NETZ_SRC_BORM_BOAF(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def V0A13(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def NETZ_SRC_COLM_CHPM(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def NETZ_SRC_BLDVAA(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

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

@task
def NETZ_SRC_Zect(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task.pyspark(conn_id="spark-local")
def Transformer_J_lnk_Source_Part(spark: SparkSession, sc: SparkContext, **kw_args):
        
    

    from ds_functions import spark_register_ds_common_functions
    spark_register_ds_common_functions(spark)
    
    
    NETZ_SRC_BORM_BOAF_lnk_Source_v=spark.table('datastage_temp_job_DBdirect_MIS006_CHPM_Extr_POC__NETZ_SRC_BORM_BOAF_lnk_Source_v')
    
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
    
    Transformer_J_v = Transformer_J_lnk_Source_Part_v.withColumn('DUDE', expr("""IF(LAST, DUEC, CONCAT_WS('', CONCAT_WS('', DUDE, ','), DUEC))""").cast('string').alias('DUDE')).withColumn('DUET', expr("""IF(LAST, DUET, CONCAT_WS('', CONCAT_WS('', DUET, ','), DUET))""").cast('string').alias('DUET')).withColumn('DC', expr("""CASE WHEN NOT DUDE IS NULL THEN SIZE(SPLIT(DUDE, ',')) ELSE 'Y' END""").cast('string').alias('DC')).withColumn('FC', expr("""CASE WHEN ISVALID('int32', DC) = 1 THEN CASE WHEN (CAST(DC AS INT) - LENGTH(DUDE) + LENGTH(REPLACE(DUDE, ',', ''))) = 0 THEN 0 ELSE (CAST(DC AS INT) - LENGTH(DUDE) + LENGTH(REPLACE(DUDE, ',', ''))) END ELSE -99 END""").cast('integer').alias('FC')).withColumn('LAST', expr("""LAST(B_KEY)""").cast('integer').alias('LAST'))
    
    Transformer_J_Remove_Dupe_v = Transformer_J_v.select(col('B_KEY').cast('string').alias('B_KEY'),col('MI006_ARRS_INT_1').cast('decimal(18,3)').alias('MI006_ARRS_INT_1'),col('MI006_BRTH_EFF_DATE').cast('integer').alias('MI006_BRTH_EFF_DATE'),col('MI006_REASON_CD').cast('integer').alias('MI006_REASON_CD'),col('DUEC').cast('string').alias('DUEC'),col('DUET').cast('string').alias('DUET'),col('MI006_BORM_RM_CODE').cast('string').alias('MI006_BORM_RM_CODE'),col('MI006_BORH_REBATE_PERC').cast('string').alias('MI006_BORH_REBATE_PERC'),col('MI006_BORM_ARR_INT_INCR').cast('decimal(18,5)').alias('MI006_BORM_ARR_INT_INCR'),expr("""IF(FC = -99, NULL, FC)""").cast('integer').alias('MI006_NO_OF_DUES_BOAF'),col('Total').cast('decimal(18,3)').alias('MI006_BILLED_AMT_UNPD_BOAF'))
    
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
    
    Join_18_v=Join_18_Colt_Part_v.join(Join_18_Colm_Part_v,['B_KEY'],'inner').join(Join_18_Remove_Dupe_Part_v,['B_KEY'],'inner').join(Join_18_Zect_Part_v,['B_KEY'],'inner')
    
    Join_18_DSLink22_v = Join_18_v.select(Join_18_Colt_Part_v.B_KEY.cast('string').alias('B_KEY'),Join_18_Remove_Dupe_Part_v.MI006_ARRS_INT_1.cast('decimal(18,3)').alias('MI006_ARRS_INT_1'),Join_18_Remove_Dupe_Part_v.MI006_BRTH_EFF_DATE.cast('integer').alias('MI006_BRTH_EFF_DATE'),Join_18_Remove_Dupe_Part_v.MI006_REASON_CD.cast('integer').alias('MI006_REASON_CD'),Join_18_Remove_Dupe_Part_v.DUEC.cast('string').alias('DUEC'),Join_18_Remove_Dupe_Part_v.DUET.cast('string').alias('DUET'),Join_18_Remove_Dupe_Part_v.MI006_BORM_RM_CODE.cast('string').alias('MI006_BORM_RM_CODE'),Join_18_Remove_Dupe_Part_v.MI006_BORH_REBATE_PERC.cast('string').alias('MI006_BORH_REBATE_PERC'),Join_18_Remove_Dupe_Part_v.MI006_BORM_ARR_INT_INCR.cast('decimal(18,5)').alias('MI006_BORM_ARR_INT_INCR'),Join_18_Colt_Part_v.MI006_NO_OF_DISB_ON_NOTE.cast('string').alias('MI006_NO_OF_DISB_ON_NOTE'),Join_18_Colm_Part_v.MI006_DESCRIPTION.cast('string').alias('MI006_DESCRIPTION'),Join_18_Colm_Part_v.MI006_CLASSIFIED_DATE.cast('integer').alias('MI006_CLASSIFIED_DATE'),Join_18_Colm_Part_v.MI006_GUA_CUSTO_NO.cast('string').alias('MI006_GUA_CUSTO_NO'),Join_18_Colm_Part_v.MI006_AMORT_FLAG.cast('string').alias('MI006_AMORT_FLAG'),Join_18_Colm_Part_v.MI006_ZECT_HANDLING_FEE.cast('decimal(18,5)').alias('MI006_ZECT_HANDLING_FEE'),Join_18_Zect_Part_v.MI006_ZECT_OTHER_FEE_sum.cast('decimal(18,5)').alias('MI006_ZECT_OTHER_FEE'),Join_18_Colm_Part_v.MI006_ZECT_EXP_HANDLING_AMT.cast('decimal(18,3)').alias('MI006_ZECT_EXP_HANDLING_AMT'),Join_18_Remove_Dupe_Part_v.MI006_NO_OF_DUES_BOAF.cast('integer').alias('MI006_NO_OF_DUES_BOAF'),Join_18_Remove_Dupe_Part_v.MI006_BILLED_AMT_UNPD_BOAF.cast('decimal(18,3)').alias('MI006_BILLED_AMT_UNPD_BOAF'),Join_18_Colm_Part_v.MI006_RIGH_NPL_COUNTER.cast('string').alias('MI006_RIGH_NPL_COUNTER'),Join_18_Colm_Part_v.MI006_RIGH_PL_DATE.cast('integer').alias('MI006_RIGH_PL_DATE'),Join_18_Colm_Part_v.MI006_ZECT_INPUTTAXNC1.cast('decimal(18,5)').alias('MI006_ZECT_INPUTTAXNC1'))
    
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
    
    Transformer_24_lnk_CHPM_Tgt_v = Transformer_24_v.select(expr("""TRIM(BOTH ' ' FROM TRIM(BOTH '\t' FROM B_KEY))""").cast('string').alias('B_KEY'),col('MI006_ARRS_INT_1').cast('decimal(18,3)').alias('MI006_ARRS_INT_1'),col('MI006_BRTH_EFF_DATE').cast('integer').alias('MI006_BRTH_EFF_DATE'),col('MI006_REASON_CD').cast('integer').alias('MI006_REASON_CD'),col('MI006_BORM_RM_CODE').cast('string').alias('MI006_BORM_RM_CODE'),col('MI006_BORH_REBATE_PERC').cast('string').alias('MI006_BORH_REBATE_PERC'),col('MI006_BORM_ARR_INT_INCR').cast('decimal(18,5)').alias('MI006_BORM_ARR_INT_INCR'),expr("""RIGHT(LPAD(CAST(0 AS STRING), 3, '0') || TRIM(MI006_NO_OF_DISB_ON_NOTE), 3)""").cast('string').alias('MI006_NO_OF_DISB_ON_NOTE'),col('MI006_DESCRIPTION').cast('string').alias('MI006_DESCRIPTION'),col('MI006_CLASSIFIED_DATE').cast('integer').alias('MI006_CLASSIFIED_DATE'),col('MI006_GUA_CUSTO_NO').cast('string').alias('MI006_GUA_CUSTO_NO'),lit(None).cast('string').alias('MI006_AMORT_FLAG'),col('MI006_ZECT_HANDLING_FEE').cast('decimal(18,5)').alias('MI006_ZECT_HANDLING_FEE'),col('MI006_ZECT_OTHER_FEE').cast('decimal(18,5)').alias('MI006_ZECT_OTHER_FEE'),col('MI006_ZECT_EXP_HANDLING_AMT').cast('decimal(18,3)').alias('MI006_ZECT_EXP_HANDLING_AMT'),col('MI006_CLASSIFIED_DATE').cast('integer').alias('MI006_NPL_CLASS_DATE'),col('MI006_CLASSIFIED_DATE').cast('integer').alias('MI006_AM_NPL_DATE'),expr("""RIGHT(CONCAT(REPEAT('0', 3), MI006_NO_OF_DUES_BOAF), 3)""").cast('string').alias('MI006_NO_OF_DUES_BOAF'),col('MI006_BILLED_AMT_UNPD_BOAF').cast('decimal(18,3)').alias('MI006_BILLED_AMT_UNPD_BOAF'),col('MI006_RIGH_NPL_COUNTER').cast('string').alias('MI006_RIGH_NPL_COUNTER'),col('MI006_RIGH_PL_DATE').cast('integer').alias('MI006_RIGH_PL_DATE'),col('MI006_ZECT_INPUTTAXNC1').cast('decimal(20,5)').alias('MI006_ZECT_INPUTTAXNC1'))
    
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
    
    spark.table("DS_TGT_CHPM_COLT_lnk_CHPM_Tgt_Part_v").write.mode("overwrite").format("parquet").save("#dbdir.pPROCESSING_DIR#MIS006_CHPM.ds")
    

####################################[Main]###################################
import airflow
with DAG(
    dag_id="zelkey_MIS006_CHPM_Extr",
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
    




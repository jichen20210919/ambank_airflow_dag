#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-19 15:35:00
# @Author  : cloudera
# @File    : airflow_JOB_TCS_40_ARLONSKEY_AR_SCD.py
# @Copyright: Cloudera.Inc

from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timedelta

_SPARK_TASK_RUNNER = os.environ.get("SPARK_TASK_RUNNER") == "1"

if not _SPARK_TASK_RUNNER:
    import airflow
    import pendulum
    from airflow.decorators import task
    from airflow.models import DAG, Variable
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
else:
    def _identity(func=None, **_kwargs):
        if func is None:
            return lambda f: f
        return func

    class _TaskDecorator:
        def __call__(self, *args, **kwargs):
            return _identity(*args, **kwargs)

        def pyspark(self, *args, **kwargs):
            return _identity(*args, **kwargs)

    task = _TaskDecorator()

    class Variable:
        @staticmethod
        def get(key, default_var=None, deserialize_json=False):
            if key == "JOB_PARAMS":
                raw = os.environ.get("JOB_PARAMS_B64")
                if raw:
                    import base64 as _base64
                    import json as _json
                    return _json.loads(_base64.b64decode(raw.encode()).decode())
            return default_var if default_var is not None else {}

    class DAG:
        pass

    class SparkSubmitOperator:
        pass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, TimestampType, DecimalType


@task
def JOB_TCS_40_ARLONSKEY_AR_SCD(**kw_args) -> str:
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))


@task
def Job_VIEW(**kw_args) -> str:
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))


@task.pyspark(conn_id="spark-local")
def RUN_TCS_40_ARLONSKEY_AR_SCD(spark: SparkSession, sc, **kw_args):
    # ==========================================
    # 1. PARAMETERS & CONFIGURATION
    # ==========================================
    job_params = Variable.get("JOB_PARAMS", deserialize_json=True)
    schema_staging = job_params.get(
        "SILVER_DB",
        job_params.get("silver_db", job_params.get("EDW_STAGING_CORE", job_params.get("edw_staging_core", "silver"))),
    )
    schema_work = job_params.get(
        "SILVER_DB",
        job_params.get("silver_db", job_params.get("EDW_WORK_CORE", job_params.get("edw_work_core", "silver"))),
    )
    schema_sor = job_params.get(
        "SILVER_DB",
        job_params.get("silver_db", job_params.get("EDW_SOR", job_params.get("edw_sor", "silver"))),
    )
    cSchemaNm1 = schema_staging  # Staging
    cSchemaNm2 = schema_work     # Work
    cSchemaNm3 = schema_sor      # SOR (System of Record)
    batch_id = int(job_params.get("BATCH_ID", job_params.get("batch_id", 0)))
    batch_seq = int(job_params.get("BATCH_SEQ", job_params.get("batch_seq", 0)))
    iBatchId = batch_id if batch_id > 0 else 12345
    iBatchSeq = batch_seq if batch_seq > 0 else 1
    required_tables = (
        f"{cSchemaNm2}.tcs_ar_lon_skey",
        f"{cSchemaNm3}.ar",
    )
    missing = [t for t in required_tables if not spark.catalog.tableExists(t)]
    if missing:
        raise ValueError("Missing required tables in expected schemas: {}".format(", ".join(missing)))
    
    # Capture Start Time (equivalent to dStrtDtTm variable)
    dStrtDtTm_value = datetime.utcnow()
    dStrtDtTm = F.lit(dStrtDtTm_value)
    
    # ==========================================
    # 2. LOAD DATA
    # ==========================================
    
    # Source: The Staging/Work Key Table containing updates/new rows
    df_skey = spark.table(f"{cSchemaNm2}.tcs_ar_lon_skey")
    
    # Filter for New or Changed records that need a new active row
    df_new_rows = df_skey.filter(F.col("keyflag").isin("SCD", "NEW"))
    
    # Map Source columns to Target schema
    df_inserts = df_new_rows.select(
        F.col("ar_id"),
        F.col("unq_id_src_stm"),
        F.col("eff_dt"),
        F.trim(F.col("ar_tp_id")).alias("ar_tp_id"),
        F.col("ar_frq_tp_id"),
        F.col("ar_pps_tp_id"),
        F.col("ar_rsn_tp_id"),
        F.col("ar_fnc_st_tp_id"),
        F.col("ar_fnc_st_dt"),
        F.col("ar_lcs_tp_id"),
        F.col("ar_lcs_dt"),
        F.col("prim_rspl_cntr_id"),
        F.col("prim_rspl_cntr_dt"),
        F.col("ar_term_tp_id"),
        F.col("orig_cnl_id"),
        F.col("dnmn_ccy_id"),
        F.col("nbr_sgn"),
        F.col("prn_ar_id"),
        F.col("est_end_dt"),
        F.col("rnew_f"),
        F.col("nxt_rnew_dt"),
        F.col("ar_nm"),
        F.col("dsc"),
        F.col("fnc_mkt_imt_tp_id"),
        F.col("pd_id"),
        F.col("bsh_rpt_cgy_id"),
        F.col("ac_ar_tp_id"),
        F.col("crn_fix_prd_end_dt"),
        F.col("ac_no"),
        F.col("int_clcn_eff_dt"),
        F.col("nxt_cmpd_int_dt"),
        F.col("xtrt_f"),
        F.col("iban_ac_no"),
        F.col("non_pymt_dflt_f").alias("am_non_pymt_dflt_f"),
        F.col("pd_cd").alias("am_pd_cd"),
        F.col("non_acr_st_dflt_f").alias("am_non_acr_st_dflt_f"),
        F.col("src_tnr_info").alias("am_src_tnr_info"),
        F.col("ac_cgy_cd").alias("am_ac_cgy_cd"),
        F.col("reprc_tp_id").alias("am_reprc_tp_id"),
        F.col("last_rprc_dt").alias("am_last_rprc_dt"),
        F.col("nxt_rprc_dt").alias("am_nxt_rprc_dt"),
        F.col("int_acr_st_tp_id").alias("am_int_acr_st_tp_id"),
        F.col("int_acr_st_dt").alias("am_int_acr_st_dt"),
        F.col("int_acr_end_dt").alias("am_int_acr_end_dt"),
        F.col("lgl_ent_id").alias("am_lgl_ent_id"),
        F.col("bnkg_cncpt_cd").alias("am_bnkg_cncpt_cd"),
        F.col("nbr_of_tnr").alias("am_nbr_of_tnr"),
        F.col("co_cd").alias("am_co_cd"),
        F.col("ac_ovrd_f").alias("am_ac_ovrd_f"),
        F.col("ac_opn_dt").alias("am_ac_opn_dt"),
        F.col("prim_idy_cl_id").alias("am_prim_idy_cl_id"),
        F.col("rltnp_mgr_cd").alias("am_rltnp_mgr_cd"),
        F.col("rprg_ou_ip_id"),
        F.col("cnrl_bnk_idy_cl_id").alias("am_cnrl_bnk_idy_cl_id"),
        F.col("rvsn_tnr_info").alias("am_rvsn_tnr_info"),
        F.col("chk_st_cd").alias("am_chk_st_cd"),
        F.col("stmt_frq").alias("am_stmt_frq"),
        F.col("fix_pft_shr_rto").alias("am_fix_pft_shr_rto"),
        F.lit(None).cast(TimestampType()).alias("am_last_fnc_dt"),
        F.col("ac_cls_dt").alias("am_ac_cls_dt"),
        F.col("int_clcn_end_dt").alias("am_int_clcn_end_dt"),
        F.col("last_stmt_dt"),
        F.col("num_of_stmt").alias("am_num_of_stmt"),
        F.col("pymt_frq_prdc_id").alias("am_pymt_frq_prdc_id"),
        F.col("own_cnt").alias("am_own_cnt"),
        F.col("grc_prd_end_dt").alias("am_grc_prd_end_dt"),
        F.col("npa_st").alias("am_npa_st"),
        F.col("nperf_dt").alias("am_nperf_dt"),
        F.col("npa_cl_dt").alias("am_npa_cl_dt"),
        F.col("rr_tp_id").alias("am_rr_tp_id"),
        F.col("rr_cgy").alias("am_rr_cgy"),
        F.col("rr_sub_cgy").alias("am_rr_sub_cgy"),
        F.col("rr_st").alias("am_rr_st"),
        F.col("rr_rsn").alias("am_rr_rsn"),
        F.col("rr_cnt").alias("am_rr_cnt"),
        F.col("rr_tag_dt").alias("am_rr_tag_dt"),
        F.trim(F.col("wtch_list_tag")).alias("am_wtch_list_tag"),
        F.col("wtch_list_tag_dt").alias("am_wtch_list_tag_dt"),
        F.trim(F.col("frs_mprd_tag")).alias("am_frs_mprd_tag"),
        F.col("frs_st_dt").alias("am_frs_st_dt"),
        F.col("prscr_rate_cd").alias("am_prscr_rate_cd"),
        F.lit(None).cast(TimestampType()).alias("am_src_rec_crt_dt"),
        F.col("mrgn_rate").alias("am_mrgn_rate"),
        F.col("reprc_rset_tp_cd").alias("am_reprc_rset_tp_cd"),
        F.col("in_dflt_f").alias("am_in_dflt_f"),
        F.col("tnr_unit_msr").alias("am_tnr_unit_msr"),
        F.col("ac_sr_no"),
        F.col("last_pymt_dt").alias("am_last_pymt_dt"),
        F.col("pft_shr_rto_bnk").alias("am_pft_shr_rto_bnk"),
        F.col("pft_shr_rto_cst").alias("am_pft_shr_rto_cst"),
        F.col("cst_ctzn_cd").alias("am_cst_ctzn_cd"),
        F.col("base_id").alias("am_base_id"),
        F.col("rate_id").alias("am_rate_id"),
        F.col("rsk_st").alias("am_rsk_st"),
        F.col("rshd_cd").alias("am_rshd_cd"),
        F.col("npl_bfr_rr_dt").alias("am_npl_bfr_rr_dt"),
        F.col("pl_dt").alias("am_pl_dt"),
        F.col("ATR_GRC_PRD").alias("AM_ATR_GRC_PRD"),
        F.col("RM_CODE_RTL_F").alias("AM_RM_CODE_RTL_F"),
        F.lit(99991231).alias("end_dt"), # New records are always active
        F.lit(None).cast(IntegerType()).alias("am_ac_age"),
        F.lit(None).cast(TimestampType()).alias("am_src_rcrd_udt_dt"),
        F.lit(None).cast(IntegerType()).alias("AM_PYMT_CNTER"),
        dStrtDtTm.alias("ppn_dt"),
        F.lit("TCS").alias("src_stm_id"),
        F.lit(iBatchId).alias("edw_btch_id"),
        F.col("frst_rr_dt").alias("am_frst_rr_dt"),
        F.col("ar_long_nm"),
        F.col("EXCL_DPD_WRTOF_VLD_IND"),
        F.col("RR_PERF_AC_REHAB_F"),
        F.col("RR_PERF_AC_PRMPT_PYMT_CNTER"),
        F.col("rr_tot_amz_pft"),
        F.col("rr_mo_amz_pft"),
        F.col("SCD_CCPT_CL"),
        F.col("EIY")
    )
    
    count_inserts = df_inserts.count()

    count_updates = spark.sql(
        f"""
        SELECT COUNT(1)
        FROM {cSchemaNm3}.ar a
        JOIN (
            SELECT ar_id
            FROM {cSchemaNm2}.tcs_ar_lon_skey
            WHERE keyflag = 'SCD'
        ) b
          ON a.ar_id = b.ar_id
        WHERE a.end_dt = 99991231
        """
    ).collect()[0][0]

    target_schema = spark.table(f"{cSchemaNm3}.ar").schema

    # Cast TIMESTAMP columns that target INTEGERs to a YYYYMMDD integer.
    target_type_map = {field.name: field.dataType for field in target_schema}
    timestamp_to_int = [
        field.name
        for field in df_inserts.schema
        if isinstance(field.dataType, TimestampType)
        and isinstance(target_type_map.get(field.name), IntegerType)
    ]
    for col_name in timestamp_to_int:
        df_inserts = df_inserts.withColumn(
            col_name,
            F.when(F.col(col_name).isNull(), None)
            .otherwise(F.date_format(F.col(col_name), "yyyyMMdd").cast(IntegerType()))
        )

    # ==========================================
    # 5. UPDATE EXISTING RECORDS (SCD)
    # ==========================================
    merge_sql = f"""
    MERGE INTO {cSchemaNm3}.ar AS tgt
    USING (
        SELECT ar_id, eff_dt
        FROM {cSchemaNm2}.tcs_ar_lon_skey
        WHERE keyflag = 'SCD'
    ) AS src
    ON tgt.ar_id = src.ar_id AND tgt.end_dt = 99991231
    WHEN MATCHED THEN UPDATE SET tgt.end_dt = src.eff_dt
    """
    spark.sql(merge_sql)

    # ==========================================
    # 6. INSERT NEW RECORDS
    # ==========================================
    aligned_columns = []
    for field in target_schema:
        if field.name in df_inserts.columns:
            aligned_columns.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            aligned_columns.append(F.lit(None).cast(field.dataType).alias(field.name))

    df_inserts_aligned = df_inserts.select(*aligned_columns)
    df_inserts_aligned.write.mode("append").insertInto(f"{cSchemaNm3}.ar")
    
    # ==========================================
    # 7. AUDIT LOGGING
    # ==========================================

    audit_data = [{
        "job_name": "job_tcs_40_arlonskey_ar_scd",
        "start_time": dStrtDtTm_value,
        "end_time": datetime.utcnow(),
        "input_rec": count_inserts,
        "update_rec": count_updates,
        "insert_rec": count_inserts,
        "batch_seq": iBatchSeq,
        "status": "SUCCESS"
    }]
    
    # Assuming Audit Table Exists
    spark.createDataFrame(audit_data) \
        .write.mode("append") \
        .saveAsTable(f"{cSchemaNm1}.job_tcs_ins_audit_recon_dtl")
    
    print(f"SUCCESS: Updates {count_updates}, Inserts {count_inserts}")

####################################[Main]###################################
if not _SPARK_TASK_RUNNER:
    _JOB_PARAMS_B64 = base64.b64encode(
        json.dumps(Variable.get("JOB_PARAMS", default_var={}, deserialize_json=True)).encode()
    ).decode()

    with DAG(
        dag_id="JOB_TCS_40_ARLONSKEY_AR_SCD",
        default_args={"owner": "airflow", "retries": 0, "retry_delay": timedelta(minutes=5)},
        schedule_interval=None,
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=False,
        tags=["spark", "storedproc", "mis006"],
    ) as dag:
        JOB_TCS_40_ARLONSKEY_AR_SCD_task = JOB_TCS_40_ARLONSKEY_AR_SCD()
        Job_VIEW_task = Job_VIEW()

        SPARK_CONF_COMMON = {
            "spark.executor.instances": "10",
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.defaultCatalog": "spark_catalog",
            "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083",
            "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar",
            "spark.dynamicAllocation.enabled": "false",
            "spark.shuffle.service.enabled": "false",
            "spark.app.name": "JOB_TCS_40_ARLONSKEY_AR_SCD",
            "spark.openlineage.appName": "JOB_TCS_40_ARLONSKEY_AR_SCD",
            "spark.openmetadata.transport.pipelineServiceName": "JOB_TCS_40_ARLONSKEY_AR_SCD",
            "spark.openmetadata.transport.pipelineName": "JOB_TCS_40_ARLONSKEY_AR_SCD",
        }

        RUN_TCS_40_ARLONSKEY_AR_SCD_task = SparkSubmitOperator(
            conf=SPARK_CONF_COMMON,
            task_id="RUN_TCS_40_ARLONSKEY_AR_SCD",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="RUN_TCS_40_ARLONSKEY_AR_SCD",
            deploy_mode="cluster",
            principal="airflow@CLOUDERA.LOCAL",
            keytab="/etc/security/keytabs/airflow.keytab",
            py_files=f"/home/ec2-user/airflow/ds_functions.py,{__file__},/home/ec2-user/airflow/py_deps/jinja2.zip,/home/ec2-user/airflow/py_deps/markupsafe.zip",
            env_vars={
                "SPARK_TASK_RUNNER": "1",
                "HADOOP_CONF_DIR": "/etc/hadoop/conf",
                "YARN_CONF_DIR": "/etc/hadoop/conf",
                "HIVE_CONF_DIR": "/etc/hive/conf",
                "JOB_PARAMS_B64": _JOB_PARAMS_B64,
            },
            application_args=["--module", __file__, "--task", "RUN_TCS_40_ARLONSKEY_AR_SCD"],
        )

        JOB_TCS_40_ARLONSKEY_AR_SCD_task >> Job_VIEW_task
        Job_VIEW_task >> RUN_TCS_40_ARLONSKEY_AR_SCD_task

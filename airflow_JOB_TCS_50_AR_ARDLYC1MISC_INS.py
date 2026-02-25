#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-10 00:34:09
# @Author  : cloudera
# @File    : airflow_JOB_TCS_50_AR_ARDLYC1MISC_INS.py
# @Copyright: Cloudera.Inc

from __future__ import annotations

import base64
import json
import logging
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

from pyspark import SparkContext
from pyspark.sql import SparkSession


@task
def JOB_TCS_50_AR_ARDLYC1MISC_INS(**kw_args) -> str:
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))


@task
def Job_VIEW(**kw_args) -> str:
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))


@task.pyspark(conn_id="spark-local")
def RUN_TCS_50_AR_ARDLYC1MISC_INS(spark: SparkSession, sc: SparkContext, **kw_args):
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, DecimalType, StringType, TimestampType
    from pyspark.sql.window import Window
    from ds_functions import spark_register_ds_common_functions

    spark_register_ds_common_functions(spark)

    log = logging.getLogger(__name__)

    job_params = Variable.get("JOB_PARAMS", deserialize_json=True)
    cSchemaNm1 = job_params.get(
        "SILVER_DB",
        job_params.get("silver_db", job_params.get("EDW_STAGING_CORE", job_params.get("edw_staging_core", "silver"))),
    )
    cSchemaNm3 = job_params.get(
        "SILVER_DB",
        job_params.get("silver_db", job_params.get("EDW_SOR", job_params.get("edw_sor", "silver"))),
    )
    cSchemaNm4 = job_params.get(
        "GOLD_DB",
        job_params.get("gold_db", job_params.get("EDW_COMMON", job_params.get("edw_common", "gold"))),
    )
    iBatchId = int(job_params.get("BATCH_ID", job_params.get("batch_id", 0)))
    iBatchSeq = int(job_params.get("BATCH_SEQ", job_params.get("batch_seq", 0)))

    required_tables = (
        f"{cSchemaNm1}.batch_run_dtl",
        f"{cSchemaNm1}.job_adt_recon_dtl",
        f"{cSchemaNm3}.ar",
        f"{cSchemaNm3}.ar_x_ar",
        f"{cSchemaNm3}.dep_ar",
        f"{cSchemaNm3}.fnc_svc_ar",
        f"{cSchemaNm3}.am_ars",
        f"{cSchemaNm3}.am_od_ar",
        f"{cSchemaNm3}.am_ar_x_pst_adr",
        f"{cSchemaNm3}.pst_adr",
        f"{cSchemaNm3}.am_ar_udf",
        f"{cSchemaNm3}.am_ar_x_coa",
        f"{cSchemaNm3}.ar_x_cl",
        f"{cSchemaNm3}.am_src_ref_code",
        f"{cSchemaNm3}.ar_x_lmt_tp",
        f"{cSchemaNm3}.am_ar_prfl",
        f"{cSchemaNm4}.ar_dly_c1",
    )
    missing = [t for t in required_tables if not spark.catalog.tableExists(t)]
    if missing:
        raise ValueError("Missing required tables in expected schemas: {}".format(", ".join(missing)))

    start_time = datetime.now()

    # ==========================================
    # 1. PARAMETERS & CONFIGURATION
    # ==========================================

    # Start Time
    dStrtDtTm = F.current_timestamp()

    # ==========================================
    # 2. FETCH BUSINESS DATE
    # ==========================================
    # Logic: Fetch bsn_date from batch_run_dtl
    row_bsn = spark.sql(f"""
        SELECT bsn_date FROM {cSchemaNm1}.batch_run_dtl 
        WHERE batch_id = {iBatchId}
    """).collect()

    if row_bsn:
        iBsnDt = row_bsn[0]['bsn_date']
    else:
        # Fallback or error handling
        iBsnDt = 20230101 # Default or raise error
        print("Warning: Business Date not found, using default.")

    # Helper to calculate previous day for history join
    # Assuming iBsnDt is integer YYYYMMDD, we convert to string, date, sub 1 day, back to int
    prev_day_expr = F.date_sub(F.to_date(F.lit(str(iBsnDt)), 'yyyyMMdd'), 1)
    iPrevBsnDt_expr = F.date_format(prev_day_expr, 'yyyyMMdd').cast(IntegerType())

    # ==========================================
    # 3. LOAD & FILTER SOURCE TABLES
    # ==========================================
    # We define a helper to load active records only
    def load_active(table_name):
        return spark.table(f"{cSchemaNm3}.{table_name}") \
                    .filter(F.col("end_dt") == 99991231) \
                    .filter(F.col("src_stm_id") == 'TCS')

    df_ar = load_active("ar").filter((F.col("ac_ar_tp_id") != 'ODFCY') | F.col("ac_ar_tp_id").isNull()).alias("ar")
    df_ar_fac = load_active("ar").alias("ar_fac") # Used for self join
    df_ar_full = load_active("ar").alias("ar_full") # Used for subqueries

    df_ar_x_ar = load_active("ar_x_ar").alias("ar_x_ar")
    df_dep_ar = load_active("dep_ar").alias("dep_ar")
    df_fnc_svc_ar = load_active("fnc_svc_ar").alias("fnc")
    df_am_ars = load_active("am_ars").alias("am_ars")
    df_am_od_ar = load_active("am_od_ar").alias("am_od_ar")

    df_am_ar_x_pst_adr = load_active("am_ar_x_pst_adr").alias("arxpstadr")
    df_pst_adr = spark.table(f"{cSchemaNm3}.pst_adr").alias("pstadr") # Address doesn't always have src_stm_id filter in SQL
    df_am_ar_udf = load_active("am_ar_udf").alias("udf")
    df_am_ar_x_coa = load_active("am_ar_x_coa").alias("amarxcoa")
    df_am_ar_prfl = load_active("am_ar_prfl").alias("prfl")
    df_ar_x_lmt_tp = load_active("ar_x_lmt_tp").alias("lmttp")

    # Reference Codes
    df_ref = spark.table(f"{cSchemaNm3}.am_src_ref_code").filter(F.col("src_stm_id") == 'TCS')
    df_ref_dep = df_ref.filter(F.trim(F.col("src_tbl_nm")) == 'DEP_TERM_PAY_FREQ').alias("repymt_frq_dep")
    df_ref_lon = df_ref.filter(F.trim(F.col("src_tbl_nm")) == 'LON_REPAY_FREQ').alias("repymt_frq_loan")

    # History Table for Interest Calculation
    # Note: SQL joins on calculated date string
    df_ar_dly_c1_h = spark.table(f"{cSchemaNm4}.ar_dly_c1").alias("ar_dly_c1_h")

    # ==========================================
    # 4. PREPARE SUB-QUERIES / AGGREGATIONS
    # ==========================================

    # 6. Sub Query - num_of_rnew
    # SELECT COUNT(DISTINCT nxt_rnew_dt), ar_id ... WHERE ar_tp_id in ('DEP','INV')
    df_rnew = df_ar_full.filter(
        F.col("nxt_rnew_dt").isNotNull() & 
        F.col("ar_tp_id").isin("DEP", "INV")
    ).groupBy("ar_id").agg(F.countDistinct("nxt_rnew_dt").alias("num_of_rnew")).alias("rnew")

    # ar_mat_dt Subquery
    # Max expy_dt from FNC joined to AR
    df_ar_mat_dt = df_ar_full.join(df_fnc_svc_ar, F.col("ar_full.ar_id") == F.col("fnc.fnc_svc_ar_id")) \
        .groupBy("ar_full.prn_ar_id") \
        .agg(F.max("fnc.expy_dt").alias("od_mat_dt")) \
        .alias("ar_mat_dt")

    # od_aprv Subquery
    # MAX ac_opn_dt where split_part(..., 4) = '01'
    df_od_aprv = df_ar_full.filter(F.col("ac_ar_tp_id") == 'ODFCY') \
        .groupBy("prn_ar_id") \
        .agg(F.max(F.when(F.split(F.col("unq_id_src_stm"), "#").getItem(3) == '01', F.col("am_ac_opn_dt"))).alias("od_aprv_dt")) \
        .alias("od_aprv")

    # prn_erly_aprv_dt Subquery
    # Pivoted max dates for ODFCY accounts
    # NOTE: Split index 4 in Netezza (1-based) is index 3 in Python (0-based)
    df_prn_erly = df_ar_full.join(df_fnc_svc_ar, F.col("ar_full.ar_id") == F.col("fnc.fnc_svc_ar_id")) \
        .filter(F.col("ar_full.ac_ar_tp_id") == 'ODFCY') \
        .filter(F.split(F.col("ar_full.unq_id_src_stm"), "#").getItem(3).isin('01', '02', '03', '04')) \
        .groupBy("ar_full.prn_ar_id") \
        .agg(
            F.max(F.when(F.split(F.col("ar_full.unq_id_src_stm"), "#").getItem(3) == '01', F.col("fnc.am_brth_eff_dt"))).alias("erly_aprv_dt"),
            F.max(F.when(F.split(F.col("ar_full.unq_id_src_stm"), "#").getItem(3) == '01', F.col("fnc.am_scr_ind_dt"))).alias("scr_ind_dt_1"),
            F.max(F.when(F.split(F.col("ar_full.unq_id_src_stm"), "#").getItem(3) == '02', F.col("fnc.am_scr_ind_dt"))).alias("scr_ind_dt_2"),
            F.max(F.when(F.split(F.col("ar_full.unq_id_src_stm"), "#").getItem(3) == '03', F.col("fnc.am_scr_ind_dt"))).alias("scr_ind_dt_3"),
            F.max(F.when(F.split(F.col("ar_full.unq_id_src_stm"), "#").getItem(3) == '04', F.col("fnc.am_scr_ind_dt"))).alias("scr_ind_dt_4")
        ).alias("prn_erly_aprv_dt")

    # AR_X_CL Filters
    def get_cl_df(scm_id):
        return load_active("ar_x_cl") \
            .filter(F.col("cl_scm_id") == scm_id) \
            .filter(F.col("ar_x_cl_tp_id") == 'IND') \
            .alias(f"arxcl_{scm_id.lower()}")

    df_arxcl_wroff = get_cl_df("WROFFIND")
    df_arxcl_restru = get_cl_df("RESTRU")
    df_arxcl_npasset = get_cl_df("NPASSET")
    df_arxcl_swift = get_cl_df("SWIFTIND")
    df_arxcl_siind = get_cl_df("SIIND")
    df_arxcl_lnscagams = get_cl_df("LNSCAGAMS")
    df_arxcl_acsoldflg = get_cl_df("ACSOLDFLG")
    df_arxcl_bkruptflg = get_cl_df("BKRUPTFLG")

    # COA max entity code (v1.17)
    df_amarxcoa_mx = load_active("am_ar_x_coa").groupBy("ar_id").agg(F.max("ent_cd").alias("gl_ent_code")).alias("amarxcoa_mx")

    # MTD Interest Accrual Logic (The complex one)
    # Join DEP_AR with History Table on previous day's batch date
    df_mtd_int = df_dep_ar.join(
        df_ar_dly_c1_h,
        (F.col("dep_ar.dep_ar_id") == F.col("ar_dly_c1_h.ar_id")) & 
        (F.substring(F.col("ar_dly_c1_h.btch_dt").cast(StringType()), 1, 8) == iPrevBsnDt_expr.cast(StringType())),
        "left"
    ).join(
        df_arxcl_swift,
        F.col("dep_ar.dep_ar_id") == F.col("arxcl_swiftind.ar_id"),
        "left"
    ).select(
        F.col("dep_ar.dep_ar_id"),
        F.when(
            F.substring(F.col("ar_dly_c1_h.btch_dt").cast(StringType()), 1, 6) != str(iBsnDt)[:6],
            F.when((F.col("dep_ar.am_pnp_bal_orig_amt") > 0) & 
                   ((F.col("arxcl_swiftind.cl_val_id") == 'N') | F.col("arxcl_swiftind.cl_val_id").isNull()),
                   F.coalesce(F.col("dep_ar.am_int_incrm_amt"), F.lit(0)))
        ).otherwise(
            F.when((F.col("dep_ar.am_pnp_bal_orig_amt") > 0) & 
                   ((F.col("arxcl_swiftind.cl_val_id") == 'N') | F.col("arxcl_swiftind.cl_val_id").isNull()),
                   F.coalesce(F.col("dep_ar.am_int_incrm_amt"), F.lit(0)) + F.coalesce(F.col("ar_dly_c1_h.int_acr_amt"), F.lit(0)))
            .otherwise(F.coalesce(F.col("ar_dly_c1_h.int_acr_amt"), F.lit(0)))
        ).alias("int_acr_amt")
    ).alias("mtd_int_acr_amt")

    # ==========================================
    # 5. MAIN JOIN SEQUENCE
    # ==========================================

    df_joined = df_ar \
        .join(df_ar_x_ar, (F.col("ar.ar_id") == F.col("ar_x_ar.obj_ar_id")) & (F.col("ar_x_ar.ar_x_ar_tp_id") == 'FACTOAC'), "left") \
        .join(df_ar_fac, F.col("ar_x_ar.sbj_ar_id") == F.col("ar_fac.ar_id"), "left") \
        .join(df_dep_ar, F.col("ar.ar_id") == F.col("dep_ar.dep_ar_id"), "left") \
        .join(df_fnc_svc_ar, F.col("ar.ar_id") == F.col("fnc.fnc_svc_ar_id"), "left") \
        .join(df_am_ars, F.col("ar.ar_id") == F.col("am_ars.ar_id"), "left") \
        .join(df_am_od_ar, F.col("ar.ar_id") == F.col("am_od_ar.od_ar_id"), "left") \
        .join(df_rnew, F.col("ar.ar_id") == F.col("rnew.ar_id"), "left") \
        .join(df_am_ar_x_pst_adr, (F.col("ar.ar_id") == F.col("arxpstadr.ar_id")), "left") \
        .join(df_pst_adr, F.col("arxpstadr.pst_adr_id") == F.col("pstadr.pst_adr_id"), "left") \
        .join(df_am_ar_udf, F.col("ar.ar_id") == F.col("udf.ar_id"), "left") \
        .join(df_am_ar_x_coa, (F.col("ar.ar_id") == F.col("amarxcoa.ar_id")) & (F.col("amarxcoa.bal_cd") == 'H01'), "left") \
        .join(df_amarxcoa_mx, F.col("ar.ar_id") == F.col("amarxcoa_mx.ar_id"), "left") \
        .join(df_arxcl_wroff, F.col("ar.ar_id") == F.col("arxcl_wroffind.ar_id"), "left") \
        .join(df_arxcl_restru, F.col("ar.ar_id") == F.col("arxcl_restru.ar_id"), "left") \
        .join(df_arxcl_npasset, F.col("ar.ar_id") == F.col("arxcl_npasset.ar_id"), "left") \
        .join(df_arxcl_swift, F.col("ar.ar_id") == F.col("arxcl_swiftind.ar_id"), "left") \
        .join(df_arxcl_siind, F.col("ar.ar_id") == F.col("arxcl_siind.ar_id"), "left") \
        .join(df_arxcl_lnscagams, F.col("ar.ar_id") == F.col("arxcl_lnscagams.ar_id"), "left") \
        .join(df_arxcl_acsoldflg, F.col("ar.ar_id") == F.col("arxcl_acsoldflg.ar_id"), "left") \
        .join(df_arxcl_bkruptflg, F.col("ar.ar_id") == F.col("arxcl_bkruptflg.ar_id"), "left") \
        .join(df_ar_mat_dt, F.col("ar.ar_id") == F.col("ar_mat_dt.prn_ar_id"), "left") \
        .join(df_ref_dep, F.col("repymt_frq_dep.src_ref_code") == F.col("ar.am_pymt_frq_prdc_id"), "left") \
        .join(df_ref_lon, F.col("repymt_frq_loan.src_ref_code") == F.col("ar.am_pymt_frq_prdc_id"), "left") \
        .join(df_od_aprv, F.col("ar.ar_id") == F.col("od_aprv.prn_ar_id"), "left") \
        .join(df_prn_erly, F.col("ar.ar_id") == F.col("prn_erly_aprv_dt.prn_ar_id"), "left") \
        .join(df_mtd_int, F.col("ar.ar_id") == F.col("mtd_int_acr_amt.dep_ar_id"), "left") \
        .join(df_ar_x_lmt_tp, (F.col("ar.ar_id") == F.col("lmttp.ar_id")) & (F.col("lmttp.lmt_tp_id") == 'ODDRLIMT'), "left") \
        .join(df_am_ar_prfl, F.col("ar.ar_id") == F.col("prfl.ar_id"), "left")

    # ==========================================
    # 6. SELECT EXPRESSIONS (LOGIC MAPPING)
    # ==========================================

    # Helper for Split Part: SPLIT_PART(col, '#', 3) in SQL is index 2 in Python
    def split_part(col, delim, idx_1_based):
        return F.split(col, delim).getItem(idx_1_based - 1)

    # Helper for Loan/Bor vs Dep/Inv logic
    # Common pattern: IF LON/BOR THEN X ELSE IF DEP/INV THEN Y
    def if_lon_bor(then_col, else_col=None):
        return F.when(F.col("ar.ar_tp_id").isin("LON", "BOR"), then_col).otherwise(else_col)

    def if_lon_bor_dep_inv(lon_val, dep_val):
        return F.when(F.col("ar.ar_tp_id").isin("LON", "BOR"), lon_val) \
                .when(F.col("ar.ar_tp_id").isin("DEP", "INV"), dep_val)

    df_final = df_joined.select(
        F.col("ar.ar_id"),
        F.col("ar.eff_dt"),
        F.col("ar.src_stm_id"),
        # fcy_num logic
        F.when(F.col("ar.ar_tp_id") == 'FAC', F.col("ar.ac_no"))
         .when(F.col("ar.ar_tp_id").isin("LON", "BOR") & F.col("ar_x_ar.sbj_ar_id").isNotNull(), F.col("ar_fac.ac_no"))
         .when(F.col("ar.ar_tp_id").isin("LON", "BOR") & F.col("ar_x_ar.sbj_ar_id").isNull(), F.col("ar.ac_no"))
         .alias("fcy_num"),
        F.col("ar.ac_no"),
        F.col("ar.dnmn_ccy_id"),
        F.col("ar.am_frs_mprd_tag").alias("frs_mprd_tag"),
        F.col("ar.am_rr_cnt").alias("rr_cnt"),
        F.col("ar.am_rr_rsn").alias("rr_rsn"),
        F.col("ar.ar_pps_tp_id").alias("pps_tp_id"),
        F.col("ar.am_own_cnt").alias("own_cnt"),
        F.col("ar.am_rltnp_mgr_cd").alias("opn_ofcr_cd"),
        F.col("ar.rprg_ou_ip_id").alias("pd_orig_br"),
        if_lon_bor_dep_inv(F.col("ar.ar_pps_tp_id"), F.col("am_od_ar.od_pps")).alias("pps_cd"),
        F.col("ar.am_cnrl_bnk_idy_cl_id").alias("sect_cd"),
        F.col("ar.est_end_dt").alias("amrz_mat_dt"),
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR"), "A")
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV") & (F.col("ar.ac_ar_tp_id") == 'O'), "A")
         .otherwise("L").alias("ast_lby_ind"),
        F.col("ar.rnew_f").alias("auto_rlov_f"),
        F.col("ar.am_ac_opn_dt").alias("ac_opn_dt"),
        F.col("ar.est_end_dt").alias("fcy_exp_dt"),
        F.col("ar.am_bnkg_cncpt_cd").alias("bnkg_cncpt_cd"),
        F.col("ar.am_lgl_ent_id").alias("lnd_ent"),
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR"), F.col("ar.est_end_dt"))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV") & (F.col("ar.ac_ar_tp_id") == 'O'), F.col("ar_mat_dt.od_mat_dt"))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV"), F.col("ar.est_end_dt"))
         .alias("mat_dt"),
        F.col("rnew.num_of_rnew"),
        F.col("ar.am_lgl_ent_id").alias("co_cd"),
        F.col("ar.am_lgl_ent_id").alias("fi_cd"),
        F.col("ar.rprg_ou_ip_id"),
        F.col("ar.am_fix_pft_shr_rto").alias("fix_pft_shr_rto"),
        F.col("am_od_ar.od_st").alias("od_st_cd"),
        F.col("ar.am_pd_cd").alias("ac_tp_id"),
        F.col("ar.pd_id").alias("pd_cd"),
        if_lon_bor_dep_inv(F.col("ar.am_ac_cgy_cd"), split_part(F.col("ar.am_pd_cd"), '#', 3)).alias("ac_cgy_cd"),
        F.col("ar.ar_lcs_tp_id"),
        F.col("ar.am_ac_cls_dt").alias("cl_dt"),
        F.col("ar.am_wtch_list_tag_dt").alias("wtch_list_tag_dt"),
        F.when(F.col("ar.am_wtch_list_tag").isNotNull(), "Y").otherwise("N").alias("wtch_list_f"),
        F.col("ar.am_src_rcrd_udt_dt").alias("src_rcrd_udt_dt"),
        F.col("ar.am_rltnp_mgr_cd").alias("od_tp_id"),
        F.when((F.col("ar.am_nxt_rprc_dt").isNull()) | (F.col("ar.am_nxt_rprc_dt") == 0), F.col("ar.est_end_dt"))
         .otherwise(F.col("ar.am_nxt_rprc_dt")).alias("nxt_pri_dt"),
        F.col("ar.pd_id").alias("txn_pd_cd"),
        F.col("ar.est_end_dt").alias("orig_mat_dt"),
        # py_dwon_dt
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR") & F.col("ar.ar_lcs_tp_id").isin("22", "40") & (F.col("ar.am_last_fnc_dt") < F.col("ar.est_end_dt")), F.col("ar.am_last_fnc_dt"))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV") & (F.col("ar.ar_lcs_tp_id") == "07") & (F.col("ar.am_last_fnc_dt") < F.col("ar.est_end_dt")), F.col("ar.am_last_fnc_dt"))
         .alias("py_dwon_dt"),
        F.col("ar.am_reprc_tp_id").alias("reprc_tp_id"),
        F.col("ar.ar_lcs_dt"),
        F.col("ar.int_clcn_eff_dt"),
        F.col("ar.am_last_rprc_dt").alias("last_rprc_dt"),
        F.col("ar.am_nxt_rprc_dt").alias("nxt_reprc_dt"),
        split_part(F.col("ar.am_pd_cd"), "#", 3).alias("sub_pd_cd"),
        # loan_tnr
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR"), F.col("ar.am_src_tnr_info"))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV") & (F.col("ar.ac_ar_tp_id") == 'O'), '12')
         .alias("loan_tnr"),
        F.col("ar.am_grc_prd_end_dt").alias("grc_prd_end_dt"),
        if_lon_bor_dep_inv(F.col("ar.am_npa_cl_dt"), F.col("dep_ar.last_odrwn_dt")).alias("mprd_dt"),
        if_lon_bor_dep_inv(F.col("fnc.am_wrtof_amt"), F.col("am_od_ar.am_wrtof_amt")).alias("wrtof_amt"),
        F.col("od_aprv.od_aprv_dt"),
        if_lon_bor_dep_inv(F.col("fnc.am_int_repy_prd_id"), F.col("dep_ar.am_int_py_tp_id")).alias("int_pymt_tp_id"),
        F.col("dep_ar.am_last_cr_dt").alias("last_in_cr_dt"),
        (F.coalesce(F.col("dep_ar.am_ac_cmmt_fee_amt"), F.lit(0)) + F.coalesce(F.col("dep_ar.am_othr_cost_amt"), F.lit(0))).alias("rev_amt"),
        F.col("dep_ar.am_hold_amt").alias("hold_val"),
        F.col("dep_ar.am_term_fm_dt").alias("term_fm_dt"),
        F.col("dep_ar.am_term_fm_dt").alias("deal_dt"),
        F.col("dep_ar.eff_avl_bal").alias("cyc_avl_bal_val"),
        if_lon_bor_dep_inv(F.col("fnc.am_crn_loan_bal"), F.col("dep_ar.am_pnp_bal_orig_amt")).alias("cyc_otsnd_bal_val"),
        if_lon_bor_dep_inv(F.col("fnc.am_udrn_amt"), F.col("dep_ar.eff_avl_bal")).alias("avl_bal"),
        # eom_misc_chrg_val
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR") & F.col("fnc.am_wrtof_st_cd").isin("P", "F1", "F2"), F.col("fnc.am_wdv_othr_chrg_bal"))
         .when(F.col("ar.ar_tp_id").isin("LON", "BOR"), F.col("fnc.am_upd_chrg_bal"))
         .alias("eom_misc_chrg_val"),
        F.col("dep_ar.am_pnp_bal_orig_amt").alias("min_due_amt"),
        F.col("dep_ar.am_term_ctf_num").alias("fix_dep_recpt_num"),
        (F.coalesce(F.col("dep_ar.am_pnp_bal_orig_amt"), F.lit(0)) - F.coalesce(F.col("dep_ar.am_last_pymt_amt"), F.lit(0))).alias("rvl_ac"),
        F.col("dep_ar.am_ernd_int_amt").alias("tot_ernd_int_amt"),
        F.col("dep_ar.last_odrwn_dt").alias("ov_lmt_strt_dt"),
        F.col("dep_ar.last_odrwn_dt").alias("ov_lmt_dt"),
        F.col("dep_ar.am_last_cr_dt").alias("last_cr_dt"),
        F.col("dep_ar.am_last_db_dt").alias("last_db_dt"),
        F.col("ar.am_last_fnc_dt").alias("last_txn_dt"),
        # cyc_misc_chrg_val (logic same as eom_misc_chrg_val)
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR") & F.col("fnc.am_wrtof_st_cd").isin("P", "F1", "F2"), F.col("fnc.am_wdv_othr_chrg_bal"))
         .when(F.col("ar.ar_tp_id").isin("LON", "BOR"), F.col("fnc.am_upd_chrg_bal"))
         .alias("cyc_misc_chrg_val"),
        if_lon_bor_dep_inv(F.col("fnc.am_crn_loan_bal"), F.col("dep_ar.am_pnp_bal_orig_amt")).alias("eom_otsnd_bal"),
        F.col("fnc.inl_tot_amt_repy").alias("repymt_amt"),
        F.col("fnc.am_dly_incr_amt").alias("late_pymt_amt"),
        F.col("ar.am_nbr_of_tnr").alias("eff_tm_mat"),
        F.col("fnc.am_upd_chrg_bal").alias("othr_chrg"),
        F.when(F.col("fnc.orig_esr_amt") == 0, 0).otherwise(F.col("fnc.am_crn_loan_bal") / F.col("fnc.orig_esr_amt")).alias("paid_to_dt"),
        F.col("fnc.am_mo_pnp_repaid_amt").alias("mo_pnp_repaid_amt"),
        F.col("fnc.am_mo_tot_paid_amt").alias("mo_tot_paid_amt"),
        F.when(F.col("dep_ar.am_pnp_bal_orig_amt") < 0, F.abs(F.col("dep_ar.am_pnp_bal_orig_amt"))).otherwise(0).alias("utlz_amt"),
        if_lon_bor_dep_inv(F.col("fnc.am_wrtof_dt"), F.col("am_od_ar.am_wrtof_dt")).alias("am_wrtof_dt"),
        # wrof_int_amt
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR") & F.col("fnc.am_wrtof_st_cd").isin("P", "F1", "F2"), 
               F.coalesce(F.col("fnc.am_cpz_upd_int_amt"), F.lit(0)) - F.coalesce(F.col("fnc.am_wdv_int_bal"), F.lit(0)))
         .alias("wrof_int_amt"),
        # wrof_pnp_amt
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR") & F.col("fnc.am_wrtof_st_cd").isin("P", "F1", "F2"), 
               F.coalesce(F.col("fnc.am_pnp_upd_bal"), F.lit(0)) - F.coalesce(F.col("fnc.am_wdv_pnp_bal"), F.lit(0)))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV") & F.col("arxcl_wroffind.cl_val_id").isin("P", "F1", "F2") & 
               (F.col("dep_ar.am_pnp_bal_orig_amt") < 0) & (F.substring(F.col("dep_ar.am_gl_cl_cd"), 17, 6) != '232610'),
               F.abs(F.coalesce(F.col("dep_ar.am_pnp_bal_orig_amt"), F.lit(0))) - F.coalesce(F.col("am_od_ar.am_wdv_pnp_bal"), F.lit(0)))
         .alias("wrof_pnp_amt"),
        # cyc_tot_ars_amt
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR") & (F.col("fnc.am_ars_amt") < 0), 0)
         .when(F.col("ar.ar_tp_id").isin("LON", "BOR"), F.col("fnc.am_ars_amt"))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV") & (F.col("dep_ar.am_pnp_bal_orig_amt") < 0), F.abs(F.col("dep_ar.am_pnp_bal_orig_amt")))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV"), 0)
         .alias("cyc_tot_ars_amt"),
        if_lon_bor_dep_inv(F.col("fnc.am_ars_amt"), F.col("am_od_ar.am_ars_amt")).alias("ars_amt"),
        # eom_tot_dlq_amt (Division logic)
        F.when(F.col("fnc.am_crn_loan_bal") == 0, 0)
         .otherwise((F.coalesce(F.col("fnc.am_clc_loan_bal"), F.lit(0)) - F.coalesce(F.col("fnc.am_crn_loan_bal"), F.lit(0))) / F.col("fnc.am_crn_loan_bal"))
         .alias("eom_tot_dlq_amt"),
        F.col("fnc.am_odue_int_bal").alias("cyc_ars_int_val"),
        F.col("fnc.am_odue_bal").alias("cyc_ars_pnp_val"),
        F.col("fnc.am_cyc_past_due_val").alias("cyc_past_due_val"),
        F.col("fnc.am_last_ars_dt").alias("last_dflt_dt"),
        if_lon_bor_dep_inv(F.col("fnc.am_wrtof_dt"), F.col("am_od_ar.am_wrtof_dt")).alias("last_wrtof_dt"),
        F.col("am_ars.out_of_ars_dt_cf").alias("last_out_of_ars_dt"),
        F.col("fnc.am_sale_dt").alias("sale_dt"),
        F.col("fnc.am_dflt_amt").alias("dflt_amt"),
        F.col("fnc.am_cyc_past_due_val").alias("eom_inst_ars_val"),
        # net_cr_loss
        if_lon_bor_dep_inv(
            F.col("fnc.am_wrtof_amt") - F.col("fnc.am_bad_dbt_rec_amt"), 
            F.col("am_od_ar.am_wrtof_amt") - F.col("am_od_ar.am_bad_debt_rec_amt")
        ).alias("net_cr_loss"),
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR"), F.col("fnc.am_bnm_tp_id").cast(StringType()))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV"), F.concat(F.coalesce(F.col("udf.udf_003"), F.lit("")), F.coalesce(F.col("udf.udf_004"), F.lit(""))))
         .alias("const_cd"),
        # age_frst_dsbr calculation
        F.when((F.col("fnc.am_frst_adv_dt") != 0) & F.col("fnc.am_frst_adv_dt").isNotNull(),
               ((F.floor(F.lit(iBsnDt) / 10000) - F.floor(F.col("fnc.am_frst_adv_dt") / 10000)) * 12) +
               ((F.floor(F.lit(iBsnDt) / 100) % 100) - (F.floor(F.col("fnc.am_frst_adv_dt") / 100) % 100))
              ).alias("age_frst_dsbr"),
        if_lon_bor_dep_inv(F.col("fnc.am_loan_orig_dt"), F.col("ar.am_ac_opn_dt")).alias("apl_dt"),
        F.when(F.col("fnc.am_lgl_actn_cd") == 12, F.col("fnc.am_lgl_actn_dt")).alias("bnkr_dt"),
        if_lon_bor_dep_inv(F.col("fnc.am_rshd_dt"), F.col("am_od_ar.rshd_dt")).alias("rshd_dt"),
        F.col("fnc.fnc_svc_rstc_st_dt").alias("ac_rstc_dt"),
        F.col("fnc.am_adv_val").alias("dsbr_amt"),
        F.col("fnc.am_pnp_rdcn_txn").alias("down_pymt"),
        F.col("ar.am_wtch_list_tag").alias("fcy_wtch_list_tag"),
        F.when(F.col("fnc.fsvc_repymt_tp_id").isin("P", "N"), F.col("fnc.am_frst_repy_dt")).alias("frst_pnp_repymt_dt"),
        if_lon_bor_dep_inv(F.col("fnc.am_frst_adv_dt"), F.col("dep_ar.am_prvn_lmt_strt_dt")).alias("frst_utlz_dt"),
        F.when(F.col("ar.ar_lcs_tp_id").isin("08", "20", "22", "23", "40"), F.col("fnc.am_adv_dt")).alias("full_dsbr_dt"),
        F.when(F.col("ar.ar_lcs_tp_id").isin("08", "20", "22", "23", "40"), "Y").otherwise("N").alias("fully_dsbr_f"),
        F.col("ar.am_frs_mprd_tag").alias("ifrs_mprd_f"),
        F.col("fnc.am_lgl_actn_dt").alias("lst_lgl_actn_dt"),
        F.col("fnc.am_lgl_actn_cd").alias("lst_lgl_actn_st"),
        F.col("fnc.am_lgl_actn_dt").alias("lgl_mrkr_dt"),
        F.col("fnc.am_lgl_actn_cd").alias("lgl_mrkr_st"),
        F.when((F.col("fnc.am_lgl_actn_cd").isNotNull()) & (F.col("fnc.am_lgl_actn_cd") != '21'), F.col("fnc.am_crn_loan_bal")).alias("lgl_mrkr_val"),
        F.col("ar.am_ac_age").alias("loan_age"),
        if_lon_bor_dep_inv(F.col("fnc.orig_esr_dt"), F.col("ar.am_ac_opn_dt")).cast(IntegerType()).alias("loan_opn_dt"),
        F.col("fnc.am_sold_amt").alias("loan_sale_amt"),
        F.col("fnc.am_sale_dt").alias("loan_sale_dt"),
        if_lon_bor_dep_inv(F.col("fnc.am_mo_on_book"), F.col("dep_ar.am_ac_age")).cast(IntegerType()).alias("mo_on_book"),
        F.col("fnc.am_ac_sold_to_nm").alias("cntpr_sold_to_nm"),
        F.col("fnc.am_nxt_int_repymt_dt").alias("nxt_int_pymt_due_dt"),
        F.col("fnc.am_nxt_repy_dt").alias("nxt_pymt_dt"),
        F.col("fnc.am_nxt_pnp_repymt_dt").cast(IntegerType()).alias("nxt_pnp_pymt_due_dt"),
        F.col("fnc.am_susp_amt").alias("on_sell_susp_pymt_amt"),
        F.col("ar.am_tnr_unit_msr").alias("orig_loan_tnr"),
        F.when(F.coalesce(F.col("fnc.am_clc_loan_bal"), F.lit(0)) > F.coalesce(F.col("fnc.am_crn_loan_bal"), F.lit(0)),
               F.coalesce(F.col("fnc.am_clc_loan_bal"), F.lit(0)) - F.coalesce(F.col("fnc.am_crn_loan_bal"), F.lit(0))
              ).alias("redrw_lmt"),
        F.col("fnc.pymt_rman_nbr").alias("rman_term"),
        F.when(F.col("ar.pd_id").like("_3__"), F.col("dep_ar.am_term_fm_dt")).otherwise(F.col("fnc.am_rnew_dt")).alias("rnew_dt"),
        F.col("ar.am_pymt_frq_prdc_id").alias("repymt_frq_cd"),
        F.when(F.col("arxcl_restru.cl_val_id") == 'Y', F.col("fnc.am_crn_loan_bal")).alias("rstc_val"),
        F.when(F.col("fnc.fnc_svc_rstc_st_id") == 'Y', F.col("ar.am_nbr_of_tnr")).alias("rstc_prd"),
        F.col("fnc.am_def_exp_dt").alias("rtc_exp_dt"),
        if_lon_bor_dep_inv(F.col("fnc.am_udrn_amt"), F.col("am_od_ar.udrn_amt")).alias("udrn_amt"),
        F.when(F.col("ar.ar_tp_id").isin("DEP", "INV"), F.col("mtd_int_acr_amt.int_acr_amt")).alias("int_acr_amt"),
        F.col("fnc.am_mo_int_paid_amt").alias("int_paid_amt"),
        F.when((F.col("fnc.am_int_only_exp_dt") == 0) | (F.col("fnc.am_int_only_exp_dt") >= iBsnDt), "Y").otherwise("N").alias("int_only_f"),
        F.col("fnc.am_orig_tot_int_amt").alias("int_rbt_amt"),
        F.col("fnc.am_unern_int_amt").alias("orig_int_rbt_amt"),
        F.col("fnc.am_cpz_upd_int_amt").alias("cpz_upd_int_amt"),
        F.col("fnc.am_unern_int_amt").alias("unern_int_amt"),
        F.col("am_ars.mo_in_ars_cf").alias("no_of_instlm_in_ars"),
        if_lon_bor_dep_inv(F.col("fnc.am_bnm_tp_id").cast(StringType()), F.concat(F.coalesce(F.col("udf.udf_003"), F.lit("")), F.coalesce(F.col("udf.udf_004"), F.lit("")))).alias("bnm_tp_id"),
        F.col("fnc.am_bnm_sbtp").alias("bnm_sbtp"),
        F.col("fnc.am_repymt_due_day_mo").alias("due_dt"),
        F.col("fnc.am_fcy_cd").alias("fcy_cd"),
        F.col("fnc.loan_tp_id").alias("loan_tp_id"),
        F.col("fnc.am_setl_dt").alias("setl_dt"),
        if_lon_bor_dep_inv(F.col("fnc.am_cp1_prvn_amt"), F.col("dep_ar.am_cp1_prvn_amt")).alias("cp1_prvn_amt"),
        if_lon_bor_dep_inv(F.col("fnc.am_cp3_prvn_amt"), F.col("dep_ar.am_cp3_prvn_amt")).alias("cp3_prvn_amt"),
        if_lon_bor_dep_inv(F.col("fnc.am_cp7_prvn_amt"), F.col("dep_ar.am_cp7_prvn_amt")).alias("cp7_amt"),
        if_lon_bor_dep_inv(F.col("fnc.am_ip_prvn_amt"), F.col("dep_ar.am_spf_prvn_amt")).alias("spf_prvn_amt"),
        # wrtof_cp_top_up_amt
        if_lon_bor_dep_inv(
            F.coalesce(F.col("fnc.am_wdv_pnp_bal"), F.lit(0)) + F.coalesce(F.col("fnc.am_wdv_int_bal"), F.lit(0)) + 
            F.coalesce(F.col("fnc.am_wdv_upd_chrg_bal"), F.lit(0)) + F.coalesce(F.col("fnc.am_wdv_othr_chrg_bal"), F.lit(0)),
            F.coalesce(F.col("am_od_ar.am_wdv_pnp_bal"), F.lit(0)) + F.coalesce(F.col("am_od_ar.am_wdv_int_amt"), F.lit(0))
        ).alias("wrtof_cp_top_up_amt"),
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR") & (F.col("ar.ar_lcs_tp_id") == '40'), F.col("fnc.am_act_disch_dt"))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV") & (F.col("ar.ar_lcs_tp_id") == '07'), F.col("ar.am_last_fnc_dt"))
         .alias("ac_cls_dt"),
        F.col("fnc.am_prpaid_adv_txn_amt").alias("adv_pymt_amt"),
        F.col("ar.est_end_dt").alias("fnl_repymt_dt"),
        F.col("fnc.am_part_pymt_bal").alias("part_pymt_bal"),
        F.col("fnc.am_due_amt").alias("pre_pymt_amt"),
        # main_ldgr
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR"), F.col("fnc.am_crn_loan_bal"))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV") & F.col("arxcl_wroffind.cl_val_id").isin("P", "F1", "F2"), 
               F.coalesce(F.col("am_od_ar.am_wdv_pnp_bal"), F.lit(0)) + F.coalesce(F.col("am_od_ar.am_wdv_int_amt"), F.lit(0)))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV"), F.col("dep_ar.am_pnp_bal_orig_amt"))
         .alias("main_ldgr"),
        # acr_int_late_chrg_amt
        if_lon_bor_dep_inv(F.col("fnc.am_ars_int_acr_amt"), 
                           F.coalesce(F.col("dep_ar.am_int_avl_amt"), F.lit(0)) + F.coalesce(F.col("am_od_ar.od_int_avl_amt"), F.lit(0))
                          ).alias("acr_int_late_chrg_amt"),
        # tot_sb_ldgr_amt
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR") & (F.col("fnc.am_wrtof_st_cd").isNull() | (F.col("fnc.am_wrtof_st_cd") == "")), 0)
         .when(F.col("ar.ar_tp_id").isin("LON", "BOR"), F.col("fnc.am_crn_loan_bal"))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV") & F.col("arxcl_wroffind.cl_val_id").isNull(), 0)
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV"), F.col("dep_ar.am_pnp_bal_orig_amt"))
         .alias("tot_sb_ldgr_amt"),
        # sb_ldgr_pnp_amt
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR") & F.col("fnc.am_wrtof_st_cd").isin("P", "F1", "F2"), 
               F.coalesce(F.col("fnc.am_cpz_upd_int_amt"), F.lit(0)) + F.coalesce(F.col("fnc.am_pnp_upd_bal"), F.lit(0)))
         .when(F.col("ar.ar_tp_id").isin("LON", "BOR"), 0)
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV") & F.col("arxcl_wroffind.cl_val_id").isNull(), 0)
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV") & F.col("arxcl_wroffind.cl_val_id").isNotNull(), F.col("am_od_ar.am_pnp_otsnd_bal_amt"))
         .alias("sb_ldgr_pnp_amt"),
        # sub_ldgr_acr_int_amt
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR"), F.col("fnc.am_upd_ars_int_amt"))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV") & F.col("arxcl_wroffind.cl_val_id").isNull(), 0)
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV") & F.col("arxcl_wroffind.cl_val_id").isNotNull(), F.col("dep_ar.am_int_otsnd_amt"))
         .alias("sub_ldgr_acr_int_amt"),
        # sb_ldgr_misc_fee_amt
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR") & (F.col("fnc.am_wrtof_st_cd").isNull() | (F.col("fnc.am_wrtof_st_cd") == "")), 0)
         .when(F.col("ar.ar_tp_id").isin("LON", "BOR"), F.col("fnc.am_sub_ldgr_misc_fee_amt"))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV"), 0)
         .alias("sb_ldgr_misc_fee_amt"),
        if_lon_bor_dep_inv(F.col("fnc.orig_esr_dt"), F.col("dep_ar.am_aprv_dt")).alias("aprv_dt"),
        if_lon_bor_dep_inv(F.col("fnc.am_tag_dt"), F.col("am_od_ar.od_rvw_dt").cast(IntegerType())).alias("rvw_dt"),
        if_lon_bor_dep_inv(F.col("fnc.am_pndg_loan_term"), F.col("dep_ar.am_term_day")).alias("pndg_loan_term"),
        F.col("fnc.am_stff_cd").alias("stff_scm"),
        F.col("fnc.am_upd_ars_int_amt").alias("upd_ars_int"),
        F.col("fnc.am_clc_pnp_upd_bal").alias("clc_pnp_upd_bal"),
        F.col("fnc.am_clc_cpz_upd_int_amt").alias("clc_cpz_upd_int"),
        F.col("fnc.am_clc_upd_chrg_bal").alias("clc_upd_chrg_bal"),
        F.col("ar.am_pymt_frq_prdc_id").alias("pymt_frq_prdc_id"),
        F.col("fnc.am_cmpn_cd").alias("cmpn_cd"),
        F.col("fnc.am_scr_ind_dt").alias("scr_ind_dt"),
        F.col("fnc.am_odue_int_bal").alias("ars_int_val"),
        F.col("fnc.am_odue_bal").alias("ars_pnp_val"),
        F.col("fnc.am_cyc_past_due_val").alias("past_due_val"),
        if_lon_bor_dep_inv(F.col("fnc.am_rec_cd"), F.col("dep_ar.am_colblty_st")).alias("cl_cd"),
        if_lon_bor_dep_inv(F.col("repymt_frq_loan.src_ref_dsc"), F.col("repymt_frq_dep.src_ref_dsc")).alias("repymt_frq"),
        F.col("fnc.orig_esr_amt").alias("orig_esr_amt"),
        F.col("fnc.inl_tot_amt_repy").alias("inl_tot_amt_repy"),
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR") & F.col("fnc.am_wrtof_st_cd").isin("P", "F1", "F2"), F.col("fnc.am_wdv_upd_chrg_bal"))
         .when(F.col("ar.ar_tp_id").isin("LON", "BOR"), F.col("fnc.am_upd_ars_int_amt"))
         .alias("late_pymt_pny"),
        if_lon_bor_dep_inv(F.col("fnc.am_bad_dbt_rec_amt"), F.col("am_od_ar.am_bad_debt_rec_amt")).alias("bad_debt_rec_amt"),
        F.col("ar.am_pymt_frq_prdc_id").alias("int_pymt_frq"),
        F.col("fnc.am_int_rt_chg_eff_dt").alias("int_rt_chg_eff_dt"),
        F.col("fnc.am_rec_cd").alias("rec_cd"),
        F.col("fnc.am_fnl_repymt").alias("fnl_repymt_amt"),
        F.col("fnc.am_pnp_repy_prd_id").cast(IntegerType()).alias("pnp_repy_prd_id"),
        F.col("fnc.am_pnp_repy_msr").alias("pnp_repy_msr"),
        F.col("prn_erly_aprv_dt.erly_aprv_dt"),
        # AM_ARS fields
        if_lon_bor_dep_inv(F.col("am_ars.day_in_ars_cf"), F.col("am_ars.day_in_ars_cf")).alias("ars_due_dys"),
        if_lon_bor_dep_inv(F.col("am_ars.day_in_ars_cf"), F.col("am_ars.day_in_ars_cf")).alias("ars_int_due_dys"),
        if_lon_bor_dep_inv(F.col("am_ars.day_in_ars_cf"), F.col("am_ars.day_in_ars_cf")).alias("ars_pymt_due_dys"),
        # dlq_st logic
        F.when(F.col("am_ars.mo_in_ars_cf") == '000', '01')
         .when(F.col("am_ars.mo_in_ars_cf") == '001', '02')
         .when(F.col("am_ars.mo_in_ars_cf").isin('002', '003'), '03')
         .when(F.col("am_ars.mo_in_ars_cf").isin('004', '005'), '04')
         .when(F.col("am_ars.mo_in_ars_cf").isin('006', '007', '008'), '05')
         .when(F.col("am_ars.mo_in_ars_cf").isin('009', '010', '011'), '06')
         .otherwise('07').alias("dlq_st"),
        F.col("am_ars.mo_in_ars_cf").alias("mo_in_ars"),
        F.col("am_ars.acr_tp_id").alias("acr_bss"),
        # UDF
        F.concat(F.coalesce(F.col("udf.udf_041"), F.lit("")), F.coalesce(F.col("udf.udf_042"), F.lit(""))).alias("udf_041_042"),
        F.col("amarxcoa.lob").alias("gl_lob_cd"),
        F.col("amarxcoa.cost_cntr_cd").alias("gl_cost_cntr_cd"),
        F.col("amarxcoa.pd_cd").alias("gl_pd_cd"),
        F.when(((F.coalesce(F.col("ar.ac_ar_tp_id"), F.lit('!')) == 'O') & F.col("ar.pd_id").like("_2__")) | (F.col("ar.pd_id").like("_4__") | F.col("ar.pd_id").like("_5__")), 'H01')
         .when(((F.coalesce(F.col("ar.ac_ar_tp_id"), F.lit('!')) != 'O') & F.col("ar.pd_id").like("_2__")) | (F.col("ar.pd_id").like("_1__") | F.col("ar.pd_id").like("_3__")), 'H07')
         .alias("gl_bal_cd"),
        F.col("arxcl_swift.cl_val_id").alias("swp_f"),
        F.col("pstadr.pstcd_area_id").alias("pstcd_area_id"),
        F.concat(F.coalesce(F.col("pstadr.am_adr_line_1"), F.lit("")), F.coalesce(F.col("pstadr.am_adr_line_2"), F.lit("")), F.coalesce(F.col("pstadr.am_adr_line_3"), F.lit("")), F.coalesce(F.col("pstadr.am_adr_line_4"), F.lit(""))).alias("ar_adr"),
        F.col("pstadr.pstcd_area_id").alias("ar_pstcd"),
        F.col("pstadr.ste_id").alias("ar_ste"),
        F.col("ar.am_rsk_st").alias("dstrs_f"),
        F.col("fnc.am_upd_chrg_bal").alias("misc_chrg_val"),
        F.col("ar.last_stmt_dt").alias("bill_dt"),
        # Null columns
        F.lit(iBsnDt).alias("rpt_dt"),
        F.lit(None).cast(StringType()).alias("alt_ac_no"),
        F.lit(None).cast(DecimalType(19, 2)).alias("flot_bal"),
        F.lit(None).cast(DecimalType(19, 2)).alias("hold_bal"),
        F.lit(None).cast(DecimalType(19, 2)).alias("mn_bal"),
        F.lit(None).cast(DecimalType(19, 2)).alias("cash_adv_bal"),
        F.lit(None).cast(DecimalType(19, 2)).alias("coll_cost_amt"),
        F.lit(None).cast(DecimalType(19, 2)).alias("iis_opn_bal"),
        F.lit(None).cast(DecimalType(19, 2)).alias("iis_susp_amt"),
        F.lit(None).cast(DecimalType(19, 2)).alias("iis_wrt_back_amt"),
        F.lit(None).cast(DecimalType(19, 2)).alias("iis_wrtof_amt"),
        F.lit(None).cast(DecimalType(19, 2)).alias("spf_prvn_chrg"),
        F.lit(None).cast(DecimalType(19, 2)).alias("spf_prvn_opn_bal"),
        F.lit(None).cast(DecimalType(19, 2)).alias("spf_prvn_wrt_back_amt"),
        F.lit(None).cast(DecimalType(19, 2)).alias("spf_prvn_wrtof_amt"),
        F.lit(None).cast(DecimalType(19, 2)).alias("ubill_bal"),
        F.lit(None).cast(DecimalType(19, 2)).alias("tot_dlq_amt"),
        F.lit(None).cast(DecimalType(19, 2)).alias("cyc_ars_fee_val"),
        F.lit(None).cast(DecimalType(19, 2)).alias("cyc_ars_ins_cmsn_val"),
        F.lit(None).cast(IntegerType()).alias("last_coll_dt"),
        F.lit(None).cast(IntegerType()).alias("last_ars_dt"),
        F.lit(None).cast(StringType()).alias("dflt_rsn"),
        F.lit(None).cast(IntegerType()).alias("ext_coll_agnc_dt"),
        F.lit(None).cast(StringType()).alias("ext_coll_agnc_f"),
        F.lit(None).cast(DecimalType(19, 2)).alias("ext_coll_agnc_val"),
        F.lit(None).cast(DecimalType(19, 2)).alias("rec_cost_amt"),
        F.lit(None).cast(DecimalType(19, 2)).alias("rec_int_amt"),
        F.lit(None).cast(DecimalType(19, 2)).alias("rec_pnp_amt"),
        F.lit(None).cast(DecimalType(19, 2)).alias("tot_rec_amt"),
        F.lit(None).cast(IntegerType()).alias("rec_dt"),
        F.lit(None).cast(IntegerType()).alias("rmndr"),
        F.lit(None).cast(DecimalType(19, 2)).alias("fraud_val"),
        F.lit(None).cast(StringType()).alias("loan_alt_ind"),
        F.lit(None).cast(IntegerType()).alias("alt_cnt"),
        F.lit(None).cast(IntegerType()).alias("last_cr_lmt_chg_dt"),
        F.lit(None).cast(DecimalType(19, 2)).alias("prev_cr_lmt"),
        F.lit(None).cast(StringType()).alias("old_sc_good_bad_f"),
        F.lit(None).cast(StringType()).alias("old_sc_good_bad_ind"),
        F.lit(None).cast(IntegerType()).alias("no_of_ret_pymt"),
        F.lit(None).cast(DecimalType(19, 2)).alias("rsk_adj_yld"),
        F.lit(None).cast(StringType()).alias("card_tp_id"),
        F.lit(None).cast(DecimalType(19, 2)).alias("high_redrw_amt"),
        F.lit(None).cast(DecimalType(19, 2)).alias("last_prch_amt"),
        F.lit(None).cast(DecimalType(19, 2)).alias("rvl_rate_pct_anr"),
        F.lit(None).cast(DecimalType(19, 2)).alias("tot_sale_amt"),
        F.lit(None).cast(DecimalType(19, 2)).alias("sz_of_redrw"),
        F.lit(None).cast(DecimalType(19, 2)).alias("usg_of_redrw"),
        F.lit(None).cast(StringType()).alias("ar_cty_cd"),
        F.lit(None).cast(DecimalType(19, 2)).alias("epp_ubill_amt"),
        F.lit(None).cast(DecimalType(19, 2)).alias("int_susp_amt"),
        F.lit(None).cast(IntegerType()).alias("num_gnr"),
        F.lit(iBsnDt).alias("btch_dt"),
        F.lit(None).cast(DecimalType(19, 2)).alias("rec_amt_fm_clt_sold"),
        F.lit(None).cast(DecimalType(19, 2)).alias("rec_amt_fm_cst"),
        F.lit(None).cast(DecimalType(19, 2)).alias("rec_amt_fm_gnr"),
        F.lit(None).cast(DecimalType(19, 2)).alias("rec_amt_fm_othr"),
        F.col("ar.ac_ar_tp_id"),
        F.col("ar.ar_tp_id"),
        F.col("dep_ar.am_fzn_dt"),
        F.col("amarxcoa.gl_co_cd"),
        F.col("ar.prn_ar_id"),
        split_part(F.col("ar.am_pd_cd"), "#", 2).alias("pd_src_tp_cd"),
        F.col("am_od_ar.od_lmt_tp_id"),
        # np_ac_ind
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR") & F.col("fnc.am_wrtof_st_cd").isNull(), F.col("fnc.am_rec_cd"))
         .when(F.col("ar.ar_tp_id").isin("LON", "BOR"), F.substring(F.col("fnc.am_wrtof_st_cd"), 1, 1))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV") & (F.col("arxcl_wroffind.cl_val_id").isNull() | (F.col("arxcl_wroffind.cl_val_id") == '')), F.col("dep_ar.am_colblty_st"))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV"), F.substring(F.col("arxcl_wroffind.cl_val_id"), 1, 1))
         .alias("np_ac_ind"),
        # crn_bal
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR"), F.col("fnc.am_crn_loan_bal"))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV") & F.col("arxcl_wroffind.cl_val_id").isin("P", "F1", "F2"), 
               F.coalesce(F.col("am_od_ar.am_wdv_pnp_bal"), F.lit(0)) + F.coalesce(F.col("am_od_ar.am_wdv_int_amt"), F.lit(0)))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV"), F.col("dep_ar.am_pnp_bal_orig_amt"))
         .alias("crn_bal"),
        if_lon_bor_dep_inv(F.col("fnc.am_dly_int_incr_amt"), F.col("dep_ar.am_int_incrm_amt")).alias("dly_int_incr"),
        # int_and_late_chrg_amt
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR"), F.col("fnc.am_upd_ars_int_amt"))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV") & (F.col("arxcl_wroffind.cl_val_id").isNull() | (F.col("arxcl_wroffind.cl_val_id") == '')), F.col("dep_ar.am_int_otsnd_amt"))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV"), F.col("am_od_ar.am_wdv_int_amt"))
         .alias("int_and_late_chrg_amt"),
        if_lon_bor_dep_inv(F.col("fnc.am_upd_chrg_bal"), F.lit(0)).alias("misc_amt"),
        # pnp_amt
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR"), F.coalesce(F.col("fnc.am_pnp_upd_bal"), F.lit(0)) + F.coalesce(F.col("fnc.am_cpz_upd_int_amt"), F.lit(0)))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV") & F.col("arxcl_wroffind.cl_val_id").isin("P", "F1", "F2"), F.col("am_od_ar.am_wdv_pnp_bal"))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV"), F.col("am_od_ar.am_pnp_otsnd_bal_amt"))
         .alias("pnp_amt"),
        # upd_chrg_bal
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR") & F.col("fnc.am_wrtof_st_cd").isin("P", "F1", "F2"), F.col("fnc.am_wdv_othr_chrg_bal"))
         .when(F.col("ar.ar_tp_id").isin("LON", "BOR"), F.col("fnc.am_upd_chrg_bal"))
         .alias("upd_chrg_bal"),
        F.col("fnc.am_frst_adv_dt").alias("loan_ac_opn_dt"),
        if_lon_bor_dep_inv(F.col("ar.am_cnrl_bnk_idy_cl_id"), F.col("dep_ar.am_bsn_tp_id")).alias("bsn_sec_cd"),
        F.col("amarxcoa.ind").alias("bnkg_cncpt_cd_gl"),
        F.col("ar.am_frs_st_dt").alias("ifrs_mprd_dt"),
        if_lon_bor(F.col("udf.scm_cd")).alias("scm_cd"),
        F.col("fnc.am_make_cd").alias("clt_make"),
        if_lon_bor_dep_inv(F.col("fnc.am_wrtof_st_cd"), F.col("arxcl_wroffind.cl_val_id")).alias("wrtof_st_cd"),
        # r_ac_opn_dt
        F.when(F.col("ar.ar_tp_id").isin("DEP", "INV") & F.col("ar.pd_id").like("_3__"), F.col("dep_ar.am_term_fm_dt"))
         .otherwise(F.col("ar.am_ac_opn_dt")).alias("r_ac_opn_dt"),
        if_lon_bor_dep_inv(F.col("ar.am_npa_st"), F.col("arxcl_npasset.cl_val_id")).alias("npa_st"),
        F.col("fnc.am_repymt_hol_mo").alias("repymt_hol_mo"),
        F.col("fnc.am_clsfd_dt").alias("clsfd_dt"),
        F.col("ar.am_npa_cl_dt").alias("npa_cl_dt"),
        F.col("am_od_ar.od_rvw_dt").alias("od_rvw_dt"),
        F.col("fnc.am_lo_ste_cd").alias("lo_ste_cd"),
        F.col("fnc.am_upd_chrg_bal").alias("fee_chrg_bal"),
        if_lon_bor_dep_inv(F.col("fnc.am_cpz_upd_int_amt"), F.col("dep_ar.am_int_otsnd_amt")).alias("int_acr_cap"),
        F.col("fnc.am_nxt_repy_amt").alias("nxt_repy_amt"),
        # cif_int_amt
        F.when(F.col("ar.ar_tp_id").isin("LON", "BOR"), F.col("fnc.am_cpz_upd_int_amt"))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV") & (F.col("dep_ar.am_pnp_bal_orig_amt") < 0), F.col("dep_ar.am_int_otsnd_amt"))
         .when(F.col("ar.ar_tp_id").isin("DEP", "INV"), F.col("dep_ar.am_acr_int_orig_amt"))
         .alias("cif_int_amt"),
        if_lon_bor_dep_inv(F.col("fnc.am_off_bal_shet_amt"), F.col("dep_ar.am_off_bal_shet_amt")).alias("off_bal_shet_amt"),
        F.col("prn_erly_aprv_dt.scr_ind_dt_1"),
        F.col("prn_erly_aprv_dt.scr_ind_dt_2"),
        F.col("prn_erly_aprv_dt.scr_ind_dt_3"),
        F.col("prn_erly_aprv_dt.scr_ind_dt_4"),
        F.col("fnc.am_cyc_ars_val").alias("CYC_ARS_VAL"),
        F.col("ar.am_last_fnc_dt").alias("LAST_FNC_DT"),
        F.col("fnc.am_last_repy_mth").alias("LAST_REPY_MTH"),
        F.col("fnc.am_stdg_insr_amt").alias("STDG_INSR_AMT"),
        F.col("arxcl_siind.cl_val_id").alias("STDG_INSR_IND"),
        F.col("fnc.am_amwin_main_tp_cd").alias("AMWIN_MAIN_TP_CD"),
        F.col("arxcl_lnscagams.cl_val_id").alias("SOLD_TO_CAGAMAS_F"),
        F.col("ar.am_nperf_dt").alias("NPERF_DT"),
        F.col("fnc.am_wdv_pnp_bal").alias("WDV_PNP_BAL"),
        F.col("fnc.am_wdv_int_bal").alias("WDV_INT_BAL"),
        F.col("fnc.am_wdv_upd_chrg_bal").alias("WDV_UPD_CHRG_BAL"),
        F.col("fnc.am_wdv_othr_chrg_bal").alias("WDV_OTHR_CHRG_BAL"),
        F.col("fnc.am_acpt_dt").alias("ACPT_DT"),
        F.col("fnc.am_repymt_due_strt_dt").alias("REPYMT_DUE_STRT_DT"),
        F.col("fnc.am_full_drn_down_dt").alias("FULL_DRN_DOWN_DT"),
        F.col("fnc.am_frst_repy_dt").alias("FRST_REPY_DT"),
        F.col("arxcl_acsoldflg.cl_val_id").alias("AC_SOLD_TO_F"),
        F.col("fnc.am_int_only_exp_dt").alias("INT_ONLY_EXP_DT"),
        F.col("fnc.am_amwin_disch_lock_prd").alias("AMWIN_DISCH_LOCK_PRD"),
        F.col("ar.am_rr_cgy").alias("RR_CGY"),
        F.col("fnc.am_clc_loan_bal").alias("CLC_LOAN_BAL"),
        F.col("fnc.am_adv_dt").alias("ADV_DT"),
        F.col("fnc.am_last_repy_dt").alias("LAST_REPYMT_DT"),
        F.col("fnc.am_last_repymt_amt").alias("LAST_REPYMT_AMT"),
        F.col("fnc.am_mo_fee_paid_amt").alias("MO_FEE_PAID_AMT"),
        F.col("fnc.am_tot_acr_cap_amt").alias("TOT_ACR_CAP_AMT"),
        F.col("fnc.am_bnkr_dt").alias("AC_BNKR_DT"),
        F.col("arxcl_bkruptflg.cl_val_id").alias("AC_BNKR_F"),
        F.col("fnc.am_repymt_shd_id").alias("REPYMT_SHD_ID"),
        F.col("fnc.am_pnp_inst_amt").alias("PNP_INST_AMT"),
        F.col("fnc.am_wrtof_int_amt").alias("WROF_INT_AMT_SRC"),
        F.col("fnc.am_wrtof_pnp_amt").alias("WROF_PNP_AMT_SRC"),
        F.col("fnc.am_islamic_cncpt").alias("ISLAMIC_CNCPT"),
        F.col("fnc.am_ccris_fcy_tp").alias("CCRIS_FCY_TP"),
        F.col("fnc.am_rshd_tag_dt").alias("RSHD_TAG_DT"),
        F.col("fnc.am_zect_hnd_fee").alias("ZECT_HND_FEE"),
        F.col("fnc.am_zect_oth_fee").alias("ZECT_OTH_FEE"),
        F.col("ar.unq_id_src_stm").alias("UNQ_ID_SRC_STM"),
        F.col("am_od_ar.od_int_avl_amt").alias("OD_INT_AVL_AMT"),
        F.col("dep_ar.am_acr_int_orig_amt").alias("ACR_INT_ORIG_AMT"),
        F.col("dep_ar.am_yr_bns_int_amt").alias("YR_BNS_INT_AMT"),
        F.col("dep_ar.am_mo_bns_int_amt").alias("MO_BNS_INT_AMT"),
        F.col("ar.am_last_pymt_dt").alias("LAST_PYMT_DT"),
        F.col("fnc.am_wrt_down_val_unern_int").alias("WRT_DOWN_VAL_UNERN_INT"),
        F.col("fnc.am_cpz_tp_id").alias("CPZ_TP_ID"),
        F.col("fnc.am_old_ac_dtl").alias("OLD_AC_DTL"),
        F.col("prfl.am_vrtl_mia").alias("VRTL_MIA"),
        F.col("prfl.am_vrtl_dpd").alias("VRTL_DPD"),
        F.col("prfl.am_prmpt_py_cnter").alias("prmpt_py_cnter"),
        F.col("prfl.am_buy_back_cnter").alias("buy_back_cnter"),
        F.col("amarxcoa_mx.gl_ent_code").alias("gl_ent_code"),
        F.col("fnc.am_step_up_prd").alias("step_up_prd"),
        F.col("dep_ar.ac_opn_pps_cd").alias("ac_opn_pps_cd"),
        F.col("dep_ar.oth_ac_opn_pps_dsc").alias("oth_ac_opn_pps_dsc"),
        F.col("dep_ar.src_fund_cd").alias("src_fund_cd"),
        F.col("dep_ar.oth_src_fund_dsc").alias("oth_src_fund_dsc"),
        F.col("dep_ar.src_wlth_cd").alias("src_wlth_cd"),
        F.col("dep_ar.oth_src_wlth_dsc").alias("oth_src_wlth_dsc"),
        F.col("ar.AR_NM_1"),
        F.col("ar.AR_NM_2")
    )

    # ==========================================
    # 7. WRITE TO TARGET
    # ==========================================

    # Truncate and Load (Overwrite)
    df_final.write.mode("overwrite").format("parquet").saveAsTable(f"{cSchemaNm4}.ar_dly_c1")

    # ==========================================
    rec_count = df_final.count()

    audit_sql = (
        "INSERT INTO {schema}.job_adt_recon_dtl "
        "(job_id, job_nm, strt_dt_tm, end_dt_tm, inpt_rcrd_cnt, udt_rcrd_cnt, isrt_rcrd_cnt, "
        "btch_id, btch_seq_id, crnt_sts, audit_time) VALUES ("
        "NULL, 'job_tcs_50_ar_ardlyc1misc_ins', "
        f"CAST('{start_time:%Y-%m-%d %H:%M:%S}' AS timestamp), "
        "CAST(current_timestamp() AS timestamp), "
        f"CAST({rec_count} AS decimal(18,0)), "
        "CAST(0 AS decimal(18,0)), "
        f"CAST({rec_count} AS decimal(18,0)), "
        f"CAST({iBatchId} AS decimal(18,0)), "
        f"CAST({iBatchSeq} AS decimal(18,0)), "
        "'SUCCESS', "
        "CAST(current_timestamp() AS timestamp))"
    ).format(schema=cSchemaNm1)

    log.info("Writing audit record")
    spark.sql(audit_sql)


if not _SPARK_TASK_RUNNER:
    _JOB_PARAMS_B64 = base64.b64encode(
        json.dumps(Variable.get("JOB_PARAMS", default_var={}, deserialize_json=True)).encode()
    ).decode()

    with DAG(
        dag_id="JOB_TCS_50_AR_ARDLYC1MISC_INS",
        default_args={"owner": "airflow", "retries": 0, "retry_delay": timedelta(minutes=5)},
        schedule_interval=None,
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=False,
        tags=["spark", "storedproc", "mis006"],
    ) as dag:
        JOB_TCS_50_AR_ARDLYC1MISC_INS_task = JOB_TCS_50_AR_ARDLYC1MISC_INS()
        Job_VIEW_task = Job_VIEW()

        SPARK_CONF_COMMON = {
            "spark.executor.instances": "10",
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.defaultCatalog": "spark_catalog",
            "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083",
            "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar",
            "spark.dynamicAllocation.enabled": "false",
            "spark.shuffle.service.enabled": "false",
            "spark.app.name": "JOB_TCS_50_AR_ARDLYC1MISC_INS",
            "spark.openlineage.appName": "JOB_TCS_50_AR_ARDLYC1MISC_INS",
            "spark.openmetadata.transport.pipelineServiceName": "JOB_TCS_50_AR_ARDLYC1MISC_INS",
            "spark.openmetadata.transport.pipelineName": "JOB_TCS_50_AR_ARDLYC1MISC_INS",
        }

        RUN_TCS_50_AR_ARDLYC1MISC_INS_task = SparkSubmitOperator(
            conf=SPARK_CONF_COMMON,
            task_id="RUN_TCS_50_AR_ARDLYC1MISC_INS",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="RUN_TCS_50_AR_ARDLYC1MISC_INS",
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
            application_args=["--module", __file__, "--task", "RUN_TCS_50_AR_ARDLYC1MISC_INS"],
        )

        JOB_TCS_50_AR_ARDLYC1MISC_INS_task >> Job_VIEW_task
        Job_VIEW_task >> RUN_TCS_50_AR_ARDLYC1MISC_INS_task

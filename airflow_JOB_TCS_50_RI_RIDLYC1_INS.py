#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-10 00:36:26
# @Author  : cloudera
# @File    : airflow_JOB_TCS_50_RI_RIDLYC1_INS.py
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
def JOB_TCS_50_RI_RIDLYC1_INS(**kw_args) -> str:
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))


@task
def Job_VIEW(**kw_args) -> str:
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))


@task.pyspark(conn_id="spark-local")
def RUN_TCS_50_RI_RIDLYC1_INS(spark: SparkSession, sc: SparkContext, **kw_args):
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType, DecimalType, StringType, TimestampType
    from ds_functions import spark_register_ds_common_functions

    spark_register_ds_common_functions(spark)

    log = logging.getLogger(__name__)

    job_params = Variable.get("JOB_PARAMS", deserialize_json=True)
    cSchemaNm1 = job_params.get(
        "SILVER_DB",
        job_params.get("silver_db", job_params.get("EDW_COMMON", job_params.get("edw_common", "silver"))),
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
        f"{cSchemaNm3}.ri",
        f"{cSchemaNm3}.ri_val",
        f"{cSchemaNm3}.pst_adr",
        f"{cSchemaNm3}.real_pty",
        f"{cSchemaNm3}.chtl",
        f"{cSchemaNm3}.am_gnt_ri",
        f"{cSchemaNm3}.ri_x_cl",
        f"{cSchemaNm3}.am_ri_remarks",
        f"{cSchemaNm3}.doc_itm",
        f"{cSchemaNm3}.ri_x_pst_adr",
        f"{cSchemaNm3}.fnc_ri",
        f"{cSchemaNm3}.ins_ar",
        f"{cSchemaNm3}.am_ri_alt_ident",
        f"{cSchemaNm3}.ar",
        f"{cSchemaNm3}.dep_ar",
        f"{cSchemaNm4}.ri_dly_c1",
    )
    missing = [t for t in required_tables if not spark.catalog.tableExists(t)]
    if missing:
        raise ValueError("Missing required tables in expected schemas: {}".format(", ".join(missing)))

    start_time = datetime.now()

    # Initialize Spark Session

    # ==========================================
    # 1. PARAMETERS & CONFIGURATION
    # ==========================================

    # Start Time
    dStrtDtTm = F.current_timestamp()

    # ==========================================
    # 2. FETCH BUSINESS DATE
    # ==========================================
    # Fetch bsn_date from batch_run_dtl
    row_bsn = spark.sql(f"""
        SELECT bsn_date FROM {cSchemaNm1}.batch_run_dtl 
        WHERE batch_id = {iBatchId}
    """).collect()

    if row_bsn:
        iBsnDt = row_bsn[0]['bsn_date']
    else:
        # Default fallback if batch not found (or raise error)
        iBsnDt = 20230101 
        print("Warning: Business Date not found, using default.")

    # ==========================================
    # 3. LOAD & FILTER SOURCE TABLES
    # ==========================================
    # Helper function to load active SOR records (end_dt = 99991231 and src_stm_id = 'TCS')
    def load_active_sor(table_name):
        return spark.table(f"{cSchemaNm3}.{table_name}") \
                    .filter(F.col("end_dt") == 99991231) \
                    .filter(F.col("src_stm_id") == 'TCS') \
                    .alias(table_name)

    # Load Primary Table
    df_ri = load_active_sor("ri").filter(F.col("plg_f") == 1)

    # Load Related Tables
    df_real_pty = load_active_sor("real_pty").alias("rpty")
    df_chtl = load_active_sor("chtl").alias("chtl")
    df_vhcl = load_active_sor("vhcl").alias("vhcl")
    df_am_gnt_ri = load_active_sor("am_gnt_ri").alias("gntri")
    df_am_ri_remarks = load_active_sor("am_ri_remarks").alias("rirmk")

    # Load RI_X_CL with specific filters (SR1001397 optimization)
    df_ri_x_cl = load_active_sor("ri_x_cl").filter(
        F.col("cl_scm_id").isin("SHARFLG", "REPOFLG", "MRTAIND", "OWNOCCFLG", "PRMYDWELFLG") & 
        (F.col("ri_x_cl_tp_id") == "IND")
    ).alias("rixcl")

    df_ri_val = load_active_sor("ri_val").alias("rival")
    df_fnc_ri = load_active_sor("fnc_ri").alias("fncri")
    df_doc_itm = load_active_sor("doc_itm").alias("docitm")

    # Load Address Tables (Specific Filter for COLLPTY)
    df_ri_x_pst_adr = load_active_sor("ri_x_pst_adr").filter(
        F.col("ri_x_pst_adr_tp_id") == "COLLPTY"
    ).alias("rixpstadr")

    df_pst_adr = spark.table(f"{cSchemaNm3}.pst_adr").alias("pstadr")

    # Load Insurance Table (Specific Filter)
    df_ins_ar = load_active_sor("ins_ar").filter(
        F.col("am_src_sys") == "COL"
    ).alias("insar")

    # Load Alt Ident (Specific Filter)
    df_am_ri_alt_ident = load_active_sor("am_ri_alt_ident").filter(
        F.col("ppn_src_stm_id") == "CLTREFRNUM"
    ).alias("rialtident")

    # Load AR Subquery (Market Price Lookup)
    # Logic: JOIN AR + DEP_AR on AR_ID
    df_ar_sub = spark.table(f"{cSchemaNm3}.ar").filter((F.col("end_dt") == 99991231) & (F.col("src_stm_id") == 'TCS')).alias("ar") \
        .join(
            spark.table(f"{cSchemaNm3}.dep_ar").filter((F.col("end_dt") == 99991231) & (F.col("src_stm_id") == 'TCS')).alias("dep"),
            F.col("ar.ar_id") == F.col("dep.dep_ar_id")
        ).select(
            F.col("ar.ac_no"),
            F.col("dep.am_pnp_bal_orig_amt").alias("mkt_prc")
        ).alias("ar_lookup")


    # ==========================================
    # 4. JOIN LOGIC
    # ==========================================

    df_joined = df_ri \
        .join(df_real_pty, F.col("ri.ri_id") == F.col("rpty.real_pty_id"), "left") \
        .join(df_chtl, F.col("ri.ri_id") == F.col("chtl.chtl_id"), "left") \
        .join(df_vhcl, F.col("ri.ri_id") == F.col("vhcl.vhcl_id"), "left") \
        .join(df_am_gnt_ri, F.col("ri.ri_id") == F.col("gntri.plg_ri_id"), "left") \
        .join(df_am_ri_remarks, F.col("ri.ri_id") == F.col("rirmk.ri_id"), "left") \
        .join(df_ri_x_cl, F.col("ri.ri_id") == F.col("rixcl.ri_id"), "left") \
        .join(df_ri_val, F.col("ri.ri_id") == F.col("rival.ri_id"), "left") \
        .join(df_fnc_ri, F.col("ri.ri_id") == F.col("fncri.fnc_ri_id"), "left") \
        .join(df_ri_x_pst_adr, F.col("ri.ri_id") == F.col("rixpstadr.ri_id"), "left") \
        .join(df_pst_adr, F.col("rixpstadr.pst_adr_id") == F.col("pstadr.pst_adr_id"), "left") \
        .join(df_doc_itm, F.col("ri.ri_id") == F.col("docitm.doc_itm_id"), "left") \
        .join(df_ins_ar, (F.col("ri.am_ins_polcy_num") == F.col("insar.ins_polcy_doc_id")) & (F.col("insar.am_key_id") == F.col("ri.ri_id")), "left") \
        .join(df_am_ri_alt_ident, F.col("ri.ri_id") == F.col("rialtident.ri_id"), "left") \
        .join(df_ar_sub, F.col("fncri.am_fnc_imt_num") == F.col("ar_lookup.ac_no"), "left")

    # ==========================================
    # 5. AGGREGATION & PROJECTION
    # ==========================================

    # Helper for Split Part
    def split_part(col, delim, idx_1_based):
        return F.split(col, delim).getItem(idx_1_based - 1)

    df_grouped = df_joined.groupBy(
        "ri.ri_id",
        "ri.eff_dt",
        "ri.src_stm_id",
        # clt_id derived from GROUP BY logic in SQL (CASE WHEN plg_f=1...)
        F.when(F.col("ri.plg_f") == 1, split_part(F.col("ri.unq_id_src_stm"), '#', 3)).alias("clt_id"),
        "ri.am_old_clt_num",
        "ri.am_doc_issu_dt", # Needed for issu_dt calculation
        "ri.am_strt_dt",     # Needed for issu_dt calculation
        "rpty.am_safe_for_ocp_cd",
        "ri.ri_lcs_tp_id",
        "ri.ri_tp_id",
        "ri.am_exp_dt",
        "ri.am_prch_dt",
        "gntri.gnt_scm",
        "gntri.dbtr_rgst_num",
        "rpty.am_land_use",
        "rialtident.ri_ident",
        "gntri.cvr_pct",
        "ri.ri_sub_tp_id",
        "gntri.scr_ar_tp_id",
        "vhcl.modl_yr",
        "rirmk.othr_dsc_1",
        "ri.am_rvw_dt",
        "vhcl.am_vhcl_st",
        "rpty.am_pty_st",
        "chtl.am_st_cd",
        "rpty.am_pty_ttl_dstc",
        "rpty.am_pty_ttl_ste_id",
        "gntri.gnt_prd",
        "chtl.am_scr_cd",
        "gntri.scr_cd",
        "rpty.am_scr_cd",
        "ri.am_scr_cd",
        "fncri.am_scr_cd",
        "docitm.am_scr_tp_id",
        "chtl.coll_make_id",
        "ri.unq_id_src_stm",
        "rpty.am_pty_town",
        "rirmk.othr_dsc_3",
        "ri.am_last_val_dt",
        "gntri.gnt_own_cst",
        "ri.cptl_relf",
        "ri.dsc"
    )

    df_final = df_grouped.agg(
        F.lit(iBsnDt).alias("pcs_dt"),
        # issu_dt
        F.when(F.col("ri.ri_tp_id").isin("01", "02", "03"), F.col("ri.am_doc_issu_dt"))
         .otherwise(F.col("ri.am_strt_dt")).alias("issu_dt"),
        # mkt_prc
        F.max(F.when(F.col("ri.ri_tp_id") == "09", F.col("ar_lookup.mkt_prc"))
               .when(F.col("rival.ri_val_tp_id") == "APPRVAL", F.col("rival.val_amt"))).alias("mkt_prc"),
        # pty_ac_admn_cost
        F.max(F.when(F.col("ri.ri_tp_id").isin("05", "06", "07"), 0)
               .when(F.col("ri.ri_tp_id") == "08", F.col("gntri.ac_amdn_cost_amt"))
               .when(F.col("ri.ri_tp_id").isin("01", "02", "03"), F.col("rpty.am_pty_ac_admn_cost"))
               .when((F.col("ri.ri_tp_id") == "09") & (F.col("rival.ri_val_tp_id") == "FDADMNCOST"), F.col("rival.val_amt"))
               .when((F.col("ri.ri_tp_id") == "10") & (F.col("rival.ri_val_tp_id") == "SHRACMADMNCOST"), F.col("rival.val_amt"))
               .when(F.col("rival.ri_val_tp_id") == "DOCACMADMNCOST", F.col("rival.val_amt"))).alias("pty_ac_admn_cost"),
        # shr_cnter_cd
        F.max(F.when(F.col("ri.ri_tp_id") == "10", F.col("fncri.am_refr_num"))).alias("shr_cnter_cd"),
        # clt_sale_dt
        F.max(F.when(F.col("ri.ri_tp_id").isin("05", "06", "07"), F.col("chtl.am_tendr_rslt_dt"))
               .when(F.col("ri.ri_tp_id") == "08", F.col("gntri.clm_rcv_dt"))
               .when(F.col("ri.ri_tp_id").isin("01", "02", "03") & (F.col("rival.ri_val_tp_id") == "HIPURAUCTNVAL"), F.col("rival.am_bus_val_dt"))
               .when(F.col("ri.ri_tp_id").isin("01", "02", "03") & (F.col("rival.ri_val_tp_id") == "PTYAUCTNVAL"), F.col("rival.am_bus_val_dt"))
               .when(F.col("ri.ri_tp_id") == "09", F.col("ri.am_displ_dt"))).alias("clt_sale_dt"),
        # clt_sale_prc
        F.max(F.when(F.col("ri.ri_tp_id").isin("05", "06", "07"), F.col("chtl.am_tendr_rslt_amt"))
               .when(F.col("ri.ri_tp_id").isin("01", "02", "03") & (F.col("rival.ri_val_tp_id") == "HIPURAUCTNVAL"), F.col("rival.val_amt"))
               .when(F.col("ri.ri_tp_id").isin("01", "02", "03") & (F.col("rival.ri_val_tp_id") == "PTYAUCTNVAL"), F.col("rival.val_amt"))
               .when(F.col("ri.ri_tp_id") == "08", F.col("gntri.clm_amt"))
               .when((F.col("ri.ri_tp_id") == "10") & (F.col("rival.ri_val_tp_id") == "SHRSALEPRC"), F.col("rival.val_amt"))
               .when(F.col("ri.ri_tp_id").isin("09", "11") & (F.col("rival.ri_val_tp_id") == "SALEVAL"), F.col("rival.val_amt"))).alias("clt_sale_prc"),
        # Direct mappings from GroupBy
        F.col("ri.ri_lcs_tp_id").alias("clt_st"),
        F.col("ri.ri_tp_id").alias("clt_tp_cd"),
        # ccy_cd (Not in GroupBy, needs Aggregation as per logic implies 1:1 or Max)
        F.max(F.col("rival.ccy_id")).alias("ccy_cd"),
        F.col("ri.am_exp_dt").alias("clt_exp_dt"),
        F.when(F.col("ri.ri_tp_id") == "08", F.col("ri.am_exp_dt")).alias("gnt_mat_dt"),
        F.when(F.col("ri.ri_tp_id") == "08", F.col("ri.am_strt_dt")).alias("gnt_strt_dt"),
        F.when(F.col("ri.ri_tp_id").isin("01", "02", "03", "05", "06", "07"), F.col("ri.am_prch_dt"))
         .otherwise(F.col("ri.am_strt_dt")).alias("prch_dt"),
        F.max(F.when(F.col("rival.ri_val_tp_id") == "BNKVAL", F.col("rival.am_bus_val_dt"))).alias("val_dt"),
        # frc_sale_val
        F.max(F.when(F.col("ri.ri_tp_id").isin("05", "06", "07") & (F.col("rival.ri_val_tp_id") == "HIPURFORCEVAL"), F.col("rival.val_amt"))
               .when(F.col("ri.ri_tp_id").isin("01", "02", "03") & (F.col("rival.ri_val_tp_id") == "FORCEVAL"), F.col("rival.val_amt"))).alias("frc_sale_val"),
        F.col("gntri.gnt_scm"),
        F.col("gntri.dbtr_rgst_num").alias("gnr_rgst_num"),
        F.col("rpty.am_land_use").alias("land_use"),
        F.col("rialtident.ri_ident").alias("clt_refr_num"),
        F.when(F.col("ri.ri_tp_id").isin("05", "06", "07"), F.max(F.col("chtl.coll_modl_dsc"))).alias("modl_cd"),
        F.when(F.col("ri.ri_tp_id").isin("05", "06", "07"), F.max(F.col("chtl.am_good_val_p")))
         .when(F.col("ri.ri_tp_id").isin("01", "02", "03"), F.max(F.col("ri.am_val_p"))).alias("val_nm"),
        F.col("gntri.cvr_pct").alias("pct_gnt"),
        F.max(F.when(F.col("ri.ri_tp_id").isin("01", "02", "03") & (F.col("rixpstadr.ri_x_pst_adr_tp_id") == "COLLPTY"), F.col("pstadr.pstcd_area_id"))).alias("pty_lo_pst_cd"),
        F.col("ri.ri_sub_tp_id"),
        # prch_prc
        F.max(F.when(F.col("ri.ri_tp_id").isin("05", "06", "07") & (F.col("rival.ri_val_tp_id") == "HIPURCSTAMT"), F.col("rival.val_amt"))
               .when(F.col("ri.ri_tp_id") == "08", F.col("gntri.scr_pvdd_amt"))
               .when(F.col("ri.ri_tp_id").isin("01", "02", "03") & (F.col("rival.ri_val_tp_id") == "PTYPURCSTAMT"), F.col("rival.val_amt"))
               .when((F.col("ri.ri_tp_id") == "09") & (F.col("rival.ri_val_tp_id") == "SECAMTFD"), F.col("rival.val_amt"))
               .when((F.col("ri.ri_tp_id") == "10") & (F.col("rival.ri_val_tp_id") == "SHRTOTVAL"), F.col("rival.val_amt"))
               .when(F.col("rival.ri_val_tp_id") == "HIPURCSTAMT", F.col("rival.val_amt"))).alias("prch_prc"),
        F.max(F.when(F.col("ri.ri_tp_id").isin("01", "02", "03"), F.col("rpty.am_rank_of_chrg"))).alias("rank_of_chrg"),
        F.max(F.when(F.col("ri.ri_tp_id").isin("01", "02", "03") & (F.col("rival.ri_val_tp_id") == "RSVVAL"), F.col("rival.val_amt"))).alias("rsrv_val"),
        F.max(F.when((F.col("rixcl.cl_scm_id") == "SHARFLG") & (F.col("rixcl.ri_x_cl_tp_id") == "IND"), F.col("rixcl.cl_val_id"))).alias("shr_f"),
        F.col("gntri.scr_ar_tp_id").alias("gnr_tp"),
        # val_tp
        F.max(F.when(F.col("ri.ri_tp_id").isin("05", "06", "07"), F.col("chtl.am_good_val_tp_id"))
               .when(F.col("ri.ri_tp_id") == "08", F.col("gntri.val_tp_id"))
               .when(F.col("ri.ri_tp_id").isin("01", "02", "03"), F.col("rpty.am_pty_val_tp_id"))
               .when(F.col("ri.ri_tp_id").isin("09", "10"), F.col("fncri.am_val_tp_id"))
               .otherwise(F.col("docitm.am_val_tp_id"))).alias("val_tp"),
        F.col("vhcl.modl_yr").alias("make_yr"),
        F.col("rirmk.othr_dsc_1").alias("scr_dsc"),
        F.col("ri.ri_lcs_tp_id"),
        F.col("ri.am_rvw_dt").alias("rvw_dt"),
        F.max(F.when(F.col("rival.ri_val_tp_id") == "BNKVAL", F.col("rival.val_amt"))).alias("bnk_val"),
        F.col("vhcl.am_vhcl_st").alias("vhcl_tp"),
        F.col("rpty.am_pty_st").alias("ri_st_pty"),
        F.col("chtl.am_st_cd").alias("ri_st_vhcl"),
        F.lit(iBsnDt).alias("btch_dt"),
        F.max(F.when(F.col("ri.am_ins_polcy_num") == F.col("insar.ins_polcy_doc_id"), F.col("insar.ins_tp_id"))).alias("ins_tp"),
        # on_sell_amt
        F.max(F.when(F.col("ri.ri_tp_id").isin("05", "06", "01", "02", "03", "07") & (F.col("rival.ri_val_tp_id") == "HIPURAUCTNVAL"), F.col("rival.val_amt"))).alias("on_sell_amt"),
        # on_sell_dt
        F.max(F.when(F.col("ri.ri_tp_id").isin("05", "06", "01", "02", "03", "07") & (F.col("rival.ri_val_tp_id") == "HIPURAUCTNVAL"), F.col("rival.am_bus_val_dt"))).alias("on_sell_dt"),
        F.max(F.when(F.col("rival.ri_val_tp_id") == "SALEVAL", F.col("rival.val_amt"))).alias("on_sell_rec"),
        F.max(F.when(F.col("ri.ri_tp_id").isin("05", "06", "07") & (F.col("rixcl.cl_scm_id") == "REPOFLG") & (F.col("rixcl.ri_x_cl_tp_id") == "IND"), F.col("rixcl.cl_val_id"))).alias("repssn_ind"),
        F.when(F.col("ri.ri_tp_id").isin("01", "02", "03") & (F.col("rpty.am_pty_st") == "26"), "Y").otherwise("N").alias("abd_prj_ind"),
        F.col("ri.am_strt_dt").alias("clt_strt_dt"),
        F.col("rpty.am_pty_ttl_dstc").alias("pty_dstc"),
        F.col("rpty.am_pty_ttl_ste_id").alias("pty_ttl_ste_id"),
        F.col("gntri.gnt_prd").alias("pct_gnt_prd"),
        # frc_sale_val_dt
        F.max(F.when(F.col("ri.ri_tp_id").isin("05", "06", "07") & (F.col("rival.ri_val_tp_id") == "HIPURFORCEVAL"), F.col("rival.am_bus_val_dt"))
               .when(F.col("ri.ri_tp_id").isin("01", "02", "03") & (F.col("rival.ri_val_tp_id") == "FORCEVAL"), F.col("rival.am_bus_val_dt"))).alias("frc_sale_val_dt"),
        # clt_sec_cd
        F.max(F.when(F.col("ri.ri_tp_id").isin("05", "06", "07"), F.col("chtl.am_scr_cd"))
               .when(F.col("ri.ri_tp_id") == "08", F.col("gntri.scr_cd"))
               .when(F.col("ri.ri_tp_id").isin("01", "02", "03"), F.col("rpty.am_scr_cd"))
               .when(F.col("ri.ri_tp_id") == "09", F.col("ri.am_scr_cd"))
               .when(F.col("ri.ri_tp_id") == "10", F.col("fncri.am_scr_cd"))
               .otherwise(F.col("docitm.am_scr_tp_id"))).alias("clt_sec_cd"),
        F.max(F.when(F.col("ri.ri_tp_id").isin("05", "06", "07"), F.col("chtl.coll_make_id"))).alias("vhcl_mk"),
        split_part(F.col("ri.unq_id_src_stm"), "#", 2).alias("lgl_ent_id"),
        F.col("rpty.am_pty_town").alias("pty_town"),
        F.max(F.when(F.col("rival.ri_val_tp_id") == "PTYLANDVAL", F.col("rival.am_bus_val_dt"))).alias("clt_val_pcs_dt"),
        F.col("rirmk.othr_dsc_3").alias("clt_othr_dsc_1"),
        F.col("ri.am_last_val_dt").alias("prp_last_val_dt"),
        F.when(F.col("ri.ri_tp_id") == "08", F.col("gntri.gnt_own_cst")).alias("gnt_own_cst"),
        F.max(F.when(F.col("ri.ri_tp_id").isin("01", "02", "03") & (F.col("rival.ri_val_tp_id") == "RSVVAL"), F.col("rival.am_bus_val_dt"))).alias("rsrv_val_dt"),
        F.max(F.when(F.col("rival.ri_val_tp_id") == "APPRVAL", F.col("rival.val_amt"))).alias("APRS_VAL"),
        F.max(F.col("rpty.am_pty_tp_seq")).alias("pty_tp_seq"),
        F.max(F.col("rpty.am_dvlpr_cd")).alias("dvlpr_cd"),
        F.max(F.col("rpty.am_pty_prj_cd")).alias("pty_prj_cd"),
        F.max(F.col("rpty.am_prj_polcy_num")).alias("prj_policy_num"),
        F.max(F.col("rpty.am_mukim_daerah")).alias("mukim_daerah"),
        F.max(F.col("rpty.am_dvlp_area")).alias("dvlp_area"),
        F.max(F.col("rpty.am_abd_prj_dt")).alias("abd_prj_dt"),
        F.max(F.col("rpty.am_lse_exp_dt")).alias("lse_exp_dt"),
        F.max(F.col("rpty.am_pty_ttl_area")).alias("pty_ttl_area"),
        F.max(F.col("rpty.am_pty_ttl_num")).alias("pty_ttl_num"),
        F.max(F.col("rpty.am_pty_ttl_tp_id")).alias("pty_ttl_tp_id"),
        F.max(F.col("rpty.am_pty_ttl_cd")).alias("pty_ttl_cd"),
        F.max(F.col("rpty.am_pty_ttl_st_id")).alias("pty_ttl_st_id"),
        F.max(F.col("rpty.am_pty_ttl_inf")).alias("pty_ttl_inf"),
        F.max(F.col("rpty.am_pty_ttl_unit_of_msr")).alias("pty_ttl_unit_of_msr"),
        F.max(F.col("rpty.am_dvlp_unit_num")).alias("dvlp_unit_num"),
        F.max(F.col("rpty.am_pty_land_offc")).alias("pty_land_offc"),
        F.max(F.col("rpty.am_cvt_exp_dt")).alias("cvt_exp_dt"),
        # pty_frcls_mkt_val
        F.max(F.when(F.col("ri.ri_tp_id").isin("01", "02", "03") & (F.col("rival.ri_val_tp_id") == "PTYMKVA"), F.col("rival.val_amt"))).alias("pty_frcls_mkt_val"),
        F.max(F.when(F.col("ri.ri_tp_id").isin("01", "02", "03") & (F.col("rival.ri_val_tp_id") == "PTYREVAL"), F.col("rival.val_amt"))).alias("rval_pty_amt"),
        F.max(F.when(F.col("ri.ri_tp_id").isin("01", "02", "03") & (F.col("rival.ri_val_tp_id") == "PTYPRVREVA"), F.col("rival.val_amt"))).alias("prev_rval_amt"),
        F.max(F.when(F.col("ri.ri_tp_id").isin("01", "02", "03") & (F.col("rival.ri_val_tp_id") == "PTYPRVREVA"), F.col("rival.am_bus_val_dt"))).alias("prev_rval_dt"),
        F.max(F.when(F.col("ri.ri_tp_id").isin("01", "02", "03") & (F.col("rival.ri_val_tp_id") == "REALZVAL"), F.col("rival.val_amt"))).alias("est_mkt_val"),
        F.max(F.when((F.col("rixcl.cl_scm_id") == "PRMYDWELFLG") & (F.col("rixcl.ri_x_cl_tp_id") == "IND"), F.col("rixcl.cl_val_id"))).alias("prim_rsdng_pty"),
        F.max(F.col("chtl.am_good_id_num")).alias("good_id_num"),
        F.max(F.col("rirmk.othr_dsc_1")).alias("othr_dsc_1"),
        F.max(F.col("gntri.clm_amt")).alias("clm_amt"),
        F.max(F.col("gntri.clm_rcv_dt")).alias("clm_rcv_dt"),
        F.max(F.col("chtl.am_tendr_rslt_dt")).alias("tendr_rslt_dt"),
        F.max(F.col("chtl.am_tendr_rslt_amt")).alias("tendr_rslt_amt"),
        F.max(F.col("rpty.am_bld_area")).alias("bld_area"),
        F.max(F.when(F.col("rival.ri_val_tp_id") == "GNTFRCSALEVAL", F.col("rival.val_amt"))).alias("GNT_FRC_SALE_VAL_AMT"),
        F.max(F.when(F.col("rival.ri_val_tp_id") == "GNTACTSALEVAL", F.col("rival.val_amt"))).alias("GNT_ACT_SALE_VAL_AMT"),
        F.max(F.when(F.col("rival.ri_val_tp_id") == "GNTRSRVVAL", F.col("rival.val_amt"))).alias("GNT_RSRV_VAL_AMT"),
        F.max(F.when(F.col("rival.ri_val_tp_id") == "GNTFRCSALEVAL", F.col("rival.am_bus_val_dt"))).alias("GNT_FRC_SALE_BUS_VAL_DT"),
        F.max(F.when(F.col("rival.ri_val_tp_id") == "GNTACTSALEVAL", F.col("rival.am_bus_val_dt"))).alias("GNT_ACT_SALE_BUS_VAL_DT"),
        F.max(F.when(F.col("rival.ri_val_tp_id") == "GNTRSRVVAL", F.col("rival.am_bus_val_dt"))).alias("GNT_RSRV_BUS_VAL_DT"),
        F.max(F.when(F.col("ri.ri_tp_id").isin("01", "02", "03") & (F.col("rival.ri_val_tp_id") == "PTYREVAL"), F.col("rival.am_bus_val_dt"))).alias("RVAL_PTY_DT"),
        F.max(F.col("rival.am_bus_val_dt")).alias("BUS_VAL_DT"),
        F.col("ri.cptl_relf").alias("cptl_relf"),
        F.max(F.col("rpty.pty_sale_prc")).alias("PTY_SALE_PRC"),
        F.max(F.col("gntri.mon_gua_amt")).alias("MON_GUA_AMT"),
        F.max(F.col("gntri.mon_od_limit")).alias("MON_OD_LIMIT"),
        F.max(F.col("gntri.mon_loan_os")).alias("MON_LOAN_OS"),
        F.col("ri.dsc").alias("dsc")
    )

    # ==========================================
    # 6. WRITE TO TARGET
    # ==========================================
    # Overwrite Target Table
    df_final.write.mode("overwrite").format("parquet").saveAsTable(f"{cSchemaNm4}.ri_dly_c1")

    # ==========================================

    rec_count = df_final.count()

    audit_sql = (
        "INSERT INTO {schema}.job_adt_recon_dtl "
        "(job_id, job_nm, strt_dt_tm, end_dt_tm, inpt_rcrd_cnt, udt_rcrd_cnt, isrt_rcrd_cnt, "
        "btch_id, btch_seq_id, crnt_sts, audit_time) VALUES ("
        "NULL, 'job_tcs_50_ri_ridlyc1_ins', "
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
        dag_id="JOB_TCS_50_RI_RIDLYC1_INS",
        default_args={"owner": "airflow", "retries": 0, "retry_delay": timedelta(minutes=5)},
        schedule_interval=None,
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=False,
        tags=["spark", "storedproc", "mis006"],
    ) as dag:
        JOB_TCS_50_RI_RIDLYC1_INS_task = JOB_TCS_50_RI_RIDLYC1_INS()
        Job_VIEW_task = Job_VIEW()

        SPARK_CONF_COMMON = {
            "spark.executor.instances": "10",
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.defaultCatalog": "spark_catalog",
            "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083",
            "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar",
            "spark.dynamicAllocation.enabled": "false",
            "spark.shuffle.service.enabled": "false",
            "spark.app.name": "JOB_TCS_50_RI_RIDLYC1_INS",
            "spark.openlineage.appName": "JOB_TCS_50_RI_RIDLYC1_INS",
            "spark.openmetadata.transport.pipelineServiceName": "JOB_TCS_50_RI_RIDLYC1_INS",
            "spark.openmetadata.transport.pipelineName": "JOB_TCS_50_RI_RIDLYC1_INS",
        }

        RUN_TCS_50_RI_RIDLYC1_INS_task = SparkSubmitOperator(
            conf=SPARK_CONF_COMMON,
            task_id="RUN_TCS_50_RI_RIDLYC1_INS",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="RUN_TCS_50_RI_RIDLYC1_INS",
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
            application_args=["--module", __file__, "--task", "RUN_TCS_50_RI_RIDLYC1_INS"],
        )

        JOB_TCS_50_RI_RIDLYC1_INS_task >> Job_VIEW_task
        Job_VIEW_task >> RUN_TCS_50_RI_RIDLYC1_INS_task

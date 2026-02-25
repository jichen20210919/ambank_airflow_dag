#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-10 00:27:35
# @Author  : cloudera
# @File    : airflow_JOB_TCS_20_MIS006DELTA_ARLONSKEY_INS.py
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
def JOB_TCS_20_MIS006DELTA_ARLONSKEY_INS(**kw_args) -> str:
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))


@task
def Job_VIEW(**kw_args) -> str:
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))


@task.pyspark(conn_id="spark-local")
def RUN_TCS_20_MIS006DELTA_ARLONSKEY_INS(spark: SparkSession, sc: SparkContext, **kw_args):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import IntegerType, StringType, TimestampType, DecimalType
    from ds_functions import spark_register_ds_common_functions

    spark_register_ds_common_functions(spark)

    log = logging.getLogger(__name__)

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
    batch_id = int(job_params.get("BATCH_ID", job_params.get("batch_id", 0)))
    batch_seq = int(job_params.get("BATCH_SEQ", job_params.get("batch_seq", 0)))

    required_tables = (
        f"{schema_staging}.batch_run_dtl",
        f"{schema_staging}.tcs_mis006_delta",
        f"{schema_work}.tcs_ar_lon_skey",
        f"{schema_sor}.bkey_ar",
        f"{schema_sor}.ar",
        f"{schema_staging}.job_adt_recon_dtl",
    )
    missing = [t for t in required_tables if not spark.catalog.tableExists(t)]
    if missing:
        raise ValueError(
            "Missing required tables in expected schemas: {}".format(", ".join(missing))
        )

    log.info("Fetching business date")
    if batch_id <= 0:
        log.info("Batch ID not provided, picking latest batch with a business date")
        fallback_batch = (
            spark.sql(
                "SELECT batch_id "
                f"FROM {schema_staging}.batch_run_dtl "
                "WHERE bsn_date IS NOT NULL "
                "ORDER BY batch_id DESC "
                "LIMIT 1"
            )
            .collect()
        )
        if fallback_batch:
            batch_id = int(fallback_batch[0]["batch_id"])
            log.info("Derived batch_id=%s from batch_run_dtl", batch_id)

    bsn_dt_rows = (
        spark.sql(
            "SELECT CAST(bsn_date AS INT) AS bsn_dt "
            f"FROM {schema_staging}.batch_run_dtl "
            f"WHERE batch_id = {batch_id}"
        )
        .collect()
    )
    if not bsn_dt_rows or bsn_dt_rows[0]["bsn_dt"] is None:
        bsn_dt = int(datetime.utcnow().strftime("%Y%m%d"))
        log.warning(
            "Business date not found for batch_id %s; using fallback %s",
            batch_id,
            bsn_dt,
        )
    else:
        bsn_dt = int(bsn_dt_rows[0]["bsn_dt"])

    start_time = datetime.now()

    log.info("Truncating target table")
    spark.sql(f"TRUNCATE TABLE {schema_work}.tcs_ar_lon_skey")

    log.info("Loading source tables")
    df_delta = spark.table(f"{schema_staging}.tcs_mis006_delta")

    unq_expr = (
        F.concat(
            F.lit("TCS#"),
            F.col("mi006_soc_no").cast(StringType()),
            F.lit("#"),
            F.col("mi006_memb_cust_ac").cast(StringType()),
        )
    )
    if "unq_id_src_stm" not in df_delta.columns:
        df_delta = df_delta.withColumn("unq_id_src_stm", unq_expr)
    df_delta = df_delta.alias("a")
    df_bkey = spark.table(f"{schema_sor}.bkey_ar").alias("b")
    df_ar = (
        spark.table(f"{schema_sor}.ar")
        .filter(F.col("end_dt") == 99991231)
        .alias("c")
    )

    df_joined = (
        df_delta.join(
            df_bkey,
            F.col("a.unq_id_src_stm") == F.col("b.unq_id_src_stm"),
            "left",
        )
        .join(
            df_ar,
            F.col("a.unq_id_src_stm") == F.col("c.unq_id_src_stm"),
            "left",
        )
    )

    def has_changed(col_a, col_c):
        return F.coalesce(F.col(f"a.{col_a}").cast(StringType()), F.lit("")) != F.coalesce(
            F.col(f"c.{col_c}").cast(StringType()), F.lit("")
        )

    def has_changed_date(col_a, col_c):
        c_a = F.col(f"a.{col_a}")
        c_c = F.col(f"c.{col_c}")
        return (c_a.isNull() & c_c.isNotNull()) | (c_a.isNotNull() & c_c.isNull()) | (
            (c_a != c_c) & c_a.isNotNull() & c_c.isNotNull()
        )

    scd_condition = (
        has_changed("mi006_borm_sys", "ar_tp_id")
        | has_changed("mi006_purpose_code_a", "ar_pps_tp_id")
        | has_changed("mi006_purpose_code_b", "ar_rsn_tp_id")
        | has_changed("mi006_stat", "ar_lcs_tp_id")
        | has_changed_date("mi006_acct_stat_dt_set", "ar_lcs_dt")
        | has_changed("mi006_int_type", "ar_term_tp_id")
        | has_changed("mi006_currency", "dnmn_ccy_id")
        | has_changed_date("mi006_maturity_date", "est_end_dt")
        | has_changed("mi006_acsn_short_name", "ar_nm")
        | has_changed("mi006_description", "dsc")
        | has_changed("mi006_act_type", "pd_id")
        | has_changed("mi006_memb_cust_ac", "ac_no")
        | has_changed_date("mi006_int_start_date", "int_clcn_eff_dt")
        | (
            F.coalesce(
                F.concat(F.lit("TCS#"), F.col("a.mi006_act_type"), F.lit("#"), F.col("a.mi006_cat")),
                F.lit(""),
            )
            != F.coalesce(F.col("c.am_pd_cd"), F.lit(""))
        )
        | has_changed("mi006_org_tenor", "am_src_tnr_info")
        | has_changed("mi006_cat", "am_ac_cgy_cd")
        | has_changed("mi006_rate_reprice_type", "am_reprc_tp_id")
        | has_changed_date("mi006_last_reprice_date", "am_last_rprc_dt")
        | has_changed_date("mi006_next_reprice_date", "am_nxt_rprc_dt")
        | has_changed("mi006_soc_no", "am_lgl_ent_id")
        | has_changed("mi006_islamic_bank", "am_bnkg_cncpt_cd")
        | has_changed_date("mi006_apprv_date", "am_ac_opn_dt")
        | has_changed("mi006_borm_rm_code", "am_rltnp_mgr_cd")
        | has_changed("mi006_br_no", "rprg_ou_ip_id")
        | has_changed("mi006_bnm_sector_code", "am_cnrl_bnk_idy_cl_id")
        | has_changed("mi006_rev_tenor", "am_rvsn_tnr_info")
        | has_changed("mi006_cas_stat_code", "am_chk_st_cd")
        | has_changed("mi006_stmt_frequency", "am_stmt_frq")
        | (
            F.coalesce(
                F.when(F.col("a.mi006_stat") == "40", F.col("a.mi006_lst_fin_date")).cast(IntegerType()),
                F.lit(-1),
            )
            != F.coalesce(F.col("c.am_ac_cls_dt"), F.lit(-1))
        )
        | has_changed("mi006_repay_freq", "am_pymt_frq_prdc_id")
        | has_changed("mi006_no_of_owner", "am_own_cnt")
        | has_changed("mi006_npl_status", "am_npa_st")
        | has_changed_date("mi006_am_npl_date", "am_nperf_dt")
        | has_changed_date("mi006_npl_class_date", "am_npa_cl_dt")
        | has_changed("mi006_rr_type", "am_rr_tp_id")
        | has_changed("mi006_rr_cat", "am_rr_cgy")
        | has_changed("mi006_rr_sub_cat", "am_rr_sub_cgy")
        | has_changed("mi006_status_rnr", "am_rr_st")
        | has_changed("mi006_rr_reason", "am_rr_rsn")
        | has_changed("mi006_rr_count", "am_rr_cnt")
        | has_changed("mi006_watchlist_tag", "am_wtch_list_tag")
        | has_changed_date("mi006_watchlist_dt", "am_wtch_list_tag_dt")
        | has_changed("mi006_boex_frs_ip_tag", "am_frs_mprd_tag")
        | has_changed_date("mi006_boex_frs_date", "am_frs_st_dt")
        | has_changed("mi006_bois_citizen_code", "am_cst_ctzn_cd")
        | has_changed("mi006_basm_base_id", "am_base_id")
        | has_changed("mi006_rate_id", "am_rate_id")
        | has_changed("mi006_loan_trm", "am_nbr_of_tnr")
        | has_changed("mi006_term_basis", "am_tnr_unit_msr")
        | has_changed_date("MI006_BOIS_LAST_PAY_DATE", "AM_LAST_PYMT_DT")
        | has_changed("mi006_distress_status", "am_rsk_st")
        | has_changed("mi006_reschedule_code", "am_rshd_cd")
        | has_changed_date("mi006_npl_bfr_rr_date", "am_npl_bfr_rr_dt")
        | has_changed_date("mi006_righ_pl_date", "am_pl_dt")
        | has_changed("MI006_ATRP_GRACE_PERIOD", "AM_ATR_GRC_PRD")
        | has_changed("MI006_RMCD_RETAIN_FLAG", "AM_RM_CODE_RTL_F")
        | has_changed_date("mi006_akpk_date", "am_rr_tag_dt")
        | has_changed_date("mi006_ori_rr_date", "am_frst_rr_dt")
        | (
            F.coalesce(
                F.concat(F.col("a.mi006_acct_name_1"), F.col("a.mi006_acct_name_2"), F.col("a.mi006_acct_name_3")),
                F.lit(""),
            )
            != F.coalesce(F.col("c.ar_long_nm"), F.lit(""))
        )
        | has_changed("MI006_EXC_DPD_WOFF_VAL", "EXCL_DPD_WRTOF_VLD_IND")
        | has_changed("MI006_RNR_PL_REHAB_FLAG", "RR_PERF_AC_REHAB_F")
        | (
            F.coalesce(F.col("a.MI006_RNR_PL_PROMPT_PAY_CNT"), F.lit(0))
            != F.coalesce(F.col("c.RR_PERF_AC_PRMPT_PYMT_CNTER"), F.lit(0))
        )
        | has_changed("mi006_rnr_rem_profit", "rr_tot_amz_pft")
        | has_changed("mi006_rnr_monthly_profit", "rr_mo_amz_pft")
        | has_changed("MI006_CCPT_CLASS", "SCD_CCPT_CL")
        | has_changed("MI006_EIY", "EIY")
    )

    max_ar_id = (
        spark.table(f"{schema_sor}.bkey_ar")
        .select(F.max("ar_id").alias("max_val"))
        .collect()[0]["max_val"]
    )
    if max_ar_id is None:
        max_ar_id = 0

    w_new_ids = Window.orderBy(F.col("a.unq_id_src_stm"))

    df_processed = (
        df_joined.withColumn(
            "calc_keyflag",
            F.when(F.col("c.ar_id").isNull(), F.lit("NEW"))
            .when(scd_condition, F.lit("SCD"))
            .otherwise(F.lit(None)),
        ).withColumn(
            "calc_ar_id",
            F.when(F.col("b.ar_id").isNotNull(), F.col("b.ar_id")).otherwise(
                F.lit(max_ar_id) + F.row_number().over(w_new_ids)
            ),
        )
    )

    df_final = df_processed.select(
        F.col("calc_ar_id").alias("ar_id"),
        F.col("a.unq_id_src_stm"),
        F.lit(bsn_dt).cast(IntegerType()).alias("eff_dt"),
        F.col("a.mi006_borm_sys").alias("ar_tp_id"),
        F.lit(None).cast(StringType()).alias("ar_frq_tp_id"),
        F.col("a.mi006_purpose_code_a").alias("ar_pps_tp_id"),
        F.col("a.mi006_purpose_code_b").alias("ar_rsn_tp_id"),
        F.lit(None).cast(StringType()).alias("ar_fnc_st_tp_id"),
        F.lit(None).cast(TimestampType()).alias("ar_fnc_st_dt"),
        F.col("a.mi006_stat").alias("ar_lcs_tp_id"),
        F.col("a.mi006_acct_stat_dt_set").alias("ar_lcs_dt"),
        F.lit(None).cast(StringType()).alias("prim_rspl_cntr_id"),
        F.lit(None).cast(TimestampType()).alias("prim_rspl_cntr_dt"),
        F.col("a.mi006_int_type").alias("ar_term_tp_id"),
        F.lit(None).cast(StringType()).alias("orig_cnl_id"),
        F.col("a.mi006_currency").alias("dnmn_ccy_id"),
        F.lit(None).cast(IntegerType()).alias("nbr_sgn"),
        F.lit(None).cast(IntegerType()).alias("prn_ar_id"),
        F.col("a.mi006_maturity_date").alias("est_end_dt"),
        F.lit(None).cast(StringType()).alias("rnew_f"),
        F.lit(None).cast(TimestampType()).alias("nxt_rnew_dt"),
        F.col("a.mi006_acsn_short_name").alias("ar_nm"),
        F.col("a.mi006_description").alias("dsc"),
        F.lit(None).cast(StringType()).alias("fnc_mkt_imt_tp_id"),
        F.col("a.mi006_act_type").cast(StringType()).alias("pd_id"),
        F.lit(None).cast(StringType()).alias("bsh_rpt_cgy_id"),
        F.lit(None).cast(StringType()).alias("ac_ar_tp_id"),
        F.lit(None).cast(TimestampType()).alias("crn_fix_prd_end_dt"),
        F.col("a.mi006_memb_cust_ac").alias("ac_no"),
        F.col("a.mi006_int_start_date").alias("int_clcn_eff_dt"),
        F.lit(None).cast(TimestampType()).alias("nxt_cmpd_int_dt"),
        F.lit(None).cast(StringType()).alias("xtrt_f"),
        F.lit(None).cast(StringType()).alias("iban_ac_no"),
        F.lit(None).cast(StringType()).alias("non_pymt_dflt_f"),
        F.concat(F.lit("TCS#"), F.col("a.mi006_act_type"), F.lit("#"), F.col("a.mi006_cat")).alias("pd_cd"),
        F.lit(None).cast(StringType()).alias("non_acr_st_dflt_f"),
        F.col("a.mi006_org_tenor").alias("src_tnr_info"),
        F.col("a.mi006_cat").alias("ac_cgy_cd"),
        F.col("a.mi006_rate_reprice_type").alias("reprc_tp_id"),
        F.col("a.mi006_last_reprice_date").alias("last_rprc_dt"),
        F.col("a.mi006_next_reprice_date").alias("nxt_rprc_dt"),
        F.lit(None).cast(StringType()).alias("int_acr_st_tp_id"),
        F.lit(None).cast(TimestampType()).alias("int_acr_st_dt"),
        F.lit(None).cast(TimestampType()).alias("int_acr_end_dt"),
        F.col("a.mi006_soc_no").alias("lgl_ent_id"),
        F.col("a.mi006_islamic_bank").alias("bnkg_cncpt_cd"),
        F.col("a.mi006_loan_trm").alias("nbr_of_tnr"),
        F.lit(None).cast(StringType()).alias("co_cd"),
        F.lit(None).cast(StringType()).alias("ac_ovrd_f"),
        F.col("a.mi006_apprv_date").alias("ac_opn_dt"),
        F.lit(None).cast(StringType()).alias("prim_idy_cl_id"),
        F.col("a.mi006_borm_rm_code").alias("rltnp_mgr_cd"),
        F.col("a.mi006_br_no").alias("rprg_ou_ip_id"),
        F.col("a.mi006_bnm_sector_code").alias("cnrl_bnk_idy_cl_id"),
        F.col("a.mi006_rev_tenor").alias("rvsn_tnr_info"),
        F.col("a.mi006_cas_stat_code").alias("chk_st_cd"),
        F.col("a.mi006_stmt_frequency").alias("stmt_frq"),
        F.lit(None).cast(DecimalType(10, 2)).alias("fix_pft_shr_rto"),
        F.when(F.col("a.mi006_stat") == "40", F.col("a.mi006_lst_fin_date")).otherwise(None).alias("ac_cls_dt"),
        F.lit(None).cast(TimestampType()).alias("int_clcn_end_dt"),
        F.lit(None).cast(TimestampType()).alias("last_stmt_dt"),
        F.lit(None).cast(IntegerType()).alias("num_of_stmt"),
        F.trim(F.col("a.mi006_repay_freq")).alias("pymt_frq_prdc_id"),
        F.col("a.mi006_no_of_owner").alias("own_cnt"),
        F.lit(None).cast(TimestampType()).alias("grc_prd_end_dt"),
        F.trim(F.col("a.mi006_npl_status")).alias("npa_st"),
        F.col("a.mi006_am_npl_date").alias("nperf_dt"),
        F.col("a.mi006_npl_class_date").alias("npa_cl_dt"),
        F.col("a.mi006_rr_type").alias("rr_tp_id"),
        F.col("a.mi006_rr_cat").alias("rr_cgy"),
        F.col("a.mi006_rr_sub_cat").alias("rr_sub_cgy"),
        F.col("a.mi006_status_rnr").alias("rr_st"),
        F.col("a.mi006_rr_reason").alias("rr_rsn"),
        F.col("a.mi006_rr_count").alias("rr_cnt"),
        F.col("a.mi006_akpk_date").alias("rr_tag_dt"),
        F.col("a.mi006_watchlist_tag").alias("wtch_list_tag"),
        F.col("a.mi006_watchlist_dt").alias("wtch_list_tag_dt"),
        F.col("a.mi006_boex_frs_ip_tag").alias("frs_mprd_tag"),
        F.col("a.mi006_boex_frs_date").alias("frs_st_dt"),
        F.lit(None).cast(StringType()).alias("prscr_rate_cd"),
        F.lit(None).cast(TimestampType()).alias("src_rec_crt_dt"),
        F.lit(None).cast(DecimalType(10, 2)).alias("mrgn_rate"),
        F.lit(None).cast(StringType()).alias("reprc_rset_tp_cd"),
        F.lit(None).cast(StringType()).alias("in_dflt_f"),
        F.col("a.mi006_term_basis").alias("tnr_unit_msr"),
        F.lit(None).cast(IntegerType()).alias("ac_sr_no"),
        F.col("a.MI006_BOIS_LAST_PAY_DATE").alias("last_pymt_dt"),
        F.lit(None).cast(DecimalType(10, 2)).alias("pft_shr_rto_bnk"),
        F.lit(None).cast(DecimalType(10, 2)).alias("pft_shr_rto_cst"),
        F.col("a.mi006_bois_citizen_code").alias("cst_ctzn_cd"),
        F.col("a.mi006_basm_base_id").alias("base_id"),
        F.col("a.mi006_rate_id").alias("rate_id"),
        F.col("a.mi006_distress_status").alias("rsk_st"),
        F.col("a.mi006_reschedule_code").alias("rshd_cd"),
        F.col("a.mi006_npl_bfr_rr_date").alias("npl_bfr_rr_dt"),
        F.col("a.mi006_righ_pl_date").alias("pl_dt"),
        F.col("a.MI006_ATRP_GRACE_PERIOD").alias("ATR_GRC_PRD"),
        F.col("a.MI006_RMCD_RETAIN_FLAG").alias("RM_CODE_RTL_F"),
        F.col("a.mi006_ori_rr_date").alias("frst_rr_dt"),
        F.concat(F.col("a.mi006_acct_name_1"), F.col("a.mi006_acct_name_2"), F.col("a.mi006_acct_name_3")).alias("ar_long_nm"),
        F.col("a.MI006_EXC_DPD_WOFF_VAL").alias("EXCL_DPD_WRTOF_VLD_IND"),
        F.col("a.MI006_RNR_PL_REHAB_FLAG").alias("RR_PERF_AC_REHAB_F"),
        F.col("a.MI006_RNR_PL_PROMPT_PAY_CNT").alias("RR_PERF_AC_PRMPT_PYMT_CNTER"),
        F.col("a.mi006_rnr_rem_profit").alias("rr_tot_amz_pft"),
        F.col("a.mi006_rnr_monthly_profit").alias("rr_mo_amz_pft"),
        F.col("a.MI006_CCPT_CLASS").alias("SCD_CCPT_CL"),
        F.col("a.MI006_EIY").alias("EIY"),
        F.col("calc_keyflag").alias("keyflag"),
    )

    log.info("Writing output table")
    df_final.write.mode("overwrite").format("parquet").saveAsTable(f"{schema_work}.tcs_ar_lon_skey")

    insert_rec = df_final.count()

    audit_sql = (
        "INSERT INTO {schema}.job_adt_recon_dtl "
        "(job_id, job_nm, strt_dt_tm, end_dt_tm, inpt_rcrd_cnt, udt_rcrd_cnt, isrt_rcrd_cnt, "
        "btch_id, btch_seq_id, crnt_sts, audit_time) VALUES ("
        "NULL, 'job_tcs_20_mis006delta_arlonskey_ins', "
        f"CAST('{start_time:%Y-%m-%d %H:%M:%S}' AS timestamp), "
        "CAST(current_timestamp() AS timestamp), "
        f"CAST({insert_rec} AS decimal(18,0)), "
        "CAST(0 AS decimal(18,0)), "
        f"CAST({insert_rec} AS decimal(18,0)), "
        f"CAST({batch_id} AS decimal(18,0)), "
        f"CAST({batch_seq} AS decimal(18,0)), "
        "'SUCCESS', "
        "CAST(current_timestamp() AS timestamp))"
    ).format(schema=schema_staging)

    log.info("Writing audit record")
    spark.sql(audit_sql)


if not _SPARK_TASK_RUNNER:
    _JOB_PARAMS_B64 = base64.b64encode(
        json.dumps(Variable.get("JOB_PARAMS", default_var={}, deserialize_json=True)).encode()
    ).decode()

    with DAG(
        dag_id="JOB_TCS_20_MIS006DELTA_ARLONSKEY_INS",
        default_args={"owner": "airflow", "retries": 0, "retry_delay": timedelta(minutes=5)},
        schedule_interval=None,
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=False,
        tags=["spark", "storedproc", "mis006"],
    ) as dag:
        JOB_TCS_20_MIS006DELTA_ARLONSKEY_INS_task = JOB_TCS_20_MIS006DELTA_ARLONSKEY_INS()
        Job_VIEW_task = Job_VIEW()

        SPARK_CONF_COMMON = {
            "spark.executor.instances": "10",
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.defaultCatalog": "spark_catalog",
            "spark.hadoop.hive.metastore.uris": "thrift://cloudera-master.internal:9083",
            "spark.jars": "/opt/cloudera/parcels/CDH-7.3.1-1.cdh7.3.1.p0.60371244/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.7.3.1.0-197.jar",
            "spark.dynamicAllocation.enabled": "false",
            "spark.shuffle.service.enabled": "false",
            "spark.app.name": "JOB_TCS_20_MIS006DELTA_ARLONSKEY_INS",
            "spark.openlineage.appName": "JOB_TCS_20_MIS006DELTA_ARLONSKEY_INS",
            "spark.openmetadata.transport.pipelineServiceName": "JOB_TCS_20_MIS006DELTA_ARLONSKEY_INS",
            "spark.openmetadata.transport.pipelineName": "JOB_TCS_20_MIS006DELTA_ARLONSKEY_INS",
        }

        RUN_TCS_20_MIS006DELTA_ARLONSKEY_INS_task = SparkSubmitOperator(
            conf=SPARK_CONF_COMMON,
            task_id="RUN_TCS_20_MIS006DELTA_ARLONSKEY_INS",
            application="/home/ec2-user/airflow/spark_apps/spark_task_runner.py",
            name="RUN_TCS_20_MIS006DELTA_ARLONSKEY_INS",
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
            application_args=["--module", __file__, "--task", "RUN_TCS_20_MIS006DELTA_ARLONSKEY_INS"],
        )

        JOB_TCS_20_MIS006DELTA_ARLONSKEY_INS_task >> Job_VIEW_task
        Job_VIEW_task >> RUN_TCS_20_MIS006DELTA_ARLONSKEY_INS_task

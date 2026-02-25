#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-19 12:00:00
# @Author  : cloudera
# @File    : seq_MIS006_2_1_to_2_13.py
# @Copyright: Cloudera.Inc

from __future__ import annotations

import os
_SPARK_TASK_RUNNER = os.environ.get("SPARK_TASK_RUNNER") == "1"

if not _SPARK_TASK_RUNNER:
    import airflow
    from airflow.models import DAG
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
else:
    class DAG:
        pass

    class TriggerDagRunOperator:
        pass


####################################[Main]###################################
if not _SPARK_TASK_RUNNER:
    with DAG(
        dag_id="seq_MIS006_2_1_to_2_13",
        start_date=airflow.utils.dates.days_ago(1),
        schedule_interval=None,
        tags=["datastage"],
    ) as dag:

        sub_seq = TriggerDagRunOperator(
            task_id="Sub_Seq_DbDirect_MIS006_Seq",
            wait_for_completion=True,
            poke_interval=30,
            trigger_dag_id="Sub_Seq_DbDirect_MIS006_Seq_POC",
            conf={"spark.executor.instances": "10"},
        )

        ds_ld = TriggerDagRunOperator(
            task_id="job_DBdirect_MIS006_DS_Ld",
            wait_for_completion=True,
            poke_interval=30,
            trigger_dag_id="job_DBdirect_MIS006_DS_Ld_POC",
            conf={"spark.executor.instances": "10"},
        )

        ld = TriggerDagRunOperator(
            task_id="job_DBdirect_MIS006_Ld",
            wait_for_completion=True,
            poke_interval=30,
            trigger_dag_id="job_DBdirect_MIS006_Ld_POC",
            conf={"spark.executor.instances": "10"},
        )

        sub_seq >> ds_ld >> ld

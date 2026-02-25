#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-19 15:30:00
# @Author  : cloudera
# @File    : seq_MIS006_STOREDPROCS.py
# @Copyright: Cloudera.Inc

from __future__ import annotations

import os
_SPARK_TASK_RUNNER = os.environ.get("SPARK_TASK_RUNNER") == "1"

STOREDPROC_DAG_IDS = [
    "JOB_TCS_10_MIS006_MIS006DELTA_CDC",
    "JOB_TCS_20_MIS006DELTA_ARLONSKEY_INS",
    "JOB_TCS_40_ARLONSKEY_BKEYAR_KEY",
    "JOB_TCS_40_ARLONSKEY_AR_SCD",
    "JOB_TCS_40_MIS006_AR_UPD",
    "JOB_TCS_50_AR_ARDLYC1MISC_INS",
    "JOB_TCS_50_RI_RIDLYC1_INS",
]

if not _SPARK_TASK_RUNNER:
    import airflow
    from airflow.exceptions import AirflowException
    from airflow.models import DAG, DagBag
    from airflow.operators.python import PythonOperator
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    def _validate_storedproc_dags() -> None:
        dagbag = DagBag(include_examples=False)
        missing = [dag_id for dag_id in STOREDPROC_DAG_IDS if dag_id not in dagbag.dags]
        if missing:
            raise AirflowException(
                "Stored-proc DAGs missing from DagBag before running seq_MIS006_STOREDPROCS: %s" % \
                (", ".join(missing))
            )
    def _trigger_conf(dag_id: str) -> dict[str, str]:
        return {
            "spark.app.name": dag_id,
            "spark.openlineage.appName": dag_id,
            "spark.openmetadata.transport.pipelineServiceName": dag_id,
            "spark.openmetadata.transport.pipelineName": dag_id,
        }
else:
    class DAG:
        pass

    class TriggerDagRunOperator:
        pass


####################################[Main]###################################
if not _SPARK_TASK_RUNNER:
    with DAG(
        dag_id="seq_MIS006_STOREDPROCS",
        start_date=airflow.utils.dates.days_ago(1),
        schedule_interval=None,
        tags=["storedproc"],
    ) as dag:

        validate_task = PythonOperator(
            task_id="validate_storedproc_dags",
            python_callable=_validate_storedproc_dags,
        )

        step_3_delta = TriggerDagRunOperator(
            task_id="JOB_TCS_10_MIS006_MIS006DELTA_CDC",
            wait_for_completion=True,
            poke_interval=30,
            trigger_dag_id="JOB_TCS_10_MIS006_MIS006DELTA_CDC",
            conf=_trigger_conf("JOB_TCS_10_MIS006_MIS006DELTA_CDC"),
        )

        step_4_arlon_ins = TriggerDagRunOperator(
            task_id="JOB_TCS_20_MIS006DELTA_ARLONSKEY_INS",
            wait_for_completion=True,
            poke_interval=30,
            trigger_dag_id="JOB_TCS_20_MIS006DELTA_ARLONSKEY_INS",
            conf=_trigger_conf("JOB_TCS_20_MIS006DELTA_ARLONSKEY_INS"),
        )

        step_5_bkey_ar = TriggerDagRunOperator(
            task_id="JOB_TCS_40_ARLONSKEY_BKEYAR_KEY",
            wait_for_completion=True,
            poke_interval=30,
            trigger_dag_id="JOB_TCS_40_ARLONSKEY_BKEYAR_KEY",
            conf=_trigger_conf("JOB_TCS_40_ARLONSKEY_BKEYAR_KEY"),
        )

        step_6_ar_scd = TriggerDagRunOperator(
            task_id="JOB_TCS_40_ARLONSKEY_AR_SCD",
            wait_for_completion=True,
            poke_interval=30,
            trigger_dag_id="JOB_TCS_40_ARLONSKEY_AR_SCD",
            conf=_trigger_conf("JOB_TCS_40_ARLONSKEY_AR_SCD"),
        )

        step_7_ar_upd = TriggerDagRunOperator(
            task_id="JOB_TCS_40_MIS006_AR_UPD",
            wait_for_completion=True,
            poke_interval=30,
            trigger_dag_id="JOB_TCS_40_MIS006_AR_UPD",
            conf=_trigger_conf("JOB_TCS_40_MIS006_AR_UPD"),
        )

        step_8_ar_dly = TriggerDagRunOperator(
            task_id="JOB_TCS_50_AR_ARDLYC1MISC_INS",
            wait_for_completion=True,
            poke_interval=30,
            trigger_dag_id="JOB_TCS_50_AR_ARDLYC1MISC_INS",
            conf=_trigger_conf("JOB_TCS_50_AR_ARDLYC1MISC_INS"),
        )

        step_9_ri_dly = TriggerDagRunOperator(
            task_id="JOB_TCS_50_RI_RIDLYC1_INS",
            wait_for_completion=True,
            poke_interval=30,
            trigger_dag_id="JOB_TCS_50_RI_RIDLYC1_INS",
            conf=_trigger_conf("JOB_TCS_50_RI_RIDLYC1_INS"),
        )

        (
            validate_task
            >> step_3_delta
            >> step_4_arlon_ins
            >> step_5_bkey_ar
            >> step_6_ar_scd
            >> step_7_ar_upd
            >> step_8_ar_dly
            >> step_9_ri_dly
        )


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-02 23:22:38
# @Author  : cloudera
# @File    : Sub_Seq_DbDirect_MIS006_Seq_POC.py
# @Copyright: Cloudera.Inc




from __future__ import annotations

import base64
from abc import abstractmethod
import os
_SPARK_TASK_RUNNER = os.environ.get("SPARK_TASK_RUNNER") == "1"

if not _SPARK_TASK_RUNNER:
    import airflow
    from airflow.decorators import task, task_group
    from airflow.models import DAG, Variable
    from airflow.models.dag import DAG
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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

    def task_group(*args, **kwargs):
        return _identity

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

    class TriggerDagRunOperator:
        pass
from datetime import datetime, timedelta
from jinja2 import Template
from pyspark.sql.functions import lit, col, input_file_name
from pyspark.sql.types import *
import json
import logging
if not _SPARK_TASK_RUNNER:
    import pendulum
import textwrap

@task
def Sub_Seq_DbDirect_MIS006_Seq_POC(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def Job_VIEW(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))
def job_DBdirect_Mis006_BLDVNN_Extr():
    trigger = TriggerDagRunOperator(task_id="job_DBdirect_Mis006_BLDVNN_Extr", wait_for_completion=True, poke_interval=30, trigger_dag_id="job_DBdirect_Mis006_BLDVNN_Extr_POC", conf={"spark.executor.instances": "10", })
    return trigger
def job_DBdirect_Mis006_BLDVWW_Extr():
    trigger = TriggerDagRunOperator(task_id="job_DBdirect_Mis006_BLDVWW_Extr", wait_for_completion=True, poke_interval=30, trigger_dag_id="job_DBdirect_Mis006_BLDVWW_Extr_POC", conf={"spark.executor.instances": "10", })
    return trigger

@task
def V0A24(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))
@task
def job_DBdirect_MIS006_BOIS_Extr_DSLink15_Part(**context):
    logger = logging.getLogger(__name__)
    dag_run = context['dag_run']
    upstream_task_id = 'job_DBdirect_Mis006_BLDVWW_Extr'
    # Get the upstream task instance
    upstream_ti = dag_run.get_task_instance(upstream_task_id)
    job_DBdirect_Mis006_BLDVWW_Extr = {'JobStatus': upstream_ti.state}
    logger.info(job_DBdirect_Mis006_BLDVWW_Extr)

    return job_DBdirect_Mis006_BLDVWW_Extr['JobStatus'] == "success" or job_DBdirect_Mis006_BLDVWW_Extr['JobStatus'] == "failed"


def job_DBdirect_MIS006_BOIS_Extr():
    trigger = TriggerDagRunOperator(task_id="job_DBdirect_MIS006_BOIS_Extr", wait_for_completion=True, poke_interval=30, trigger_dag_id="job_DBdirect_MIS006_BOIS_Extr_POC", conf={"spark.executor.instances": "10", })
    return trigger
@task
def job_DBdirect_Mis006_PITA_LONP_Extr_DSLink13_Part(**context):
    logger = logging.getLogger(__name__)
    dag_run = context['dag_run']
    upstream_task_id = 'job_DBdirect_MIS006_BOIS_Extr'
    # Get the upstream task instance
    upstream_ti = dag_run.get_task_instance(upstream_task_id)
    job_DBdirect_MIS006_BOIS_Extr = {'JobStatus': upstream_ti.state}
    logger.info(job_DBdirect_MIS006_BOIS_Extr)

    return job_DBdirect_MIS006_BOIS_Extr['JobStatus'] == "success" or job_DBdirect_MIS006_BOIS_Extr['JobStatus'] == "failed"


def job_DBdirect_Mis006_PITA_LONP_Extr():
    trigger = TriggerDagRunOperator(task_id="job_DBdirect_Mis006_PITA_LONP_Extr", wait_for_completion=True, poke_interval=30, trigger_dag_id="job_DBdirect_Mis006_PITA_LONP_Extr_POC", conf={"spark.executor.instances": "10", })
    return trigger
@task
def job_DBdirect_Mis006_BLDVTT_ALL_Extr_DSLink14_Part(**context):
    logger = logging.getLogger(__name__)
    dag_run = context['dag_run']
    upstream_task_id = 'job_DBdirect_Mis006_PITA_LONP_Extr'
    # Get the upstream task instance
    upstream_ti = dag_run.get_task_instance(upstream_task_id)
    job_DBdirect_Mis006_PITA_LONP_Extr = {'JobStatus': upstream_ti.state}
    logger.info(job_DBdirect_Mis006_PITA_LONP_Extr)

    return job_DBdirect_Mis006_PITA_LONP_Extr['JobStatus'] == "success" or job_DBdirect_Mis006_PITA_LONP_Extr['JobStatus'] == "failed"


def job_DBdirect_Mis006_BLDVTT_ALL_Extr():
    trigger = TriggerDagRunOperator(task_id="job_DBdirect_Mis006_BLDVTT_ALL_Extr", wait_for_completion=True, poke_interval=30, trigger_dag_id="job_DBdirect_Mis006_BLDVTT_ALL_Extr_POC", conf={"spark.executor.instances": "10", })
    return trigger
@task
def job_DBdirect_MIS006_BORM_Extr_DSLink11_Part(**context):
    logger = logging.getLogger(__name__)
    dag_run = context['dag_run']
    upstream_task_id = 'job_DBdirect_Mis006_BLDVTT_ALL_Extr'
    # Get the upstream task instance
    upstream_ti = dag_run.get_task_instance(upstream_task_id)
    job_DBdirect_Mis006_BLDVTT_ALL_Extr = {'JobStatus': upstream_ti.state}
    logger.info(job_DBdirect_Mis006_BLDVTT_ALL_Extr)

    return job_DBdirect_Mis006_BLDVTT_ALL_Extr['JobStatus'] == "success" or job_DBdirect_Mis006_BLDVTT_ALL_Extr['JobStatus'] == "failed"


def job_DBdirect_MIS006_BORM_Extr():
    trigger = TriggerDagRunOperator(task_id="job_DBdirect_MIS006_BORM_Extr", wait_for_completion=True, poke_interval=30, trigger_dag_id="job_DBdirect_MIS006_BORM_Extr_POC", conf={"spark.executor.instances": "10", })
    return trigger
@task
def job_DBdirect_MIS006_RELM_Extr_DSLink12_Part(**context):
    logger = logging.getLogger(__name__)
    dag_run = context['dag_run']
    upstream_task_id = 'job_DBdirect_MIS006_BORM_Extr'
    # Get the upstream task instance
    upstream_ti = dag_run.get_task_instance(upstream_task_id)
    job_DBdirect_MIS006_BORM_Extr = {'JobStatus': upstream_ti.state}
    logger.info(job_DBdirect_MIS006_BORM_Extr)

    return job_DBdirect_MIS006_BORM_Extr['JobStatus'] == "success" or job_DBdirect_MIS006_BORM_Extr['JobStatus'] == "failed"


def job_DBdirect_MIS006_RELM_Extr():
    trigger = TriggerDagRunOperator(task_id="job_DBdirect_MIS006_RELM_Extr", wait_for_completion=True, poke_interval=30, trigger_dag_id="job_DBdirect_MIS006_RELM_Extr_POC", conf={"spark.executor.instances": "10", })
    return trigger
@task
def job_DBdirect_Mis006_CUSVAA_Extr_DSLink16_Part(**context):
    logger = logging.getLogger(__name__)
    dag_run = context['dag_run']
    upstream_task_id = 'job_DBdirect_MIS006_RELM_Extr'
    # Get the upstream task instance
    upstream_ti = dag_run.get_task_instance(upstream_task_id)
    job_DBdirect_MIS006_RELM_Extr = {'JobStatus': upstream_ti.state}
    logger.info(job_DBdirect_MIS006_RELM_Extr)

    return job_DBdirect_MIS006_RELM_Extr['JobStatus'] == "success" or job_DBdirect_MIS006_RELM_Extr['JobStatus'] == "failed"


def job_DBdirect_Mis006_CUSVAA_Extr():
    trigger = TriggerDagRunOperator(task_id="job_DBdirect_Mis006_CUSVAA_Extr", wait_for_completion=True, poke_interval=30, trigger_dag_id="job_DBdirect_Mis006_CUSVAA_Extr_POC", conf={"spark.executor.instances": "10", })
    return trigger
@task
def job_DBdirect_MIS006_CHPM_Extr_DSLink17_Part(**context):
    logger = logging.getLogger(__name__)
    dag_run = context['dag_run']
    upstream_task_id = 'job_DBdirect_Mis006_CUSVAA_Extr'
    # Get the upstream task instance
    upstream_ti = dag_run.get_task_instance(upstream_task_id)
    job_DBdirect_Mis006_CUSVAA_Extr = {'JobStatus': upstream_ti.state}
    logger.info(job_DBdirect_Mis006_CUSVAA_Extr)

    return job_DBdirect_Mis006_CUSVAA_Extr['JobStatus'] == "success" or job_DBdirect_Mis006_CUSVAA_Extr['JobStatus'] == "failed"


def job_DBdirect_MIS006_CHPM_Extr():
    trigger = TriggerDagRunOperator(task_id="job_DBdirect_MIS006_CHPM_Extr", wait_for_completion=True, poke_interval=30, trigger_dag_id="job_DBdirect_MIS006_CHPM_Extr_POC", conf={"spark.executor.instances": "10", })
    return trigger
@task
def job_DBdirect_Mis006_RRMD_Extr_DSLink18_Part(**context):
    logger = logging.getLogger(__name__)
    dag_run = context['dag_run']
    upstream_task_id = 'job_DBdirect_MIS006_CHPM_Extr'
    # Get the upstream task instance
    upstream_ti = dag_run.get_task_instance(upstream_task_id)
    job_DBdirect_MIS006_CHPM_Extr = {'JobStatus': upstream_ti.state}
    logger.info(job_DBdirect_MIS006_CHPM_Extr)

    return job_DBdirect_MIS006_CHPM_Extr['JobStatus'] == "success" or job_DBdirect_MIS006_CHPM_Extr['JobStatus'] == "failed"


def job_DBdirect_Mis006_RRMD_Extr():
    trigger = TriggerDagRunOperator(task_id="job_DBdirect_Mis006_RRMD_Extr", wait_for_completion=True, poke_interval=30, trigger_dag_id="job_DBdirect_Mis006_RRMD_Extr_POC", conf={"spark.executor.instances": "10", })
    return trigger
@task
def job_DBdirect_Mis006_BDEH_Extr_DSLink22_Part(**context):
    logger = logging.getLogger(__name__)
    dag_run = context['dag_run']
    upstream_task_id = 'job_DBdirect_Mis006_RRMD_Extr'
    # Get the upstream task instance
    upstream_ti = dag_run.get_task_instance(upstream_task_id)
    job_DBdirect_Mis006_RRMD_Extr = {'JobStatus': upstream_ti.state}
    logger.info(job_DBdirect_Mis006_RRMD_Extr)

    return job_DBdirect_Mis006_RRMD_Extr['JobStatus'] == "success" or job_DBdirect_Mis006_RRMD_Extr['JobStatus'] == "failed"


def job_DBdirect_Mis006_BDEH_Extr():
    trigger = TriggerDagRunOperator(task_id="job_DBdirect_Mis006_BDEH_Extr", wait_for_completion=True, poke_interval=30, trigger_dag_id="job_DBdirect_MIS006_BDEH_LONP_Extr_POC", conf={"spark.executor.instances": "10", })
    return trigger

####################################[Main]###################################
if not _SPARK_TASK_RUNNER:
    _JOB_PARAMS_B64 = base64.b64encode(json.dumps(Variable.get("JOB_PARAMS", default_var={}, deserialize_json=True)).encode()).decode()
    with DAG(
        dag_id="Sub_Seq_DbDirect_MIS006_Seq_POC",
        start_date=airflow.utils.dates.days_ago(1),
        schedule_interval=None,
        tags=['datastage'],
    ) as dag:
        
        Sub_Seq_DbDirect_MIS006_Seq_POC_task = Sub_Seq_DbDirect_MIS006_Seq_POC()
        
        Job_VIEW_task = Job_VIEW()
        
        job_DBdirect_Mis006_BLDVNN_Extr_task = job_DBdirect_Mis006_BLDVNN_Extr()
        
        job_DBdirect_Mis006_BLDVWW_Extr_task = job_DBdirect_Mis006_BLDVWW_Extr()
        
        V0A24_task = V0A24()
        
        job_DBdirect_MIS006_BOIS_Extr_DSLink15_Part_task = job_DBdirect_MIS006_BOIS_Extr_DSLink15_Part()
        
        job_DBdirect_MIS006_BOIS_Extr_task = job_DBdirect_MIS006_BOIS_Extr()
        
        job_DBdirect_Mis006_PITA_LONP_Extr_DSLink13_Part_task = job_DBdirect_Mis006_PITA_LONP_Extr_DSLink13_Part()
        
        job_DBdirect_Mis006_PITA_LONP_Extr_task = job_DBdirect_Mis006_PITA_LONP_Extr()
        
        job_DBdirect_Mis006_BLDVTT_ALL_Extr_DSLink14_Part_task = job_DBdirect_Mis006_BLDVTT_ALL_Extr_DSLink14_Part()
        
        job_DBdirect_Mis006_BLDVTT_ALL_Extr_task = job_DBdirect_Mis006_BLDVTT_ALL_Extr()
        
        job_DBdirect_MIS006_BORM_Extr_DSLink11_Part_task = job_DBdirect_MIS006_BORM_Extr_DSLink11_Part()
        
        job_DBdirect_MIS006_BORM_Extr_task = job_DBdirect_MIS006_BORM_Extr()
        
        job_DBdirect_MIS006_RELM_Extr_DSLink12_Part_task = job_DBdirect_MIS006_RELM_Extr_DSLink12_Part()
        
        job_DBdirect_MIS006_RELM_Extr_task = job_DBdirect_MIS006_RELM_Extr()
        
        job_DBdirect_Mis006_CUSVAA_Extr_DSLink16_Part_task = job_DBdirect_Mis006_CUSVAA_Extr_DSLink16_Part()
        
        job_DBdirect_Mis006_CUSVAA_Extr_task = job_DBdirect_Mis006_CUSVAA_Extr()
        
        job_DBdirect_MIS006_CHPM_Extr_DSLink17_Part_task = job_DBdirect_MIS006_CHPM_Extr_DSLink17_Part()
        
        job_DBdirect_MIS006_CHPM_Extr_task = job_DBdirect_MIS006_CHPM_Extr()
        
        job_DBdirect_Mis006_RRMD_Extr_DSLink18_Part_task = job_DBdirect_Mis006_RRMD_Extr_DSLink18_Part()
        
        job_DBdirect_Mis006_RRMD_Extr_task = job_DBdirect_Mis006_RRMD_Extr()
        
        job_DBdirect_Mis006_BDEH_Extr_DSLink22_Part_task = job_DBdirect_Mis006_BDEH_Extr_DSLink22_Part()
        
        job_DBdirect_Mis006_BDEH_Extr_task = job_DBdirect_Mis006_BDEH_Extr()
        
        
        Sub_Seq_DbDirect_MIS006_Seq_POC_task >> Job_VIEW_task
        
        Job_VIEW_task >> job_DBdirect_Mis006_BLDVNN_Extr_task
        
        Job_VIEW_task >> job_DBdirect_Mis006_BLDVWW_Extr_task
        
        Job_VIEW_task >> V0A24_task
        
        job_DBdirect_Mis006_BLDVWW_Extr_task >> job_DBdirect_MIS006_BOIS_Extr_DSLink15_Part_task
        
        job_DBdirect_MIS006_BOIS_Extr_DSLink15_Part_task >> job_DBdirect_MIS006_BOIS_Extr_task
        
        job_DBdirect_MIS006_BOIS_Extr_task >> job_DBdirect_Mis006_PITA_LONP_Extr_DSLink13_Part_task
        
        job_DBdirect_Mis006_PITA_LONP_Extr_DSLink13_Part_task >> job_DBdirect_Mis006_PITA_LONP_Extr_task
        
        job_DBdirect_Mis006_PITA_LONP_Extr_task >> job_DBdirect_Mis006_BLDVTT_ALL_Extr_DSLink14_Part_task
        
        job_DBdirect_Mis006_BLDVTT_ALL_Extr_DSLink14_Part_task >> job_DBdirect_Mis006_BLDVTT_ALL_Extr_task
        
        job_DBdirect_Mis006_BLDVTT_ALL_Extr_task >> job_DBdirect_MIS006_BORM_Extr_DSLink11_Part_task
        
        job_DBdirect_MIS006_BORM_Extr_DSLink11_Part_task >> job_DBdirect_MIS006_BORM_Extr_task
        
        job_DBdirect_MIS006_BORM_Extr_task >> job_DBdirect_MIS006_RELM_Extr_DSLink12_Part_task
        
        job_DBdirect_MIS006_RELM_Extr_DSLink12_Part_task >> job_DBdirect_MIS006_RELM_Extr_task
        
        job_DBdirect_MIS006_RELM_Extr_task >> job_DBdirect_Mis006_CUSVAA_Extr_DSLink16_Part_task
        
        job_DBdirect_Mis006_CUSVAA_Extr_DSLink16_Part_task >> job_DBdirect_Mis006_CUSVAA_Extr_task
        
        job_DBdirect_Mis006_CUSVAA_Extr_task >> job_DBdirect_MIS006_CHPM_Extr_DSLink17_Part_task
        
        job_DBdirect_MIS006_CHPM_Extr_DSLink17_Part_task >> job_DBdirect_MIS006_CHPM_Extr_task
        
        job_DBdirect_MIS006_CHPM_Extr_task >> job_DBdirect_Mis006_RRMD_Extr_DSLink18_Part_task
        
        job_DBdirect_Mis006_RRMD_Extr_DSLink18_Part_task >> job_DBdirect_Mis006_RRMD_Extr_task
        
        job_DBdirect_Mis006_RRMD_Extr_task >> job_DBdirect_Mis006_BDEH_Extr_DSLink22_Part_task
        
        job_DBdirect_Mis006_BDEH_Extr_DSLink22_Part_task >> job_DBdirect_Mis006_BDEH_Extr_task
        
    
    

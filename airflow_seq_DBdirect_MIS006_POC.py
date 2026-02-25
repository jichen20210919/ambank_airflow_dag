
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-04 12:53:23
# @Author  : cloudera
# @File    : seq_DBdirect_MIS006_POC.py
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
def seq_DBdirect_MIS006_POC(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def Job_VIEW(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def V0A12(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))

@task
def UserVariables_BinaryDate(**kw_args) -> str:
    # TODO: this is a dummy implementation, do your detailed job here
    keys = kw_args.keys
    return "({})".format(",".join(kw_args.keys()))
@task
def Sub_Seq_DbDirect_MIS006_Seq_Sub_Seq_DbDirect_MIS006_Seq_Part(**context):
    logger = logging.getLogger(__name__)
    dag_run = context['dag_run']
    upstream_task_id = 'UserVariables_BinaryDate'
    # Get the upstream task instance
    upstream_ti = dag_run.get_task_instance(upstream_task_id)
    UserVariables_BinaryDate = {'JobStatus': upstream_ti.state}
    logger.info(UserVariables_BinaryDate)

    return True


def Sub_Seq_DbDirect_MIS006_Seq():
    trigger = TriggerDagRunOperator(task_id="Sub_Seq_DbDirect_MIS006_Seq", wait_for_completion=True, poke_interval=30, trigger_dag_id="Sub_Seq_DbDirect_MIS006_Seq_POC", conf={"spark.executor.instances": "10", })
    return trigger
@task
def job_DBdirect_MIS006_DS_Ld_Dataset_Load_Part(**context):
    logger = logging.getLogger(__name__)
    dag_run = context['dag_run']
    upstream_task_id = 'Sub_Seq_DbDirect_MIS006_Seq'
    # Get the upstream task instance
    upstream_ti = dag_run.get_task_instance(upstream_task_id)
    Sub_Seq_DbDirect_MIS006_Seq = {'JobStatus': upstream_ti.state}
    logger.info(Sub_Seq_DbDirect_MIS006_Seq)

    return Sub_Seq_DbDirect_MIS006_Seq['JobStatus'] == "success" or Sub_Seq_DbDirect_MIS006_Seq['JobStatus'] == "failed"


def job_DBdirect_MIS006_DS_Ld():
    trigger = TriggerDagRunOperator(task_id="job_DBdirect_MIS006_DS_Ld", wait_for_completion=True, poke_interval=30, trigger_dag_id="job_DBdirect_MIS006_DS_Ld_POC", conf={"spark.executor.instances": "10", })
    return trigger
@task
def job_DBdirect_MIS006_Ld_Final_NZ_Load_Part(**context):
    logger = logging.getLogger(__name__)
    dag_run = context['dag_run']
    upstream_task_id = 'job_DBdirect_MIS006_DS_Ld'
    # Get the upstream task instance
    upstream_ti = dag_run.get_task_instance(upstream_task_id)
    job_DBdirect_MIS006_DS_Ld = {'JobStatus': upstream_ti.state}
    logger.info(job_DBdirect_MIS006_DS_Ld)

    return True


def job_DBdirect_MIS006_Ld():
    trigger = TriggerDagRunOperator(task_id="job_DBdirect_MIS006_Ld", wait_for_completion=True, poke_interval=30, trigger_dag_id="job_DBdirect_MIS006_Ld_POC", conf={"spark.executor.instances": "10", })
    return trigger

####################################[Main]###################################
if not _SPARK_TASK_RUNNER:
    _JOB_PARAMS_B64 = base64.b64encode(json.dumps(Variable.get("JOB_PARAMS", default_var={}, deserialize_json=True)).encode()).decode()
    with DAG(
        dag_id="seq_DBdirect_MIS006_POC",
        start_date=airflow.utils.dates.days_ago(1),
        schedule_interval=None,
        tags=['datastage'],
    ) as dag:
        
        seq_DBdirect_MIS006_POC_task = seq_DBdirect_MIS006_POC()
        
        Job_VIEW_task = Job_VIEW()
        
        V0A12_task = V0A12()
        
        UserVariables_BinaryDate_task = UserVariables_BinaryDate()
        
        Sub_Seq_DbDirect_MIS006_Seq_Sub_Seq_DbDirect_MIS006_Seq_Part_task = Sub_Seq_DbDirect_MIS006_Seq_Sub_Seq_DbDirect_MIS006_Seq_Part()
        
        Sub_Seq_DbDirect_MIS006_Seq_task = Sub_Seq_DbDirect_MIS006_Seq()
        
        job_DBdirect_MIS006_DS_Ld_Dataset_Load_Part_task = job_DBdirect_MIS006_DS_Ld_Dataset_Load_Part()
        
        job_DBdirect_MIS006_DS_Ld_task = job_DBdirect_MIS006_DS_Ld()
        
        job_DBdirect_MIS006_Ld_Final_NZ_Load_Part_task = job_DBdirect_MIS006_Ld_Final_NZ_Load_Part()
        
        job_DBdirect_MIS006_Ld_task = job_DBdirect_MIS006_Ld()
        
        
        seq_DBdirect_MIS006_POC_task >> Job_VIEW_task
        
        Job_VIEW_task >> V0A12_task
        
        Job_VIEW_task >> UserVariables_BinaryDate_task
        
        UserVariables_BinaryDate_task >> Sub_Seq_DbDirect_MIS006_Seq_Sub_Seq_DbDirect_MIS006_Seq_Part_task
        
        Sub_Seq_DbDirect_MIS006_Seq_Sub_Seq_DbDirect_MIS006_Seq_Part_task >> Sub_Seq_DbDirect_MIS006_Seq_task
        
        Sub_Seq_DbDirect_MIS006_Seq_task >> job_DBdirect_MIS006_DS_Ld_Dataset_Load_Part_task
        
        job_DBdirect_MIS006_DS_Ld_Dataset_Load_Part_task >> job_DBdirect_MIS006_DS_Ld_task
        
        job_DBdirect_MIS006_DS_Ld_task >> job_DBdirect_MIS006_Ld_Final_NZ_Load_Part_task
        
        job_DBdirect_MIS006_Ld_Final_NZ_Load_Part_task >> job_DBdirect_MIS006_Ld_task
        
    
    

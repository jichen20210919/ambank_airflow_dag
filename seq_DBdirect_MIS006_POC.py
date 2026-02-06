
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026-02-04 12:53:23
# @Author  : cloudera
# @File    : seq_DBdirect_MIS006_POC.py
# @Copyright: Cloudera.Inc




from __future__ import annotations
from abc import abstractmethod
from airflow.decorators import task
from airflow.decorators import task, task_group
from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import State
from datetime import datetime, timedelta
from jinja2 import Template
from pyspark.sql.functions import lit, col, input_file_name
from pyspark.sql.types import *
import json
import logging
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
    trigger = TriggerDagRunOperator(task_id="Sub_Seq_DbDirect_MIS006_Seq", wait_for_completion=True, poke_interval=30, trigger_dag_id="Sub_Seq_DbDirect_MIS006_Seq_POC", conf={})
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
    trigger = TriggerDagRunOperator(task_id="job_DBdirect_MIS006_DS_Ld", wait_for_completion=True, poke_interval=30, trigger_dag_id="job_DBdirect_MIS006_DS_Ld_POC", conf={})
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
    trigger = TriggerDagRunOperator(task_id="job_DBdirect_MIS006_Ld", wait_for_completion=True, poke_interval=30, trigger_dag_id="job_DBdirect_MIS006_Ld_POC", conf={})
    return trigger

####################################[Main]###################################
import airflow
with DAG(
    dag_id="seq_DBdirect_MIS006_POC",
    start_date=airflow.utils.dates.days_ago(1),
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
    



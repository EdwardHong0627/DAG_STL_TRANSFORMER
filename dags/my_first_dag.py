import os
import time
import json
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from joblib import load, dump
import pandas as pd
from STL_Transformer import All_Positive_Transformer, STL_Transformer, Periodicity_Transformer
import smtplib
default_args = {
    'owner': 'Edward Hong',
    'start_date': datetime(2021, 9, 16, 0, 0),
    'schedule_interval': '@daily',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


def read_csv(path, **context):
    apl_data = list()
    with open(path, 'rb') as file:
        apl_data = load(file)
    # apl_df = pd.DataFrame()
    item = apl_data[0]
    item = item.set_index('DATE_TIME')
    item.columns = map(str.lower, item.columns)
    coil_info_col = ['coil_id', 'schedule_id', 'routing_seq', 'leading_coil', 'next_coil', 'real_in',
                     'real_out',
                     'length', 'grade_no', 'prescrition', 'apl_description', 'time_dif', 'time_dif_for_meters',
                     'moving_meters',
                     'cumsum_meters', 'defect_tag']

    coil_info_data = item[coil_info_col]
    item = item.drop(columns=coil_info_col)
    item = item.drop(columns=item.filter(regex='dif_').columns)
    origin_col = item.columns
    with open('tmp.pkl', 'wb') as file:
        dump(item, file)

    return 'tmp.pkl'


def transform_all_positive(**context):
    path = context['task_instance'].xcom_pull(task_ids='read_csv')
    data = pd.DataFrame()
    with open(path, 'rb') as file:
        data = load(file)
    all_positive = All_Positive_Transformer().transform(data)
    with open('all_positive.pkl', 'wb') as file:
        dump(all_positive, file)
    return 'all_positive.pkl'


def transform_STL(**context):
    path = context['task_instance'].xcom_pull(task_ids='Transform_all_Positive')
    data = pd.DataFrame()
    with open(path, 'rb') as file:
        data = load(file)
    STL = STL_Transformer().transform(data, lmbda=0.5, if_seasonal=False)
    return '1231232'




with DAG('STL_Transform', default_args=default_args) as dag:
    latest_only = LatestOnlyOperator(task_id='latest_only')
    read_raw_data = PythonOperator(
        task_id='read_csv',
        python_callable=read_csv,
        op_args=['gcs/dags/all_defect_20072102.pkl'],
        provide_context=True
    )

    all_positive_data = PythonOperator(
        task_id='Transform_all_Positive',
        python_callable=transform_all_positive,
        provide_context=True
    )
    STL_data = PythonOperator(
        task_id='Transform_STL',
        python_callable=transform_STL,
        provide_context=True
    )
    latest_only >> read_raw_data
    read_raw_data >> all_positive_data >> STL_data 

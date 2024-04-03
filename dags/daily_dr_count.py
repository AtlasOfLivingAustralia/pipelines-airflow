from io import StringIO

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from ala import ala_config, ala_helper
import pandas as pd
import requests
import json
import logging as log
from botocore.exceptions import ClientError
from datetime import date, timedelta
import boto3

DAG_ID = 'daily_dr_count'

with DAG(
        dag_id=DAG_ID,
        catchup=False,
        default_args=ala_helper.get_default_args(),
        description="Run the daily dr count",
        dagrun_timeout=timedelta(hours=2),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=['daily-dr']
) as dag:

    def get_daily_dr_count():
        log.info("daily_dr_count starts here")
        todays_date = date.today()
        yesterdays_date = todays_date - timedelta(days=1)
        bucket_name = ala_config.S3_BUCKET
        url = "https://biocache-ws.ala.org.au/ws/occurrence/facets?q=*%3A*&facets=data_resource_uid&count=true&lookup=true&flimit=10000"
        yesterday_file = f"stats/dr_count_{yesterdays_date}.csv"
        today_file = f"stats/dr_count_{todays_date}.csv"
        diff_file = f"stats/dr_diff_{todays_date}.csv"

        def load_data(url):
            result = requests.get(url)
            records = json.loads(result.text)
            df = pd.json_normalize(records, record_path=['fieldResult'])
            log.debug(f'Number of records fetched: {len(df)}')
            df = df.rename(columns=
                           {'label': 'name',
                            'i18nCode': 'data_resource_uid',
                            'count': 'count'
                            })
            df = df.drop(columns=['fq'])
            df['data_resource_uid'] = df['data_resource_uid'].str.replace("data_resource_uid.", "", regex=False)

            tmp_file_name = "/tmp/daily_dr_count_temp.csv"
            df.to_csv(tmp_file_name, sep=',')
            s3 = boto3.resource('s3')
            s3.Bucket(ala_config.S3_BUCKET).upload_file(tmp_file_name, today_file)

            return df


        def count_diff(dfnew):

            try:
                client = boto3.client('s3')
                csv_obj = client.get_object(Bucket=ala_config.S3_BUCKET, Key=yesterday_file)
                body = csv_obj['Body']
                csv_string = body.read().decode('utf-8')
                dfold = pd.read_csv(StringIO(csv_string))

                dfold = dfold.rename(columns={'count': 'prev_week_count'})
                dfold = dfold.drop(columns=['name'])
                df = dfnew.merge(dfold, on='data_resource_uid', how='outer')
                df['prev_week_count'] = df['prev_week_count'].fillna(0)
                df['count'] = df['count'].fillna(0)
                df['count_diff'] = df['count'] - df['prev_week_count']
                df = df[df['count_diff'] != 0]
                if df.empty is True:
                    s = pd.Series([None, None, None, None, None])
                    df = pd.concat([df,s], ignore_index=True)

                tmp_file_name = "/tmp/daily_dr_count_diff_temp.csv"
                df.to_csv(tmp_file_name, sep=',')
                s3 = boto3.resource('s3')
                s3.Bucket(ala_config.S3_BUCKET).upload_file(tmp_file_name, diff_file)
            except ClientError as ex:
                if ex.response['Error']['Code'] == 'NoSuchKey':
                    print('Yesterdays counts not available')

        df = load_data(url)
        count_diff(df)

    get_daily_dr_count = PythonOperator(
        task_id='get_daily_dr_count',
        provide_context=True,
        op_kwargs={},
        python_callable=get_daily_dr_count)

    get_daily_dr_count
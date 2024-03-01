import uuid

import json
import boto3

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

from datetime import timedelta, timezone, datetime

from ala import ala_config, cluster_setup, ala_helper
from ala.ala_helper import step_bash_cmd, get_default_args
from ala.doi_service import S3Service
from ala.doi_service import CollectoryService
from requests_oauth2client import OAuth2Client, BearerAuth
import requests

DAG_ID = 'Export_event_core'


def get_spark_steps(dataset_list,  request_id, search_query):
    """
    :param dataset_list:
    :param request_id:
    :param search_query:  the search url of the Events website
    :param query_time: when the search happened
    :return:
    """
    return [
        step_bash_cmd("a. Download AVRO from S3", f" /tmp/download-avro-hdfs-for-downloads.sh {ala_config.S3_BUCKET_AVRO} {dataset_list}"),
        step_bash_cmd("b. Download Query JSON from S3", f" aws s3 cp s3://{ala_config.S3_BUCKET_AVRO}/exports/{request_id}/query.json /tmp/query.json"),
        step_bash_cmd("c. Run Export", f" la-pipelines predicate-export {dataset_list} --cluster --jobId={request_id} --queryFile=/tmp/query.json"),
        step_bash_cmd("d. Add Frictionless Data package.json",  f" /tmp/frictionless.sh {dataset_list}"),
        step_bash_cmd("e. Create DOI, add extra metadata to the generate ZIP, and upload to S3",  f" sudo -u hadoop python3 /tmp/doi_service.py --request_id={request_id} --apiKey={ala_config.DOI_API_KEY} --dataset_list=\"{dataset_list}\" --doi_server={ala_config.DOI_URL} --collectory_server={ala_config.REGISTRY_URL} --s3_bucket={ala_config.S3_BUCKET_AVRO} --search_query=\"{search_query}\"")
    ]


with DAG(
        dag_id=DAG_ID,
        default_args=get_default_args(),
        description="Debug Export Event Core DwCA",
        dagrun_timeout=timedelta(hours=2),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=['emr', 'multiple-dataset'],
        params={
            "datasetId": "dr18391",
            "creator": "Dave Martin",
            "notificationAddresses": ["david.martin@csiro.au"],
            "predicate": {
                "type": "and",
                "predicates": [
                    {
                        "type": "in",
                        "key": "stateProvince",
                        "values": [
                            "Victoria"
                        ]
                    }
                ]
            }
        }
) as dag:

    def write_request_info(dataset_list, predicate, request_id, start_time, finish_time, status):
        content = json.dumps({
            "datasetId": dataset_list,
            "predicate": predicate,
            "status": status,
            "startTime": start_time,
            "finishTime": finish_time
        }, indent=2, sort_keys=False)

        # Upload the file
        s3 = boto3.resource('s3')
        s3.Object(ala_config.S3_BUCKET_AVRO, f'exports/{request_id}/status.json').put(Body=content)
        return True


    def mark_as_complete(**kwargs):
        dataset_list = kwargs['dag_run'].conf['datasetId']
        predicate = kwargs['dag_run'].conf['predicate']
        request_id = kwargs['ti'].xcom_pull(key='requestID')
        start_time = kwargs['ti'].xcom_pull(key='startTime')
        finish_time = datetime.now(timezone.utc).isoformat()
        write_request_info(dataset_list, predicate, request_id, start_time, finish_time, "COMPLETE")


    def construct_steps_with_options(**kwargs):
        # validate
        dataset_list = kwargs['dag_run'].conf['datasetId']
        predicate = kwargs['dag_run'].conf['predicate']
        creator = kwargs['dag_run'].conf['creator']
        notification_addresses = kwargs['dag_run'].conf['notificationAddresses']
        search_query = kwargs['dag_run'].conf.get('eventQueryUrl')
        query_time = kwargs['dag_run'].conf.get('eventQueryOn')

        print(
            f"Download request with datasetId: {dataset_list}, {creator}, {json.dumps(notification_addresses)}, {json.dumps(predicate)}")

        request_id = ""
        if 'requestID' in kwargs['dag_run'].conf:
            request_id = kwargs['dag_run'].conf['requestID']
        else:
            request_id = str(uuid.uuid4())

        start_time = datetime.now(timezone.utc).isoformat()

        kwargs['ti'].xcom_push(key='requestID', value=request_id)
        kwargs['ti'].xcom_push(key='startTime', value=start_time)

        write_request_info(dataset_list, predicate, request_id, start_time, "", "STARTED")

        # write the query to S3
        s3 = boto3.resource('s3')
        s3.Object(ala_config.S3_BUCKET_AVRO, f'exports/{request_id}/query.json').put(Body=json.dumps(predicate, indent=4))

        spark_steps = []
        spark_steps.extend(get_spark_steps(dataset_list, request_id, search_query))
        return spark_steps


    # Fetch doi details for email generation
    def read_user_details(**kwargs):
        """
        login name or email address
        :param kwargs:
        :return:
        """
        creator = kwargs['dag_run'].conf['creator']

        oauth2client = OAuth2Client(
            token_endpoint=ala_config.AUTH_TOKEN_URL,
            auth=(ala_config.AUTH_CLIENT_ID, ala_config.AUTH_CLIENT_SECRET),
        )

        # We may not need to use the real resource
        token = oauth2client.client_credentials(scope="users/read",
                                                resource=ala_config.USER_DETAILS_ENDPOINT)
        resp = requests.post(
            ala_config.USER_DETAILS_ENDPOINT + "?userName=" + creator + "&includeProps=true", auth=BearerAuth(token))
        user_info = {"id": creator, 'firstName': creator, 'lastName':creator, 'email':creator}
        if resp.status_code == 200:
            resp_json = json.loads(resp.text)
            user_info = {"id" : resp_json['userId'], 'firstName': resp_json['firstName'], 'lastName':resp_json['lastName'], 'email':resp_json['email']}
            print("Hi {}, we will send email to {}".format(user_info.get('id'), user_info.get('email')))
        else:
            print("Warning: cannot get user information of user:" + creator);

        kwargs['ti'].xcom_push(key='userId', value=user_info.get('id'))
        kwargs['ti'].xcom_push(key='firstName', value=user_info.get('firstName'))
        kwargs['ti'].xcom_push(key='lastName', value=user_info.get('lastName'))
        kwargs['ti'].xcom_push(key='email', value=user_info.get('email'))


    def read_doi_info(**kwargs):
        """
        Fetch doi details for email generation.
        """
        dataset_list_str = kwargs['dag_run'].conf['datasetId']
        request_id = kwargs['ti'].xcom_pull(key='requestID')
        s3_service = S3Service.init_on_aws()
        dataset_list = dataset_list_str.split()

        for dataset_id in dataset_list:
            doi_details = s3_service.read_doi(ala_config.S3_BUCKET_AVRO, dataset_id, request_id)
            file_size_mb = s3_service.get_size(ala_config.S3_BUCKET_AVRO, dataset_id, request_id)
            print('URL of the DOI record: ' + doi_details.get('url'))
            kwargs['ti'].xcom_push(key=f'doi_url_{dataset_id}', value=doi_details.get('url'))
            kwargs['ti'].xcom_push(key=f'doi_{dataset_id}', value=doi_details.get('doi'))
            kwargs['ti'].xcom_push(key=f'file_size_mb_{dataset_id}', value=file_size_mb)


    def generate_email(**kwargs):

        dataset_list_str = kwargs['dag_run'].conf['datasetId']
        notification_addresses = kwargs['dag_run'].conf['notificationAddresses']
        kwargs['ti'].xcom_push(key='email_address', value=notification_addresses[0])

        dataset_list = dataset_list_str.split()
        dataset_list_html = ""

        for dataset_id in dataset_list:
            dataset_info = CollectoryService.get_resource_info(ala_config.REGISTRY_URL, dataset_id)
            data_resource_name = "Untitled dataset"
            if dataset_info['status']:
                data_resource_name = dataset_info['name']

            doi = kwargs['ti'].xcom_pull(key=f'doi_{dataset_id}')
            doi_url = kwargs['ti'].xcom_pull(key=f'doi_url_{dataset_id}')
            file_size_mb = kwargs['ti'].xcom_pull(key=f'file_size_mb_{dataset_id}')

            # construct download size string
            file_size_str = f"{file_size_mb}MB"
            if float(file_size_mb) > 1024:
                file_size_str = f"{round(float(file_size_mb)/1024, 2)}GB"

            dataset_list_html += f"""<li><span><a href="{doi_url}">{data_resource_name}</a></span>
                     <ul>
                        <li>Compressed size: {file_size_str} </li>
                        <li>Format: Darwin core archive / Frictionless data package </li>
                        <li>Digital Object Identifier (DOI): {doi}</li>
                        <li>Metadata: {doi_url}</li> 
                     </ul>       
                </li>"""

        html = f"""<html>
            <head>
            <title>Your downloads are ready</title>
            <style>
                body {{ font-family: 'Roboto'; }}
            </style>
            </head>
            <body>
                <a href="https://www.ala.org.au" title="visit the Atlas website">
                   <img src="https://www.ala.org.au/app/uploads/2020/06/ALA_Logo_Inline_RGB-300x63.png" alt="ALA logo" >
                </a>
                <div class="email-body">
                    <br/><br/>  
                    Your download is available at the following address:
                    <br/>
                    <ul>  
                        {dataset_list_html}
                    </ul>   
                    <br/>
                    <br/>
                    Atlas of Living Australia
                </div>
            </body>
        </html>"""
        kwargs['ti'].xcom_push(key='html_email', value=html)


    def generate_error_email(**kwargs):

        dataset_list_str = kwargs['dag_run'].conf['datasetId']
        notification_addresses = kwargs['dag_run'].conf['notificationAddresses']
        kwargs['ti'].xcom_push(key='email_address', value=notification_addresses[0])

        dataset_list = dataset_list_str.split()
        dataset_list_html = ""
        request_id = kwargs['ti'].xcom_pull(key='requestID')

        for dataset_id in dataset_list:
            dataset_info = CollectoryService.get_resource_info(ala_config.REGISTRY_URL, dataset_id)
            data_resource_name = "Untitled dataset"
            if dataset_info['status']:
                data_resource_name = dataset_info['name']

            dataset_list_html += f"""<li><span>{data_resource_name}</span></li>"""

        html = f"""<html>
            <head>
            <title>There was a problem generating your download</title>
            <style>
                body {{ font-family: 'Roboto'; }}
            </style>
            </head>
            <body>
                <a href="https://www.ala.org.au" title="visit the Atlas website">
                   <img src="https://www.ala.org.au/app/uploads/2020/06/ALA_Logo_Inline_RGB-300x63.png" alt="ALA logo" >
                </a>
                <div class="email-body">
                    <br/><br/>  
                    There was a problem generating your download of the following datasets:
                    <ul>{dataset_list_html}</ul>
                    <br/>
                    Please forward this email to support@ala.org.au if this problem continues.
                    <br/>
                    The request ID for this download was {request_id}.
                    <br/>
                    <br/>
                    Atlas of Living Australia
                </div>
            </body>
        </html>"""
        kwargs['ti'].xcom_push(key='html_error_email', value=html)

    collect_user_info = PythonOperator(
        dag=dag,
        task_id='collect_user_info',
        provide_context=True,
        op_kwargs={},
        python_callable=read_user_details
    )

    construct_steps = PythonOperator(
        dag=dag,
        task_id='construct_steps',
        provide_context=True,
        op_kwargs={},
        python_callable=construct_steps_with_options
    )

    read_doi = PythonOperator(
        dag=dag,
        task_id='read_doi',
        provide_context=True,
        op_kwargs={},
        python_callable=read_doi_info
    )

    # construct_steps
    construct_email = PythonOperator(
        dag=dag,
        task_id='construct_email',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        provide_context=True,
        op_kwargs={},
        python_callable=generate_email
    )

    construct_error_email = PythonOperator(
        dag=dag,
        task_id='construct_error_email',
        trigger_rule=TriggerRule.ONE_FAILED,
        provide_context=True,
        op_kwargs={},
        python_callable=generate_error_email
    )

    completion_email = EmailOperator(
        task_id='completion_email',
        to="{{ task_instance.xcom_pull(task_ids='construct_email', key='email_address') }}",
        subject='Your ALA download is ready',
        html_content="{{ task_instance.xcom_pull(task_ids='construct_email', key='html_email') }} ",
        dag=dag
    )

    error_email = EmailOperator(
        task_id='error_email',
        to="{{ task_instance.xcom_pull(task_ids='construct_error_email', key='email_address') }}",
        subject='There was a problem generating your download',
        html_content="{{ task_instance.xcom_pull(task_ids='construct_error_email', key='html_error_email') }} ",
        dag=dag
    )

    cluster_creator = EmrCreateJobFlowOperator(
        dag=dag,
        task_id='create_emr_cluster',
        emr_conn_id='emr_default',
        job_flow_overrides=cluster_setup.get_large_cluster(DAG_ID, "bootstrap-export-event-core-actions.sh"),
        aws_conn_id='aws_default'
    )

    step_adder = EmrAddStepsOperator(
        dag=dag,
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps="{{ task_instance.xcom_pull(task_ids='construct_steps', key='return_value') }}"
    )

    step_checker = EmrStepSensor(
        dag=dag,
        task_id='watch_step',
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )

    wait_for_termination = EmrJobFlowSensor(
        dag=dag,
        task_id='wait_for_cluster_termination',
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default'
    )

    mark_as_complete = PythonOperator(
        dag=dag,
        task_id='mark_as_complete',
        provide_context=True,
        op_kwargs={},
        python_callable=mark_as_complete
    )

    collect_user_info >> construct_steps >> cluster_creator >> step_adder >> step_checker >> wait_for_termination >> mark_as_complete >> read_doi >> [construct_email, construct_error_email]
    construct_email >> completion_email
    construct_error_email >> error_email

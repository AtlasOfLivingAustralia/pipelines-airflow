from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from ala import ala_config
from ala.ala_helper import get_default_args
from distutils.util import strtobool
import logging

DAG_ID = 'Delete_dataset_dag'

with DAG(
        dag_id=DAG_ID,
        default_args=get_default_args(),
        description="Delete datasets from pipelines",
        dagrun_timeout=timedelta(hours=8),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=['emr', 'multiple-dataset'],
        params={"datasetIds": "dr1 dr2",
                "remove_records_in_solr": "false",
                "remove_records_in_es": "false",
                "delete_avro_files": "false",
                "retain_dwca": "true",
                "retain_uuid": "true"}
) as dag:

    def delete_dataset_files(**kwargs):
        """
        Delete dataset files in s3 by aws cli command
        :param kwargs:
        :return: None
        """

        def get_s3_del_cmd(path, extra_options=''):
            del_cmd = f"aws s3 rm s3://{path}"
            return f"{del_cmd} --recursive  --include '*.*' {extra_options}" if path.endswith("/") else del_cmd

        def get_dr_cmds(dr, delete_avro_files, retain_dwca, retain_uuid):
            cmds = []
            if (not retain_dwca):
                cmds.append(get_s3_del_cmd(path=f"{ala_config.S3_BUCKET_DWCA}/dwca-imports/{dr}/"))

            if (delete_avro_files):
                cmds.append(get_s3_del_cmd(path=f"{ala_config.S3_BUCKET_AVRO}/pipelines-all-datasets/index-record/{dr}/"))
                cmds.append(get_s3_del_cmd(path=f"{ala_config.S3_BUCKET_AVRO}/dwca-exports/{dr}.zip"))
                exclude_uuid = ''
                if (retain_uuid):
                    exclude_uuid = "--exclude '1/identifiers/*.*' --exclude 'identifiers-backup/*.*'"

                cmds.append(get_s3_del_cmd(f"{ala_config.S3_BUCKET_AVRO}/pipelines-data/{dr}/",
                                            extra_options=exclude_uuid))

            return cmds

        datasets_param = kwargs['dag_run'].conf['datasetIds']
        delete_avro_files = strtobool(kwargs['dag_run'].conf['delete_avro_files'])
        retain_dwca = strtobool(kwargs['dag_run'].conf['retain_dwca'])
        retain_uuid = strtobool(kwargs['dag_run'].conf['retain_uuid'])

        dataset_list = datasets_param.split()
        for dr in dataset_list:
            cmds = get_dr_cmds(dr=dr, delete_avro_files=delete_avro_files,
                                    retain_dwca=retain_dwca, retain_uuid=retain_uuid)
            for count, cmd in enumerate(cmds):
                bash_operator = BashOperator(
                    task_id=f"delete_{dr}_task{str(count)}",
                    bash_command=cmd)

                bash_operator.execute(context=kwargs)

    delete_dataset_in_s3 = PythonOperator(
        task_id='delete_dataset',
        provide_context=True,
        op_kwargs={},
        python_callable=delete_dataset_files)

    def remove_from_solr(**kwargs):
        """
        Delete dr records from the solr collection alias.
        Although the records are removed from solr collection, querying dataResourceUid facet query will still show the dr with 0 count.
        :param kwargs:
        :return: None
        """

        remove_records_in_solr = strtobool(kwargs['dag_run'].conf['remove_records_in_solr'])
        if not remove_records_in_solr:
            logging.info ("Records not removed from solr")
            return

        def get_cmd(dr):
            solr_ws = f"{ala_config.SOLR_URL}/{ala_config.SOLR_COLLECTION}/update?commit=true"
            return f"curl {solr_ws} -H 'Content-Type: text/xml'  --data-binary '<delete><query>dataResourceUid:{dr}</query></delete>'"

        datasets_param = kwargs['dag_run'].conf['datasetIds']
        dataset_list = datasets_param.split()

        for count, dr in enumerate(dataset_list):
            bash_operator = BashOperator(
                task_id=f"delete_solr_{dr}_task{str(count)}",
                bash_command=get_cmd(dr=dr))

            bash_operator.execute(context=kwargs)

    delete_dataset_in_solr = PythonOperator(
        task_id='delete_dataset_in_solr',
        provide_context=True,
        op_kwargs={},
        python_callable=remove_from_solr)

    def remove_from_es(**kwargs):
        """
        Delete dataset document record from the events elasticsearch.
        :param kwargs:
        :return: None
        """

        remove_document_in_es = strtobool(kwargs['dag_run'].conf['remove_records_in_es'])
        if not remove_document_in_es:
            logging.info ("Skipped removing from es")
            return

        def get_cmd(dr):
            es_hosts = ala_config.ES_HOSTS.split(',')
            es_host = es_hosts[0] if len(es_hosts) > 0 else "" #for eg: http://aws-events-es-2022-1.ala:9200
            es_ws = f"{es_host}/{ala_config.ES_ALIAS}_{dr}"
            return f"curl -X DELETE {es_ws}"

        datasets_param = kwargs['dag_run'].conf['datasetIds']
        dataset_list = datasets_param.split()

        for count, dr in enumerate(dataset_list):
            bash_operator = BashOperator(
                task_id=f"delete_es_{dr}_task{str(count)}",
                bash_command=get_cmd(dr=dr))

            bash_operator.execute(context=kwargs)

    delete_dataset_in_es = PythonOperator(
        task_id='delete_dataset_in_es',
        provide_context=True,
        op_kwargs={},
        python_callable=remove_from_es)

    delete_dataset_in_s3 >> delete_dataset_in_solr >> delete_dataset_in_es
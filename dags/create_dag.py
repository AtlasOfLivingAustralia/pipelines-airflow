from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from ala import ala_config, ala_helper
from dataclasses import dataclass

load_images = ala_config.LOAD_IMAGES
s3_job_config_path = ala_config.JOB_SCHEDULE_CONFIG


@dataclass
class CONFIG_KEY:
    REGISTRY_UID = 'datasetId'
    TAG_LABEL = 'tags'
    JOB_NAME = 'job_name'
    DESCRIPTION = 'description'
    SCHEDULE = 'schedule'
    TRIGGER_DAG_ID = 'trigger_dag_id'
    DAG_PARAMS = 'params'
    PREINGEST_TRIGGER_DAG_ID = 'Preingest_datasets'
    PREINGEST_DATASET_ID = 'datasetId'
    PREINGEST_EXTRA_ARGS = 'extra_args'
    PREINGEST_INSTANCE_TYPE = 'instanceType'
    PREINGEST_LOAD_IMAGES = 'load_images'


class CreateDagOperator:

    @staticmethod
    def get_config_dictionary(s3_path: str):
        import json
        import boto3

        (bucket_name, path) = s3_path.replace("s3://", "").split("/", 1)
        s3_resource = boto3.resource('s3')
        config_job = s3_resource.Object(bucket_name, path)
        return json.load(config_job.get()['Body'])

    @staticmethod
    def create_dag(dag_id: str, trigger_dag_id: str, description: str, schedule: str, tags: [], params: {}):

        """
        Creates a scheduled wrapper dag. Schedule dag calls either preingest data resource or preingest provider dag.
        There is current bug in airflow 2.2.2 whereby upon deploying a new scheduled dag, the dag gets triggered twice.
        https://github.com/apache/airflow/issues/25942, regardsless of whether we set catchup=False
        Workaround, set max-active-runs=1 to makes sure they don't run at the same time.

        :param dag_id: dag id to setup
        :param trigger_dag_id: dag id to trigger
        :param description: description of dag
        :param schedule: cron schedule
        :param tags: dag tags
        :param params: params to pass into trigger dag
        :return: dag
        """

        def get_start_date(cron_schedule_exp: str, catchup: bool):
            """
            param: schedule cron expression
            """
            from croniter import croniter
            from datetime import datetime
            import pendulum

            if catchup:
                base = pendulum.now(tz="Australia/Sydney")
                iter = croniter(cron_schedule_exp, base)
                iter.get_prev(datetime)
                return iter.get_prev(datetime)
            else:
                return pendulum.datetime(2022, 1, 1, tz="Australia/Sydney")

        catchup = False
        start_date = get_start_date(schedule, catchup)

        dag = DAG(dag_id=dag_id, description=description, start_date=start_date, schedule_interval=schedule,
                  default_args=ala_helper.get_default_args(), catchup=catchup, tags=tags, is_paused_upon_creation=True,
                  max_active_runs=1)

        with dag:
            triggered_task = TriggerDagRunOperator(
                dag=dag,
                task_id=trigger_dag_id,
                trigger_dag_id=trigger_dag_id,
                wait_for_completion=True,
                trigger_rule=TriggerRule.NONE_SKIPPED,
                conf=params
            )

            triggered_task

        return dag


job_list = []

if s3_job_config_path:
    job_list = CreateDagOperator.get_config_dictionary(s3_job_config_path)

for job in job_list:

    tags = job[CONFIG_KEY.TAG_LABEL].split()
    dag_id = job[CONFIG_KEY.JOB_NAME]
    description = job[CONFIG_KEY.DESCRIPTION]
    schedule = job[CONFIG_KEY.SCHEDULE]

    # IF no specific trigger id given, this is considered preingest data load job
    if CONFIG_KEY.TRIGGER_DAG_ID in job:
        trigger_dag_id = job[CONFIG_KEY.TRIGGER_DAG_ID]
        params = job[CONFIG_KEY.DAG_PARAMS]
        # Handle tuple for (None, None)
        params = {key: (None, None) if value == "(None, None)" else value for key, value in params.items()}
    else:
        trigger_dag_id = CONFIG_KEY.PREINGEST_TRIGGER_DAG_ID
        registry_entity_uid = job[CONFIG_KEY.PREINGEST_DATASET_ID]
        instanceType = job[CONFIG_KEY.PREINGEST_INSTANCE_TYPE] if 'instanceType' in job else 'm6g.xlarge'
        if not any(registry_entity_uid in ele for ele in tags):
            tags.insert(0, registry_entity_uid)
        tags.append(instanceType)
        extra_args = job[CONFIG_KEY.PREINGEST_EXTRA_ARGS] if CONFIG_KEY.PREINGEST_EXTRA_ARGS in job else {}
        load_images = ala_config.LOAD_IMAGES if CONFIG_KEY.PREINGEST_LOAD_IMAGES not in job else job[
            CONFIG_KEY.PREINGEST_LOAD_IMAGES]
        params = {"datasetIds": registry_entity_uid, CONFIG_KEY.PREINGEST_LOAD_IMAGES: load_images,
                  "instanceType": instanceType, "extra_args": extra_args, "override_uuid_percentage_check": "false"}

    globals()[dag_id] = CreateDagOperator.create_dag(dag_id=dag_id,
                                                     trigger_dag_id=trigger_dag_id,
                                                     description=description,
                                                     schedule=schedule,
                                                     tags=tags,
                                                     params=params)

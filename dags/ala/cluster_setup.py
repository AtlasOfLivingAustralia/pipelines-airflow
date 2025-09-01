import logging as log
import re
from dataclasses import dataclass, field
from enum import Enum

from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from ala import ala_config
from ala.ala_helper import (
    get_dr_count,
    get_success_notification_operator,
    step_bash_cmd,
)


class ClusterType(Enum):
    PREINGESTION = "Preingestion"
    PIPELINES_SMALL = "Pipelines local"
    PIPELINES_LARGE = "Pipelines cluster"


def run_large_emr(
    dag,
    spark_steps,
    bootstrap_script,
    ebs_size_in_gb=ala_config.EC2_LARGE_EBS_SIZE_IN_GB,
    cluster_size=ala_config.EC2_LARGE_INSTANCE_COUNT,
    dataset_ids='ALL',
):
    """
    Creates and manages an AWS EMR cluster workflow within an Airflow DAG.
    This function sets up a sequence of Airflow tasks to:
    1. Create a large EMR cluster.
    2. Add Spark steps to the cluster.
    3. Monitor the execution of the Spark steps.
    4. Wait for the EMR cluster to terminate.
    5. Send a success notification upon completion.
    Args:
        dag (airflow.models.DAG): The Airflow DAG to which the tasks will be added.
        spark_steps (list): A list of Spark step definitions to be executed on the EMR cluster.
        bootstrap_script (str): The path or URI to the bootstrap script to initialize the cluster.
        ebs_size_in_gb (int, optional): The size of the EBS volume in GB for each instance. Defaults to ala_config.EC2_LARGE_EBS_SIZE_IN_GB.
        cluster_size (int, optional): The number of EC2 instances in the cluster. Defaults to ala_config.EC2_LARGE_INSTANCE_COUNT.
    Returns:
        None: The function defines task dependencies within the DAG but does not return a value.
    """

    cluster_creator = PythonOperator(
        task_id="create_emr_cluster",
        python_callable=setup_cluster,
        op_kwargs={
            'dag_id': dag.dag_id,
            "dataset_ids": dataset_ids,
            "inst_type": "None",
            "cluster_type": ClusterType.PIPELINES_LARGE,
            "bootstrap_script": bootstrap_script,
        },
        provide_context=True,
    )

    step_adder = EmrAddStepsOperator(
        dag=dag,
        task_id="add_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=spark_steps,
    )

    step_checker = EmrStepSensor(
        dag=dag,
        task_id="watch_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id="aws_default",
    )

    wait_for_termination = EmrJobFlowSensor(
        dag=dag,
        task_id="wait_for_cluster_termination",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
    )

    # Set task dependencies for Airflow DAG
    cluster_creator >> step_adder >> step_checker >> wait_for_termination >> get_success_notification_operator()


def obj_as_dict(obj):
    """Return a dictionary with the attributes of obj that are in the
    __add_to_dict__ list.

    :param obj: object to be converted to a dictionary
    :type obj: object
    :return: dictionary with the attributes of obj that are in the
        __add_to_dict__ list

    """
    return {attr: getattr(obj, attr) for attr in getattr(obj, "__add_to_dict__", [])}


def sanitize_tag(input_string):
    """
    Sanitizes the input string by removing any characters that are not alphanumeric,
    whitespace, period, underscore, colon, forward slash, equals sign, plus, or minus.

    Args:
        input_string (str): The string to be sanitized.

    Returns:
        str: The sanitized string containing only allowed characters.
    """
    allowed_characters = re.compile(r"[^a-zA-Z0-9\s\._:/=+\-]")
    sanitized_string = re.sub(allowed_characters, "", input_string)
    return sanitized_string


def get_environment():
    """
    Determines the current environment type based on the value of `ala_config.ENVIRONMENT_TYPE`.

    Returns:
        str: A string representing the environment type. Possible values are:
            - "production" if the environment type contains "prod"
            - "development" if the environment type contains "dev"
            - "testing" if the environment type contains "test"
    """
    if "prod" in ala_config.ENVIRONMENT_TYPE.lower():
        return "production"
    if "dev" in ala_config.ENVIRONMENT_TYPE.lower():
        return "development"
    if "test" in ala_config.ENVIRONMENT_TYPE.lower():
        return "testing"


@dataclass
class EMRConfig:
    """Base class for EMR configurations."""

    name: str
    dag_id: str
    data_resources: str

    # This is a list of attributes that will be added to the exported dictionary as the configuration for EMR cluster., ignoring attribs like `name` and `dag_id`
    __add_to_dict__ = [
        "Name",
        "ReleaseLabel",
        "Configurations",
        "BootstrapActions",
        "Applications",
        "VisibleToAllUsers",
        "JobFlowRole",
        "ServiceRole",
        "Tags",
        "LogUri",
        "Instances",
    ]
    ReleaseLabel: str
    Configurations: list
    BootstrapActions: list
    Applications: list
    VisibleToAllUsers: bool = field(default=True, init=False)
    JobFlowRole: str = field(default=ala_config.JOB_FLOW_ROLE, init=False)
    ServiceRole: str = field(default=ala_config.SERVICE_ROLE, init=False)
    Tags: list = field(
        default_factory=lambda: [
            {"Key": "for-use-with-amazon-emr-managed-policies", "Value": "true"},
            {"Key": "component", "Value": "ingestion"},
            {"Key": "environment", "Value": get_environment()},
        ],
        init=False,
    )

    @property
    def Name(self) -> str:
        return f"{self.name} [{ala_config.ENVIRONMENT_TYPE.upper()}]"

    @property
    def LogUri(self) -> str:
        return f"s3://{ala_config.S3_BUCKET}/airflow/logs/{self.dag_id}"

    @property
    def instance_groups(self):
        raise NotImplementedError("Subclasses should implement this!")

    @property
    def Instances(self):
        return {
            "InstanceGroups": self.instance_groups,
            "KeepJobFlowAliveWhenNoSteps": ala_config.KEEP_EMR_ALIVE,
            "TerminationProtected": ala_config.KEEP_EMR_ALIVE,
            "Ec2KeyName": ala_config.EC2_KEY_NAME,
            "Ec2SubnetId": ala_config.EC2_SUBNET_ID,
            "AdditionalMasterSecurityGroups": ala_config.EC2_ADDITIONAL_MASTER_SECURITY_GROUPS,
            "AdditionalSlaveSecurityGroups": ala_config.EC2_ADDITIONAL_SLAVE_SECURITY_GROUPS,
        }

    def __post_init__(self):
        self.Tags += [
            {"Key": "Name", "Value": sanitize_tag(self.Name)},
            {"Key": "data-resources", "Value": sanitize_tag(self.data_resources)},
        ]


@dataclass
class PreIngestionEMRConfig(EMRConfig):
    """Configuration for EMR cluster for pre-ingestion."""

    instance_type: str
    ebs_size_in_gb: int

    ReleaseLabel: str = field(default=ala_config.EMR_RELEASE_PREINGESTION, init=False)

    @property
    def instance_groups(self):
        return [
            {
                "Name": "Master nodes",
                "Market": ala_config.MASTER_MARKET,
                "InstanceRole": "MASTER",
                "InstanceType": self.instance_type,
                "CustomAmiId": ala_config.PREINGESTION_AMI,
                "InstanceCount": 1,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {"VolumeSpecification": {"SizeInGB": self.ebs_size_in_gb, "VolumeType": "standard"}}
                    ]
                },
            }
        ]

    Applications: list = field(default_factory=lambda: [], init=False)
    Configurations: list = field(default_factory=lambda: [], init=False)
    BootstrapActions: list = field(
        default_factory=lambda: [
            {
                "Name": "Bootstrap action",
                "ScriptBootstrapAction": {
                    "Args": [f"{ala_config.S3_BUCKET}"],
                    "Path": f"s3://{ala_config.S3_BUCKET}/airflow/dags/bootstrap-preingestion-actions.sh",
                },
            }
        ],
        init=False,
    )

    def __post_init__(self):
        super().__post_init__()
        self.Tags += [{"Key": "product", "Value": "preingestion"}]
        self.Tags += [{"Key": "emrcluster", "Value": "single node"}]
        self.Tags += [{"Key": "operation", "Value": "preingestion"}]


@dataclass
class PipelinesEMRConfig(EMRConfig):
    """Base configuration for EMR cluster for pipelines."""

    ReleaseLabel: str = field(default=ala_config.EMR_RELEASE, init=False)
    Applications: list = field(default_factory=lambda: [{"Name": "Spark"}, {"Name": "Hadoop"}], init=False)
    Configurations: list = field(
        default_factory=lambda: [{"Classification": "spark", "Properties": ala_config.SPARK_PROPERTIES}], init=False
    )

    def __post_init__(self):
        super().__post_init__()
        self.Tags += [{"Key": "product", "Value": "pipeline"}]
        self.Tags += [{"Key": "operation", "Value": self.dag_id}]


@dataclass
class PipelinesSingleEMRConfig(PipelinesEMRConfig):
    """Configuration for EMR cluster for pipelines with a single node."""

    instance_type: str
    ebs_size_in_gb: int

    @property
    def instance_groups(self):
        return [
            {
                "Name": "Master nodes",
                "Market": ala_config.MASTER_MARKET,
                "InstanceRole": "MASTER",
                "InstanceType": self.instance_type,
                "InstanceCount": 1,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {"VolumeSpecification": {"SizeInGB": self.ebs_size_in_gb, "VolumeType": "standard"}}
                    ]
                },
            }
        ]

    def __post_init__(self):
        super().__post_init__()
        self.Tags += [{"Key": "emrcluster", "Value": "single node"}]


@dataclass
class PipelinesMultiEMRConfig(PipelinesEMRConfig):
    """Configuration for EMR cluster for pipelines with multiple nodes."""

    slave_instance_count: int
    instance_type: str
    ebs_size_in_gb: int

    @property
    def instance_groups(self):
        return [
            {
                "Name": "Master nodes",
                "Market": ala_config.MASTER_MARKET,
                "InstanceRole": "MASTER",
                "InstanceType": ala_config.EC2_LARGE_INSTANCE_TYPE,
                "InstanceCount": 1,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {"VolumeSpecification": {"SizeInGB": self.ebs_size_in_gb, "VolumeType": "standard"}}
                    ]
                },
            },
            {
                "Name": "Slave nodes",
                "Market": ala_config.SLAVE_MARKET,
                "InstanceRole": "CORE",
                "InstanceType": ala_config.EC2_LARGE_INSTANCE_TYPE,
                "InstanceCount": self.slave_instance_count,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {"VolumeSpecification": {"SizeInGB": self.ebs_size_in_gb, "VolumeType": "standard"}}
                    ]
                },
            },
        ]

    def __post_init__(self):
        super().__post_init__()
        self.Tags += [{"Key": "emrcluster", "Value": f"{1+self.slave_instance_count} node"}]


def get_pre_ingestion_cluster(dag_id, instance_type, name, drs=""):
    """
    Creates and returns a dictionary representation of a PreIngestionEMRConfig object for a pre-ingestion EMR cluster.

    Args:
        dag_id (str): The DAG identifier associated with the EMR cluster.
        instance_type (str): The type of EC2 instance to use for the cluster.
        name (str): The name to assign to the EMR cluster.
        drs (str, optional): Data resource string(s) to be used by the cluster. Defaults to an empty string.

    Returns:
        dict: A dictionary representation of the PreIngestionEMRConfig object.
    """
    return obj_as_dict(
        PreIngestionEMRConfig(
            dag_id=dag_id,
            instance_type=instance_type,
            name=name,
            ebs_size_in_gb=ala_config.PREINGESTION_EBS_SIZE_IN_GB,
            data_resources=drs,
        )
    )


def get_small_cluster(dag_id, bootstrap_actions_script, ebs_size_in_gb=ala_config.EC2_SMALL_EBS_SIZE_IN_GB, drs=""):
    """
    Creates and returns a dictionary representation of a small EMR cluster configuration.

    Args:
        dag_id (str): The DAG identifier for the cluster.
        bootstrap_actions_script (str): Path or identifier for the bootstrap actions script.
        ebs_size_in_gb (int, optional): Size of the EBS volume in GB. Defaults to ala_config.EC2_SMALL_EBS_SIZE_IN_GB.
        drs (str, optional): Data resource string or identifier. Defaults to an empty string.

    Returns:
        dict: Dictionary representation of the PipelinesSingleEMRConfig object for a small cluster.
    """
    bootstrap_actions = ala_config.get_bootstrap_actions(bootstrap_actions_script)
    return obj_as_dict(
        PipelinesSingleEMRConfig(
            dag_id=dag_id,
            instance_type=ala_config.EC2_SMALL_INSTANCE_TYPE,
            name=f"{dag_id} {drs}",
            ebs_size_in_gb=ebs_size_in_gb,
            BootstrapActions=bootstrap_actions,
            data_resources=drs,
        )
    )


def get_medium_cluster(
    dag_id,
    bootstrap_actions_script,
    ebs_size_in_gb=ala_config.EC2_MEDIUM_EBS_SIZE_IN_GB,
    cluster_size=ala_config.EC2_MEDIUM_INSTANCE_COUNT,
    drs="",
):
    """
    Creates and returns a dictionary representation of a medium-sized EMR cluster configuration.

    Args:
        dag_id (str): The DAG identifier for the cluster.
        bootstrap_actions_script (str): Path or identifier for the bootstrap actions script.
        ebs_size_in_gb (int, optional): Size of the EBS volume in GB for each instance. Defaults to ala_config.EC2_MEDIUM_EBS_SIZE_IN_GB.
        cluster_size (int, optional): Number of slave instances in the cluster. Defaults to ala_config.EC2_MEDIUM_INSTANCE_COUNT.
        drs (str, optional): Data resources string or identifier. Defaults to an empty string.

    Returns:
        dict: Dictionary representation of the PipelinesMultiEMRConfig for the medium cluster.
    """
    bootstrap_actions = ala_config.get_bootstrap_actions(bootstrap_actions_script)

    return obj_as_dict(
        PipelinesMultiEMRConfig(
            dag_id=dag_id,
            instance_type=ala_config.EC2_LARGE_INSTANCE_TYPE,
            name=f"{dag_id} {drs}",
            ebs_size_in_gb=ebs_size_in_gb,
            slave_instance_count=cluster_size,
            BootstrapActions=bootstrap_actions,
            data_resources=drs,
        )
    )


def get_large_cluster(
    dag_id,
    bootstrap_actions_script,
    ebs_size_in_gb=ala_config.EC2_LARGE_EBS_SIZE_IN_GB,
    cluster_size=ala_config.EC2_LARGE_INSTANCE_COUNT,
    drs="",
):
    """
    Creates and returns a dictionary representation of a large EMR cluster configuration.

    Args:
        dag_id (str): The DAG identifier for the cluster.
        bootstrap_actions_script (str): Path or identifier for the bootstrap actions script.
        ebs_size_in_gb (int, optional): Size of the EBS volume in GB for each instance. Defaults to ala_config.EC2_LARGE_EBS_SIZE_IN_GB.
        cluster_size (int, optional): Number of slave instances in the cluster. Defaults to ala_config.EC2_LARGE_INSTANCE_COUNT.
        drs (str, optional): Data resources string or configuration. Defaults to an empty string.

    Returns:
        dict: Dictionary representation of the PipelinesMultiEMRConfig for the large cluster.
    """
    bootstrap_actions = ala_config.get_bootstrap_actions(bootstrap_actions_script)
    return obj_as_dict(
        PipelinesMultiEMRConfig(
            dag_id=dag_id,
            instance_type=ala_config.EC2_LARGE_INSTANCE_TYPE,
            name=f"{dag_id} {drs}",
            ebs_size_in_gb=ebs_size_in_gb,
            slave_instance_count=cluster_size,
            BootstrapActions=bootstrap_actions,
            data_resources=drs,
        )
    )


def setup_cluster(dag_id, dataset_ids, cluster_type: ClusterType, inst_type, **kwargs):
    """
    Sets up an EMR cluster for pre-ingestion processing based on the provided dataset IDs and instance type.

    Parameters:
        dag_id (str): The DAG identifier for the Airflow workflow.
        dataset_ids (str): A whitespace-separated string of dataset IDs to be processed.
        inst_type (str): The EC2 instance type to use for the cluster. If set to "None", the instance type is determined based on dataset record counts.
        **kwargs: Additional keyword arguments passed from Airflow context, including task instance (ti) for XCom operations.

    Behavior:
        - Logs the dataset IDs and determines a display string for logging.
        - If inst_type is "None", calculates the maximum record count among datasets and selects an appropriate EC2 instance type.
        - Configures the EMR cluster using `get_pre_ingestion_cluster`.
        - Creates the EMR cluster using `EmrCreateJobFlowOperator` and pushes the resulting job flow ID to XCom.

    Returns:
        None. The function executes the EMR cluster creation and pushes the job flow ID to XCom.
    """
    log.info("DatasetIds are: %s", dataset_ids)
    dataset_list = dataset_ids.split()
    if len(dataset_list) > 20:
        display_drs = ",".join(dataset_list[:20]) + "..."
    else:
        display_drs = ",".join(dataset_list)

    instance_type = inst_type
    if instance_type == "None":
        rec_count_list = [get_dr_count(dr) for dr in dataset_list]
        max_dr_count = max(rec_count_list)
        instance_type = ala_config.EC2_SMALL_INSTANCE_TYPE
        if max_dr_count > ala_config.DR_REC_COUNT_THRESHOLD:
            instance_type = ala_config.EC2_XLARGE_INSTANCE_TYPE
        log.info("Number of records in dr is dr_count=%s", max_dr_count)
    log.info("instanceType is set to %s", instance_type)

    if cluster_type == ClusterType.PREINGESTION:
        emr_config = get_pre_ingestion_cluster(
            dag_id, instance_type=instance_type, name=f"Preingest {display_drs}", drs=dataset_ids
        )
    elif cluster_type == ClusterType.PIPELINES_SMALL:
        emr_config = get_small_cluster(dag_id, bootstrap_actions_script=kwargs.get("bootstrap_script"), drs=dataset_ids)
    else:
        emr_config = get_large_cluster(dag_id, bootstrap_actions_script=kwargs.get("bootstrap_script"), drs=dataset_ids)

    log.info("emr_config is configured as %s", emr_config)

    result = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        emr_conn_id="emr_default",
        job_flow_overrides=emr_config,
        aws_conn_id="aws_default",
        do_xcom_push=True,
    ).execute(kwargs)
    kwargs["ti"].xcom_push(key="job_flow_id", value=result)
    return result

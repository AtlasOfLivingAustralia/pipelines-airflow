from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from ala import ala_config
from dataclasses import dataclass, field, fields, asdict


def run_large_emr(dag, spark_steps, bootstrap_script, ebs_size_in_gb=ala_config.EC2_LARGE_EBS_SIZE_IN_GB, cluster_size=ala_config.EC2_LARGE_INSTANCE_COUNT):
    cluster_creator = EmrCreateJobFlowOperator(
        dag=dag,
        task_id='create_emr_cluster',
        emr_conn_id='emr_default',
        job_flow_overrides=get_large_cluster(dag.dag_id, bootstrap_script, ebs_size_in_gb=ebs_size_in_gb, cluster_size=cluster_size),
        aws_conn_id='aws_default'
    )

    step_adder = EmrAddStepsOperator(
        dag=dag,
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=spark_steps
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

    cluster_creator >> step_adder >> step_checker >> wait_for_termination


def obj_as_dict(obj):
    """Return a dictionary with the attributes of obj that are in the
    __add_to_dict__ list.

    :param obj: object to be converted to a dictionary
    :type obj: object
    :return: dictionary with the attributes of obj that are in the
        __add_to_dict__ list
    
    """
    return {attr: getattr(obj, attr) for attr in getattr(obj, '__add_to_dict__', [])}


@dataclass
class EMRConfig:
    """Base class for EMR configurations.
    """
    name: str
    dag_id: str

    # This is a list of attributes that will be added to the exported dictionary as the configuration for EMR cluster., ignoring attribs like `name` and `dag_id`
    __add_to_dict__ = ['Name', 'ReleaseLabel', 'Configurations', 'BootstrapActions', 'Applications', 'VisibleToAllUsers', 'JobFlowRole', 'ServiceRole', 'Tags', 'LogUri', 'Instances']
    ReleaseLabel: str
    Configurations: list
    BootstrapActions: list
    Applications: list
    VisibleToAllUsers: bool = field(default=True, init=False)
    JobFlowRole: str = field(default=ala_config.JOB_FLOW_ROLE, init=False)
    ServiceRole: str = field(default=ala_config.SERVICE_ROLE, init=False)
    Tags: list = field(default_factory=lambda: [{
        'Key': 'for-use-with-amazon-emr-managed-policies',
        'Value': 'true'
    },
        {
            'Key': 'pipelines',
            'Value': 'Preingest_datasets'
        }], init=False)

    @property
    def Name(self) -> str:
        return f'{self.name} ' + ala_config.S3_BUCKET

    @property
    def LogUri(self) -> str:
        return f"s3://{ala_config.S3_BUCKET}/airflow/logs/{self.dag_id}"

    @property
    def instance_groups(self):
        raise NotImplementedError("Subclasses should implement this!")

    @property
    def Instances(self):
        return {
            'InstanceGroups': self.instance_groups,
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2KeyName': ala_config.EC2_KEY_NAME,
            'Ec2SubnetId': ala_config.EC2_SUBNET_ID,
            'AdditionalMasterSecurityGroups': ala_config.EC2_ADDITIONAL_MASTER_SECURITY_GROUPS,
            'AdditionalSlaveSecurityGroups': ala_config.EC2_ADDITIONAL_SLAVE_SECURITY_GROUPS
        }


@dataclass
class PreIngestionEMRConfig(EMRConfig):
    """Configuration for EMR cluster for pre-ingestion. """
    instance_type: str
    ebs_size_in_gb: int

    ReleaseLabel: str = field(default=ala_config.EMR_RELEASE_PREINGESTION, init=False)

    @property
    def instance_groups(self):
        return [
            {
                'Name': "Master nodes",
                'Market': ala_config.MASTER_MARKET,
                'InstanceRole': 'MASTER',
                'InstanceType': self.instance_type,
                'CustomAmiId': ala_config.PREINGESTION_AMI,
                'InstanceCount': 1,
                'EbsConfiguration': {
                    'EbsBlockDeviceConfigs': [
                        {
                            'VolumeSpecification': {
                                'SizeInGB': self.ebs_size_in_gb,
                                'VolumeType': 'standard'
                            }
                        }
                    ]
                }
            }
        ]
    Applications: list = field( default_factory = lambda: [], init=False)
    Configurations: list = field(default_factory=lambda: [], init=False)
    BootstrapActions: list = field(default_factory=lambda: [{
        "Name": "Bootstrap action",
        "ScriptBootstrapAction": {
            "Args": [f"{ala_config.S3_BUCKET}"],
            "Path": f"s3://{ala_config.S3_BUCKET}/airflow/dags/bootstrap-preingestion-actions.sh",
        }
    }], init=False)


@dataclass
class PipelinesEMRConfig(EMRConfig):
    """Base configuration for EMR cluster for pipelines. """
    ReleaseLabel: str = field(default=ala_config.EMR_RELEASE, init=False)
    Applications: list = field(default_factory=lambda: [
        {'Name': 'Spark'},
        {'Name': 'Hadoop'}], init=False)
    Configurations: list = field(default_factory=lambda: [
        {
            "Classification": "spark",
            "Properties": ala_config.SPARK_PROPERTIES}], init=False)


@dataclass
class PipelinesSingleEMRConfig(PipelinesEMRConfig):
    """Configuration for EMR cluster for pipelines with a single node. """
    instance_type: str
    ebs_size_in_gb: int

    @property
    def instance_groups(self):
        return [
            {
                'Name': "Master nodes",
                'Market': ala_config.MASTER_MARKET,
                'InstanceRole': 'MASTER',
                'InstanceType': self.instance_type,
                'CustomAmiId': ala_config.PREINGESTION_AMI,
                'InstanceCount': 1,
                'EbsConfiguration': {
                    'EbsBlockDeviceConfigs': [
                        {
                            'VolumeSpecification': {
                                'SizeInGB': self.ebs_size_in_gb,
                                'VolumeType': 'standard'
                            }
                        }
                    ]
                }
            }
        ]


@dataclass
class PipelinesMultiEMRConfig(PipelinesEMRConfig):
    """Configuration for EMR cluster for pipelines with multiple nodes. """
    slave_instance_count: int
    instance_type: str
    ebs_size_in_gb: int

    @property
    def instance_groups(self):
        return [
            {
                'Name': "Master nodes",
                'Market': ala_config.MASTER_MARKET,
                'InstanceRole': 'MASTER',
                'InstanceType': ala_config.EC2_LARGE_INSTANCE_TYPE,
                'InstanceCount': 1,
                'EbsConfiguration': {
                    'EbsBlockDeviceConfigs': [
                        {
                            'VolumeSpecification': {
                                'SizeInGB': self.ebs_size_in_gb,
                                'VolumeType': 'standard'
                            }
                        }
                    ]
                }
            },
            {
                'Name': "Slave nodes",
                'Market': ala_config.SLAVE_MARKET,
                'InstanceRole': 'CORE',
                'InstanceType': ala_config.EC2_LARGE_INSTANCE_TYPE,
                'InstanceCount': self.slave_instance_count,
                'EbsConfiguration': {
                    'EbsBlockDeviceConfigs': [
                        {
                            'VolumeSpecification': {
                                'SizeInGB': self.ebs_size_in_gb,
                                'VolumeType': 'standard'
                            }
                        }
                    ]
                }
            }
        ]


def get_pre_ingestion_cluster(dag_id, instance_type, name):
    return obj_as_dict(PreIngestionEMRConfig(dag_id=dag_id, instance_type=instance_type, name=name, ebs_size_in_gb=ala_config.PREINGESTION_EBS_SIZE_IN_GB))


def get_small_cluster(dag_id, bootstrap_actions_script, ebs_size_in_gb=ala_config.EC2_SMALL_EBS_SIZE_IN_GB):
    bootstrap_actions = ala_config.get_bootstrap_actions(bootstrap_actions_script)
    return obj_as_dict(PipelinesSingleEMRConfig(dag_id=dag_id, instance_type=ala_config.EC2_SMALL_INSTANCE_TYPE, name=dag_id, ebs_size_in_gb=ebs_size_in_gb,
                                                BootstrapActions=bootstrap_actions))


def get_medium_cluster(dag_id, bootstrap_actions_script, ebs_size_in_gb=ala_config.EC2_MEDIUM_EBS_SIZE_IN_GB, cluster_size=ala_config.EC2_MEDIUM_INSTANCE_COUNT):
    bootstrap_actions = ala_config.get_bootstrap_actions(bootstrap_actions_script)

    return obj_as_dict(PipelinesMultiEMRConfig(dag_id=dag_id, instance_type=ala_config.EC2_LARGE_INSTANCE_TYPE, name=dag_id, ebs_size_in_gb=ebs_size_in_gb,
                                               slave_instance_count=cluster_size, BootstrapActions=bootstrap_actions))


def get_large_cluster(dag_id, bootstrap_actions_script, ebs_size_in_gb=ala_config.EC2_LARGE_EBS_SIZE_IN_GB, cluster_size=ala_config.EC2_LARGE_INSTANCE_COUNT):
    bootstrap_actions = ala_config.get_bootstrap_actions(bootstrap_actions_script)
    return obj_as_dict(PipelinesMultiEMRConfig(dag_id=dag_id, instance_type=ala_config.EC2_LARGE_INSTANCE_TYPE, name=dag_id, ebs_size_in_gb=ebs_size_in_gb,
                                               slave_instance_count=cluster_size, BootstrapActions=bootstrap_actions))

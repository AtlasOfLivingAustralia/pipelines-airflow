import re
import json
from airflow.models import Variable

ALA_API_KEY = Variable.get("ala_api_key")
DOI_API_KEY = Variable.get("doi_api_key")

AUTH_TOKEN = Variable.get('auth_token')
AUTH_CLIENT_ID = Variable.get('auth_client_id')
AUTH_CLIENT_SECRET = Variable.get('auth_client_secret')
AUTH_TOKEN_URL = Variable.get('ala_oidc_url')
USER_DETAILS_ENDPOINT = Variable.get("user_details_endpoint")
ALERT_EMAIL = Variable.get("alert_email")
BACKUP_LOCATION = Variable.get("s3_bucket_backup")
BIOCACHE_URL = Variable.get("biocache_url")
COLLECTORY_SERVER = Variable.get("registry_url")
DOI_URL = Variable.get("doi_url")
DR_REC_COUNT_THRESHOLD = int(Variable.get("dr_rec_count_threshold"))
EC2_KEY_NAME = Variable.get("ec2_key_name")
EC2_SMALL_INSTANCE_TYPE = Variable.get("ec2_small_instance_type")
EC2_LARGE_EBS_SIZE_IN_GB = int(Variable.get("ec2_large_ebs_size_in_gb"))
EC2_LARGE_INSTANCE_COUNT = int(Variable.get("ec2_large_instance_count"))
EC2_LARGE_INSTANCE_TYPE = Variable.get("ec2_large_instance_type")
EC2_XLARGE_INSTANCE_TYPE = Variable.get("ec2_large_instance_type")
PREINGESTION_EBS_SIZE_IN_GB = int(Variable.get("preingestion_ebs_size_in_gb"))
EC2_SUBNET_ID = Variable.get("ec2_subnet_id")
EC2_SMALL_EBS_SIZE_IN_GB = int(Variable.get("ec2_small_ebs_size_in_gb"))
EC2_MEDIUM_EBS_SIZE_IN_GB = int(Variable.get("ec2_medium_ebs_size_in_gb"))
EC2_MEDIUM_INSTANCE_COUNT = int(Variable.get("ec2_medium_instance_count"))
EMR_RELEASE = Variable.get("emr_release")
EMR_RELEASE_PREINGESTION = Variable.get("emr_release_preingestion")
ENVIRONMENT_TYPE = Variable.get("environment")
ES_ALIAS = Variable.get("es_alias")
ES_HOSTS = Variable.get("es_hosts")
EVENTS_URL = Variable.get("events_url")
EXCLUDED_DATASETS = re.split(r'\s+', Variable.get("excluded_datasets", ''))
IMAGES_URL = Variable.get("images_url")
JOB_FLOW_ROLE = Variable.get("job_flow_role")
JOB_SCHEDULE_CONFIG = Variable.get("job_schedule_config")
LISTS_URL = Variable.get("lists_url")
LOAD_IMAGES = Variable.get("load_images")
MASTER_MARKET = Variable.get("master_market")
MIN_DRS_PER_BATCH = int(Variable.get("min_drs_per_batch"))
NO_OF_DATASET_BATCHES = int(Variable.get("no_of_dataset_batches"))
NAME_MATCHING_URL = Variable.get("name_matching_url")
REGISTRY_URL = COLLECTORY_SERVER
PREINGESTION_AMI = Variable.get("preingestion_ami")
S3_ALA_UPLOADED_BUCKET = Variable.get("s3_bucket_ala_uploaded")
S3_BACKUP_BUCKET = Variable.get("s3_bucket_backup")
S3_BUCKET = Variable.get("s3_bucket")
S3_BUCKET_DWCA = Variable.get("s3_bucket_dwca")
S3_BUCKET_AVRO = Variable.get("s3_bucket_avro")  # renamed from s3_bucket_databox
S3_BUCKET_DWCA_EXPORTS = Variable.get("s3_bucket_dwca_exports")
SAMPLING_URL = Variable.get("sampling_url")
SDS_URL = Variable.get("sds_url")
SERVICE_ROLE = Variable.get("service_role")
SLAVE_MARKET = Variable.get("slave_market")
SOLR_COLLECTION = Variable.get("solr_collection")
SOLR_COLLECTION_TO_KEEP = Variable.get("solr_collection_to_keep")
SOLR_CONFIGSET = Variable.get("solr_configset")
SOLR_URL = Variable.get("solr_url")
SOLR_REPLICATION_FACTOR = Variable.get("solr_collection_rf")
SPARK_PROPERTIES = json.loads(Variable.get("spark_properties"))
ZK_URL = Variable.get("zk_url")

EC2_ADDITIONAL_MASTER_SECURITY_GROUPS = Variable.get('ec2_additional_master_security_groups').split(',')
EC2_ADDITIONAL_SLAVE_SECURITY_GROUPS = Variable.get('ec2_additional_slave_security_groups').split(',')


def get_bootstrap_actions(bootstrap_script):
    return [
        {
            "Name": "Bootstrap actions for datasets",
            "ScriptBootstrapAction": {
                "Args": [f"{S3_BUCKET}"],
                "Path": f"s3://{S3_BUCKET}/airflow/dags/{bootstrap_script}",
            }
        },
        get_bootstrap_config()
    ]


def get_bootstrap_config():
    return {
        "Name": "Pipelines YAML config",
        "ScriptBootstrapAction": {
            "Args": [
                f"{S3_BUCKET}",  # 1
                f"{ALA_API_KEY}",  # 2
                f"{REGISTRY_URL}",  # 3
                f"{LISTS_URL}",  # 4
                f"{SDS_URL}",  # 5
                f"{DOI_URL}",  # 6
                f"{NAME_MATCHING_URL}",  # 7
                f"{SAMPLING_URL}",  # 8
                f"{IMAGES_URL}",  # 9
                f"{SOLR_URL}",  # 10
                f"{ZK_URL}",  # 11
                f"{SOLR_COLLECTION}",  # 12
                f"{SOLR_CONFIGSET}",  # 13
                f"{ES_HOSTS}",  # 14
                f"{ES_ALIAS}"  # 15
            ],
            "Path": f"s3://{S3_BUCKET}/airflow/dags/bootstrap-la-pipelines-config.sh",
        }
    }

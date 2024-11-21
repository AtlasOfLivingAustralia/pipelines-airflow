import concurrent.futures
import json
import logging
import os
import re
from datetime import date, datetime, timedelta
from urllib.parse import urlparse
import boto3
import requests

from ala import ala_config

log: logging.log = logging.getLogger("airflow")
log.setLevel(logging.INFO)

def call_url(url, headers=None) -> requests.Response:
    try:
        print(f"Calling URL: {url}")
        with requests.get(url, headers=headers) as response:
            response.raise_for_status()
            print(f"Response: {response}")
            return response
    except requests.exceptions.HTTPError as err:
        logging.error(f"Error encountered during request {url}", err)
        raise IOError(err)

def join_url(*url_fragments:str) -> str:
    """
    Joins multiple URL fragments into a single URL.

    :param url_fragments: URL fragments to be joined.
    :return: A single URL string.
    """
    return '/'.join(fragment.strip('/') for fragment in url_fragments)

def json_parse(base_url: str, url_path: str, params={}, headers=None):
    """
    Calls the specified URL and returns the JSON response.
    :param base_url: like https://collections.ala.org.au/ws
    :param url_path: like /dataResource/dr000
    :param params: is a dictionary of parameters to be passed to the API
    :return: is the json response from the URL
    """
    try:
        full_url = join_url(base_url, url_path)
        with requests.get(full_url, params, headers=headers) as response:
            response.raise_for_status()
            json_result = json.loads(response.content)
            return json_result
    except requests.exceptions.HTTPError as err:
        logging.error(f"Error encountered during request {full_url} with params {params}", err)
        raise IOError(err)


def get_dr_count(dr: str):
    """
    Retrieves the count of records for the specified data resource Uid from Biocache.

    :param dr: A string representing the data resource Uid.
    :type dr: str
    :return: The count of records in Biocache.
    :rtype: int
    :raises requests.exceptions.HTTPError: If an HTTP error occurs while making the request to Biocache.
    """
    try:
        biocache_json = json_parse(ala_config.BIOCACHE_WS, '/occurrences/search', {'q': f'data_resource_uid:{dr}', 'pageSize': '0'})
        return biocache_json['totalRecords']
    except requests.exceptions.HTTPError as err:
        return 0


def search_biocache(query: str):
    """
    Queries the Biocache web service for occurrence records matching the given query.

    :param query: A query string in the format of a Biocache web service search URL.
    :return: A dictionary containing the JSON response from the Biocache web service.
    :raises requests.exceptions.HTTPError: If the Biocache web service returns a non-200 HTTP status code.

    Example usage:
    >>> query = 'q=taxon_name:Acacia&fq=country:Australia&facet=off&pageSize=0'
    >>> result = search_biocache(query)
    >>> count = result['totalRecords']

    See https://biocache.ala.org.au/ws for more information on the Biocache web service.
    """
    return json_parse(ala_config.BIOCACHE_WS, '/occurrences/search', query)


def get_image_sync_steps(s3_bucket_avro: str, dataset_list: []):
    """
    Generates a list of four steps to download, synchronize, index, and upload datasets from an S3 bucket.

    Args:
        s3_bucket_avro (str): The name of the S3 bucket containing the datasets in Avro format.
        dataset_list (List[str]): A list of dataset names to be processed.

    Returns:
        List[Dict[str, str]]: A list of four steps, each containing a description and a command to be executed.

    Step 1: Download datasets avro
        This step downloads the specified datasets in Avro format from the S3 bucket to the local machine.

    Step 2: Image sync
        This step synchronizes all images in the specified datasets to the local machine using the la-pipelines tool.

    Step 3: Index
        This step indexes all images in the specified datasets using the la-pipelines tool.

    Step 4: Upload datasets avro
        This step uploads the processed datasets in Avro format back to the S3 bucket.

    Example Usage:
        s3_bucket_avro = "ala-s3-bucket"
        dataset_list = ["dr000", "dr001"]
        steps = get_image_sync_steps(s3_bucket_avro, dataset_list)
        for step in steps:
            print(step["description"])
            print(step["command"])
    """
    return [
        step_bash_cmd("Download datasets avro",
                      f" /tmp/download-datasets-image-sync.sh {s3_bucket_avro} {dataset_list}"),
        step_bash_cmd("Image sync", f" la-pipelines image-sync {dataset_list} --cluster"),
        step_bash_cmd("Index", f" la-pipelines index {dataset_list} --cluster --extra-args timeBufferInMillis=604800000"),
        step_bash_cmd("Upload datasets avro",
                      f" /tmp/upload-indexed-image-sync-datasets.sh {s3_bucket_avro} {dataset_list}")]


def read_solr_collection_date() -> str:
    """
    Fetches the date of the Solr collection named 'biocache' and returns it as a string
    in the format 'YYYY-MM-DD'.

    Returns:
        A string representing the date of the 'biocache' Solr collection in 'YYYY-MM-DD' format.

    Raises:
        HTTPError: If an HTTP error occurs while requesting the Solr collection URL.
        ValueError: If the 'biocache' Solr collection is not found or its name is not in the expected format.
    """

    json_str = json_parse(ala_config.SOLR_URL, '/admin/collections', {'action': 'CLUSTERSTATUS'})
    biocache_collection = json_str['cluster']['aliases']['biocache']
    matches = re.findall(r'.*-(\d{4}-\d{2}-\d{2})-.*', biocache_collection)
    return matches[0]


def get_recent_images_drs(start_date: str, end_date: str = None):
    """
    Returns a dictionary containing the number of images loaded into data resources (DRs) between start_date and end_date.
    The keys of the dictionary are the data resource UIDs and the values are the corresponding image counts.

    :param start_date: A string representing the start date of the range in ISO format (e.g. '2020-01-01').
    :param end_date: A string representing the end date of the range in ISO format (e.g. '2021-12-01').
                     If None, today's date will be used as the end date.
    :return: A dictionary containing the number of images loaded into each data resource between start_date and end_date.

    Example:
    >>> get_recent_images_drs('2022-01-01', '2022-01-05')
    {'dr000': 10, 'dr001': 5, 'dr002': 3}
    """

    def generate_dates_str():
        """
        :return: generates the dates between start and end date
        """
        from_date = date.fromisoformat(start_date)
        to_date = date.fromisoformat(end_date)
        while from_date <= to_date:
            yield from_date.isoformat()
            from_date += timedelta(days=1)

    if not end_date:
        end_date = datetime.today().date().isoformat()

    drs = {}
    for date_str in generate_dates_str():
        images_json = json_parse(ala_config.IMAGES_URL, '/ws/facet', {'q': '*:*', 'fq': f'dateUploaded:{date_str}',
                                                                      'facet': 'dataResourceUid'})
        images_dr = images_json['dataResourceUid']
        for dr, images_count in images_dr.items():
            drs[dr] = images_count + (0 if dr not in [drs] else drs[dr])
    return {d: c for d, c in drs.items() if d != 'no_dataresource'}



def s3_cp(step_name, src, dest, action_on_failure='TERMINATE_CLUSTER'):
    return {
        "Name": step_name,
        "ActionOnFailure": action_on_failure,
        "HadoopJarStep": {
            "Jar": "/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar",
            "Args": [
                f"--src={src}",
                f"--dest={dest}"
            ]
        }
    }


def step_bash_cmd(step_name, cmd, action_on_failure='TERMINATE_CLUSTER'):
    """
      Returns a dictionary that represents an AWS EMR step
      to execute a Bash command on the cluster.

      Args:
          step_name (str): The name of the step.
          cmd (str): The Bash command to execute.
          action_on_failure (str, optional): The action to take if the step fails.
              Defaults to 'TERMINATE_CLUSTER'.

      Returns:
          dict: A dictionary representing the EMR step to execute the Bash command.
      """
    return {
        "Name": step_name,
        "ActionOnFailure": action_on_failure,
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "bash",
                "-c",
                cmd
            ]
        }
    }


def emr_python_step(name, full_py_cmd, action_on_failure='TERMINATE_CLUSTER'):
    return {
        "Name": name,
        "ActionOnFailure": action_on_failure,
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "bash",
                "-c",
                f" sudo -u hadoop python3 {full_py_cmd}"
            ]
        }
    }


def step_cmd_args(step_name, cmd_arr, action_on_failure='TERMINATE_CLUSTER'):
    """
    Create a dictionary representing an EMR step.

    Args:
        step_name (str): The name of the step.
        cmd_arr (List[str]): A list of command-line arguments for the step.
        action_on_failure (str, optional): The action to take if the step fails. Defaults to 'TERMINATE_CLUSTER'.

    Returns:
        Dict[str, Any]: A dictionary representing the EMR step, with the following keys:
            - 'Name': The name of the step.
            - 'ActionOnFailure': The action to take if the step fails.
            - 'HadoopJarStep': A dictionary with the following keys:
                - 'Jar': The name of the JAR file to run for the step (in this case, 'command-runner.jar').
                - 'Args': A list of command-line arguments for the step (in this case, `cmd_arr`).

    """
    return {
        "Name": step_name,
        "ActionOnFailure": action_on_failure,
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': cmd_arr
        }
    }


def list_objects_in_bucket(bucket_name, obj_prefix, obj_key_regex, sub_dr_folder='', time_range=(None, None)):
    def list_folders_in_bucket(bucket_name, obj_prefix, delimiter, regex):
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=obj_prefix, Delimiter=delimiter)
        folders = []
        while True:
            folders += [o.get('Prefix').split('/')[-2] for o in response.get('CommonPrefixes')]

            # Check if there are more results to retrieve
            if response.get('NextContinuationToken'):
                # Make another request with the continuation token
                response = s3.list_objects_v2(Bucket=bucket_name, Prefix=obj_prefix,
                                              Delimiter=delimiter,
                                              ContinuationToken=response.get('NextContinuationToken'))
            else:
                break

        dr_pattern = re.compile(regex)
        drs = [dr for dr in list(filter(dr_pattern.match, folders)) if dr not in ala_config.EXCLUDED_DATASETS]
        return drs

    def process_prefix(l_prefix, l_dr):
        results = {}
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=l_prefix)

        for page in page_iterator:
            for my_bucket_object in page.get('Contents', []):
                matches = list(re.findall(obj_key_regex, my_bucket_object['Key']))
                if matches:
                    lower_range = True
                    higher_range = True
                    if time_range[0]:
                        lower_range = my_bucket_object['LastModified'].isoformat() >= time_range[0]
                    if time_range[1]:
                        higher_range = my_bucket_object['LastModified'].isoformat() <= time_range[1]
                    if lower_range and higher_range:
                        results[l_dr] = my_bucket_object['Size']

        return results

    log.info(f"Reading from bucket: {bucket_name}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        session = boto3.Session()
        s3 = session.client('s3')
        drs = list_folders_in_bucket(bucket_name, obj_prefix, delimiter='/', regex=r'^dr[0-9]+$')
        futures = []
        for dr in drs:
            prefix = f'{obj_prefix}{dr}/{sub_dr_folder}'
            futures.append(executor.submit(process_prefix, prefix, dr))
        datasets = {}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            for key, value in result.items():
                if key in datasets:
                    datasets[key] += value
                else:
                    datasets[key] = value
    datasets = dict(sorted(datasets.items(), key=lambda item: item[1], reverse=True))
    return datasets


def list_drs_dwca_in_bucket(**kwargs):
    return list_objects_in_bucket(kwargs['bucket'], 'dwca-imports/', r'^.*/(dr[0-9]+)\.zip$', sub_dr_folder='')


def list_drs_index_avro_in_bucket(**kwargs):
    return list_objects_in_bucket(kwargs['bucket'], 'pipelines-all-datasets/index-record/',
                                  r'^.*/dr[0-9]+/dr[0-9]+[\-0-9of]*\.avro$', sub_dr_folder='',
                                  time_range=(None, None) if "time_range" not in kwargs else kwargs[
                                      "time_range"])

def list_drs_verbatim_avro_in_bucket(**kwargs):

    return list_objects_in_bucket(kwargs['bucket'], 'pipelines-data/',
                                  r'^.*/dr[0-9]+/1/verbatim/verbatim+[\-0-9of]*\.avro$', sub_dr_folder='',
                                  time_range=(None, None) if "time_range" not in kwargs else kwargs[
                                      "time_range"])

def list_drs_ingested_since(**kwargs):
    return list_objects_in_bucket(kwargs['bucket'], 'pipelines-data/',
                                  r'.*', sub_dr_folder='1/interpretation-metrics.yml', time_range=kwargs['time_range'])


def step_s3_cp_file(dr, source_path, target_path):
    return step_cmd_args(f"Copy {source_path} to {target_path} for {dr}", [
        "aws",
        "s3",
        "cp",
        source_path,
        target_path])


def hdfs_dwca_exist(s3_pipelines_dwca) -> dict:
    parsed_url = urlparse(s3_pipelines_dwca, allow_fragments=False)
    s3 = boto3.resource(parsed_url.scheme)
    pipelines_s3_bucket = s3.Bucket(parsed_url.hostname)
    print(f"Checking s3 bucket {parsed_url.hostname} for resource {parsed_url.path}")
    file_exist = bool(list(pipelines_s3_bucket.objects.filter(Prefix=parsed_url.path.lstrip('/'))))
    print(f"Check {s3_pipelines_dwca}. Resource returned: {file_exist}")
    return file_exist


def get_type(path):
    multimedia_list = ['multimedia', 'image', 'images', 'sounds']

    def is_multimedia(path):
        return any(part_str in path for part_str in multimedia_list)

    # returns occurrence by default
    return 'multimedia' if is_multimedia(path) else 'occurrence'


# raise error if the path is not a file and is a directory. Considered a file if it has extension
def get_file_name(path):
    info = os.path.split(path)
    if not info[1]:
        raise SystemExit("Error not valid file path: " + path)

    return info[1]


def get_default_args():
    return {
        'owner': 'airflow',
        'depends_on_past': False,
        'email': [ala_config.ALERT_EMAIL],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
    }

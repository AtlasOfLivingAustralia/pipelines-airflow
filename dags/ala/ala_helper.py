import concurrent.futures
import json
import logging
import os
import re
import zipfile
from datetime import date, datetime, timedelta
from enum import StrEnum
from urllib.parse import urlparse

import boto3
import requests
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from ala import ala_config

log: logging.log = logging.getLogger("airflow")
log.setLevel(logging.INFO)


def strtobool(val: str) -> bool:
    """Lightweight, non-deprecated alternative to distutils.util.strtobool."""
    return str(val).strip().lower() in {"y", "yes", "t", "true", "on", "1"}


def call_url(url, headers=None, timeout=60) -> requests.Response:
    """
    Sends a GET request to the specified URL with optional headers and returns the response.

    Args:
        url (str): The URL to send the GET request to.
        headers (dict, optional): Optional HTTP headers to include in the request.

    Returns:
        requests.Response: The response object resulting from the GET request.

    Raises:
        IOError: If an HTTP error occurs during the request.
    """
    try:
        print(f"Calling URL: {url}")
        with requests.get(url, headers=headers, timeout=timeout) as response:
            response.raise_for_status()
            print(f"Response: {response}")
            return response
    except requests.exceptions.HTTPError as err:
        logging.error("Error encountered during request %s", url, exc_info=err)
        raise IOError(err) from err


def join_url(*url_fragments: str) -> str:
    """
    Joins multiple URL fragments into a single URL.

    :param url_fragments: URL fragments to be joined.
    :return: A single URL string.
    """
    return "/".join(fragment.strip("/") for fragment in url_fragments)


def json_parse(base_url: str, url_path: str, params=None, headers=None, method="GET"):
    """
    Calls the specified URL and returns the JSON response.
    :param base_url: like https://collections.ala.org.au/ws
    :param url_path: like /dataResource/dr000
    :param params: is a dictionary of parameters to be passed to the API
    :return: is the json response from the URL
    """

    try:
        full_url = join_url(base_url, url_path)
        if method == "GET":
            with requests.get(full_url, params, headers=headers, timeout=60) as response:
                response.raise_for_status()
                json_result = json.loads(response.content)
                return json_result
        elif method == "POST":
            with requests.post(full_url, json=params, headers=headers, timeout=60) as response:
                response.raise_for_status()
                return response.content
    except requests.exceptions.HTTPError as err:
        logging.error("Error encountered during request %s with params %s", full_url, params, exc_info=err)
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
        biocache_json = json_parse(
            ala_config.BIOCACHE_WS, "/occurrences/search", {"q": f"data_resource_uid:{dr}", "pageSize": "0"}
        )
        return biocache_json["totalRecords"]
    except requests.exceptions.HTTPError as err:
        log.error("Error encountered during request for data resource %s", dr, exec_info=err)
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
    return json_parse(ala_config.BIOCACHE_WS, "/occurrences/search", query)


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
        step_bash_cmd(
            "Download datasets avro", f" /tmp/download-datasets-image-sync.sh {s3_bucket_avro} {dataset_list}"
        ),
        step_bash_cmd("Image sync", f" la-pipelines image-sync {dataset_list} --cluster"),
        step_bash_cmd(
            "Index", f" la-pipelines index {dataset_list} --cluster --extra-args timeBufferInMillis=604800000"
        ),
        step_bash_cmd(
            "Upload datasets avro", f" /tmp/upload-indexed-image-sync-datasets.sh {s3_bucket_avro} {dataset_list}"
        ),
    ]


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

    json_str = json_parse(ala_config.SOLR_URL, "/admin/collections", {"action": "CLUSTERSTATUS"})
    biocache_collection = json_str["cluster"]["aliases"]["biocache"]
    matches = re.findall(r".*-(\d{4}-\d{2}-\d{2})-.*", biocache_collection)
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
        images_json = json_parse(
            ala_config.IMAGES_URL,
            "/ws/facet",
            {"q": "*:*", "fq": f"dateUploaded:{date_str}", "facet": "dataResourceUid"},
        )
        images_dr = images_json["dataResourceUid"]
        for dr, images_count in images_dr.items():
            drs[dr] = images_count + (0 if dr not in [drs] else drs[dr])
    return {d: c for d, c in drs.items() if d != "no_dataresource"}


def s3_cp(step_name, src, dest, action_on_failure="TERMINATE_CLUSTER"):
    """
    Creates a configuration dictionary for an EMR step that copies files between S3 and HDFS using s3-dist-cp.

    Args:
        step_name (str): The name of the EMR step.
        src (str): The source path (S3 or HDFS) to copy files from.
        dest (str): The destination path (S3 or HDFS) to copy files to.
        action_on_failure (str, optional): Action to take if the step fails. Defaults to "TERMINATE_CLUSTER".

    Returns:
        dict: A dictionary representing the EMR step configuration for s3-dist-cp.
    """
    return {
        "Name": step_name,
        "ActionOnFailure": action_on_failure,
        "HadoopJarStep": {
            "Jar": "/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar",
            "Args": [f"--src={src}", f"--dest={dest}"],
        },
    }


def step_bash_cmd(step_name, cmd, action_on_failure="TERMINATE_CLUSTER"):
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
        "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["bash", "-c", f"{cmd} 1>&2"]},
    }


def emr_python_step(name, full_py_cmd, action_on_failure="TERMINATE_CLUSTER"):
    """
    Creates a dictionary representing an EMR step to run a Python script using a bash command.

    Args:
        name (str): The name of the EMR step.
        full_py_cmd (str): The full Python command or script to execute.
        action_on_failure (str, optional): Action to take if the step fails. Defaults to "TERMINATE_CLUSTER".

    Returns:
        dict: A dictionary formatted for use as an EMR step definition.
    """
    return {
        "Name": name,
        "ActionOnFailure": action_on_failure,
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["bash", "-c", f" sudo -u hadoop python3 {full_py_cmd} 1>&2"],
        },
    }


def step_cmd_args(step_name, cmd_arr, action_on_failure="TERMINATE_CLUSTER"):
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
        "HadoopJarStep": {"Jar": "command-runner.jar", "Args": cmd_arr},
    }


def list_objects_in_bucket(bucket_name, obj_prefix, obj_key_regex, sub_dr_folder="", time_range=(None, None)):
    """
    Lists and aggregates the sizes of objects in an S3 bucket that match a given key regex,
    grouped by dataset folders (e.g., "dr" folders), optionally filtered by a time range.

    Args:
        bucket_name (str): Name of the S3 bucket to search.
        obj_prefix (str): Prefix to filter objects within the bucket.
        obj_key_regex (str): Regular expression to match object keys.
        sub_dr_folder (str, optional): Subfolder within each dataset folder to search. Defaults to "".
        time_range (tuple, optional): Tuple of (start_time, end_time) in ISO format strings.
            Only objects with 'LastModified' within this range are included. Defaults to (None, None).

    Returns:
        dict: A dictionary where keys are dataset folder names (e.g., "dr123") and values are the
            total size (in bytes) of matching objects within each folder, sorted in descending order by size.
    """

    def list_folders_in_bucket(bucket_name, obj_prefix, delimiter, regex):
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=obj_prefix, Delimiter=delimiter)
        folders = []
        while True:
            folders += [o.get("Prefix").split("/")[-2] for o in response.get("CommonPrefixes")]

            # Check if there are more results to retrieve
            if response.get("NextContinuationToken"):
                # Make another request with the continuation token
                response = s3.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=obj_prefix,
                    Delimiter=delimiter,
                    ContinuationToken=response.get("NextContinuationToken"),
                )
            else:
                break

        dr_pattern = re.compile(regex)
        drs = [dr for dr in list(filter(dr_pattern.match, folders)) if dr not in ala_config.EXCLUDED_DATASETS]
        return drs

    def process_prefix(l_prefix, l_dr):
        results = {}
        paginator = s3.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=l_prefix)

        for page in page_iterator:
            for my_bucket_object in page.get("Contents", []):
                matches = list(re.findall(obj_key_regex, my_bucket_object["Key"]))
                if matches:
                    lower_range = True
                    higher_range = True
                    if time_range[0]:
                        lower_range = my_bucket_object["LastModified"].isoformat() >= time_range[0]
                    if time_range[1]:
                        higher_range = my_bucket_object["LastModified"].isoformat() <= time_range[1]
                    if lower_range and higher_range:
                        results[l_dr] = my_bucket_object["Size"]

        return results

    log.info("Reading from bucket: %s", bucket_name)

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        session = boto3.Session()
        s3 = session.client("s3")
        drs = list_folders_in_bucket(bucket_name, obj_prefix, delimiter="/", regex=r"^dr[0-9]+$")
        futures = []
        for dr in drs:
            prefix = f"{obj_prefix}{dr}/{sub_dr_folder}"
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
    """
    Lists Darwin Core Archive (DWCA) files for DRs in the specified bucket.

    Args:
        **kwargs: Arbitrary keyword arguments. Must include:
            bucket (str): The name of the bucket to search for DWCA files.

    Returns:
        list: A list of objects in the bucket matching the DRs DWCA file pattern.

    Raises:
        KeyError: If 'bucket' is not provided in kwargs.

    Note:
        This function searches for files in the 'dwca-imports/' directory of the bucket
        that match the pattern 'dr[0-9]+.zip'.
    """
    return list_objects_in_bucket(kwargs["bucket"], "dwca-imports/", r"^.*/(dr[0-9]+)\.zip$", sub_dr_folder="")


def list_drs_index_avro_in_bucket(**kwargs):
    """
    Lists DRs index Avro files in a specified bucket and path.

    This function searches for Avro files matching the DRs index pattern within a given bucket and prefix.
    It delegates the actual listing to the `list_objects_in_bucket` function, applying a regular expression
    to filter files and optionally restricting results to a specified time range.

    Keyword Args:
        bucket (str): The name of the bucket to search in.
        time_range (tuple, optional): A tuple (start_time, end_time) to filter files by modification time.
            If not provided, all files are considered.

    Returns:
        list: A list of objects (typically file paths or metadata) matching the DRS index Avro pattern.
    """
    return list_objects_in_bucket(
        kwargs["bucket"],
        "pipelines-all-datasets/index-record/",
        r"^.*/dr[0-9]+/dr[0-9]+[\-0-9of]*\.avro$",
        sub_dr_folder="",
        time_range=(None, None) if "time_range" not in kwargs else kwargs["time_range"],
    )


def list_drs_verbatim_avro_in_bucket(**kwargs):
    """
    Lists Avro files matching the DRs verbatim avro pattern in a specified bucket.

    This function searches for Avro files within the 'pipelines-data/' directory of the given bucket,
    matching the regex pattern for DRs verbatim files. Optionally, a time range can be specified to filter results.

    Args:
        **kwargs: Arbitrary keyword arguments.
            bucket (str): The name of the bucket to search in.
            time_range (tuple, optional): A tuple (start_time, end_time) to filter files by modification time.
                If not provided, all files are considered.

    Returns:
        list: A list of object keys (file paths) matching the DRS verbatim Avro pattern within the bucket.
    """

    return list_objects_in_bucket(
        kwargs["bucket"],
        "pipelines-data/",
        r"^.*/dr[0-9]+/1/verbatim/verbatim+[\-0-9of]*\.avro$",
        sub_dr_folder="",
        time_range=(None, None) if "time_range" not in kwargs else kwargs["time_range"],
    )


def list_drs_ingested_since(**kwargs):
    """
    Lists objects in a specified bucket that have been ingested since a given time range.

    This function calls `list_objects_in_bucket` to retrieve objects from the provided bucket,
    searching within the "pipelines-data/" prefix and filtering by the specified sub-directory
    and time range.

    Args:
        **kwargs: Arbitrary keyword arguments. Expected keys:
            - bucket (str): The name of the bucket to search.
            - time_range (tuple): A tuple specifying the start and end times for filtering objects.

    Returns:
        list: A list of objects in the bucket that match the criteria.
    """
    return list_objects_in_bucket(
        kwargs["bucket"],
        "pipelines-data/",
        r".*",
        sub_dr_folder="1/interpretation-metrics.yml",
        time_range=kwargs["time_range"],
    )


def step_s3_cp_file(dr, source_path, target_path):
    """
    Copies a file from a source path to a target path in AWS S3 using the AWS CLI.

    Args:
        dr (str): Data Resource Uid
        source_path (str): The S3 URI or local path of the source file to copy.
        target_path (str): The S3 URI or local path where the file should be copied to.

    Returns:
        dict: A step command argument dictionary generated by `step_cmd_args` for the copy operation.
    """
    return step_cmd_args(
        f"Copy {source_path} to {target_path} for {dr}", ["aws", "s3", "cp", source_path, target_path, "1>&2"]
    )


def get_type(path):
    """
    Determines the type of a given path as either 'multimedia' or 'occurrence'.

    Args:
        path (str): The path string to be evaluated.

    Returns:
        str: 'multimedia' if the path contains any multimedia-related keywords
             ('multimedia', 'image', 'images', 'sounds'), otherwise 'occurrence'.
    """
    multimedia_list = ["multimedia", "image", "images", "sounds"]

    def is_multimedia(path):
        return any(part_str in path for part_str in multimedia_list)

    return "multimedia" if is_multimedia(path) else "occurrence"


def get_file_name(path):
    """
    Extracts the file name from a given file path.

    Args:
        path (str): The full file path.

    Returns:
        str: The file name extracted from the path.

    Raises:
        SystemExit: If the provided path does not contain a valid file name.
    """
    info = os.path.split(path)
    if not info[1]:
        raise SystemExit("Error not valid file path: " + path)

    return info[1]


class Status(StrEnum):
    FAILED = "failed"
    SUCCESS = "success"


def get_slack_blocks(status):
    """
    Generate Slack message blocks for Airflow DAG notifications based on the DAG run status.

    Args:
        status (Status): The status of the DAG run, typically an enum with values such as Status.FAILED.

    Returns:
        list: A list of dictionaries representing Slack message blocks formatted for use with the Slack API.

    The generated blocks include:
        - A header with an icon indicating success or failure, environment, DAG ID, and DAG run ID.
        - A section with a link to the DAG in the Airflow web UI.
        - If the status is FAILED, an additional section with a link to the failed task's log.
        - A section with fields showing execution start time, logical date, prior successful DAG run start date, and parameters.
        - A divider at the end.

    Note:
        The function assumes the use of Jinja templating for variables such as `dag`, `dag_run`, `task_instance`, etc.
    """
    if status == Status.FAILED:
        icon = ":red_circle:"
    else:
        icon = ":large_green_circle:"

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": icon + "[{{ var.value.environment }}] {{ dag.dag_id }}    #{{ dag_run.id }}",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Access the DAG link here: <{{ conf.get('webserver', 'base_url') }}/dags/{{dag.dag_id}}/grid?dag_run_id={{dag_run.run_id.replace('+','%2B')}}&tab=graph|{{dag.dag_id}}>",
            },
        },
    ]
    if status == Status.FAILED:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*{{task_instance.task_id}}* log is here: <{{task_instance.log_url}} | {{task_instance.task_id}}>",
                },
            }
        )
    blocks.append(
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": "*Execution started at :*"},
                {"type": "mrkdwn", "text": "{{macros.datetime_diff_for_humans(dag_run.start_date)}}"},
                {"type": "mrkdwn", "text": "*DAG run logical date:*"},
                {"type": "mrkdwn", "text": "{{ logical_date.in_timezone('Australia/Sydney') | ts }}"},
                {"type": "mrkdwn", "text": "*Start date of prior successful Dag run :*"},
                {
                    "type": "mrkdwn",
                    "text": "{{ prev_start_date_success.in_timezone('Australia/Sydney') | ts if prev_start_date_success is not none else 'No previous start date available' }}",
                },
                {"type": "mrkdwn", "text": "*params:*"},
                {"type": "mrkdwn", "text": "{{ params }}"},
            ],
        }
    )
    blocks.append({"type": "divider"})
    return blocks


def slack_alert(status: Status):
    """
    Sends a Slack alert notification if Slack notifications are enabled in the configuration.

    Args:
        status (Status): The status object containing information to be included in the Slack notification.

    Returns:
        list or None: A list containing the Slack notification task if notifications are enabled, otherwise None.
    """
    slack_notification = None
    if ala_config.SLACK_NOTIFICATION:
        slack_notification = [
            send_slack_notification(
                slack_conn_id="slack_api_conn", channel=ala_config.SLACK_ALERTS_CHANNEL, blocks=get_slack_blocks(status)
            )
        ]

    return slack_notification


def get_success_notification_operator():
    """
    Creates and returns an Airflow operator for sending a success notification.

    If Slack notifications are enabled in the configuration, this function returns a SlackAPIPostOperator
    that sends a success message to the configured Slack channel. Otherwise, it returns an EmptyOperator
    as a placeholder.

    Returns:
        BaseOperator: An Airflow operator for success notification, either SlackAPIPostOperator or EmptyOperator.
    """
    if ala_config.SLACK_NOTIFICATION:
        return SlackAPIPostOperator(
            slack_conn_id="slack_api_conn",
            task_id="slack_success_notification",
            channel=ala_config.SLACK_ALERTS_CHANNEL,
            blocks=get_slack_blocks(Status.SUCCESS),
            text="Fallback message",
        )
    else:
        return EmptyOperator(task_id="slack_success_notification")


def get_default_args():
    """
    Returns a dictionary of default arguments for Airflow DAGs.

    The returned dictionary includes standard Airflow parameters such as owner, email notifications, retry behavior, and callbacks for task failure and success.

    Returns:
        dict: Default arguments for configuring Airflow DAGs.
    """

    return {
        "owner": "airflow",
        "depends_on_past": False,
        "email": [ala_config.ALERT_EMAIL],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "on_failure_callback": slack_alert(Status.FAILED),
        "on_success_callback": None,
    }


def validate_jar(jar_file):
    """
    Validates whether the given file is a valid Java archive (JAR) file.

    Attempts to open the specified file as a ZIP archive and checks for the presence of essential JAR metadata,
    such as the MANIFEST.MF file, and at least one Java class file (.class). Raises an exception if the file is
    not a valid JAR, does not contain any class files, or if the file is not found.

    Args:
        jar_file (str or file-like object): Path to the JAR file or a file-like object.

    Returns:
        bool: True if the file is a valid Java archive.

    Raises:
        ValueError: If the file is not a valid JAR archive or does not contain any Java class files.
        FileNotFoundError: If the specified JAR file does not exist.
    """
    try:
        # Try to open the file as a ZIP archive (JAR files are ZIP archives)
        with zipfile.ZipFile(jar_file, "r") as jar:
            # Check for essential JAR metadata
            if not any(entry.endswith("MANIFEST.MF") for entry in jar.namelist()):
                log.error("%s does not contain a valid MANIFEST.MF file", jar_file)

            # Optional: Additional checks
            if not any(entry.endswith(".class") for entry in jar.namelist()):
                raise ValueError("No Java class files found in the archive")

            print(f"{jar_file} is a valid Java archive")
            return True
    except zipfile.BadZipFile:
        raise ValueError(f"{jar_file} is not a valid Java archive")
    except FileNotFoundError:
        raise FileNotFoundError(f"JAR file {jar_file} not found")


def enable_debugpy():
    """
    Enables remote debugging using debugpy.

    This function checks if a debugpy client is already connected. If not, it starts
    listening for a debugpy client on all interfaces at port 10001 and waits for a client
    to connect before proceeding. Useful for attaching a debugger to a running process.

    Note:
        This function blocks execution until a debugpy client connects if not already connected.
    """
    import debugpy

    if debugpy.is_client_connected():
        print("Debugger already connected")
    else:
        debugpy.listen(("0.0.0.0", 10001))
        print("waiting for client to connect")
        debugpy.wait_for_client()


def get_assertion_records_count():
    """
    Fetches the total number of occurrence records with user assertions from the ALA Biocache service.

    Returns:
        int: The total count of records that have user assertions. Returns 0 if the count is not found.

    Raises:
        requests.RequestException: If the HTTP request to the Biocache service fails.
        json.JSONDecodeError: If the response from the Biocache service is not valid JSON.
    """
    try:
        response = call_url(f"{ala_config.BIOCACHE_URL}/occurrence/search?q=userAssertions%3A*&pageSize=0", timeout=60)
        records_with_assertions = json.loads(response.text)
        return records_with_assertions.get("totalRecords", 0)
    except Exception as e:
        log.error("Error fetching assertion records count: %s", e)
        return -1


def get_metadata_as_json(registry_base_url, uid, ala_api_key):
    """
    Fetches metadata for a dataset from the registry (Collectory) using the API key.

    Args:
        dataset_uid (str): The unique identifier of the dataset.
        registry_url (str): The URL of the registry (Collectory).
        ala_api_key (str): The API key for authentication.

    Returns:
        dict: The metadata of the dataset, or an error message if not found.
    """

    if uid.startswith("dr"):
        resource_path = f"dataResource/{uid}"
    elif uid.startswith("dp"):
        resource_path = f"dataProvider/{uid}"
    else:
        raise ValueError("Not a valid dataset or data provider uid: %s", uid)

    try:
        jresponse = json_parse(registry_base_url, resource_path, headers={"Authorization": ala_api_key})
        return jresponse
    except Exception as e:
        print(f"Error fetching metadata for {uid}: {str(e)}")
        return {"error": str(e)}


def update_registry_metadata(registry_base_url, uid, ala_api_key, metadata):
    """
    Updates metadata for a dataset in the registry (Collectory) using the API key.

    Args:
        registry_base_url (str): The base URL of the registry (Collectory).
        uid (str): The unique identifier of the dataset.
        ala_api_key (str): The API key for authentication.
        metadata (dict): The metadata to update.

    Returns:
        dict: The updated metadata of the dataset, or an error message if not found.
    """
    if uid.startswith("dr"):
        resource_path = f"dataResource/{uid}"
    elif uid.startswith("dp"):
        resource_path = f"dataProvider/{uid}"
    else:
        raise ValueError("Not a valid dataset or data provider uid: %s", uid)

    try:
        jresponse = json_parse(
            registry_base_url, resource_path, headers={"Authorization": ala_api_key}, method="POST", params=metadata
        )
        return jresponse
    except Exception as e:
        print(f"Error fetching metadata for {uid}: {str(e)}")

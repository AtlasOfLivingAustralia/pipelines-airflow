import zipfile
import boto3
import requests
import json
from enum import Enum
from datetime import date
import io
from string import Template
import os.path
import tempfile
from pathlib import Path
import glob
import argparse
import logging
from datetime import datetime
from ala import ala_helper

logger = logging.getLogger(__name__)


class ResourceType(str, Enum):
    Audiovisual = 'Audiovisual'
    Collection = 'Collection'
    DataPaper = 'DataPaper'
    Dataset = 'Dataset'
    Event = 'Event'
    Image = 'Image'
    InteractiveResource = 'InteractiveResource'
    Model = 'Model'
    PhysicalObject = 'PhysicalObject'
    Service = 'Service'
    Software = 'Software'
    Sound = 'Sound'
    Text = 'Text'
    Workflow = 'Workflow'
    Other = 'Other'


class DOIInfo:
    """
    Compulsory fields:
    provider , applicationUrl, providerMetadata, title, authors, description
    Missing compulsory fields will fail without explicit error messages from DOI
    provider:  must be DATACITE
    providerMetadata -> resourceType: enum[Image]
    """

    def __init__(self):
        today = date.today().strftime("%Y-%m-%d")
        self.provider = ''
        self.title = 'Survey download records-' + today  # Survey download record_date
        self.authors = 'Atlas Of Living Australia'
        self.description = ''
        self.applicationUrl = ''
        self.fileUrl = ''
        self.licence = []  # same as dataset
        self.userId = ''
        self.provider = 'DATACITE'  # ONLY DATACITE now
        self.authorisedRoles = []

        self.applicationMetadata = {"datasets": [], "qualityFilters": [], "searchUrl": "", "requestedOn": ""}
        #  request time - String

        self.providerMetadata = {
            "contributors": [],
            "distributions": [],
            "creator": [],
            "publisher": "Atlas Of Living Australia",
        }
        # Provider cannot be null or empty

        self.customLandingPageUrl = ''
        self.active = True

    def new(collectory_info):
        doi_info = DOIInfo()
        doi_info.description = collectory_info['description']

        doi_info.add_provider_metadata('authors', ['Atlas Of Living Australia'])  # Compulsory
        doi_info.add_provider_metadata('title', collectory_info['name'])
        doi_info.add_provider_metadata('resourceType', ResourceType.Dataset)
        doi_info.add_provider_metadata('resourceText', 'event info')

        # Append dataset
        # List of  {"uid","name", "licence","count"}
        if collectory_info:
            dataset = {"uid": collectory_info["uid"], "name": collectory_info["name"]}

            if collectory_info.get("rights"):
                dataset["licence"] = collectory_info["rights"]
            elif collectory_info.get("licenseType"):
                dataset["licence"] = collectory_info["licenseType"]
            else:
                dataset["licence"] = ""

            doi_info.add_licence(dataset["licence"])

            doi_info.applicationMetadata["datasets"].append(dataset)

            # creator: list of {name, type:'Producer'}
            doi_info.providerMetadata["creator"].append({"name": collectory_info["name"], "type": "Producer"})

        return doi_info

    def add_creator(self, who_request_download):
        self.userId = who_request_download
        self.providerMetadata["contributors"].append({"name": who_request_download, "type": "Distributor"})

    # Demo purpose only
    @staticmethod
    def extra_provider_metadata(doi_info):
        # contributors: list of {name, type:'Distributor'}
        # name should be the dataset name
        doi_info.providerMetadata["contributors"].append(
            {"name": "User who created this download", "type": "Distributor"}
        )
        # creator: list of {name, type:'Producer'}
        doi_info.providerMetadata["creator"].append({"name": "dataset name", "type": "Producer"})

    @staticmethod
    def extra_application_metadata(doi_info):
        doi_info.applicationMetadata["searchUrl"] = "Query url from Event UI"
        doi_info.applicationMetadata["requestedOn"] = ""
        doi_info.applicationMetadata["recordCount"] = ""
        doi_info.applicationMetadata["queryTitle"] = ""
        doi_info.applicationMetadata["dataProfile"] = ""

    def add_licence(self, licence):
        self.licence.append(licence)

    def add_authorised_role(self, role):
        self.authorisedRoles.append(role)

    def add_provider_metadata(self, key, value):
        self.providerMetadata[key] = value

    def add_application_metadata(self, key, value):
        self.applicationMetadata[key] = value


class DOIService:
    @staticmethod
    def create(doi_server, apikey, doi_info):
        headers = {'content-type': 'application/json', 'apiKey': apikey}
        json_str = json.dumps(doi_info.__dict__)
        response = ala_helper.http_request_with_retry("POST", doi_server + '/api/doi', data=json_str, headers=headers)
        if response.status_code == 200 or response.status_code == 201:
            result = response.json()
            return {'status': True, 'doi': result['doi'], 'uuid': result['uuid'], 'url': result['landingPage']}
        else:
            return {'status': False}

    @staticmethod
    # updated_info : JSON object.
    def update(doi_server, apikey, uuid, updated_info):
        headers = {'content-type': 'application/json', 'apiKey': apikey}
        data = json.dumps(updated_info)
        response = ala_helper.http_request_with_retry(
            "PUT", doi_server + '/api/doi/' + uuid, data=data, headers=headers
        )
        if response.status_code == 200 or response.status_code == 201:
            result = response.json()
            return {'status': True, 'doi': result['doi'], 'uuid': result['uuid']}
        else:
            print("Status code: " + str(response.status_code))
            print("Status code: " + response.reason)
            print("Status code: " + response.text)
            return {'status': False}

    @staticmethod
    def upload_file(doi_server, apikey, doi_id, content):
        headers = {'apiKey': apikey}
        upload_file = io.StringIO(content)
        files = {'file': ('doi.txt', upload_file)}

        data = {'json': (None, json.dumps({}), 'application/json')}

        response = ala_helper.http_request_with_retry(
            "PUT", doi_server + '/api/doi/' + doi_id, files=files, data=data, headers=headers
        )
        if response.status_code == 200 or response.status_code == 201:
            result = response.json()
            return {'status': True, 'doi': result['doi'], 'uuid': result['uuid']}
        else:
            return {'status': False}

    def list(doi_server):
        response = ala_helper.http_request_with_retry("GET", doi_server + '/api/doi')
        return response

    # Cannot write file to S3
    # S3 does not offer file manipulation
    @staticmethod
    def generate_docs(doi_download_url, data_provider, data_provider_url, doi_info, dst_folder):
        files = [dst_folder + "/doi.txt", dst_folder + "/README.HTML"]
        try:
            with open(dst_folder + "/doi.txt", "w") as doi_file:
                doi_file.write(doi_download_url)

            readme_template_files = glob.glob(os.getcwd() + "/**/README.HTML.template", recursive=True)
            # Template file is stored in /tmp on EMR
            readme_tmp_template_files = glob.glob("/tmp/README.HTML.template", recursive=True)
            readme_template_files += readme_tmp_template_files
            if len(readme_template_files) > 0:
                with open(readme_template_files[0], 'r') as file:
                    readme_template = file.read()
                    template = Template(readme_template)
                    html_result = template.safe_substitute(
                        search_query=doi_info.applicationMetadata["searchUrl"],
                        query_time=doi_info.applicationMetadata["requestedOn"],
                        doi_download_url=doi_download_url,
                        data_provider=data_provider,
                        data_provider_url=data_provider_url,
                    )
                    with open(dst_folder + "/README.HTML", "w") as readme_file:
                        readme_file.write(html_result)
            else:
                print("Cannot generate README.html due to missing README.HTML template")

        except Exception as e:
            print("Generating documents failed. " + e)

        return files

    def append_docs_to_zip(src_files, target_zip):
        for src_file in src_files:
            with zipfile.ZipFile(target_zip, 'a') as zipf:
                if os.path.exists(src_file):
                    zipf.write(src_file, os.path.basename(src_file))


class CollectoryService:
    # Collect info of datasets
    @staticmethod
    def get_resource_info(collectory_server, dataset_uid):
        resource_url = f'{collectory_server}dataResource/{dataset_uid}'
        print('Using URL ' + resource_url)
        response = ala_helper.http_request_with_retry("GET", resource_url)
        if response.status_code == 200 or response.status_code == 201:
            result = response.json()
            provider = ''
            if result.get('provider'):
                provider = result['provider'].get('name')
            return {
                'status': True,
                'uid': result['uid'],
                'name': result['name'],
                'description': result['pubDescription'],
                'websiteUrl': result['websiteUrl'],
                'alaPublicUrl': result['alaPublicUrl'],
                'provider': provider,
                'rights': result.get('rights'),
                'licenseType': result.get('licenseType'),
            }
        else:
            print(f'Status code from collectory {response.status_code}')
            return {'status': False}


class S3Service:
    # Running on AWS directly
    @staticmethod
    def init_on_aws():
        s3_service = S3Service()
        s3_service.s3_client = boto3.client('s3', region_name='ap-southeast-2')
        return s3_service

    # Running on your local dev machine
    # Make sure profile of 'prod-extended-data' exist in .aws/config
    @staticmethod
    def init_on_local():
        s3_service = S3Service()
        s3_session = boto3.Session(profile_name='prod-extended-data')
        s3_service.s3_client = s3_session.client('s3', region_name='ap-southeast-2')
        return s3_service

    def generate_download_url(self, s3_bucket, dataset_id, file_id):
        file_key = 'exports/' + file_id + '/' + dataset_id + '.zip'
        s3_url = self.s3_client.generate_presigned_url(
            ClientMethod='get_object', Params={'Bucket': s3_bucket, 'Key': file_key}, ExpiresIn=86400
        )
        return s3_url

    def download_file(self, s3_bucket, dataset_id, file_id, dst_folder):
        file_key = 'exports/' + file_id + '/' + dataset_id + '.zip'
        dst_file = dst_folder + '/' + dataset_id + '-' + file_id + '.zip'
        self.s3_client.download_file(s3_bucket, file_key, dst_file)
        if os.path.exists(dst_file):
            return dst_file

    # Upload data ([file_id].zip) file to S3
    def upload_file(self, s3_bucket, dataset_id, file_id, src_folder):
        file_key = 'exports/' + file_id + '/' + dataset_id + '.zip'
        self.s3_client.upload_file(src_folder + '/' + dataset_id + '.zip', s3_bucket, file_key)

    # Upload file to s3, e.g. doi.txt
    def upload_doc(self, s3_bucket, src_file, dst_file):
        self.s3_client.upload_file(src_file, s3_bucket, dst_file)

    # Read URL generated by DOI
    def read_doi(self, s3_bucket, dataset_id, file_id):
        doi_file = 'exports/' + file_id + f'/{dataset_id}_doi.json'
        f = io.BytesIO()
        self.s3_client.download_fileobj(s3_bucket, doi_file, f)
        return json.loads(f.getvalue())

    def get_size(self, s3_bucket, dataset_id, file_id):
        file_key = 'exports/' + file_id + '/' + dataset_id + '.zip'
        response = self.s3_client.head_object(Bucket=s3_bucket, Key=file_key)
        size = response['ContentLength']
        return str(round((size / 1024) / 1024, 2))

    @staticmethod
    def create_tmp_folder(file_id):
        tmp_folder = tempfile.gettempdir() + "/" + file_id
        Path(tmp_folder).mkdir(parents=True, exist_ok=True)
        return tmp_folder


if __name__ == '__main__':
    """
    args:
    --user=Tester --OPTIONAL
    --request_id=Request ID
    --apiKey=API key from the corresponding auth server
    --dataset_list="dr10487 dr18527"
    --doi_server=https://doi-test.ala.org.au
    --collectory_server=https://collections-test.ala.org.au
    --s3_bucket=ala-extendeddata
    --search_query=https://event.ala.org.au/search
    """
    parser = argparse.ArgumentParser(
        description='Append doi.txt, README.HTML to the generated data file and uploat it to S3, and finally create a DOI record'
    )
    parser.add_argument("--user")
    parser.add_argument("--request_id", type=str, required=True)
    parser.add_argument("--dataset_list", required=True)
    parser.add_argument("--s3_bucket", required=True)
    parser.add_argument("--doi_server", required=True)
    parser.add_argument("--apiKey", required=True)
    parser.add_argument("--collectory_server", required=True)
    parser.add_argument("--search_query", required=False)
    parser.add_argument("--query_time", required=False)
    parser.add_argument("--quality_filters", required=False)

    print("Starting DOI service")
    args = vars(parser.parse_args())

    user = args['user'] if args['user'] else 'Atlas Living Australia'
    request_id = args['request_id']
    apiKey = args['apiKey']
    dataset_list_str = args['dataset_list']
    doi_server = args['doi_server']
    collectory_server = args['collectory_server']
    s3_bucket = args['s3_bucket']
    search_query = args['search_query']

    # /tmp/pipelines-export/{requestID}/
    tmp_folder = "/tmp/pipelines-export/" + request_id
    Path(tmp_folder).mkdir(parents=True, exist_ok=True)

    # split the space separated list
    dataset_list = dataset_list_str.split()

    # for each datasetID, upload to DOI server
    for dataset_id in dataset_list:
        # This data_file will be copied to S3
        data_file = f"/tmp/pipelines-export/{request_id}/{dataset_id}.zip"

        print("Checking file available: " + data_file)

        # Check if the generated dataset zip file exists
        if os.path.exists(data_file):

            print("Checking CollectoryService. File available " + data_file)

            dataset_info = CollectoryService.get_resource_info(collectory_server, dataset_id)
            if dataset_info['status']:

                print("creating DOIInfo ")
                doi_info = DOIInfo.new(dataset_info)
                if search_query and args['search_query'].lower() != 'none':
                    doi_info.applicationMetadata["searchUrl"] = search_query
                else:
                    doi_info.applicationMetadata["searchUrl"] = "https://events-test.ala.org.au"
                #  request time - String
                if args['query_time'] and args['query_time'].lower() != 'none':
                    doi_info.applicationMetadata["requestedOn"] = args['query_time']
                else:
                    doi_info.applicationMetadata["requestedOn"] = datetime.now().strftime("%d/%m/%Y, %H:%M:%S")

                doi_info.applicationUrl = doi_info.applicationMetadata["searchUrl"]
                doi_info.add_creator(user)

                # Create a record first
                print("Starting DOI process ")
                # print(json.dumps(doi_info.__dict__, indent=2))
                result = DOIService.create(doi_server, apiKey, doi_info)

                if result['status']:
                    print("DOI Record created: " + result['url'])
                    # generate readme.html, doi.text and headers.csv
                    docs = DOIService.generate_docs(
                        result['url'], dataset_info['name'], dataset_info['alaPublicUrl'], doi_info, tmp_folder
                    )
                    DOIService.append_docs_to_zip(docs, data_file)

                    # Create S3 client on AWS
                    s3_service = S3Service.init_on_aws()
                    # Create S3 client on local with a given profile
                    # s3_service = S3Service.init_on_local()
                    s3_service.upload_file(s3_bucket, dataset_id, request_id, tmp_folder)
                    download_url = s3_service.generate_download_url(s3_bucket, dataset_id, request_id)
                    file_size_mb = s3_service.get_size(s3_bucket, dataset_id, request_id)

                    # Upload extra doi info to S3 for airflow' reading ONLY
                    doi_src_file = tmp_folder + f'/{dataset_id}_doi.json'
                    doi_dst_file = 'exports/' + request_id + f'/{dataset_id}_doi.json'
                    with open(doi_src_file, 'w') as f:
                        json.dump(result, f)

                    print("pushing from " + doi_src_file)
                    print("pushing to s3://" + s3_bucket + "/" + doi_dst_file)
                    s3_service.upload_doc(s3_bucket, doi_src_file, doi_dst_file)
                    print(doi_dst_file + " uploaded to S3")

                    uuid = result['uuid']
                    data_file = {"fileUrl": download_url}
                    # update data file
                    updated = DOIService.update(doi_server, apiKey, uuid, data_file)
                    if updated['status']:
                        print('DOI updated')
                    else:
                        print('Updating DOI failed!')
                else:
                    print('Creating DOI failed!')
            else:
                print('Bad response from collectory server')
                print(dataset_info)
        else:
            print(data_file + " does not exist.")

    print("Finished")

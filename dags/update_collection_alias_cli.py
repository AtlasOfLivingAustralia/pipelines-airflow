import json
import random
import sys
import urllib.request as request
import urllib.parse
import argparse
import re
from distutils.util import strtobool
import time

checks_available = [
    "check_total_count",
    "check_data_resources",
    "check_min_fields_for_random_records",
    "check_compare_random_records",
    "check_conservation_status",
    "check_spatial_layer_country",
    "check_spatial_layer_state",
    "check_sensitivity",
    "check_species_list_uid",
    "check_spatial_layer_cl959",
    "check_spatial_layer_cl1048",
    "check_spatial_layer_cl966",
    "check_spatial_layer_cl21"
]
checks_available.sort()

RETRY_COUNT = 10
RETRY_SLEEP_MILLI_SEC = 1 * 60 * 1000
NUMBER_OF_COLLECTIONS_TO_KEEP = 1
MIN_FACET_RECORD_COUNT = 1000

class ResultStatus:
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    SKIP = "SKIP"


def parse_params(kwargs):
    global collection_to_keep
    global minimum_field_count
    global random_records_count
    global new_collection
    global old_collection
    global run_checks_only
    global remove_collection_if_fails
    global solr_base
    global solr_base_new
    global solr_alias
    if 'solr_base' not in globals():
        solr_base = kwargs['solr_base']
        solr_base_new = kwargs['solr_base_new']
        collection_to_keep = kwargs['collection_to_keep']
        minimum_field_count = kwargs.get('minimum_field_count', 10)
        random_records_count = kwargs.get('random_records_count', 50)
        new_collection = kwargs['new_collection']
        old_collection = kwargs['old_collection']
        run_checks_only = kwargs.get('run_checks_only', 'false')
        remove_collection_if_fails = kwargs.get('remove_collection_if_fails', 'false')
        solr_alias = kwargs['solr_alias']


def json_parse(base_url: str, params, solr_cluster: str):
    url_str = solr_cluster + '/' + base_url + "?"
    retry_count = RETRY_COUNT
    while retry_count > 0:
        try:
            encoded_parameters = urllib.parse.urlencode(params)
            response = request.urlopen(url_str + encoded_parameters)
            response = json.loads(response.read().decode('utf-8'))
            return response
        except Exception as e:
            print(f"WARNING- Error reading url:{url_str} params:{params} Exception:{e}")
            retry_count -= 1
            if retry_count > 0:
                print(f"Retrying in {RETRY_SLEEP_MILLI_SEC / 1000} seconds.")
                time.sleep(RETRY_SLEEP_MILLI_SEC / 1000)
            else:
                print(f"SEVERE- Error in processing URL:{url_str} params:{params}")
                raise RuntimeError(f'Error in processing URL:{url_str} params:{params}')
    return None

def get_total_count(collection: str, solr_cluster: str):
    result = json_parse(f"{collection}/select", {'q': '*:*', 'rows': 0, 'wt': 'json', 'facet': 'false'},
                        solr_cluster=solr_cluster)
    if result is None:
        raise IOError(
            f"SEVERE- Error getting total count for collection: {collection}")
    return result['response']['numFound']


def check_minimum_field_count(minimum_field_count: int, record_id: str, collection):
    status = ResultStatus.PASS
    result = json_parse(f'{collection}/select', {'q': f'id:"{record_id}"', 'rows': "10", 'wt': 'json', 'facet': 'false'},
                        solr_cluster=solr_base_new if solr_base_new else solr_base)
    if result is None:
        print(
            f"SEVERE- Error checking minimum fields count. : minFieldCount: {minimum_field_count:,}, recordId: {record_id}, collection: {collection}")
        status = ResultStatus.FAIL
    else:
        docs = result['response']['docs']
        if len(docs) == 0 :
            status = ResultStatus.FAIL
            print(
                f"SEVERE- RECORD ID: {record_id} - No records found for the id in collection: {collection}")
        elif len(docs) > 1:
            status = ResultStatus.FAIL
            print(
                f"SEVERE- RECORD ID: {record_id} - There are {len(docs):,} records for the id in collection: {collection}")
        else:
            doc = docs[0]
            if len(doc) < minimum_field_count:
                status = ResultStatus.FAIL
                print(
                    f"SEVERE- RECORD ID: {record_id} - Number of fields are {len(doc):,} and minimum fields of {minimum_field_count:,} not met.")
    return status


def compare_records(record_id: str):
    # These fields are  part of the assertions and are not expected to be in the new index till the assertion-sync job runs
    assertion_fields = ['userAssertions', 'userVerified', 'hasUserAssertions', 'assertionUserId', 'lastAssertionDate']
    status = ResultStatus.PASS
    result = json_parse(f'{new_collection}/select',
                        {'q': 'id:' + record_id, 'rows': "10",
                         'wt': 'json', 'facet': 'false'},
                        solr_cluster=solr_base_new if solr_base_new else solr_base)
    if result is None:
        print(
            f"SEVERE- Error in comparing records. recordId: {record_id}, newCollection: {new_collection}, oldCollection: {old_collection}")
        return ResultStatus.FAIL
    new_docs = result['response']['docs']
    result = json_parse(f'{old_collection}/select',
                        {'q': 'id:' + record_id, 'rows': "10", 'wt': 'json', 'facet': 'false'}, solr_cluster=solr_base)
    if result is None:
        print(
            f"SEVERE- Error in comparing records. recordId: {record_id}, newCollection: {new_collection}, oldCollection: {old_collection}")
        return ResultStatus.FAIL
    old_docs = result['response']['docs']
    if len(new_docs) != 1 or len(old_docs) != 1:
        print(
            f"SEVERE- RECORD ID: {record_id} - There should be only one record for the id in collection: {new_collection} , newDocs#:{len(new_docs):,}, oldDocs#: {len(old_docs):,}")
        return ResultStatus.FAIL
    else:
        new_doc = new_docs[0]
        old_doc = old_docs[0]
        missing_field_set = old_doc.keys() - assertion_fields - new_doc.keys()
        if len(missing_field_set):
            print(
                f"SEVERE- RECORD ID:{record_id} - There are {len(missing_field_set):,} fields which are missing in the new records. Here is the list: {missing_field_set}")
            return ResultStatus.FAIL
        else:
            changed_fields = []
            new_changed_fields = []
            for key, value in new_doc.items():
                if key in old_doc:
                    if value is not None and value != old_doc[key]:
                        changed_fields.append(
                            {'field': key, 'old': old_doc[key], 'new': value})
                else:
                    new_changed_fields.append({'field': key, 'old': "None", 'new': value})
            if len(changed_fields):
                status = ResultStatus.WARN
                print(
                    f"WARNING- RECORD ID:{record_id} - There are {len(changed_fields):,} fields which have different values. Here is the list of the fields and their values: {changed_fields}")
            if len(new_changed_fields):
                print(
                    f"WARNING- RECORD ID:{record_id} - There are {len(new_changed_fields):,} fields which have different values. Here is the list of the fields and their values: {new_changed_fields}")

        added_field_set = new_doc.keys() - old_doc.keys()
        if len(added_field_set):
            status = ResultStatus.WARN
            print(
                f"WARNING- RECORD ID:{record_id} - There are {len(added_field_set):,} fields which are added to the new record. Here is the list: {added_field_set}")

    print(f"INFO- returning status: {status}")
    return status


def check_min_fields_for_random_records(**kwargs):
    parse_params(kwargs)
    records_number = random_records_count
    record_ids = get_random_record_ids(old_collection, records_number, solr_cluster=solr_base)
    if record_ids is None:
        print(
            f"SEVERE- Error in the check of min fields for random records. recordsNumber:{records_number}")
        return ResultStatus.FAIL

    if len(record_ids) < records_number:
        # This can occur legitimately when entire data resources are deleted and end up with 0 as the record facet count
        print("WARNING- Found less than the expected number of random record ids")
    ret_status = None
    status_list = []
    for i in range(len(record_ids)):
        if record_ids[i] is None:
            print("SEVERE- Found a null missing recordId (min field check), failing.")
            status_list.append(ResultStatus.FAIL)
        elif record_ids[i].lower() == "null":
            print("SEVERE- Found a null literal recordId (min field check), failing.")
            status_list.append(ResultStatus.FAIL)
        else:
            status_list.append(check_minimum_field_count(
                minimum_field_count, record_ids[i], new_collection))

    if ResultStatus.FAIL in status_list:
        ret_status = ResultStatus.FAIL
        print(
            f"SEVERE- There are {len([s for s in status_list if s == ResultStatus.FAIL]):,} FAILED records out of {records_number:,} checked ones.")
    elif ResultStatus.WARN in status_list:
        ret_status = ResultStatus.WARN
        print(
            f"WARNING- There are {len([s for s in status_list if s == ResultStatus.WARN]):,}  WARNING records out of {records_number:,} checked ones.")
    else:
        ret_status = ResultStatus.PASS
        print(
            f"INFO- All {records_number:,} records checks passed successfully.")

    return ret_status


def get_facet_data(collection: str, facet_field: str, q: str, facet_limit: int, sort: str,
                   solr_cluster: str):
    result = json_parse(f'{collection}/select',
                        {'q': q, 'rows': 0, 'wt': 'json', 'facet.field': facet_field, 'facet': 'on',
                         'facet.limit': facet_limit, 'json.nl': 'map', "facet.sort": sort},
                        solr_cluster=solr_cluster)
    if result is None:
        print(
            f"SEVERE- Error in getting facet data. collection:{collection}, facetField:{facet_field}, q:{q}, faceLimit:{facet_limit}, sort:{sort}")
        return None
    return result['facet_counts']['facet_fields']


def get_data_resource_list(collection: str, solr_cluster: str):
    data_resources = \
        get_facet_data(collection, 'dataResourceUid', 'dataResourceUid:*', -1, "index", solr_cluster=solr_cluster)[
            'dataResourceUid']
    return data_resources


def get_random_record_ids(collection: str, records_number: int, solr_cluster: str):
    retries = RETRY_COUNT
    while True:
        data_resources = get_data_resource_list(collection, solr_cluster)
        if data_resources is None:
            print(
                f"SEVERE- Error in getting data resource list. collection: {collection}")
            return None

        if len(data_resources) < 1:
            retries -= 1
            if retries == 0:
                print(
                    f"SEVERE- Error in getting data resource list after {RETRY_COUNT} retries. collection:{collection}")
                return None
            print(
                f"WARNING- Problem in getting data resource list for collection:{collection}")
            print(f"Retrying in {RETRY_SLEEP_MILLI_SEC / 1000} seconds.")
        else:
            break
    record_ids = []
    for i in range(records_number):
        dr_offset = random.randrange(0, len(data_resources))
        data_resource = list(data_resources.keys())[dr_offset]
        if data_resources[data_resource] <= 0:
            print(
                f"WARNING - Randomly selected data resource {data_resource} has no records.")
            continue
        record_offset = random.randrange(
            0, min(data_resources[data_resource], 20000))

        print(
            f"Getting id for a record at offset {record_offset:,} in data resource {data_resource}, Dr#={data_resources[data_resource]} DrOffset={dr_offset:,}")

        result = json_parse(f'{collection}/select',
                            {'q': 'dataResourceUid:' + data_resource,
                             'rows': "1",
                             'wt': 'json',
                             'start': record_offset,
                             'fl': 'id',
                             'facet': 'false'}, solr_cluster)

        if result is None:
            print(
                f"SEVERE- Error in getting random record Ids. dataResource:{data_resource}, recordOffset:{record_offset:,}")
            return None
        if result['response']['docs'][0]['id'] is None:
            print(
                f"SEVERE- Error in getting random record Ids. result contained a null id. dataResource:{data_resource}, recordOffset:{record_offset:}, response:{result['response']}")
            return None

        if result['response']['docs'][0]['id'].lower() == "null":
            print(
                f"SEVERE- Error in getting random record Ids. result contained the literal null. dataResource:{data_resource}, recordOffset:{record_offset:,}, response:{result['response']}")
            return None

        next_record_id = result['response']['docs'][0]['id']

        print(
            f"Selected id {next_record_id} from data resource {data_resource}")

        record_ids.append(next_record_id)
    if len(record_ids) == 0:
        print("SEVERE - Unable to get any random record Ids.")
        return None

    return record_ids


def check_facet(facet: str, q: str, check_size: int = -1, fetch_size: int = -1, sort: str = "index"):
    old_facet_data = get_facet_data(
        old_collection, facet, q, check_size, sort, solr_cluster=solr_base)[facet]
    old_facet_data = {dr:cnt for dr, cnt in old_facet_data.items() if cnt > 0}
    new_facet_data = get_facet_data(new_collection, facet, q, fetch_size, sort,
                                    solr_cluster=solr_base_new if solr_base_new else solr_base)[
        facet]
    if new_facet_data is None or old_facet_data is None:
        print(
            f"SEVERE- Error in checking facet. facet:{facet} q:{q} limit:{check_size:,} sort:{sort}")
        return ResultStatus.FAIL
    status = ResultStatus.PASS
    ret_status = []
    for key, value in old_facet_data.items():
        if key in new_facet_data:
            cur_value = value
            new_value = new_facet_data[key]
            diff_count = new_value - cur_value
            diff_percentage = round(((cur_value - new_value) / cur_value) * 100)
            record_count_change_severe = True
            if cur_value < MIN_FACET_RECORD_COUNT and new_value > 0 :
                record_count_change_severe = False
            # More than 2% of records are missing this field value in the new index
            if diff_percentage > 2 and record_count_change_severe:
                status = ResultStatus.FAIL
                print(f"SEVERE- {diff_percentage:,}% of records don't have {facet}=\"{key}\" . Counts- Current#:{cur_value:,} New#: {new_value:,} Diff#:{diff_count:,}")
            elif new_facet_data[key] < value:
                status = ResultStatus.WARN
                print(
                    f"WARNING- {diff_percentage:,}% of records don't have {facet}=\"{key}\" . Counts- Current#:{cur_value:,} New#:{new_value:,} Diff#:{diff_count:,}")
            elif new_facet_data[key] > value:
                status = ResultStatus.PASS
                print(
                    f"INFO- There are more record having {facet}=\"{key}\" in the new index. Counts- Current#:{cur_value:,} New#:{new_value:,} Diff#:{diff_count:,}")
            else:
                status = ResultStatus.PASS
                print(
                    f"INFO- There are same number of records for {facet}=\"{key}\" in both indexes. Counts#:{cur_value:,}")
        else:
            status = ResultStatus.FAIL
            print(
                f"SEVERE- The new index doesn't have any record for {facet}=\"{key}\" .")
        ret_status.append(status)
    if ResultStatus.FAIL in ret_status:
        return ResultStatus.FAIL
    elif ResultStatus.WARN in ret_status:
        return ResultStatus.WARN
    return ResultStatus.PASS


def check_compare_random_records(**kwargs):
    parse_params(kwargs)
    records_number = random_records_count
    record_ids = get_random_record_ids(old_collection, records_number, solr_cluster=solr_base)
    ret_status = ResultStatus.PASS
    if not record_ids:
        print(
            f"SEVERE- Error in the check of comparing random records. recordsNumber:{records_number:,}")
        ret_status = ResultStatus.FAIL
    else:
        if len(record_ids) < records_number:
            # This can occur legitimately when entire data resources are deleted and end up with 0 as the record facet count
            print("WARNING- Found less than the expected number of random record ids")

        status_list = []
        for i in range(len(record_ids)):
            if record_ids[i] is None:
                print("SEVERE- Found a null missing recordId, failing.")
                status_list.append(ResultStatus.FAIL)
            elif record_ids[i].lower() == "null":
                print("SEVERE- Found a null literal recordId, failing.")
                status_list.append(ResultStatus.FAIL)
            else:
                status_list.append(compare_records(record_ids[i]))

        failure_count = len([it for it in status_list if it == ResultStatus.FAIL])
        warning_count = len([it for it in status_list if it == ResultStatus.WARN])
        if failure_count:
            ret_status = ResultStatus.FAIL
            print(
                f"SEVERE- There are {failure_count:,} FAILED records and {warning_count:,} WARNING records out of {len(record_ids):,} checked ones.")
        elif failure_count or warning_count:
            ret_status = ResultStatus.WARN
            print(
                f"WARNING- There are {failure_count:,} FAILED records and {warning_count:,} WARNING records out of {len(record_ids):,} checked ones.")
        else:
            ret_status = ResultStatus.PASS
            print(
                f"INFO- All {len(record_ids):,} records checks passed successfully.")
    return ret_status


def check_total_count(**kwargs):
    parse_params(kwargs)
    ret = ResultStatus.PASS
    old_count = get_total_count(old_collection, solr_cluster=solr_base)
    new_count = get_total_count(new_collection,
                                solr_cluster=solr_base_new if solr_base_new else solr_base)
    diff_count = new_count - old_count
    if old_count < 0 or new_count < 0:
        raise ValueError("SEVERE- Error getting total count for collections")
    if new_count == old_count:
        print(
            f"INFO- There are equal number of records in the new index to the current one. CURRENT#: {old_count:,} NEW#: {new_count:,} DIFF#: {diff_count:,}")
        ret = ResultStatus.PASS
    elif new_count > old_count:
        print(
            f"INFO- There are more records in the new index than the current one. CURRENT#: {old_count:,} NEW#: {new_count:,} DIFF#: {diff_count:,}")
        ret = ResultStatus.PASS
    else:
        print(
            f"SEVERE- There are less records in the new index than the current one. CURRENT#: {old_count:,} NEW#: {new_count:,} DIFF#: {diff_count:,}")
        ret = ResultStatus.FAIL
    return ret


def check_data_resources(**kwargs):
    parse_params(kwargs)
    return check_facet("dataResourceUid", "dataResourceUid:*")


def check_sensitivity(**kwargs):
    parse_params(kwargs)

    return check_facet("sensitive", "sensitive:*")


def check_species_list_uid(**kwargs):
    parse_params(kwargs)

    return check_facet("speciesListUid", "speciesListUid:*", 100, 100, "count")


def check_conservation_status(**kwargs):
    parse_params(kwargs)

    return check_facet("stateConservation", "stateConservation:*")


def check_spatial_layer_country(**kwargs):
    parse_params(kwargs)

    return check_facet("country", "country:*", 10, 100, "count")


def check_spatial_layer_state(**kwargs):
    parse_params(kwargs)

    return check_facet("stateProvince", "stateProvince:*", 10, 100, "count")


# Local government

def check_spatial_layer_cl959(**kwargs):
    parse_params(kwargs)

    return check_facet("cl959", "cl959:*", 10, 100, "count")


# IBRA 7 region
def check_spatial_layer_cl1048(**kwargs):
    parse_params(kwargs)

    return check_facet("cl1048", "cl1048:*", 10, 100, "count")


# IMCRA Meso-scale Bioregions
def check_spatial_layer_cl966(**kwargs):
    parse_params(kwargs)

    return check_facet("cl966", "cl966:*", 10, 100, "count")


# IMCRA 4 Regions
def check_spatial_layer_cl21(**kwargs):
    parse_params(kwargs)

    return check_facet("cl21", "cl21:*", 10, 100, "count")


def switch_collection_alias(**kwargs):
    parse_params(kwargs)
    ret_status = ResultStatus.PASS
    if run_checks_only:
        print(f"INFO- Switching collection alias is skipped as the run_checks_only param set to {run_checks_only}")
        ret_status = ResultStatus.SKIP
    else:
        result = json_parse('admin/collections',
                            {'action': 'CREATEALIAS', 'collections': new_collection, 'name': solr_alias,
                             'wt': 'json'}, solr_cluster=solr_base)
        if result is None:
            print(f"SEVERE- Error in switching alias {solr_alias} with collection:{new_collection}")
            ret_status = ResultStatus.FAIL
        elif result['responseHeader']['status'] == 0:
            print(f"PASS- The {solr_alias} alias is update successfully and now pointing to {new_collection}.")
            result = json_parse('admin/collections',
                                {'action': 'REBALANCELEADERS', 'collection': new_collection, 'wt': 'json'},
                                solr_cluster=solr_base)
            if result is None:
                print(f"SEVERE- Error in re-balancing collections. collection:{new_collection}")
                ret_status = ResultStatus.WARN
            else:
                ret_status = ResultStatus.PASS
        else:
            print(
                f"SEVERE- The {solr_alias} alias is not updated successfully to point to {new_collection}. "
                f"Here is the response from the server:\n {result['error']['msg']}")
            ret_status = ResultStatus.FAIL
    return ret_status


def remove_collection(collection):
    print('deleting COLLECTION:', collection)
    ret = ResultStatus.PASS
    result = json_parse('admin/collections', {'action': 'DELETE', 'name': collection, 'wt': 'json'},
                        solr_cluster=solr_base)
    if result is None:
        print('SEVERE- Error in deleting old collections. collection:', collection)
        ret = ResultStatus.FAIL
    elif result['responseHeader']['status'] == 0:
        print("INFO- The collection", collection, "is removed from the cluster successfully.")
        ret = ResultStatus.PASS
    else:
        print("WARNING- The collection", collection, "is not removed from the cluster successfully. ERROR:",
              result['error']['msg'])
        ret = ResultStatus.WARN
    return ret


def remove_old_collections(**kwargs):
    parse_params(kwargs)
    ret_status = ResultStatus.PASS
    if run_checks_only:
        print(f"INFO- Removing old collections is skipped as the run_checks_only param set to {run_checks_only}")
        ret_status = ResultStatus.SKIP
    else:
        result = json_parse('admin/collections', {'action': 'CLUSTERSTATUS', 'wt': 'json'}, solr_cluster=solr_base)
        if result is None:
            print("SEVERE- Error in getting list of collections on the SOLR cluster")
            ret_status = ResultStatus.FAIL
        elif result['responseHeader']['status'] == 0:
            collections = list(result['cluster']['collections'].keys())
            collections.sort(reverse=True)
            collections = [c for c in collections if
                           c not in [collection_to_keep, result['cluster']['aliases'][solr_alias]] and re.match(
                               r'^' + solr_alias + r'-[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{2}$', c)]
            print('current COLLECTIONS =', collections)
            for i in range(NUMBER_OF_COLLECTIONS_TO_KEEP, len(collections)):
                if remove_collection(collections[i]) != ResultStatus.PASS:
                    raise ValueError(f"Couldn't delete the collection successfully : {collections[i]}")
            ret_status = ResultStatus.PASS
        else:
            print("WARNING- The cluster status API is was not successful. ERROR: " + result['error']['msg'])
            ret_status = ResultStatus.FAIL

    return ret_status


def auto_all(**kwargs):
    parse_params(kwargs)
    ret_status = ResultStatus.PASS
    check_results = {func: globals()[func]() for func in checks_available}
    checks_passed = True
    for check, check_result in check_results.items():
        if check_result == ResultStatus.FAIL:
            print(f"SEVERE- The {check} failed. Please check the logs for more details.")
            checks_passed = False
    if checks_passed:
        print("INFO- All checks passed successfully. Switching collection...")
        sca_result = switch_collection_alias()
        if sca_result == ResultStatus.PASS:
            print("INFO- Switching collection alias is successful.")
            print("INFO- Removing old collections...")
            roc_result = remove_old_collections()
            if roc_result == ResultStatus.PASS:
                print("INFO- Removing old collections is successful.")
            else:
                print(f"INFO- Removing old collections returned {roc_result}.")
                ret_status = roc_result
        else:
            print(f"INFO- Switching collection didn't pass. Here is the result {sca_result}.")
            print(f"INFO- Removing old collections is skipped as the switch collection alias returned {sca_result}.")
            ret_status = sca_result
    else:
        print("SEVERE- Some checks failed. Please check the logs for more details.")
        print("INFO- Switching collection alias is skipped as some checks failed.")
        ret_status = ResultStatus.FAIL
    return ret_status


def main():
    funcs = ['auto_all'] + checks_available + ['switch_collection_alias', 'remove_old_collections']
    parser = argparse.ArgumentParser(
        description="Creates Solr Collection")
    parser.add_argument('-s', '--solr_base', help="Solr url eg: http://localhost:8983/solr", required=True)
    parser.add_argument('-l', '--solr_base_new', help="Solr url for another cluster to compare with the solar_base",
                        default=None)
    parser.add_argument('-n', '--new_collection', help="New Solr collection", required=True)
    parser.add_argument('-o', '--old_collection', help="Old Solr collection", default='biocache')
    parser.add_argument('-a', '--solr_alias', help="Solr collection alias", default='biocache')
    parser.add_argument('-k', '--collection_to_keep',
                        help="Solr collection that won't be deleted after alias update. eg: dev-collection", default='')
    parser.add_argument('-m', '--minimum_field_count',
                        help="Minimum number of fields having non-empty values for records", default=10, type=int)
    parser.add_argument('-r', '--random_records_count', help="Number of random records to check", default=50, type=int)
    parser.add_argument('-y', '--run_checks_only', help="Run the checks only without changes on the Solr cluster",
                        default=False, type=strtobool)
    parser.add_argument('-f', '--remove_collection_if_fails', help="Remove the created collection if the checks fail",
                        default=False, type=strtobool)
    parser.add_argument('-v', '--verbose', help='Verbose logging', type=strtobool, default=False)
    subparsers = parser.add_subparsers(title='subcommands', dest='action')
    for func in funcs:
        subparser = subparsers.add_parser(func)


    args = parser.parse_args()
    parse_params(vars(args))
    for func in funcs:
        if func == args.action:
            if func == 'auto_all':
                sys.exit(0 if auto_all() in [ResultStatus.PASS, ResultStatus.SKIP] else 1)
            elif globals()[func]() == ResultStatus.FAIL:
                sys.exit(1)
            break
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()

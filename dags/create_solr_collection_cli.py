import json
import urllib.request as request
import argparse


def fetch_data(url):
    print(f'Getting url:{url}')
    with request.urlopen(url) as response:
        if response.getcode() == 200:
            response = json.loads(response.read().decode('utf-8'))
            return response
        else:
            raise Exception(f'Error in getting URL: {url} \n Here is the response: {response.text}')


def get_live_nodes(solr_url):
    url = f"{solr_url}/admin/collections?action=CLUSTERSTATUS"
    live_nodes = fetch_data(url)['cluster']['live_nodes']
    live_nodes.sort()
    print(f'There are {len(live_nodes)} live nodes in the cluster: {live_nodes}')
    return live_nodes


def create_solr_collection(collection, solr_url, solr_config_set):
    nodes = get_live_nodes(solr_url)
    url = f"{solr_url}/admin/collections?action=CREATE&name={collection}&numShards={len(nodes)}&createNodeSet=EMPTY&" \
          f"collection.configName={solr_config_set}"
    fetch_data(url)
    print(f'Solr schema is created successfully using URL:{url}')


def add_initial_replicas(collection, solr_url):
    nodes = get_live_nodes(solr_url)
    for i in range(0, len(nodes)):
        url = f"{solr_url}/admin/collections?action=ADDREPLICA&collection={collection}&shard=shard{i + 1}&" \
              f"node={nodes[i]}&preferredLeader=true"
        fetch_data(url)
        print(f'Solr replica is added successfully using URL:{url}')


def add_additional_replicas(collection, solr_url, rf=1):
    nodes = get_live_nodes(solr_url)
    for i in range(1, len(nodes) + 1):
        for j in range(1, rf):
            node_idx = (i + j - 1) % len(nodes)
            url = f"{solr_url}/admin/collections?action=ADDREPLICA&collection={collection}&shard=shard{i}&" \
                  f"node={nodes[node_idx]}&preferredLeader=false"
            fetch_data(url)
            print(f'Solr replica is added successfully using URL:{url}')


def main():
    parser = argparse.ArgumentParser(
        description="Creates Solr Collection")
    parser.add_argument('-s', '--solr', help="Solr url eg: http://localhost:8983/solr", required=True)
    parser.add_argument('-c', '--configset', help="Solr config set", required=True)
    parser.add_argument('-r', '--rf', help="Replication factor for Solr collection. 0 means no additional replica", default=0, type=int)
    parser.add_argument('-a', '--action', help="Action to execute", required=True,
                        choices=['create_solr_collection', 'add_initial_replicas', 'add_additional_replicas'])
    parser.add_argument('-v', '--verbose', help='Verbose logging', action='store_true', default=False)
    parser.add_argument('collection', type=str, help='Solr collection name')

    args = parser.parse_args()
    collection = args.collection
    solr_url = args.solr
    solr_config_set = args.configset
    action = args.action

    if action == 'create_solr_collection':
        create_solr_collection(collection, solr_url, solr_config_set)
    elif action == 'add_initial_replicas':
        add_initial_replicas(collection, solr_url)
    elif action == 'add_additional_replicas':
        add_additional_replicas(collection, solr_url, args.rf)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()

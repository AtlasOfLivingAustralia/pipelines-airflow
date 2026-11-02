import argparse
import json
import sys
import time
import urllib.request as request


def fetch_data(url, timeout=30):
    print(f'Getting url:{url}')
    with request.urlopen(url, timeout=timeout) as response:
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


def wait_for_replicas_active(collection, solr_url, expected_rf=None, timeout_seconds=1800, poll_interval=15):
    """Wait until all shards and replicas for collection are active and on live nodes.

    Verifies:
      a) Every shard exists and has state == "active".
      b) If expected_rf is provided, every shard has at least expected_rf replicas.
      c) Every replica in every shard has state == "active".
      d) The node hosting the replica is present in live_nodes.
    """
    start_time = time.time()
    url = f"{solr_url}/admin/collections?action=CLUSTERSTATUS&collection={collection}&wt=json"

    while True:
        elapsed = int(time.time() - start_time)
        try:
            data = fetch_data(url, timeout=30)
            cluster = data.get('cluster', {})
            collections = cluster.get('collections', {})
            live_nodes = set(cluster.get('live_nodes', []))

            if collection not in collections:
                print(f"Collection {collection} not found in CLUSTERSTATUS (elapsed: {elapsed}s). Retrying...")
            else:
                shards = collections[collection].get('shards', {})
                num_shards = len(shards)
                total_replicas = 0
                active_replicas = 0
                all_shards_active = True
                all_replicas_active = True
                inactive_details = []

                if num_shards == 0:
                    all_shards_active = False
                    inactive_details.append("No shards found")

                for shard_name, shard_info in shards.items():
                    shard_state = shard_info.get('state')
                    if shard_state != 'active':
                        all_shards_active = False
                        inactive_details.append(f"Shard {shard_name} state='{shard_state}'")

                    replicas = shard_info.get('replicas', {})
                    if expected_rf is not None and len(replicas) < expected_rf:
                        all_replicas_active = False
                        inactive_details.append(
                            f"Shard {shard_name} has {len(replicas)} replicas, expected at least {expected_rf}"
                        )

                    for replica_name, replica_info in replicas.items():
                        total_replicas += 1
                        rep_state = replica_info.get('state')
                        rep_node = replica_info.get('node_name')
                        node_live = rep_node in live_nodes

                        if rep_state == 'active' and node_live:
                            active_replicas += 1
                        else:
                            all_replicas_active = False
                            reasons = []
                            if rep_state != 'active':
                                reasons.append(f"state='{rep_state}'")
                            if not node_live:
                                reasons.append(f"node '{rep_node}' not in live_nodes")
                            inactive_details.append(f"Replica {replica_name} ({shard_name}): {', '.join(reasons)}")

                print(
                    f"Checking replica status: {active_replicas}/{total_replicas} replicas active across "
                    f"{num_shards} shards (elapsed: {elapsed}s)"
                )

                if all_shards_active and all_replicas_active and total_replicas > 0:
                    print(f"All {total_replicas} replicas across {num_shards} shards are active and healthy.")
                    return True

                if inactive_details:
                    print(f"Waiting for replicas/shards to become active: {'; '.join(inactive_details)}")

        except Exception as e:
            print(f"Error querying Solr cluster status (elapsed: {elapsed}s): {e}")

        if time.time() - start_time >= timeout_seconds:
            raise TimeoutError(
                f"Timed out after {timeout_seconds}s waiting for replicas of collection '{collection}' to become active."
            )

        time.sleep(poll_interval)


def add_additional_replicas(collection, solr_url, rf=1, wait=True, timeout_seconds=1800, poll_interval=15):
    nodes = get_live_nodes(solr_url)
    for i in range(1, len(nodes) + 1):
        for j in range(1, rf):
            node_idx = (i + j - 1) % len(nodes)
            url = f"{solr_url}/admin/collections?action=ADDREPLICA&collection={collection}&shard=shard{i}&" \
                  f"node={nodes[node_idx]}&preferredLeader=false"
            fetch_data(url)
            print(f'Solr replica is added successfully using URL:{url}')

    if wait and rf > 1:
        print(f"Waiting for replicas to become active (expected rf={rf})...")
        wait_for_replicas_active(
            collection=collection,
            solr_url=solr_url,
            expected_rf=rf,
            timeout_seconds=timeout_seconds,
            poll_interval=poll_interval,
        )


def main():
    parser = argparse.ArgumentParser(
        description="Creates Solr Collection")
    parser.add_argument('-s', '--solr', help="Solr url eg: http://localhost:8983/solr", required=True)
    parser.add_argument('-c', '--configset', help="Solr config set", required=False, default=None)
    parser.add_argument('-r', '--rf', help="Replication factor for Solr collection. 0 means no additional replica", default=0, type=int)
    parser.add_argument('-a', '--action', help="Action to execute", required=True,
                        choices=['create_solr_collection', 'add_initial_replicas', 'add_additional_replicas', 'wait_for_replicas_active'])
    parser.add_argument('-v', '--verbose', help='Verbose logging', action='store_true', default=False)
    parser.add_argument('--wait', dest='wait', action='store_true', default=True, help='Wait for replicas to become active')
    parser.add_argument('--no-wait', dest='wait', action='store_false', help='Do not wait for replicas to become active')
    parser.add_argument('--timeout', help='Timeout in seconds when waiting for replicas (default 1800)', type=int, default=1800)
    parser.add_argument('collection', type=str, help='Solr collection name')

    args = parser.parse_args()
    collection = args.collection
    solr_url = args.solr
    solr_config_set = args.configset
    action = args.action

    if action == 'create_solr_collection':
        if not solr_config_set:
            parser.error("--configset is required for create_solr_collection")
        create_solr_collection(collection, solr_url, solr_config_set)
    elif action == 'add_initial_replicas':
        add_initial_replicas(collection, solr_url)
    elif action == 'add_additional_replicas':
        add_additional_replicas(collection, solr_url, args.rf, wait=args.wait, timeout_seconds=args.timeout)
    elif action == 'wait_for_replicas_active':
        wait_for_replicas_active(collection, solr_url, expected_rf=args.rf if args.rf > 0 else None, timeout_seconds=args.timeout)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()

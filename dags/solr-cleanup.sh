#!/bin/bash

# Set the directory to sort
DIR="/data/solr"

# Size threshold in bytes for indexes to be deleted, default 3MB
SIZE_THRESHOLD=3000000

# Alias pattern to match the collections starting with biocache-
ALIAS_PATTERN="biocache"

# Number of cores that can stay on the node, default 4
NUM_CORES_THRESHOLD=4
# Get number of nodes in the cluster
NUM_NODES=$(curl --silent "http://localhost:8983/solr/admin/collections?action=CLUSTERSTATUS&wt=json" | jq '.cluster.live_nodes | length')
echo "Number of nodes in the cluster: $NUM_NODES"
echo
# Get list of the collections
COLLECTIONS=$(curl --silent "http://localhost:8983/solr/admin/collections?action=LIST&wt=json" | jq -r '.collections[]')
echo "Collections on the cluster: $COLLECTIONS"
echo
# Get all subdirectories over threshold size and sort them alphabetically descending by name
# Then skip the top 4 and just return the rest

# Rebalancing function that accepts list of collections
function rebalance_cluster() {
  COLLECTIONS=$1
  echo "Rebalancing the cluster for existing collections"
  for collection in $COLLECTIONS
  do
      echo "redistributing leaders for collection: $collection"
      curl --silent "http://localhost:8983/solr/admin/collections?action=BALANCESHARDUNIQUE&collection=${collection}&property=preferredLeader&wt=json"
      echo "Rebalancing collection: $collection"
      curl --silent "http://localhost:8983/solr/admin/collections?action=REBALANCELEADERS&collection=${collection}&wt=json"
  done
  echo
  echo
}


# Rebalance the cluster before start
echo "Rebalancing the cluster before start"
rebalance_cluster "$COLLECTIONS"

# FILEPATH: Untitled-1
collection_dirs=$(du -s $DIR/$ALIAS_PATTERN-*)

# FILEPATH: Untitled-1
echo "collection_dirs: $collection_dirs"
echo
#Find directories over threshold size
over_size_dirs=$(echo "${collection_dirs}" | awk -v size=$SIZE_THRESHOLD '$1 > size {print $1, $2}')
echo "$ALIAS_PATTERN index dirs: $over_size_dirs"
echo
#Sort by name and skip the top 4
delete_dirs=$(echo "${over_size_dirs}" | sort -k2 -r | awk -v threshold=$NUM_CORES_THRESHOLD 'NR>threshold {print $2}')

if [ -z "$delete_dirs" ]
then
    echo "No directories to delete"
    exit 0
fi

echo "Directories to be deleted: $delete_dirs"
echo "${delete_dirs}" | while read FILENAME
do
    # Get the size and directory name
    DIRNAME=$(echo $FILENAME | cut -d' ' -f2)
    echo "Deleting $DIRNAME"
    rm -rf $DIRNAME
done
echo
echo
# Restart solr
echo "Restarting solr"
service solr restart

# Waiting till solr is up
echo "Waiting till solr is up"
until $(curl --output /dev/null --silent --head --fail http://localhost:8983/solr); do
    printf '.'
    sleep 5
done
echo
echo

#Check if the node has joined to the cluster
echo "Checking if the node has joined to the cluster by checking number of live nodes using jq"
until [ $(curl --silent "http://localhost:8983/solr/admin/collections?action=CLUSTERSTATUS&wt=json" | jq '.cluster.live_nodes | length') -eq $NUM_NODES ]; do
    printf '.'
    sleep 5
done
echo
echo
# Rebalance the cluster
echo "Rebalancing the cluster"
rebalance_cluster "$COLLECTIONS"


echo "All done!"
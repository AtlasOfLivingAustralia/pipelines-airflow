#!/bin/bash
set -x
# Check if an argument was provided
if [ -z "$1" ]; then
  echo "Error: No argument provided."
  exit 1
fi

IFS=',' read -ra es_hosts <<< "$1"

es_hosts_cleaned=()
for element in "${es_hosts[@]}"; do
  es_host="$(echo -e "${element}" | tr -d '[:space:]')"
  es_hosts_cleaned+=("$es_host")
done

es_host_to_use="${es_hosts_cleaned[0]}"

es_alias=$2

for ((i = 3; i <= $#; i++ )); do
  export datasetId=${!i}
  echo "Deleting dataset from elastic: $datasetId"
  echo "curl -X DELETE $es_host_to_use/$es_alias_$datasetId"
  response_code=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "${es_host_to_use}/${es_alias}_$datasetId")
  echo "Response code: $response_code"
done

echo 'Completed delete of datasets from elastic'
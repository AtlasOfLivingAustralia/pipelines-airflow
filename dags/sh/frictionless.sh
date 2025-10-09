#!/bin/bash
set -x
for ((i = 1; i <= $#; i++ )); do

  export datasetId=${!i}

  echo $datasetId ' - Frictionless'

  sudo -u hadoop /usr/local/bin/fdwca /tmp/pipelines-export/${datasetId}.zip /tmp/pipelines-export/${datasetId}-fr.zip
  sudo -u hadoop mv /tmp/pipelines-export/${datasetId}-fr.zip /tmp/pipelines-export/${datasetId}.zip

done

echo 'Completed upload of datasets'
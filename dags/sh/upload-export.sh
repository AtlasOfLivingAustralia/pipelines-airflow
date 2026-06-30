#!/bin/bash
set -x
s3_bucket=$1
sudo -u hadoop aws s3 cp /tmp/pipelines-export/ s3://$s3_bucket/dwca-exports/ --recursive --exclude "*" --include "*.zip"
echo 'Completed upload of datasets'
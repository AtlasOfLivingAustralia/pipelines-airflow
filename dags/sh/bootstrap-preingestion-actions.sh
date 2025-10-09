#!/bin/bash
set -x
sudo update-alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3.9 3
ami_id=`curl http://169.254.169.254/latest/meta-data/ami-id`
echo "CUSTOM_AMI_ID: $ami_id"

export S3_BUCKET=$1
echo "S3 bucket to use: $S3_BUCKET"

# create directories
sudo mkdir -p /data/dmgt/preingestion

sudo aws s3 cp s3://$S3_BUCKET/preingestion/  /data/dmgt/preingestion  --recursive --include "*"
sudo chmod +x /data/dmgt/preingestion/preingest.sh

lsb_release -a
uname -a

python3 -V
python3 -m pip -V
python3 -m pip install -r /data/dmgt/preingestion/requirements.txt
echo "Running sudo chown -R hadoop:hadoop /data/"
sudo chown -R hadoop:hadoop /data/
echo "Running ls -lah /data/"
ls -lah /data/

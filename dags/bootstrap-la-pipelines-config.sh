#!/bin/bash
set -x
export S3_BUCKET=$1
export ALA_API_KEY=$2
export REGISTRY_URL=$3
export LISTS_URL=$4
export SDS_URL=$5
export DOI_URL=$6
export NAME_MATCHING_URL=$7
export SAMPLING_URL=$8
export IMAGES_URL=$9
export SOLR_URL=${10}
export ZK_URL=${11}
export SOLR_COLLECTION=${12}
export SOLR_CONFIGSET=${13}
export ES_HOSTS=${14}
export ES_ALIAS=${15}

echo "S3_BUCKET $S3_BUCKET"
echo "ALA_API_KEY $ALA_API_KEY"
echo "REGISTRY_URL $REGISTRY_URL"
echo "LISTS_URL $LISTS_URL"
echo "SDS_URL $SDS_URL"
echo "DOI_URL $DOI_URL"
echo "NAME_MATCHING_URL $NAME_MATCHING_URL"
echo "SAMPLING_URL $SAMPLING_URL"
echo "IMAGES_URL $IMAGES_URL"
echo "SOLR_URL $SOLR_URL"
echo "ZK_URL $ZK_URL"
echo "SOLR_COLLECTION $SOLR_COLLECTION"
echo "SOLR_CONFIGSET $SOLR_CONFIGSET"
echo "ES_HOSTS $ES_HOSTS"
echo "ES_ALIAS $ES_ALIAS"

# add API key
echo 'Setting API key'
sed -i "s/S3_BUCKET/$S3_BUCKET/g" /data/la-pipelines/config/la-pipelines-local.yaml
sed -i "s/ALA_API_KEY/$ALA_API_KEY/g" /data/la-pipelines/config/la-pipelines-local.yaml

# ala services
echo 'ALA services urls'
sed -i "s~REGISTRY_URL~$REGISTRY_URL~g" /data/la-pipelines/config/la-pipelines-local.yaml
sed -i "s~LISTS_URL~$LISTS_URL~g" /data/la-pipelines/config/la-pipelines-local.yaml
sed -i "s~SDS_URL~$SDS_URL~g" /data/la-pipelines/config/la-pipelines-local.yaml
sed -i "s~DOI_URL~$DOI_URL~g" /data/la-pipelines/config/la-pipelines-local.yaml
sed -i "s~NAME_MATCHING_URL~$NAME_MATCHING_URL~g" /data/la-pipelines/config/la-pipelines-local.yaml
sed -i "s~SAMPLING_URL~$SAMPLING_URL~g" /data/la-pipelines/config/la-pipelines-local.yaml
sed -i "s~IMAGES_URL~$IMAGES_URL~g" /data/la-pipelines/config/la-pipelines-local.yaml

# solr
echo 'Setting solr URL'
sed -i "s~SOLR_URL~$SOLR_URL~g" /data/la-pipelines/config/la-pipelines-local.yaml
sed -i "s~ZK_URL~$ZK_URL~g" /data/la-pipelines/config/la-pipelines-local.yaml
sed -i "s~SOLR_COLLECTION~$SOLR_COLLECTION~g" /data/la-pipelines/config/la-pipelines-local.yaml
sed -i "s~SOLR_CONFIGSET~$SOLR_CONFIGSET~g" /data/la-pipelines/config/la-pipelines-local.yaml

# elastic search
echo 'Setting elastic URL'
sed -i "s~ES_HOSTS~$ES_HOSTS~g" /data/la-pipelines/config/la-pipelines-local.yaml
sed -i "s~ES_ALIAS~$ES_ALIAS~g" /data/la-pipelines/config/la-pipelines-local.yaml

echo "$(< /data/la-pipelines/config/la-pipelines-local.yaml )"

echo 'Finished setting config'
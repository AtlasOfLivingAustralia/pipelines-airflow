BUCKET=s3://ala-databox-dev
LOCAL_DAG_DIR=./dags
today=$(date +%Y_%m_%d_%H_%M_%S)
aws s3 mv $BUCKET/airflow/dags $BUCKET/airflow/dags-$today --recursive
aws s3 cp $LOCAL_DAG_DIR $BUCKET/airflow/dags/  --include "*.*"  --recursive --exclude "*__pycache__*" --exclude ".*" --exclude "*.iml"
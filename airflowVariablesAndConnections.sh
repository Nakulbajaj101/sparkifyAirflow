#!/bin/bash
#Create the redshift cluster and create iam role binding with s3 bucket read policy
#Run the bash script passing arn and cluster host: bash airflowVariablesAndConnections.sh <redshift_iam_arn> <cluster_host>
redshift_iam_arn=$1
cluster_host=$2
airflow variables --set schema_name udacity
airflow variables --set redshift_iam_arn $redshift_iam_arn
airflow connections --add --conn_id=redshift --conn_uri=postgresql://dwh_user:dwhPassword008@$cluster_host:5439/dwh

echo "run bash /opt/airflow/start.sh and switch on the dag"


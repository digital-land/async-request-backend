#!/usr/bin/env bash

set -euo pipefail

# enable debug
# set -x

echo "configuring s3"
echo "==================="
LOCALSTACK_HOST=localhost
AWS_REGION=eu-west-2

create_upload_bucket() {
    local BUCKET_NAME_TO_CREATE=$1
    awslocal --endpoint-url=http://${LOCALSTACK_HOST}:4566 s3api create-bucket --bucket ${BUCKET_NAME_TO_CREATE} --region ${AWS_REGION} --create-bucket-configuration LocationConstraint=${AWS_REGION}
    awslocal --endpoint-url=http://${LOCALSTACK_HOST}:4566 s3api put-bucket-cors --bucket ${BUCKET_NAME_TO_CREATE} --cors-configuration file:///etc/localstack/init/ready.d/cors-config.json
}

upload_file_to_bucket() {
    local FILENAME=$1
    local FILEPATH=$2
    local BUCKET_NAME=$3
    awslocal s3api put-object --bucket ${BUCKET_NAME} --key ${FILENAME} --body ${FILEPATH}
}

create_upload_bucket "dluhc-data-platform-request-files-local"
upload_file_to_bucket "B1E16917-449C-4FC5-96D1-EE4255A79FB1.jpg" \
  "/etc/localstack/init/ready.d/bogdan-farca-CEx86maLUSc-unsplash.jpg" \
  "dluhc-data-platform-request-files-local"
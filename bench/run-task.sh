#!/bin/bash

if [[ -z $1 || -n $2 ]]; then
    echo "Usage: $(basename $0) <step-template.json>" >&2
    exit 1
fi

TEMPLATE="$1"

if [[ ! -f $TEMPLATE ]]; then
    echo "Not a readable file: $TEMPLATE" >&2
    exit 2
fi

MONITOR_HOST=$(cd terraform; terraform output monitor_instance)
CLUSTER_ID=$(cd terraform; terraform output cluster_id)
S3_BUCKET=$(grep 's3_data_bucket' terraform/terraform.tfvars | cut -d \" -f 2)

STEP_JSON=$(sed < $TEMPLATE \
    -e "s/{{monitor_host}}/${MONITOR_HOST}/g" \
    -e "s/{{s3_data_bucket}}/${S3_BUCKET}/g")

aws emr add-steps \
    --cluster-id ${CLUSTER_ID} \
    --steps "${STEP_JSON}"

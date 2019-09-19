#!/bin/bash
set -e

RAW_MESSAGE=$(aws sqs receive-message --wait-time-seconds 20 --visibility-timeout 300 --queue-url "${SQS_QUEUE_URL}")

if [[ "${RAW_MESSAGE}" != "" ]]; then
  echo
  echo "received event from SQS"
  MESSAGEBODY=$(echo "${RAW_MESSAGE}" | jq --raw-output '.Messages[0].Body')
  MESSAGERH=$(echo "${RAW_MESSAGE}" | jq --raw-output '.Messages[0].ReceiptHandle')
  EVENTTYPE=$(echo "${MESSAGEBODY}" | jq --raw-output '.Records[0].eventName')

  if [[ "${EVENTTYPE}" == "ObjectCreated:Put" ]]; then
    S3PATH=$(echo "${MESSAGEBODY}" | jq --raw-output '"s3://" + .Records[0].s3.bucket.name + "/" + .Records[0].s3.object.key')
    FILENAME=$(echo "${MESSAGEBODY}" | jq --raw-output '.Records[0].s3.object.key')

    aws s3 cp "${S3PATH}" "${FILENAME}"

    if [ -d data ]; then
      rm -rf data
    fi
    mkdir data

    unzip -o "${FILENAME}" -d data/
    rm "${FILENAME}"

    echo "invoking fits-loader"
    ./fits-loader --data-dir data

    rm -rf data
    aws s3 rm "${S3PATH}"
  else
    echo "event was not an S3 Bucket Notification"
  fi

  echo "removing processed event"
  aws sqs delete-message --queue-url "${SQS_QUEUE_URL}" --receipt-handle "${MESSAGERH}"
fi


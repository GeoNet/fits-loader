#!/usr/bin/env bash

export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_DEFAULT_REGION=ap-southeast-2

if [ ! -d "${1}" ]; then
  echo "'${1}' is not a directory"
  exit 1
fi

FILENAME=$(date +%s)

zip -jr "${FILENAME}.zip" "${1}"

aws s3 cp "${FILENAME}.zip" s3://fits-spool/

rm "${FILENAME}.zip"
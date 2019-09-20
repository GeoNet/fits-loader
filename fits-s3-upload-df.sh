#!/usr/bin/env bash

export AWS_DEFAULT_REGION=ap-southeast-2

if [ ! -d "${1}" ]; then
  echo "'${1}' is not a directory"
  exit 1
fi

FILENAME=$(date +%s)

tar -czvf ${FILENAME}.tar.gz -C "${1}" .

aws s3 cp "df.${FILENAME}.tar.gz" s3://fits-spool/

rm "${FILENAME}.tar.gz"
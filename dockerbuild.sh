#!/usr/bin/env bash

set -e # Exit if any command fails

ACCOUNT=$(aws sts get-caller-identity --output text --query 'Account')
VERSION="git-$(git rev-parse --short HEAD)"

eval $(aws ecr get-login --no-include-email --region ap-southeast-2)

docker build -t ${ACCOUNT}.dkr.ecr.ap-southeast-2.amazonaws.com/fits-loader:latest -f fits-loader-docker/Dockerfile .

docker tag ${ACCOUNT}.dkr.ecr.ap-southeast-2.amazonaws.com/fits-loader:latest ${ACCOUNT}.dkr.ecr.ap-southeast-2.amazonaws.com/fits-loader:${VERSION}

docker push ${ACCOUNT}.dkr.ecr.ap-southeast-2.amazonaws.com/fits-loader:latest
docker push ${ACCOUNT}.dkr.ecr.ap-southeast-2.amazonaws.com/fits-loader:${VERSION}

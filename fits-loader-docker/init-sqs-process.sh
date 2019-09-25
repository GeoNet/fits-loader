#!/bin/bash

FITSCONFIG=$(cat <<EOT
{
	"DataBase": {
		"Host": "${DB_HOST}",
		"User": "${DB_USER}",
		"Password": "${DB_PASSWD}",
		"MaxOpenConns": 2,
		"MaxIdleConns": 1,
		"SSLMode": "require"
	}
}
EOT
)

echo ${FITSCONFIG} > fits-loader.json

while true; do
  ./sqs-process.sh
  sleep 1
done
#!/bin/bash

while [ true ]
do
    curl -X DELETE "http://localhost:8083/connectors/cassandra-json-sink/status"
    curl -X POST -H "Content-Type: application/json" "http://localhost:8083/connectors" -d @connector-config.json &> /dev/null
    echo "Posting connector config"
    if [[ "$?" -eq 0 ]]; then
        echo "Connector config set"
        break
    fi
done

exec /etc/confluent/docker/run  "$@"
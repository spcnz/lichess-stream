#!/bin/bash

if [[ ! -z "$CASSANDRA_KEYSPACE" && $1 = 'cassandra' ]]; then
  # Create default keyspace for single node cluster
  CQL="CREATE KEYSPACE IF NOT EXISTS $CASSANDRA_KEYSPACE WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};"
  until echo $CQL | cqlsh; do
    echo "cqlsh: Cassandra is unavailable - retry later"
    sleep 2
  done &
fi

if [[ ! -z "$CASSANDRA_KEYSPACE" && "$CASSANDRA_TABLE" && $1 = 'cassandra' ]]; then
  # Create default keyspace for single node cluster
  CQL_TABLE="CREATE TABLE IF NOT EXISTS $CASSANDRA_KEYSPACE.$CASSANDRA_TABLE (game_id text, fen text, lm text, wc int, bc int, PRIMARY KEY(game_id, wc));"
  until echo $CQL_TABLE | cqlsh; do
    echo "cqlsh: Cassandra is unavailable - retry later table"
    sleep 2
  done &
fi


exec /docker-entrypoint.sh "$@"
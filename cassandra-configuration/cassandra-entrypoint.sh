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
  CQL_TABLE="CREATE TABLE IF NOT EXISTS $CASSANDRA_KEYSPACE.$CASSANDRA_TABLE (day date, game_id text, played_at timestamp, fen text, lm text, wc int, bc int, next_best1 text, next_best2 text,evaluation int, PRIMARY KEY((day), played_at, game_id)) WITH CLUSTERING ORDER BY (played_at DESC);"
  until echo $CQL_TABLE | cqlsh; do
    echo "cqlsh: Cassandra is unavailable - retry later table"
    sleep 2
  done &
fi


if [[ ! -z "$CASSANDRA_KEYSPACE"  && $1 = 'cassandra' ]]; then
  # Create default keyspace for single node cluster
  CQL_GAME_TABLE="CREATE TABLE IF NOT EXISTS $CASSANDRA_KEYSPACE.games_by_id (game_id text, status text, moves_num int, speed text, winner text, PRIMARY KEY(game_id));"
  until echo $CQL_GAME_TABLE | cqlsh; do
    echo "cqlsh: Cassandra is unavailable - retry later game table"
    sleep 2
  done &
fi


if [[ ! -z "$CASSANDRA_KEYSPACE"  && $1 = 'cassandra' ]]; then
  # Create default keyspace for single node cluster
  CQL_GAME_TABLE="CREATE TABLE IF NOT EXISTS $CASSANDRA_KEYSPACE.games_by_id_counter (game_id text, mistakes counter, correct counter, PRIMARY KEY(game_id));"
  until echo $CQL_GAME_TABLE | cqlsh; do
    echo "cqlsh: Cassandra is unavailable - retry later game table"
    sleep 2
  done &
fi


exec /docker-entrypoint.sh "$@"
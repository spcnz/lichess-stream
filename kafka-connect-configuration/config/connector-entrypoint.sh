#!/bin/bash
exec /etc/confluent/docker/run
bash -c '\
echo -e "\n\n=============\nWaiting for Kafka Connect to start listening on localhost ⏳\n=============\n"
while [ $(curl -s -o /dev/null -w %{http_code} http://localhost:8083/connectors) -ne 200 ] ; do
  echo -e "\t" $(date) " Kafka Connect listener HTTP state: " $(curl -s -o /dev/null -w %{http_code} http://localhost:8083/connectors) " (waiting for 200)"
  sleep 5
done
echo -e $(date) "\n\n--------------\n\o/ Kafka Connect is ready! Listener HTTP state: " $(curl -s -o /dev/null -w %{http_code} http://localhost:8083/connectors) "\n--------------\n"

curl -X DELETE "http://localhost:8083/connectors/cassandra-json-sink"

curl -X POST -H "Content-Type: application/json" "http://localhost:8083/connectors" -d '"'"'{
   "name": "cassandra-json-sink",
   "config": {
     "connector.class": "com.datastax.oss.kafka.sink.CassandraSinkConnector",
     "tasks.max": "1",
     "topics": "cassandra-sink",
     "contactPoints": "cass1",
     "loadBalancing.localDc": "Mars",
     "topic.cassandra-sink.chess.moves_by_game_id.mapping": "day=value.day, played_at=value.played_at, game_id=value.game_id, fen=value.fen, lm=value.lm, wc=value.wc,  bc=value.bc",
     "topic.cassandra-sink.chess.moves_by_game_id.query": "INSERT INTO chess.moves_by_game_id (game_id, fen, lm, wc, bc, played_at, day) VALUES (:game_id, :fen, :lm, :wc, :bc, :played_at, :day)",
     "topic.cassandra-sink.chess.moves_by_game_id.consistencyLevel": "LOCAL_ONE",
     "topic.cassandra-sink.chess.moves_by_game_id.deletesEnabled": false,
     "topic.cassandra-sink.chess.moves_by_game_id.timestampTimeUnit":"SECONDS",
     "key.converter.schemas.enable": false,
     "value.converter.schemas.enable": false
   }
}'"'"

import os
import json
import requests
from jsonschema import validate
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

GAMES_URL = "https://lichess.org/api/tv/{speed}"
MOVES_URL = "https://lichess.org/api/stream/game/{id}"

move_schema = {
    "type" : "object",
    "additionalProperties": False,
    "properties" : {
        "fen": {"type" : "string"},
        "lm": {"type" : "string"},
        "wc" : {"type" : "number"},
        "bc" : {"type" : "number"}
    }
}

producer = KafkaProducer(bootstrap_servers=[
    os.environ["KAFKA1"],
    os.environ["KAFKA2"],
    os.environ["KAFKA3"],
], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def create_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers=[
        os.environ["KAFKA1"],
        os.environ["KAFKA2"],
        os.environ["KAFKA3"],
    ],
        client_id='test'
    )

    topic_list = []
    topic_list.append(NewTopic(name="game-moves", num_partitions=1, replication_factor=3))
    topic_list.append(NewTopic(name="game-events", num_partitions=3, replication_factor=3))
    topic_list.append(NewTopic(name="cassandra-sink", num_partitions=1, replication_factor=3))
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except Exception as e:
        print("Failed to add topic reason: {}".format(e))



def is_move_event(json_data):
    try:
        validate(json_data, schema=move_schema)
        return True
    except:
        return False


def load_game():
    games = requests.get(GAMES_URL.replace("{speed}", "channels")).json()
    return games['Rapid']


def start_streaming():
    id = None
    while True:
        game = load_game()
        if id == game["gameId"]:
            continue
        id = game["gameId"]
        response = requests.get(MOVES_URL.replace("{id}", id), stream=True)
        for line in response.iter_lines():
            json_data = json.loads(line)
            if (is_move_event(json_data)):
                producer.send('game-moves', json_data)
            else:
                producer.send('game-events', json_data)

if __name__ == "__main__":
    create_topics()
    start_streaming()
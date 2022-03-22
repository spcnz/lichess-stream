from kafka import KafkaProducer
import json
import requests
from jsonschema import validate

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

def is_move_event(json_data):
    try:
        validate(json_data, schema=move_schema)
        return True
    except:
        return False



producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def load_game():
    games = requests.get(GAMES_URL.replace("{speed}", "channels")).json()
    return games['Bullet']


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
            print(json_data)
        print("=============================")
        print("GAME ENDED, ID : ", game["gameId"])

if __name__ == "__main__":
    start_streaming()
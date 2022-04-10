import os
import faust
from faust.models.fields import Optional
from stockfish import Stockfish

class GameEvent(faust.Record):
    id: int
    name: str


class GameMove(faust.Record, serializer='json'):
    game_id: Optional[int]
    fen: str
    lm: Optional[str]
    wc: int
    bc: int


app = faust.App('consumer', broker=[
            "kafka://" + os.environ['KAFKA1'],
            "kafka://" + os.environ['KAFKA2'],
            "kafka://" + os.environ['KAFKA3'],
        ], key_serializer='json')
moves_topic = app.topic('game-moves', value_type=GameMove)
events_topic = app.topic('game-events', value_type=bytes)
cassandra_sink = app.topic('cassandra-sink', value_type=bytes)

stockfish = Stockfish(path="./stockfish_14.1_linux_x64")

GAME_ID = None

@app.agent(moves_topic)
async def move_played(moves):
    async for move in moves:
        move.game_id = GAME_ID
        print(f'New move:  {move}')
        await cassandra_sink.send(key=GAME_ID, value=move.dumps())


@app.agent(events_topic)
async def game_event(events):
    global GAME_ID
    async for event in events:
        print(f'New EVENT:  {event}')
        GAME_ID = event["id"]
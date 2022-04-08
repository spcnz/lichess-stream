import os
import faust
from faust.models.fields import Optional
from stockfish import Stockfish

class GameEvent(faust.Record):
    id: int
    name: str


class GameMove(faust.Record):
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
stockfish = Stockfish(path="./stockfish_14.1_linux_x64")

@app.agent(moves_topic)
async def move_played(moves):
    async for move in moves:
        print(stockfish.get_fen_position())
        stockfish.set_position(["e2e4", "e7e6"])

        print(stockfish.get_board_visual())

        print(f'New move:  {move}')


@app.agent(events_topic)
async def game_event(events):
    async for event in events:
        print(f'New EVENT:  {event}')
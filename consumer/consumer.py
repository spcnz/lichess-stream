import os
import faust
from faust.models.fields import Optional

class GameEvent(faust.Record):
    id: int
    name: str


class GameMove(faust.Record):
    fen: str
    lm: Optional[str]
    wc: int
    bc: int




app = faust.App('consumer', broker='kafka://' + os.environ['KAFKA'], key_serializer='json')
moves_topic = app.topic('game-moves', value_type=GameMove)
events_topic = app.topic('game-events', value_type=bytes)

@app.agent(moves_topic)
async def move_played(moves):
    async for move in moves:
        print(f'New move:  {move}')


@app.agent(events_topic)
async def game_event(events):
    async for event in events:
        print(f'New EVENT:  {event}')
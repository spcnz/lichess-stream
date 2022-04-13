import faust
from faust.models.fields import Optional


class GameStatus(faust.Record):
    id: int
    name: str

class GameEvent(faust.Record):
    id: int
    status: GameStatus
    speed: str
    winner: Optional[str]

class GameMove(faust.Record, serializer='json'):
    game_id: Optional[int]
    fen: str
    lm: Optional[str]
    wc: int
    bc: int
    played_at: Optional[str]
    day: Optional[str]
    is_correct: Optional[bool]
    next_best1: Optional[str]
    next_best2: Optional[str]
    evaluation: Optional[int]
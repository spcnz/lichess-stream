import faust
from faust.models.fields import Optional

class GameEvent(faust.Record):
    id: int
    name: str


class GameMove(faust.Record, serializer='json'):
    game_id: Optional[int]
    fen: str
    lm: Optional[str]
    wc: int
    bc: int
    played_at: Optional[str]
    day: Optional[str]
    is_correct: Optional[bool]



import faust
from model import GameMove, GameEvent, GameStatus
from stockfish import Stockfish
from datetime import datetime
import uuid
from db import *


app = faust.App('consumer', broker=[
            "kafka://" + os.environ['KAFKA1'],
            "kafka://" + os.environ['KAFKA2'],
            "kafka://" + os.environ['KAFKA3'],
        ], key_serializer='json')

moves_topic = app.topic('game-moves', value_type=GameMove)
events_topic = app.topic('game-events', value_type=bytes)
cassandra_sink = app.topic('cassandra-sink', value_type=bytes)

#faust internal
games_topic = app.topic('games_topic', value_type=GameEvent)
moves_channel = app.channel()
correct_channel = app.channel()
mistakes_channel = app.channel()

# mistake_moves_by_game = app.Table('mistake_moves_by_game',  key_type=str, value_type=int, default=int)
moves_by_game = app.Table('moves_by_game', key_type=str, value_type=int, default=int)


#Chess engine
stockfish = Stockfish(path="./stockfish_14.1_linux_x64")
GAME_ID = uuid.uuid1()

#sends move to cassandra via cassandra sink, sends moves to moves_chanhel for further processing
@app.agent(moves_topic,  sink=[moves_channel])
async def save_played_move(moves):
    async for move in moves:
        move.game_id = GAME_ID
        move.played_at =  datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        move.day = datetime.now().strftime("%Y-%m-%d")
        top_moves = stockfish.get_top_moves(2)
        move.next_best1, move.next_best2 = top_moves[0]['Move'], top_moves[1]['Move']
        move.evaluation = stockfish.get_evaluation()["value"]
        print(f'New move:  {move}')

        await cassandra_sink.send(value=move.dumps())
        yield  move


#saves game to db, forwards game to game event topic
@app.agent(events_topic)
async def game_event(events):
    global GAME_ID
    async for event in events:
        GAME_ID = event["id"]
        print(f"NEW EVENT : {event}")
        stockfish.set_fen_position(event["fen"])
        game_status = GameStatus(name=event['status']['name'], id=event['status']['id'])
        game_event =  GameEvent(id=event['id'], speed=event['speed'], status=game_status, winner=event.get("winner", 'none'))

        insert_game(game_event, moves_by_game[game_event.id])



#eq to kafka streams branch
@app.agent(moves_channel)
async def process_move(stream):
    async for move in stream:
        moves_by_game[move.game_id] += 1
        move.is_correct = stockfish.is_move_correct(move.lm)
        if move.is_correct:
            await correct_channel.send(key=move.game_id, value=move)
        else:
            await mistakes_channel.send(key=move.game_id, value=move)
        stockfish.set_fen_position(move.fen)

#Updates correct column in db
@app.task()
async def moves_by_game_corr():
    stream = app.stream(correct_channel)
    async for moves in stream.group_by(GameMove.game_id).take(15, within=5):
        move = moves[0]
        update_counter_column(move.game_id, 'correct', len(moves))
        print(f'Correct moves by game : {move.game_id}: {len(moves)}')



#Updates mistake column in db
@app.task()
async def moves_by_game_mis():
    stream = app.stream(mistakes_channel)
    async for moves in stream.group_by(GameMove.game_id).take(15, within=5):
        move = moves[0]
        update_counter_column(move.game_id, 'mistakes', len(moves))
        print(f'Mistake moves by game : {move.game_id}: {len(moves)}')
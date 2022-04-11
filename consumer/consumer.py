import os
import faust
from model import GameMove, GameEvent
from stockfish import Stockfish
from datetime import datetime
import uuid
from db import session, batch_template


app = faust.App('consumer', broker=[
            "kafka://" + os.environ['KAFKA1'],
            "kafka://" + os.environ['KAFKA2'],
            "kafka://" + os.environ['KAFKA3'],
        ], key_serializer='json')

moves_topic = app.topic('game-moves', value_type=GameMove)
events_topic = app.topic('game-events', value_type=bytes)
cassandra_sink = app.topic('cassandra-sink', value_type=bytes)



correct_channel = app.channel()
mistakes_channel = app.channel()
moves_channel = app.channel()

correct_moves_by_game = app.Table('correct_moves_by_game',  key_type=str, value_type=int, default=int)
mistake_moves_by_game = app.Table('mistake_moves_by_game',  key_type=str, value_type=int, default=int)


stockfish = Stockfish(path="./stockfish_14.1_linux_x64")

GAME_ID = uuid.uuid1()

@app.agent(moves_topic,  sink=[moves_channel])
async def save_played_move(moves):
    async for move in moves:
        move.game_id = GAME_ID
        move.played_at =  datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        move.day = datetime.now().strftime("%Y-%m-%d")
        # print(f'New move:  {move}')
        await cassandra_sink.send(value=move.dumps())
        yield  move


@app.agent(events_topic)
async def game_event(events):
    global GAME_ID
    async for event in events:
        print(f'New EVENT:  {event}')
        GAME_ID = event["id"]
        stockfish.set_fen_position(event["fen"])
        cql = f"insert into games_by_id (game_id, status, mistakes, correct) values ('{GAME_ID}', '{event['status']['name']}', 0, 0);"
        session.execute(cql)


#eq to kafka branch
@app.agent(moves_channel)
async def process_move(stream):
    async for move in stream:
        move.is_correct = stockfish.is_move_correct(move.lm)
        if move.is_correct:
            await correct_channel.send(key=move.game_id, value=move)
        else:
            await mistakes_channel.send(key=move.game_id, value=move)
        stockfish.set_fen_position(move.fen)


@app.task()
async def moves_by_game_corr():
    stream = app.stream(correct_channel)
    async for moves in stream.group_by(GameMove.game_id).take(15, within=10):
        cql = "BEGIN "
        for move in moves:
            correct_moves_by_game[move.game_id] += 1
            print(f'Correct moves by game : {move.game_id}: {correct_moves_by_game[move.game_id]}')
            cql += batch_template(move.game_id, correct_moves_by_game[move.game_id])
        cql += " APPLY BATCH;"
        print(cql)
        session.execute(cql)


@app.task()
async def moves_by_game_mis():
    stream = app.stream(mistakes_channel)
    async for move in stream.group_by(GameMove.game_id):
        game_id = move.game_id
        mistake_moves_by_game[game_id] += 1
        print(f'Mistake moves by game : {game_id}: {mistake_moves_by_game[game_id]}')


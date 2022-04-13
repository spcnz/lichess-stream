from cassandra.cluster import Cluster
import os

KEYSPACE = os.environ["CASSANDRA_KEYSPACE"]
cluster = Cluster([os.environ['CASSANDRA_CLUSTER']], port=9042)
session = cluster.connect(KEYSPACE, wait_for_all_pools=True)



def update_counter_column(id, column_name, value):
    cql = f"UPDATE games_by_id_counter SET {column_name} = {column_name} + {value} WHERE game_id = '{id}';"
    session.execute(cql)
    print("EXECUTED CQL STATEMENT : ", cql)
    return cql



def insert_game(game, moves_num):
    cql = f"INSERT INTO games_by_id (game_id, status, winner, moves_num, speed) VALUES ('{game.id}', '{game.status.name}', '{game.winner}', {moves_num}, '{game.speed}');"
    session.execute(cql)
    print("EXECUTED CQL STATEMENT : \n\n", cql)

    cql = f"UPDATE games_by_id_counter SET mistakes = mistakes + 0 WHERE game_id = '{game.id}';"
    session.execute(cql)
    print("EXECUTED CQL STATEMENT : ", cql)
    return cql

from cassandra.cluster import Cluster
import os

KEYSPACE = os.environ["CASSANDRA_KEYSPACE"]
cluster = Cluster([os.environ['CASSANDRA_CLUSTER']], port=9042)
session = cluster.connect(KEYSPACE, wait_for_all_pools=True)



def batch_template(id, value):
    template = "BATCH INSERT INTO games_by_id (game_id, correct) VALUES (:id, :value);"

    return template.replace(":id", id).replace(":value", value)
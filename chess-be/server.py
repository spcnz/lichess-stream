from http.server import BaseHTTPRequestHandler, HTTPServer
from cassandra.cluster import Cluster
import json
import os
from json import JSONEncoder
from uuid import UUID
from datetime import datetime, date


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

KEYSPACE = os.environ["CASSANDRA_KEYSPACE"]
cluster = Cluster([os.environ['CASSANDRA_CLUSTER']], port=9042)
session = cluster.connect(KEYSPACE, wait_for_all_pools=True)

class Server(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

    def do_HEAD(self):
        self._set_headers()

    def do_GET(self):
        self._set_headers()
        row = session.execute('SELECT game_id, fen, lm, wc , bc , next_best1 , next_best2 ,evaluation FROM moves_by_game_id limit 1;')
        response = {}
        for obj in row:
            response = json.dumps(obj._asdict(), default=json_serial)
        self.wfile.write(response.encode(encoding='utf_8'))


def run(server_class=HTTPServer, handler_class=Server, port=8008):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)

    print('Starting httpd on port 8008')
    httpd.serve_forever()


if __name__ == "__main__":
    run()
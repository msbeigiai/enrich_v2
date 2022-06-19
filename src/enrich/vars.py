import os
from dotenv import load_dotenv
import json

load_dotenv()

topics = {
    "rtst_topic": "DebeziumTestServer.dbo.RETAILTRANSACTIONSALESTRANS",
}

sql_conf = {
    "driver": 'ODBC Driver 17 for SQL Server',
    "server": 'tcp:172.31.70.20,1433',
    "database": 'MicrosoftDynamicsAX',
    "username": os.environ.get("DATABASE_USERNAME"),
    "password": os.environ.get("DATABASE_PASS"),
}

kafka_consumer_conf = {
    "topic": topics["rtst_topic"],
    "bootstrap_servers": ['172.31.70.22:9092'],
    "auto_offset_reset": "earliest",
    "enable_auto_commit": True,
    "group_id": topics["rtst_topic"] + '__group',
}

kafka_producer_conf = {
    'bootstrap_servers': kafka_consumer_conf["bootstrap_servers"],
    "value_serializer": lambda x: json.dumps(x).encode('utf-8')
}

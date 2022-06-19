from kafka import KafkaConsumer, KafkaProducer
from src.enrichment.vars import *
import json
import redis
from src.enrichment.sql_config import *

# Making a Kafka consumer
consumer = KafkaConsumer(
    topics["rtst_topic"],
    bootstrap_servers=['172.31.70.22:9092'],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=topics["rtst_topic"] + '__group01',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Making a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['172.31.70.22:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Uses 'sql_config' module to make a connection to SQL Server
cursor = sql_initialize(sql_conf)

# Make an Instance of Redis to help fetch/ingest from/to Redis
r = redis.Redis(host="172.31.70.21", port=6379, db=0)


def fetch_needed_columns(nested_data):
    needed_columns = ["ITEMID", "RECID", "TRANSACTIONID", "PRICE", "DISCAMOUNT", "COUSTACCOUNT", "STORE"]
    return {k: v for (k, v) in nested_data.items() if k in [item for item in needed_columns]}


def fetch_name_aliases(msg_cleand, item_ids):
    """
    Since, each product name (Name Alias) is needed for data de-normalization process and should
    be located beside item_id in enriched table for OLAP.
    This function helps to search each name_alias according to its corresponding transaction_id.
    Therefore, it determines each name_alias by searching over Redis.
    If it there won't be the name_alias according to its key, SQL query will fetch data from
    main database. Otherwise, Redis returns name_alias.
    Data structures are a bit complicated in this function. It first fetched item_id, then will
    search through name_aliases.
    :param msg_cleand:
    :param item_ids:
    :return: List of name_aliases
    """

    for item_id in item_ids:
        name_alias = r.get(item_id)
        if name_alias is not '':
            msg_cleand["ITEMID"] = name_alias

        else:
            query = "select i.NAMEALIAS FROM RETAILTRANSACTIONSALESTRANS r " \
                                  "INNER JOIN INVENTTABLE i " \
                                  "ON i.ITEMID = '%s'" % item_id
            cursor.execute(query)
            msg_cleand["ITEMID"] = cursor.fetchone()


# for msg in consumer:
#     if msg is None:
#         continue
#
#     # Save data coming from Kafka
#     msg = msg.value
#
#     # Make msg cleaned and keep columns just needed.
#     msg_cleaned = fetch_needed_columns(msg["after"])
#
#     # Fetch name_aliases from Redis or main database
#     fetch_name_aliases(msg_cleand, msg_cleaned["ITEMID"])


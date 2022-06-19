from kafka import KafkaConsumer, KafkaProducer
from vars import *
import json
import redis
import sql_config

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
cursor = sql_config.sql_initialize(sql_conf)

# Make an Instance of Redis to help fetch/ingest from/to Redis
r = redis.Redis(host="172.31.70.21", port=6379, db=0)


def fetch_needed_columns(data):
    columns_needed = ["ITEMID", "RECID", "TRANSACTIONID", "PRICE", "DISCAMOUNT", "COUSTACCOUNT", "STORE"]
    return {k: v for (k, v) in data if data[k] in columns_needed}


def rtst_fetch_namealiases_redis(transaction_id):
    """
    Since, each product name (Name Alias) is needed for data de-normalization process and should
    be located beside item_id in enriched table for OLAP.
    This function helps to search each name_alias according to its corresponding transaction_id.
    Therefore, it determines each name_alias by searching over Redis.
    If it there won't be the name_alias according to its key, SQL query will fetch data from
    main database. Otherwise, Redis returns name_alais.
    Data structures are a bit complicated in this function. It first fetched item_id, then will
    search through name_aliases.
    :param transaction_id:
    :return: List of name_aliases
    """
    name_item = {}

    item_ids = rtst_fetch_itemid(transaction_id)

    for item in item_ids:
        temp_name = r.get(item)
        if temp_name:
            if item in name_item.keys():
                name_item[item + " "] = temp_name.decode('utf-8')
                continue
            name_item[item] = temp_name.decode('utf-8')

        else:
            name_item[item] = None

    if None in name_item.values():
        for item in item_ids:
            if name_item[item] is None:
                query_transactionid = "select b.NAMEALIAS from RETAILTRANSACTIONTABLE c " \
                                      " inner join RETAILTRANSACTIONSALESTRANS d on " \
                                      "c.TRANSACTIONID = d.TRANSACTIONID " \
                                      "inner join INVENTTABLE b on " \
                                      f"b.ITEMID = {item} where c.TRANSACTIONID = '%s'" % transaction_id
                cursor.execute(query_transactionid)
                name_item[item] = cursor.fetchone()[0]
                r.set(item, str(name_item[item]))

    return name_item


for msg in consumer:
    if msg is None:
        continue

    # Save data coming from Kafka
    msg = msg.value


    # msg_cleaned =



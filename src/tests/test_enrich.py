from src.enrichment.enrich_v2 import *
from redis import Redis
import json


def test_redis():
    r_ = r
    assert isinstance(r_, Redis)


def test_fetch_needed_columns():
    nested_data = {"ITEMID": "itemid", "PRICE": "price", "a": 1, "b": 2, "c": 3, "d": 4}
    cleaned_data = fetch_needed_columns(nested_data)
    assert cleaned_data == {"ITEMID": "itemid", "PRICE": "price"}


def test_fetch_name_aliases_from_redis():
    item_id = "1001603"
    r_ = r
    value = r_.get(item_id)
    assert value.decode('utf-8') == 'رنگارنگ مینو14.5 گرم'


def test_fetch_name_aliases_from_sql():
    item_id = "1001603"
    query = "select i.NAMEALIAS FROM RETAILTRANSACTIONSALESTRANS r " \
            "INNER JOIN INVENTTABLE i " \
            "ON i.ITEMID = '%s'" % item_id

    cursor_ = cursor
    cursor_.execute(query)
    value = cursor_.fetchone()[0]
    assert value.decode('utf-8') == 'رنگارنگ مینو14.5 گرم'


from src.enrichment.enrich_v2 import *


def test_fetch_needed_columns():
    nested_data = {"ITEMID": "itemid", "PRICE": "price", "a": 1, "b": 2, "c": 3, "d": 4}
    cleaned_data = fetch_needed_columns(nested_data)
    assert cleaned_data == {"ITEMID": "itemid", "PRICE": "price"}

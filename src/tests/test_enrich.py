from enrich import enrich_v2

def test_fetch_needed_columns():
    # c = enrich_v2
    msg = {"a": 1, "b": 2, "c": 3, "d": 4}
    columns_needed = ["a", "b"]

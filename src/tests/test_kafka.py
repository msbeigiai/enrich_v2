from src.enrichment.enrich_v2 import *


def test_consumer_exist():
    consumer_ = consumer
    assert isinstance(consumer_, KafkaConsumer)

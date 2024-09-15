from confluent_kafka import Producer
import json
from flask import current_app

def get_kafka_producer():
    return Producer({'bootstrap.servers': current_app.config['KAFKA_BOOTSTRAP_SERVERS']})

def send_to_kafka(products):
    producer = get_kafka_producer()
    for product in products:
        producer.produce(
            current_app.config['KAFKA_TOPIC'],
            key=str(product['id']),
            value=json.dumps(product)
        )
    producer.flush()

def unify_products(raw_products):
    unified_products = []
    for product in raw_products:
        unified_product = {
            'name': product.get('name', ''),
            'price': float(product.get('price', 0)),
            'description': product.get('description', ''),
            'source': product.get('source', 'unknown')
        }
        unified_products.append(unified_product)
    return unified_products
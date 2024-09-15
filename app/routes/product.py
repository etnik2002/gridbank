from flask import Blueprint, jsonify
from models import product
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

Product = product.Product

bp = Blueprint('main', __name__)

@bp.route('/products')
def get_products():
    products = get_unified_products()
    return jsonify(products)




def get_unified_products():
    raw_products = fetch_products_from_apis()
    
    unified_products = unify_products(raw_products)
    
    for product in unified_products:
        db_product = Product(**product)
        db.session.add(db_product)
    db.session.commit()
    
    send_to_kafka(unified_products)
    
    return unified_products

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

API_ENDPOINTS = [
    'https://fakestoreapi.com/products',
    'https://api.escuelajs.co/api/v1/products',
    'https://freetestapi.com/api/v1/products'
]

def fetch_products_from_api(endpoint):
    response = requests.get(endpoint)
    if response.status_code == 200:
        return response.json()
    return []


def fetch_products_from_apis():
    all_products = []
    
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_products_from_api, endpoint) for endpoint in API_ENDPOINTS]
        
        for future in as_completed(futures):
            products = future.result()
            all_products.extend(products)

    return all_products

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
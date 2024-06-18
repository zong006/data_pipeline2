import yaml
import logging
from kafka import KafkaProducer  # type: ignore
import json
import random
from datetime import datetime
import time
import pandas as pd

products_df = pd.read_csv("../dags/data/products.csv")
product_prices = products_df.set_index('ProductID')['Price'].to_dict()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with open('../secrets.yml', 'r') as f:
    secrets = yaml.safe_load(f)

with open('../config.yml', 'r') as f:
    config = yaml.safe_load(f)

def generate_order_item(product_prices):
    """
    1. choose whether to buy item. eg >40% chance.
    2. if buy item, choose from within list of 20. equal chance
    3. choose quantity number between 1 to 3 
    4. add timestamp
    """

    product_id = random.randint(1001, 1020)
    quantity = random.randint(1, 5)
    price_per_unit = product_prices[product_id]
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

    return {
            "product_id": product_id,
            "quantity": quantity,
            "price_per_unit": price_per_unit,
            "timestamp": timestamp
            }
    
def create_kafkaProducer():
    """
    create a kafka producer and connect to cluster in upstash.
    """
    producer = KafkaProducer(
        bootstrap_servers = secrets['upstash_info']['bootstrap_server'],
        sasl_mechanism = 'SCRAM-SHA-256',
        security_protocol = 'SASL_SSL',
        sasl_plain_username = secrets['upstash_info']['sasl_plain_username'],
        sasl_plain_password = secrets['upstash_info']['sasl_plain_password'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    return producer

def push_data():

    logger.info("Starting data pushing to Kafka...")

    producer = create_kafkaProducer()

    end_time = time.time() + config["duration"]

    while True:
        if time.time() > end_time:
            break

        if random.random() < 0.4:
            order_item = generate_order_item(product_prices)
            try:
                
                logger.info("Sending data to Kafka...")

                producer.send(config["cluster_variables"]["topic"], value=order_item)
                
            except Exception as e:
                logger.error(f"Error sending data to Kafka: {e}")

        time.sleep(5)

    producer.close()
    logger.info("Data pushing completed.")
        
while True:
    try:
        push_data()
        break
    except Exception as e:
        print(f"Error: {e}. Retrying in 2 seconds...")
        time.sleep(2)

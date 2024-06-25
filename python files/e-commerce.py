# All rights reserved to this software and its documentation are reserved under "OMAR H. ATTIA"
# Read the requring documentation before executing this script.
# If you have any questions please feel free to contact me via my LinkedIn account.
# This script mocks the streaming of a big data sources and its dependencies and helps with debugging and testing them properly.


import random
from faker import Faker
import json
from kafka import KafkaProducer
import time
import logging

# Initialize the Faker object
fake = Faker()

# List of product names for mobile phones and laptops
mobile_product_names = [
    "iPhone 13 Plus", "iPhone 8 Plus", "iPhone 13 mini", "iPhone 14 Pro",
    "Samsung Galaxy S22", "Google Pixel 6", "OnePlus 9", "Sony Xperia 1",
    "Nokia 8.3", "Huawei P30", "Samsung Galaxy S21", "Xiaomi Mi 11",
    "Oppo Find X3", "Motorola Edge", "iPhone 15 Pro Max"
]

laptop_product_names = [
    "MacBook Pro", "Dell XPS 13", "HP Spectre x360", "Lenovo ThinkPad X1",
    "Microsoft Surface Laptop 4", "Asus ZenBook 13", "Acer Swift 3",
    "Razer Blade 15", "MSI GS66 Stealth", "LG Gram 17"
]

# Configure logging to a file
log_file = '/path/to/logs/e-commerce_data_producer.log'  # Replace with your log file path
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_file
)

# Function to generate synthetic e-commerce data for mobile phones and laptops
def generate_data():
    product_type = random.choice(['mobile', 'laptop'])
    if product_type == 'mobile':
        product_name = random.choice(mobile_product_names)
    else:
        product_name = random.choice(laptop_product_names)

    return {
        'order_id': fake.uuid4(),
        'user_id': fake.uuid4(),
        'product_id': fake.uuid4(),
        'product_name': product_name,
        'quantity': random.randint(1, 10),
        'price': round(random.uniform(5.0, 500.0), 2),
        'order_timestamp': fake.iso8601(),
        'customer_name': fake.name(),
        'customer_email': fake.email(),
        'payment_method': random.choice(['Credit Card', 'PayPal', 'Bitcoin']),
        'shipping_address': {
            'street': fake.street_address(),
            'city': fake.city(),
            'state': fake.state(),
            'zipcode': fake.zipcode()
        }
    }

# Function to initialize Kafka producer with optimizations
def kafka_producer():
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        batch_size=16384,  # 16 KB batch size
        linger_ms=0,       # Send batches immediately
        compression_type='gzip',  # Compression type for better network throughput
        retries=5,
        acks='all'
    )

# Function to produce data to Kafka topic for exactly 800 records
def produce_data(producer, topic, record_limit=800):
    record_count = 0
    try:
        while record_count < record_limit:
            data = generate_data()
            producer.send(topic, value=data)  # Send data to Kafka topic
            logging.info(f'Produced: {data}')  # Log produced data
            record_count += 1
            time.sleep(0.1)  # Optional: Adjust sleep time to control production rate
    except Exception as e:
        logging.error(f"An error occurred while producing data: {str(e)}")
    finally:
        producer.flush()
        producer.close()

# Main function to run the producer
if __name__ == '__main__':
    producer = kafka_producer()
    try:
        produce_data(producer, 'e-commerce_data_streaming_topic', record_limit=800)
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        producer.flush()
        producer.close()

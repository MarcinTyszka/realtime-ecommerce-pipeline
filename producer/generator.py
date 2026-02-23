import time
import json
import random
from confluent_kafka import Producer
from faker import Faker
from datetime import datetime

KAFKA_TOPIC = "clicks"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
FREQ = 0.5

faker = Faker()

PRODUCT_CATALOG = {
    "Electronics": [
        {"name": "MacBook Pro M3", "price_range": (8000, 15000)},
        {"name": "Gaming Laptop MSI", "price_range": (4000, 9000)},
        {"name": "iPhone 15", "price_range": (4000, 6000)},
        {"name": "Samsung Galaxy S24", "price_range": (3500, 5500)},
        {"name": "Sony WH-1000XM5", "price_range": (1200, 1600)},
        {"name": "Logitech MX Master 3", "price_range": (400, 550)},
        {"name": "4K Monitor Dell", "price_range": (1500, 3000)}
    ],
    "Fashion": [
        {"name": "Nike Air Jordan", "price_range": (500, 1200)},
        {"name": "Adidas Ultraboost", "price_range": (600, 900)},
        {"name": "Levi's 501 Jeans", "price_range": (300, 500)},
        {"name": "North Face Jacket", "price_range": (800, 1500)},
        {"name": "Ray-Ban Aviator", "price_range": (400, 700)},
        {"name": "Cotton T-Shirt Basic", "price_range": (50, 120)}
    ],
    "Home & Garden": [
        {"name": "Dyson Vacuum V15", "price_range": (2500, 3500)},
        {"name": "Philips Hue Starter Kit", "price_range": (600, 900)},
        {"name": "IKEA Standing Desk", "price_range": (800, 1500)},
        {"name": "KitchenAid Mixer", "price_range": (2000, 3000)},
        {"name": "Nespresso Machine", "price_range": (400, 800)}
    ],
    "Books & Media": [
        {"name": "Python for Data Analysis", "price_range": (80, 150)},
        {"name": "Clean Code", "price_range": (90, 140)},
        {"name": "The Witcher Saga Set", "price_range": (200, 400)},
        {"name": "E-Reader Kindle", "price_range": (500, 800)}
    ]
}

TRAFFIC_SOURCES = ["Google Ads", "Organic Search", "Facebook Ads", "Instagram Influencer", "Email Newsletter", "Direct"]
PAYMENT_METHODS = ["Credit Card", "BLIK", "PayPal", "Apple Pay", "Google Pay"]
MEMBERSHIP_LEVELS = ["Free", "Bronze", "Silver", "Gold", "Platinum"]

def get_weighted_choice(options, weights):
    return random.choices(options, weights=weights, k=1)[0]

def generate_click_data():
    category = random.choice(list(PRODUCT_CATALOG.keys()))
    product_info = random.choice(PRODUCT_CATALOG[category])
    
    base_price = random.uniform(product_info["price_range"][0], product_info["price_range"][1])
    price = round(base_price, 2)

    user_id = random.randint(10000, 99999)
    gender = random.choice(["M", "F"])
    age_group = get_weighted_choice(["18-24", "25-34", "35-44", "45-54", "55+"], [0.2, 0.4, 0.2, 0.1, 0.1])
    membership = get_weighted_choice(MEMBERSHIP_LEVELS, [0.5, 0.2, 0.15, 0.1, 0.05])
    
    city = faker.city()
    country = "Poland" 
    device = get_weighted_choice(["Mobile", "Desktop", "Tablet"], [0.6, 0.35, 0.05])
    os = "iOS" if device == "Mobile" and random.random() > 0.5 else "Android" if device == "Mobile" else "Windows"

    source = get_weighted_choice(TRAFFIC_SOURCES, [0.3, 0.2, 0.2, 0.1, 0.1, 0.1])
    event_type = get_weighted_choice(["view_product", "add_to_cart", "purchase"], [0.75, 0.20, 0.05])
    payment_method = random.choice(PAYMENT_METHODS) if event_type == "purchase" else None

    data = {
        "event_id": faker.uuid4(),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "user_id": user_id,
        "user_session": faker.sha256()[:10],
        "membership_level": membership,
        "gender": gender,
        "age_group": age_group,
        "ip_address": faker.ipv4(),
        "city": city,
        "country": country,
        "category": category,
        "product_name": product_info["name"],
        "price": price,
        "currency": "PLN",
        "event_type": event_type,
        "traffic_source": source,
        "device_type": device,
        "os": os,
        "payment_method": payment_method
    }
    
    return data

def delivery_report(err, msg):
    """Callback triggered by Kafka to check delivery status."""
    if err is not None:
        print(f"Delivery failed: {err}")

def main():
    print(f"Starting data generator. Sending to {KAFKA_TOPIC}...")
    
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    producer = Producer(conf)

    try:
        while True:
            event = generate_click_data()
            
            producer.produce(
                KAFKA_TOPIC, 
                value=json.dumps(event).encode('utf-8'),
                callback=delivery_report
            )
            
            # Trigger delivery callbacks
            producer.poll(0)
            
            tag = "[PURCHASE]" if event['event_type'] == 'purchase' else "[ADD]" if event['event_type'] == 'add_to_cart' else "[VIEW]"
            print(f"{tag} {event['timestamp']} | {event['product_name']} | {event['price']} PLN | {event['traffic_source']}")
            
            time.sleep(random.uniform(0.1, FREQ))
            
    except KeyboardInterrupt:
        print("\nStopping generator...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
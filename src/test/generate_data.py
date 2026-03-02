import json
import random
from faker import Faker
from datetime import datetime
import pandas as pd

fake = Faker()

def generate_raw_property_listings():

    cities = ["Pune", "Mumbai", "Bangalore", "Hyderabad", "Chennai"]
    property_types = ["Apartment", "Villa", "Studio", "Plot"]

    properties = []

    for i in range(1, 101):  # 100 records
        property_record = {
            "propertyId": f"P{1000 + i}",
            "type": random.choice(property_types),
            "price": f"{random.randint(50, 150) * 100000:,}",  # formatted with commas
            "bedrooms": str(random.randint(1, 5)),
            "brokerId": f"BRK{random.randint(1, 10):02}",
            "city": random.choice(cities),
            "listingDate": fake.date_between(start_date="-1y", end_date="today").strftime("%d-%m-%Y")
        }
        properties.append(property_record)

    with open("../../data/raw/raw_property_listings.json", "w") as f:
        json.dump(properties, f, indent=4)

    print("raw_property_listings.json created")


def generate_raw_broker():
    companies = ["ABC Realty", "Prime Estates", "Skyline Realty", "Urban Homes"]

    brokers = []

    for i in range(1, 11):  # 10 brokers
        brokers.append({
            "broker_id": f"BRK{i:02}",
            "broker_name": fake.name(),
            "company_name": random.choice(companies)
        })

    broker_df = pd.DataFrame(brokers)
    broker_df.to_csv("raw_broker.csv", index=False)

    print("raw_broker.csv created")

def generate_raw_transactions():
    transactions = []

    for i in range(1, 201):  # 200 transactions
        sale_price = random.randint(50, 150) * 100000

        transactions.append({
            "Transaction_ID": f"T{i:04}",
            "Property_ID": f"P{1000 + random.randint(1, 100)}",
            "SalePrice": f"{sale_price:,}",
            "SaleDate": fake.date_between(start_date="-6m", end_date="today").strftime("%d/%m/%Y"),
            "TaxAmount": int(sale_price * 0.05)
        })

    transaction_df = pd.DataFrame(transactions)
    transaction_df.to_csv("raw_transactions.csv", index=False)

    print("raw_transactions.csv created")


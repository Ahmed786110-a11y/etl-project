"""
generate_data.py
----------------
Generates synthetic customer, product, and order data with Faker and writes
each entity in three formats (CSV, JSON, YAML) -> 9 files total.

Run from the project root:
    python scripts/generate_data.py

Output goes to the data/ folder next to the scripts/ folder.
"""

import json
import random
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from faker import Faker

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
NUM_RECORDS = 1000          # records per entity (assignment requires exactly 1,000)
SEED = 42                   # fixed seed -> same data every run (reproducible)

# data/ folder lives in the project root, one level above scripts/
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------
def generate_customers(n):
    """Create n customer records. customer_id and email are unique."""
    customers = []
    for i in range(1, n + 1):
        customers.append({
            "customer_id": i,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),          # .unique guarantees no duplicates
            "phone": fake.numerify("###-###-####"),  # fits VARCHAR(20)
            "city": fake.city(),
            "registration_date": fake.date_between(start_date="-5y", end_date="today").isoformat(),
            "customer_type": random.choices(["Regular", "Premium", "VIP"], weights=[70, 20, 10])[0],
        })
    return customers


def generate_products(n):
    """Create n product records with realistic categories and brands."""
    categories = ["Electronics", "Clothing", "Home & Kitchen", "Books",
                  "Sports", "Toys", "Beauty", "Grocery"]
    products = []
    for i in range(1, n + 1):
        category = random.choice(categories)
        products.append({
            "product_id": i,
            "product_name": f"{fake.color_name()} {fake.word().capitalize()} {category.split()[0]}",
            "category": category,
            "brand": fake.company().split()[0].strip(","),
            "unit_price": round(random.uniform(1.99, 999.99), 2),
            "stock_quantity": random.randint(0, 500),
            "is_active": random.random() < 0.9,      # ~90% of products are active
        })
    return products


def generate_orders(n, customer_ids):
    """
    Create n order records.
    Referential integrity: every customer_id is picked from the real
    customer list, so every order points to a customer that exists.
    """
    orders = []
    for i in range(1, n + 1):
        total = round(random.uniform(10, 2000), 2)
        orders.append({
            "order_id": i,
            "customer_id": random.choice(customer_ids),
            "order_date": fake.date_time_between(start_date="-2y", end_date="now")
                              .strftime("%Y-%m-%d %H:%M:%S"),
            "order_status": random.choice(["Pending", "Shipped", "Delivered"]),
            "payment_method": random.choice(["Credit Card", "Debit Card", "PayPal",
                                             "Bank Transfer", "Cash on Delivery"]),
            "total_amount": total,
            "discount_amount": round(total * random.choice([0, 0, 0.05, 0.10, 0.15]), 2),
            "shipping_cost": round(random.choice([0, 4.99, 9.99, 14.99]), 2),
        })
    return orders


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------
def write_all_formats(records, name):
    """Write one entity to CSV, JSON, and YAML."""
    pd.DataFrame(records).to_csv(DATA_DIR / f"{name}.csv", index=False)

    with open(DATA_DIR / f"{name}.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    with open(DATA_DIR / f"{name}.yaml", "w", encoding="utf-8") as f:
        yaml.safe_dump(records, f, sort_keys=False)

    print(f"  {name}: {len(records)} records -> {name}.csv, {name}.json, {name}.yaml")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    start = datetime.now()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Generating data into {DATA_DIR}")

    customers = generate_customers(NUM_RECORDS)
    products = generate_products(NUM_RECORDS)
    orders = generate_orders(NUM_RECORDS, [c["customer_id"] for c in customers])

    write_all_formats(customers, "customers")
    write_all_formats(products, "products")
    write_all_formats(orders, "orders")

    files = sorted(p.name for p in DATA_DIR.iterdir() if p.suffix in {".csv", ".json", ".yaml"})
    print(f"Done: {len(files)} files created in {(datetime.now() - start).total_seconds():.1f}s")


if __name__ == "__main__":
    main()

"""
generate_data.py — Synthetic e-commerce data generator
Produces customers.csv, products.csv, orders.csv with realistic nulls & duplicates
"""

import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

OUTPUT_DIR = "data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Constants ─────────────────────────────────────────────────────────────────
CITIES = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai",
          "Kolkata", "Pune", "Ahmedabad", "Jaipur", "Lucknow"]
SEGMENTS = ["Consumer", "Corporate", "Home Office"]
CATEGORIES = ["Electronics", "Clothing", "Books", "Home & Kitchen",
              "Sports", "Beauty", "Toys", "Automotive"]
BRANDS = ["Samsung", "Nike", "Penguin", "Prestige", "Adidas",
          "L'Oreal", "Lego", "Bosch", None]
STATUSES = ["Delivered", "Shipped", "Cancelled", "Returned", "Pending"]
CHANNELS = ["Website", "Mobile App", "In-Store", "Phone"]
PAYMENT_METHODS = ["Credit Card", "Debit Card", "UPI", "Net Banking", "COD"]


# ── Customers ─────────────────────────────────────────────────────────────────
def generate_customers(n=500):
    ids = [f"C{str(i).zfill(4)}" for i in range(1, n + 1)]
    names = [f"Customer_{i}" for i in range(1, n + 1)]
    ages = np.random.randint(18, 70, n).astype(float)
    ages[np.random.choice(n, 20, replace=False)] = np.nan          # inject nulls

    emails = [f"user{i}@example.com" for i in range(1, n + 1)]
    bad_idx = np.random.choice(n, 15, replace=False)
    for i in bad_idx:
        emails[i] = "not-an-email"                                  # bad emails

    phones = [f"9{random.randint(100000000, 999999999)}" for _ in range(n)]
    phone_arr = np.array(phones, dtype=object)
    phone_arr[np.random.choice(n, 30, replace=False)] = np.nan

    signup_dates = [
        (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1460))).strftime("%Y-%m-%d")
        for _ in range(n)
    ]

    df = pd.DataFrame({
        "customer_id": ids,
        "name": names,
        "email": emails,
        "phone": phone_arr,
        "city": [random.choice(CITIES) for _ in range(n)],
        "segment": [random.choice(SEGMENTS) for _ in range(n)],
        "age": ages,
        "signup_date": signup_dates,
    })

    # Inject duplicates
    dupes = df.sample(10)
    df = pd.concat([df, dupes], ignore_index=True)
    df.to_csv(f"{OUTPUT_DIR}/customers.csv", index=False)
    print(f"[generate_data] customers.csv → {len(df)} rows")
    return df


# ── Products ──────────────────────────────────────────────────────────────────
def generate_products(n=100):
    ids = [f"P{str(i).zfill(3)}" for i in range(1, n + 1)]
    categories = [random.choice(CATEGORIES) for _ in range(n)]
    names = [f"{cat}_Product_{i}" for i, cat in enumerate(categories, 1)]
    cost_prices = np.round(np.random.uniform(50, 5000, n), 2)
    sell_prices = np.round(cost_prices * np.random.uniform(1.1, 2.5, n), 2)
    sell_prices[np.random.choice(n, 5, replace=False)] = 0          # bad prices

    brands = [random.choice(BRANDS) for _ in range(n)]

    df = pd.DataFrame({
        "product_id": ids,
        "product_name": names,
        "category": categories,
        "brand": brands,
        "cost_price": cost_prices,
        "sell_price": sell_prices,
    })
    df.to_csv(f"{OUTPUT_DIR}/products.csv", index=False)
    print(f"[generate_data] products.csv → {len(df)} rows")
    return df


# ── Orders ────────────────────────────────────────────────────────────────────
def generate_orders(customers_df, products_df, n=5000):
    cust_ids = customers_df["customer_id"].dropna().unique()
    prod_ids = products_df["product_id"].unique()
    prod_prices = products_df.set_index("product_id")["sell_price"].to_dict()

    order_ids = [f"O{str(i).zfill(5)}" for i in range(1, n + 1)]
    cust = [random.choice(cust_ids) for _ in range(n)]
    prod = [random.choice(prod_ids) for _ in range(n)]
    qty = np.random.randint(1, 10, n)
    discounts = np.round(np.random.uniform(0, 0.4, n), 2)
    discounts[np.random.choice(n, 20, replace=False)] = round(random.uniform(1.5, 3.0), 2)  # bad

    revenues = np.round(
        np.array([prod_prices.get(p, 100) for p in prod]) * qty * (1 - discounts), 2
    )

    order_dates = [
        (datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))).strftime("%Y-%m-%d")
        for _ in range(n)
    ]

    cities = [random.choice(CITIES + [None]) for _ in range(n)]  # some null cities
    statuses = [random.choice(STATUSES) for _ in range(n)]
    channels = [random.choice(CHANNELS) for _ in range(n)]
    payments = [random.choice(PAYMENT_METHODS) for _ in range(n)]

    df = pd.DataFrame({
        "order_id": order_ids,
        "customer_id": cust,
        "product_id": prod,
        "order_date": order_dates,
        "quantity": qty,
        "discount": discounts,
        "revenue": revenues,
        "shipping_city": cities,
        "status": statuses,
        "channel": channels,
        "payment_method": payments,
    })

    # Inject duplicates
    dupes = df.sample(50)
    df = pd.concat([df, dupes], ignore_index=True)
    df.to_csv(f"{OUTPUT_DIR}/orders.csv", index=False)
    print(f"[generate_data] orders.csv → {len(df)} rows")
    return df


if __name__ == "__main__":
    c = generate_customers()
    p = generate_products()
    generate_orders(c, p)
    print("[generate_data] ✅ All raw CSV files created in data/raw/")

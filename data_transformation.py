"""
data_transformation.py — Stage 3: Build star schema + aggregation tables
Produces dim_customers, dim_products, fact_orders, and 6 pre-aggregated views.
"""

import pandas as pd
import numpy as np
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("transformation")

TODAY = pd.Timestamp("today").normalize()


# ── Dimension: Customers ───────────────────────────────────────────────────────
def build_dim_customers(df: pd.DataFrame) -> pd.DataFrame:
    dim = df[["customer_id", "name", "city", "segment", "age", "signup_date"]].copy()
    dim["signup_date"] = pd.to_datetime(dim["signup_date"], errors="coerce")

    # Age group buckets
    bins = [0, 25, 35, 45, 60, 120]
    labels = ["18-25", "26-35", "36-45", "46-60", "60+"]
    dim["age_group"] = pd.cut(dim["age"], bins=bins, labels=labels, right=True)

    # Tenure
    dim["tenure_days"] = (TODAY - dim["signup_date"]).dt.days
    dim["customer_since_year"] = dim["signup_date"].dt.year

    logger.info(f"[transform] dim_customers → {len(dim)} rows")
    return dim


# ── Dimension: Products ────────────────────────────────────────────────────────
def build_dim_products(df: pd.DataFrame) -> pd.DataFrame:
    dim = df[["product_id", "product_name", "category", "brand",
              "cost_price", "sell_price", "margin_pct"]].copy()

    # Price tier
    def price_tier(p):
        if p < 200:    return "Budget"
        if p < 1000:   return "Mid-Range"
        if p < 5000:   return "Premium"
        return "Luxury"

    dim["price_tier"] = dim["sell_price"].apply(price_tier)
    logger.info(f"[transform] dim_products → {len(dim)} rows")
    return dim


# ── Fact: Orders ───────────────────────────────────────────────────────────────
def build_fact_orders(orders: pd.DataFrame,
                      customers: pd.DataFrame,
                      products: pd.DataFrame) -> pd.DataFrame:
    fact = orders.copy()
    fact["order_date"] = pd.to_datetime(fact["order_date"], errors="coerce")

    # Date parts
    fact["order_year"]    = fact["order_date"].dt.year
    fact["order_month"]   = fact["order_date"].dt.month
    fact["order_quarter"] = fact["order_date"].dt.quarter
    fact["order_week"]    = fact["order_date"].dt.isocalendar().week.astype(int)
    fact["day_of_week"]   = fact["order_date"].dt.day_name()
    fact["year_month"]    = fact["order_date"].dt.to_period("M").astype(str)

    # Denormalise category & segment
    prod_map = products.set_index("product_id")[["category", "price_tier"]]
    cust_map = customers.set_index("customer_id")[["segment"]]
    fact = fact.join(prod_map, on="product_id", how="left")
    fact = fact.join(cust_map, on="customer_id", how="left")

    # Financial metrics
    fact["discount_amount"] = (fact["revenue"] / (1 - fact["discount"].clip(0, 0.9999))
                               * fact["discount"]).round(2)
    fact["net_revenue"] = fact["revenue"]   # already post-discount from generation

    logger.info(f"[transform] fact_orders → {len(fact)} rows")
    return fact


# ── Pre-aggregations ──────────────────────────────────────────────────────────
def build_aggregations(fact: pd.DataFrame, customers: pd.DataFrame,
                        products: pd.DataFrame) -> dict:
    delivered = fact[fact["status"] == "Delivered"]

    agg = {}

    agg["monthly_revenue"] = (
        delivered.groupby("year_month")
        .agg(orders=("order_id", "count"), revenue=("net_revenue", "sum"))
        .reset_index()
    )

    agg["category_revenue"] = (
        delivered.groupby("category")
        .agg(revenue=("net_revenue", "sum"), units=("quantity", "sum"))
        .reset_index()
    )

    top_cust = (
        delivered.groupby("customer_id")["net_revenue"].sum()
        .reset_index().rename(columns={"net_revenue": "total_spent"})
        .merge(customers[["customer_id", "name", "city", "segment"]], on="customer_id")
        .sort_values("total_spent", ascending=False)
        .head(10)
    )
    agg["top_customers"] = top_cust

    top_prod = (
        delivered.groupby("product_id")
        .agg(units=("quantity", "sum"), revenue=("net_revenue", "sum"))
        .reset_index()
        .merge(products[["product_id", "product_name", "category"]], on="product_id")
        .sort_values("revenue", ascending=False)
        .head(10)
    )
    agg["top_products"] = top_prod

    agg["quarterly_growth"] = (
        delivered.groupby(["order_year", "order_quarter"])["net_revenue"]
        .sum().reset_index().rename(columns={"net_revenue": "quarterly_revenue"})
    )
    agg["quarterly_growth"]["delta"] = (
        agg["quarterly_growth"]["quarterly_revenue"].diff().round(2)
    )

    agg["segment_revenue"] = (
        delivered.groupby("segment")["net_revenue"].sum().reset_index()
    )

    logger.info(f"[transform] Built {len(agg)} aggregation tables")
    return agg


# ── Entry point ───────────────────────────────────────────────────────────────
def transform_all(cleaned: dict) -> dict:
    logger.info("=" * 60)
    logger.info("STAGE 3 — TRANSFORMATION")
    logger.info("=" * 60)

    dim_c = build_dim_customers(cleaned["customers"])
    dim_p = build_dim_products(cleaned["products"])
    fact  = build_fact_orders(cleaned["orders"], dim_c, dim_p)
    aggs  = build_aggregations(fact, dim_c, dim_p)

    result = {
        "dim_customers": dim_c,
        "dim_products":  dim_p,
        "fact_orders":   fact,
        **aggs,
    }
    logger.info("Stage 3 complete ✅")
    return result


if __name__ == "__main__":
    from data_ingestion import ingest_all
    from data_cleaning import clean_all
    transform_all(clean_all(ingest_all()))

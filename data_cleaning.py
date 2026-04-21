"""
data_cleaning.py — Stage 2: Clean & validate each dataset
Handles nulls, duplicates, bad formats, outliers, and inconsistent values.
"""

import pandas as pd
import numpy as np
import logging
import os
import re
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("cleaning")

PROCESSED_DIR = "data/processed"
os.makedirs(PROCESSED_DIR, exist_ok=True)

EMAIL_RE = re.compile(r"^[\w.+-]+@[\w-]+\.[a-z]{2,}$", re.IGNORECASE)


# ── Customers ─────────────────────────────────────────────────────────────────
def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset=["customer_id"])
    logger.info(f"[customers] Removed {before - len(df)} duplicate rows")

    median_age = df["age"].median()
    df["age"] = df["age"].fillna(median_age)
    logger.info(f"[customers] Filled null ages with median={median_age:.1f}")

    bad_emails = ~df["email"].apply(lambda e: bool(EMAIL_RE.match(str(e))))
    df.loc[bad_emails, "email"] = np.nan
    logger.info(f"[customers] Invalidated {bad_emails.sum()} malformed email(s)")

    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")

    df["city"] = df["city"].str.strip().str.title()
    df["segment"] = df["segment"].str.strip()

    df.to_csv(f"{PROCESSED_DIR}/customers_clean.csv", index=False)
    logger.info(f"[customers] Clean dataset → {len(df)} rows saved")
    return df


# ── Products ──────────────────────────────────────────────────────────────────
def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    df["brand"] = df["brand"].fillna("Unknown")

    bad_price = df["sell_price"] <= 0
    logger.info(f"[products] Dropping {bad_price.sum()} product(s) with invalid price")
    df = df[~bad_price].copy()

    df["margin_pct"] = ((df["sell_price"] - df["cost_price"]) / df["sell_price"] * 100).round(2)

    df.to_csv(f"{PROCESSED_DIR}/products_clean.csv", index=False)
    logger.info(f"[products] Clean dataset → {len(df)} rows saved")
    return df


# ── Orders ────────────────────────────────────────────────────────────────────
def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset=["order_id"])
    logger.info(f"[orders] Removed {before - len(df)} duplicate rows")

    # Clip discounts to valid [0, 1] range
    bad_disc = (df["discount"] < 0) | (df["discount"] > 1)
    logger.info(f"[orders] Clipping {bad_disc.sum()} out-of-range discount(s)")
    df["discount"] = df["discount"].clip(0, 1)

    # Fill missing shipping city
    df["shipping_city"] = df["shipping_city"].fillna("Unknown")

    # Standardise status to title-case
    df["status"] = df["status"].str.strip().str.title()

    # IQR outlier flag on revenue (flag, do NOT delete)
    Q1 = df["revenue"].quantile(0.25)
    Q3 = df["revenue"].quantile(0.75)
    IQR = Q3 - Q1
    df["is_outlier"] = ((df["revenue"] < Q1 - 1.5 * IQR) | (df["revenue"] > Q3 + 1.5 * IQR)).astype(int)
    logger.info(f"[orders] Flagged {df['is_outlier'].sum()} revenue outlier(s)")

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    df.to_csv(f"{PROCESSED_DIR}/orders_clean.csv", index=False)
    logger.info(f"[orders] Clean dataset → {len(df)} rows saved")
    return df


# ── Entry point ───────────────────────────────────────────────────────────────
def clean_all(dfs: dict) -> dict:
    logger.info("=" * 60)
    logger.info("STAGE 2 — DATA CLEANING")
    logger.info("=" * 60)
    cleaned = {
        "customers": clean_customers(dfs["customers"].copy()),
        "products": clean_products(dfs["products"].copy()),
        "orders": clean_orders(dfs["orders"].copy()),
    }
    logger.info("Stage 2 complete ✅")
    return cleaned


if __name__ == "__main__":
    from data_ingestion import ingest_all
    clean_all(ingest_all())

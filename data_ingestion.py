"""
data_ingestion.py — Stage 1: Load & validate raw CSV files
Logs row/column counts and null totals; halts if required columns are missing.
"""

import pandas as pd
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("ingestion")

RAW_DIR = "data/raw"

REQUIRED_COLUMNS = {
    "customers": {"customer_id", "name", "email", "city", "segment", "signup_date"},
    "products": {"product_id", "product_name", "category", "cost_price", "sell_price"},
    "orders": {
        "order_id", "customer_id", "product_id", "order_date",
        "quantity", "revenue", "status",
    },
}


def load_csv(name: str) -> pd.DataFrame:
    path = os.path.join(RAW_DIR, f"{name}.csv")
    if not os.path.exists(path):
        logger.error(f"File not found: {path}")
        raise FileNotFoundError(f"Missing source file: {path}")

    df = pd.read_csv(path)
    logger.info(f"[{name}] Loaded {len(df):,} rows × {len(df.columns)} columns")

    missing = REQUIRED_COLUMNS[name] - set(df.columns)
    if missing:
        logger.error(f"[{name}] Missing required columns: {missing}")
        raise ValueError(f"Schema validation failed for {name}: {missing}")

    null_totals = df.isnull().sum()
    null_cols = null_totals[null_totals > 0]
    if not null_cols.empty:
        logger.info(f"[{name}] Nulls detected:\n{null_cols.to_string()}")

    return df


def ingest_all() -> dict:
    logger.info("=" * 60)
    logger.info("STAGE 1 — DATA INGESTION")
    logger.info("=" * 60)
    dfs = {}
    for name in ["customers", "products", "orders"]:
        dfs[name] = load_csv(name)
    logger.info("Stage 1 complete ✅")
    return dfs


if __name__ == "__main__":
    ingest_all()

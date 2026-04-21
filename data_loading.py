"""
data_loading.py — Stage 4: Load star schema into SQLite & run SQL reports
Creates the warehouse, loads all tables, and exports 10 analytical CSV reports.
"""

import sqlite3
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
logger = logging.getLogger("loading")

DB_PATH     = "data/ecommerce_warehouse.db"
REPORTS_DIR = "data/reports"
os.makedirs(REPORTS_DIR, exist_ok=True)

SQL_REPORTS = {
    "01_total_kpis": """
        SELECT
            COUNT(DISTINCT order_id)   AS total_orders,
            COUNT(DISTINCT customer_id) AS total_customers,
            ROUND(SUM(net_revenue), 2)  AS total_revenue,
            ROUND(AVG(net_revenue), 2)  AS avg_order_value,
            SUM(quantity)               AS total_units
        FROM fact_orders
        WHERE status = 'Delivered'
    """,
    "02_monthly_revenue": """
        SELECT year_month,
               COUNT(order_id)           AS orders,
               ROUND(SUM(net_revenue),2)  AS revenue
        FROM fact_orders
        WHERE status = 'Delivered'
        GROUP BY year_month
        ORDER BY year_month
    """,
    "03_top_10_customers": """
        SELECT dc.name, dc.city, dc.segment,
               ROUND(SUM(fo.net_revenue),2) AS total_spent
        FROM fact_orders fo
        JOIN dim_customers dc ON fo.customer_id = dc.customer_id
        WHERE fo.status = 'Delivered'
        GROUP BY fo.customer_id
        ORDER BY total_spent DESC
        LIMIT 10
    """,
    "04_best_selling_products": """
        SELECT dp.product_name, dp.category,
               SUM(fo.quantity)           AS units_sold,
               ROUND(SUM(fo.net_revenue),2) AS revenue
        FROM fact_orders fo
        JOIN dim_products dp ON fo.product_id = dp.product_id
        WHERE fo.status = 'Delivered'
        GROUP BY fo.product_id
        ORDER BY revenue DESC
        LIMIT 10
    """,
    "05_category_analysis": """
        SELECT category,
               COUNT(order_id)             AS orders,
               SUM(quantity)               AS units,
               ROUND(SUM(net_revenue),2)   AS revenue,
               ROUND(AVG(net_revenue),2)   AS avg_order_value
        FROM fact_orders
        WHERE status = 'Delivered'
        GROUP BY category
        ORDER BY revenue DESC
    """,
    "06_quarterly_growth": """
        SELECT order_year, order_quarter,
               ROUND(SUM(net_revenue),2) AS quarterly_revenue
        FROM fact_orders
        WHERE status = 'Delivered'
        GROUP BY order_year, order_quarter
        ORDER BY order_year, order_quarter
    """,
    "07_status_breakdown": """
        SELECT status,
               COUNT(order_id)            AS orders,
               ROUND(SUM(net_revenue),2)  AS revenue
        FROM fact_orders
        GROUP BY status
        ORDER BY orders DESC
    """,
    "08_channel_performance": """
        SELECT channel,
               COUNT(order_id)            AS orders,
               ROUND(SUM(net_revenue),2)  AS revenue
        FROM fact_orders
        WHERE status = 'Delivered'
        GROUP BY channel
        ORDER BY revenue DESC
    """,
    "09_customer_segments": """
        SELECT dc.segment,
               COUNT(DISTINCT fo.customer_id) AS customers,
               ROUND(SUM(fo.net_revenue),2)   AS revenue
        FROM fact_orders fo
        JOIN dim_customers dc ON fo.customer_id = dc.customer_id
        WHERE fo.status = 'Delivered'
        GROUP BY dc.segment
    """,
    "10_price_tier_revenue": """
        SELECT dp.price_tier,
               COUNT(fo.order_id)          AS orders,
               ROUND(SUM(fo.net_revenue),2) AS revenue
        FROM fact_orders fo
        JOIN dim_products dp ON fo.product_id = dp.product_id
        WHERE fo.status = 'Delivered'
        GROUP BY dp.price_tier
        ORDER BY revenue DESC
    """,
}


def load_to_sqlite(transformed: dict) -> None:
    logger.info("=" * 60)
    logger.info("STAGE 4 — LOADING")
    logger.info("=" * 60)

    con = sqlite3.connect(DB_PATH)
    logger.info(f"Connected to SQLite warehouse: {DB_PATH}")

    star_tables = ["dim_customers", "dim_products", "fact_orders"]
    for tbl in star_tables:
        df = transformed[tbl]
        df.to_sql(tbl, con, if_exists="replace", index=False)
        logger.info(f"Loaded {len(df):,} rows → {tbl}")

    # Run SQL reports
    logger.info("Running 10 analytical SQL reports …")
    for name, sql in SQL_REPORTS.items():
        try:
            result = pd.read_sql_query(sql.strip(), con)
            out_path = f"{REPORTS_DIR}/{name}.csv"
            result.to_csv(out_path, index=False)
            logger.info(f"  ✓ {name}.csv ({len(result)} rows)")
        except Exception as e:
            logger.error(f"  ✗ {name} failed: {e}")

    con.close()
    logger.info(f"Stage 4 complete ✅  Warehouse: {DB_PATH}")


if __name__ == "__main__":
    from data_ingestion import ingest_all
    from data_cleaning import clean_all
    from data_transformation import transform_all
    load_to_sqlite(transform_all(clean_all(ingest_all())))

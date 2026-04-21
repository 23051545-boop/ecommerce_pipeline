"""
main.py — Orchestrator: runs the full ETL pipeline
Usage:
    python main.py          # run once
    python main.py --schedule   # run daily at 06:00 (blocking)
"""

import logging
import sys
import os
import argparse
import time

# Ensure logs directory exists before any logger is set up
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("main")


def run_pipeline():
    """Execute all four ETL stages in sequence."""
    logger.info("🚀  E-Commerce Data Pipeline — START")
    try:
        from generate_data import generate_customers, generate_products, generate_orders
        from data_ingestion import ingest_all
        from data_cleaning import clean_all
        from data_transformation import transform_all
        from data_loading import load_to_sqlite

        # Generate synthetic data if raw files don't exist
        if not os.path.exists("data/raw/customers.csv"):
            logger.info("Raw data not found — generating synthetic dataset …")
            c = generate_customers()
            p = generate_products()
            generate_orders(c, p)

        raw      = ingest_all()
        cleaned  = clean_all(raw)
        transformed = transform_all(cleaned)
        load_to_sqlite(transformed)

        logger.info("🏁  Pipeline finished successfully")
    except Exception as exc:
        logger.exception(f"Pipeline failed: {exc}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="E-Commerce ETL Pipeline")
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Run pipeline on a daily schedule (blocks forever)",
    )
    args = parser.parse_args()

    if args.schedule:
        try:
            import schedule
        except ImportError:
            logger.error("Install 'schedule': pip install schedule")
            sys.exit(1)

        schedule.every().day.at("06:00").do(run_pipeline)
        logger.info("Scheduler active — pipeline will run daily at 06:00")
        run_pipeline()  # run immediately on first start
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        run_pipeline()


if __name__ == "__main__":
    main()

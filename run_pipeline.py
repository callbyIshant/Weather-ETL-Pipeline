import sys
import time
import argparse
from tabulate import tabulate
from etl.config import logger
from etl.db import get_db_cursor
from etl.pipeline import run_etl_pipeline
import schedule

def verify_database_counts():
    """
    Query database table row counts across Bronze, Silver, and Gold layers.
    """
    queries = {
        "bronze.raw_weather_payloads": "SELECT COUNT(*) as cnt FROM bronze.raw_weather_payloads;",
        "silver.weather_hourly": "SELECT COUNT(*) as cnt FROM silver.weather_hourly;",
        "silver.weather_daily": "SELECT COUNT(*) as cnt FROM silver.weather_daily;",
        "gold.daily_city_summary": "SELECT COUNT(*) as cnt FROM gold.daily_city_summary;",
        "gold.weather_anomalies": "SELECT COUNT(*) as cnt FROM gold.weather_anomalies;"
    }

    results = []
    with get_db_cursor(commit=False) as cursor:
        for table, sql in queries.items():
            cursor.execute(sql)
            cnt = cursor.fetchone()["cnt"]
            results.append([table, cnt])

    print("\n" + "=" * 50)
    print("DATABASE LAYER ROW COUNTS VERIFICATION")
    print("=" * 50)
    print(tabulate(results, headers=["Table Name", "Row Count"], tablefmt="grid"))
    print("=" * 50 + "\n")

def main():
    parser = argparse.ArgumentParser(description="Open-Meteo REST API -> PostgreSQL Medallion ETL Runner")
    parser.add_argument("--run-once", action="store_true", help="Run the ETL pipeline once immediately and exit (default)")
    parser.add_argument("--schedule", type=int, metavar="MINUTES", help="Run pipeline on a recurring interval (in minutes)")
    parser.add_argument("--verify", action="store_true", help="Inspect current row counts in PostgreSQL Medallion schemas")

    args = parser.parse_args()

    if args.verify:
        verify_database_counts()
        return

    if args.schedule:
        minutes = args.schedule
        logger.info(f"Scheduling ETL pipeline to run every {minutes} minute(s). Press Ctrl+C to stop.")
        # Execute first run immediately
        run_etl_pipeline()
        schedule.every(minutes).minutes.do(run_etl_pipeline)
        while True:
            try:
                schedule.run_pending()
                time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Scheduler stopped by user.")
                sys.exit(0)
    else:
        # Default behavior: run once
        success = run_etl_pipeline()
        if success:
            verify_database_counts()
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()

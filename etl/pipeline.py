import uuid
from tabulate import tabulate
from etl.config import logger, CONFIGURED_CITIES
from etl.db import test_db_connection, get_db_cursor
from etl.extract import get_http_session, fetch_weather_for_city
from etl.transform import transform_to_bronze, transform_to_silver, transform_silver_to_gold
from etl.load import load_bronze, load_silver, load_gold

def run_etl_pipeline():
    """
    Main orchestration routine for Open-Meteo REST API -> PostgreSQL Medallion Pipeline.
    """
    logger.info("=" * 60)
    logger.info("STARTING OPEN-METEO MEDALLION ETL PIPELINE RUN")
    logger.info("=" * 60)

    # 1. Verify DB Connection
    if not test_db_connection():
        logger.critical("Database connection test failed. Aborting pipeline execution.")
        return False

    batch_id = uuid.uuid4()
    logger.info(f"Pipeline Batch ID: {batch_id}")

    session = get_http_session()

    total_bronze = 0
    total_silver_hourly = 0
    total_silver_daily = 0
    total_gold_summaries = 0
    total_gold_anomalies = 0

    all_silver_daily = []
    all_silver_hourly = []

    # 2. Iterate Cities & Process Bronze + Silver
    for city in CONFIGURED_CITIES:
        try:
            # Extract
            extracted = fetch_weather_for_city(city, session=session)

            # Bronze Transform & Load
            bronze_rec = transform_to_bronze(extracted, batch_id)
            load_bronze(bronze_rec)
            total_bronze += 1

            # Silver Transform & Load
            silver_recs = transform_to_silver(extracted, batch_id)
            h_count, d_count = load_silver(silver_recs)
            total_silver_hourly += h_count
            total_silver_daily += d_count

            all_silver_daily.extend(silver_recs["daily"])
            all_silver_hourly.extend(silver_recs["hourly"])

        except Exception as e:
            logger.error(f"Error processing ETL pipeline for city '{city}': {e}", exc_info=True)

    # 3. Process Gold Layer (Aggregations & Anomalies)
    if all_silver_daily:
        try:
            gold_recs = transform_silver_to_gold(all_silver_daily, all_silver_hourly)
            g_sum_count, g_anom_count = load_gold(gold_recs)
            total_gold_summaries += g_sum_count
            total_gold_anomalies += g_anom_count
        except Exception as e:
            logger.error(f"Error processing Gold layer transformation: {e}", exc_info=True)

    # 4. Generate Summary Report
    summary_data = [
        ["Bronze Layer (Raw Payloads)", total_bronze],
        ["Silver Layer (Hourly Records)", total_silver_hourly],
        ["Silver Layer (Daily Records)", total_silver_daily],
        ["Gold Layer (Daily Summaries)", total_gold_summaries],
        ["Gold Layer (Anomalies Flagged)", total_gold_anomalies]
    ]

    report = tabulate(summary_data, headers=["Medallion Pipeline Layer", "Records Processed"], tablefmt="grid")
    logger.info("\n" + report)
    logger.info("OPEN-METEO MEDALLION ETL PIPELINE COMPLETED SUCCESSFULLY.")
    logger.info("=" * 60)
    return True

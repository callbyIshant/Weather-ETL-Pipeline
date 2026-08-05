import json
from etl.db import get_db_cursor
from etl.config import logger

def load_bronze(bronze_record: dict) -> int:
    """
    Load raw API payload into bronze.raw_weather_payloads table.
    Returns inserted record ID.
    """
    sql = """
        INSERT INTO bronze.raw_weather_payloads (
            batch_id, city, latitude, longitude, source_url, raw_json, status_code, ingested_at
        ) VALUES (
            %(batch_id)s, %(city)s, %(latitude)s, %(longitude)s, %(source_url)s, %(raw_json)s, %(status_code)s, %(ingested_at)s
        ) RETURNING id;
    """
    params = {
        "batch_id": bronze_record["batch_id"],
        "city": bronze_record["city"],
        "latitude": bronze_record["latitude"],
        "longitude": bronze_record["longitude"],
        "source_url": bronze_record["source_url"],
        "raw_json": json.dumps(bronze_record["raw_json"]),
        "status_code": bronze_record["status_code"],
        "ingested_at": bronze_record["ingested_at"]
    }

    with get_db_cursor(commit=True) as cursor:
        cursor.execute(sql, params)
        res = cursor.fetchone()
        inserted_id = res["id"]
        logger.info(f"Loaded Bronze record ID {inserted_id} for {bronze_record['city']}.")
        return inserted_id

def load_silver(silver_data: dict) -> tuple:
    """
    Idempotent UPSERT loading into Silver schema (silver.weather_hourly, silver.weather_daily).
    Uses ON CONFLICT DO UPDATE to prevent duplicate rows upon pipeline re-runs.
    """
    hourly_rows = silver_data.get("hourly", [])
    daily_rows = silver_data.get("daily", [])

    sql_hourly_upsert = """
        INSERT INTO silver.weather_hourly (
            city, latitude, longitude, timestamp, temperature_2m, relative_humidity_2m,
            dew_point_2m, apparent_temperature, precipitation, rain, weather_code,
            weather_condition, surface_pressure, wind_speed_10m, ingested_batch_id, updated_at
        ) VALUES (
            %(city)s, %(latitude)s, %(longitude)s, %(timestamp)s, %(temperature_2m)s, %(relative_humidity_2m)s,
            %(dew_point_2m)s, %(apparent_temperature)s, %(precipitation)s, %(rain)s, %(weather_code)s,
            %(weather_condition)s, %(surface_pressure)s, %(wind_speed_10m)s, %(ingested_batch_id)s, CURRENT_TIMESTAMP
        )
        ON CONFLICT (city, timestamp) DO UPDATE SET
            temperature_2m = EXCLUDED.temperature_2m,
            relative_humidity_2m = EXCLUDED.relative_humidity_2m,
            dew_point_2m = EXCLUDED.dew_point_2m,
            apparent_temperature = EXCLUDED.apparent_temperature,
            precipitation = EXCLUDED.precipitation,
            rain = EXCLUDED.rain,
            weather_code = EXCLUDED.weather_code,
            weather_condition = EXCLUDED.weather_condition,
            surface_pressure = EXCLUDED.surface_pressure,
            wind_speed_10m = EXCLUDED.wind_speed_10m,
            ingested_batch_id = EXCLUDED.ingested_batch_id,
            updated_at = CURRENT_TIMESTAMP;
    """

    sql_daily_upsert = """
        INSERT INTO silver.weather_daily (
            city, date, weather_code, weather_condition, temperature_2m_max, temperature_2m_min,
            apparent_temperature_max, apparent_temperature_min, precipitation_sum, rain_sum,
            wind_speed_10m_max, ingested_batch_id, updated_at
        ) VALUES (
            %(city)s, %(date)s, %(weather_code)s, %(weather_condition)s, %(temperature_2m_max)s, %(temperature_2m_min)s,
            %(apparent_temperature_max)s, %(apparent_temperature_min)s, %(precipitation_sum)s, %(rain_sum)s,
            %(wind_speed_10m_max)s, %(ingested_batch_id)s, CURRENT_TIMESTAMP
        )
        ON CONFLICT (city, date) DO UPDATE SET
            weather_code = EXCLUDED.weather_code,
            weather_condition = EXCLUDED.weather_condition,
            temperature_2m_max = EXCLUDED.temperature_2m_max,
            temperature_2m_min = EXCLUDED.temperature_2m_min,
            apparent_temperature_max = EXCLUDED.apparent_temperature_max,
            apparent_temperature_min = EXCLUDED.apparent_temperature_min,
            precipitation_sum = EXCLUDED.precipitation_sum,
            rain_sum = EXCLUDED.rain_sum,
            wind_speed_10m_max = EXCLUDED.wind_speed_10m_max,
            ingested_batch_id = EXCLUDED.ingested_batch_id,
            updated_at = CURRENT_TIMESTAMP;
    """

    loaded_hourly = 0
    loaded_daily = 0

    with get_db_cursor(commit=True) as cursor:
        for row in hourly_rows:
            cursor.execute(sql_hourly_upsert, row)
            loaded_hourly += 1

        for row in daily_rows:
            cursor.execute(sql_daily_upsert, row)
            loaded_daily += 1

    logger.info(f"Loaded {loaded_hourly} hourly rows and {loaded_daily} daily rows into Silver schema.")
    return (loaded_hourly, loaded_daily)

def load_gold(gold_data: dict) -> tuple:
    """
    Idempotent UPSERT loading into Gold schema (gold.daily_city_summary, gold.weather_anomalies).
    Also updates rolling 7-day average temperatures.
    """
    summaries = gold_data.get("summaries", [])
    anomalies = gold_data.get("anomalies", [])

    sql_summary_upsert = """
        INSERT INTO gold.daily_city_summary (
            city, summary_date, avg_temp_c, min_temp_c, max_temp_c, temp_range_c,
            avg_humidity_pct, total_precipitation_mm, max_wind_speed_kmh, comfort_index, updated_at
        ) VALUES (
            %(city)s, %(summary_date)s, %(avg_temp_c)s, %(min_temp_c)s, %(max_temp_c)s, %(temp_range_c)s,
            %(avg_humidity_pct)s, %(total_precipitation_mm)s, %(max_wind_speed_kmh)s, %(comfort_index)s, CURRENT_TIMESTAMP
        )
        ON CONFLICT (city, summary_date) DO UPDATE SET
            avg_temp_c = EXCLUDED.avg_temp_c,
            min_temp_c = EXCLUDED.min_temp_c,
            max_temp_c = EXCLUDED.max_temp_c,
            temp_range_c = EXCLUDED.temp_range_c,
            avg_humidity_pct = EXCLUDED.avg_humidity_pct,
            total_precipitation_mm = EXCLUDED.total_precipitation_mm,
            max_wind_speed_kmh = EXCLUDED.max_wind_speed_kmh,
            comfort_index = EXCLUDED.comfort_index,
            updated_at = CURRENT_TIMESTAMP;
    """

    sql_anomalies_upsert = """
        INSERT INTO gold.weather_anomalies (
            city, anomaly_date, anomaly_type, metric_name, metric_value,
            threshold_value, severity, description, detected_at
        ) VALUES (
            %(city)s, %(anomaly_date)s, %(anomaly_type)s, %(metric_name)s, %(metric_value)s,
            %(threshold_value)s, %(severity)s, %(description)s, CURRENT_TIMESTAMP
        )
        ON CONFLICT (city, anomaly_date, anomaly_type) DO UPDATE SET
            metric_value = EXCLUDED.metric_value,
            threshold_value = EXCLUDED.threshold_value,
            severity = EXCLUDED.severity,
            description = EXCLUDED.description,
            detected_at = CURRENT_TIMESTAMP;
    """

    # SQL to recalculate rolling 7-day average temperature in gold.daily_city_summary
    sql_recalculate_rolling_avg = """
        WITH calculated AS (
            SELECT
                city,
                summary_date,
                ROUND(AVG(avg_temp_c) OVER (
                    PARTITION BY city
                    ORDER BY summary_date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ), 2) AS calc_rolling_7d_avg
            FROM gold.daily_city_summary
        )
        UPDATE gold.daily_city_summary g
        SET rolling_7d_avg_temp = c.calc_rolling_7d_avg
        FROM calculated c
        WHERE g.city = c.city AND g.summary_date = c.summary_date;
    """

    loaded_summaries = 0
    loaded_anomalies = 0

    with get_db_cursor(commit=True) as cursor:
        for row in summaries:
            cursor.execute(sql_summary_upsert, row)
            loaded_summaries += 1

        for row in anomalies:
            cursor.execute(sql_anomalies_upsert, row)
            loaded_anomalies += 1

        # Calculate & update window metric
        cursor.execute(sql_recalculate_rolling_avg)

    logger.info(f"Loaded {loaded_summaries} summaries and {loaded_anomalies} anomalies into Gold schema.")
    return (loaded_summaries, loaded_anomalies)

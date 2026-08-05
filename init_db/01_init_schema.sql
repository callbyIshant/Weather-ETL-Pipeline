-- Medallion Architecture Schema Initialization Script

-- 1. Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- 2. Bronze Schema (Raw Data Ingestion)
CREATE TABLE IF NOT EXISTS bronze.raw_weather_payloads (
    id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    city VARCHAR(100) NOT NULL,
    latitude NUMERIC(8, 5),
    longitude NUMERIC(8, 5),
    source_url TEXT NOT NULL,
    raw_json JSONB NOT NULL,
    status_code INT NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_bronze_batch_id ON bronze.raw_weather_payloads(batch_id);
CREATE INDEX IF NOT EXISTS idx_bronze_city ON bronze.raw_weather_payloads(city);

-- 3. Silver Schema (Cleaned & Standardized Time Series Data)
CREATE TABLE IF NOT EXISTS silver.weather_hourly (
    city VARCHAR(100) NOT NULL,
    latitude NUMERIC(8, 5) NOT NULL,
    longitude NUMERIC(8, 5) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    temperature_2m NUMERIC(5, 2),
    relative_humidity_2m NUMERIC(5, 2),
    dew_point_2m NUMERIC(5, 2),
    apparent_temperature NUMERIC(5, 2),
    precipitation NUMERIC(6, 2),
    rain NUMERIC(6, 2),
    weather_code INT,
    weather_condition VARCHAR(50),
    surface_pressure NUMERIC(7, 2),
    wind_speed_10m NUMERIC(5, 2),
    ingested_batch_id UUID,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (city, timestamp)
);

CREATE TABLE IF NOT EXISTS silver.weather_daily (
    city VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    weather_code INT,
    weather_condition VARCHAR(50),
    temperature_2m_max NUMERIC(5, 2),
    temperature_2m_min NUMERIC(5, 2),
    apparent_temperature_max NUMERIC(5, 2),
    apparent_temperature_min NUMERIC(5, 2),
    precipitation_sum NUMERIC(6, 2),
    rain_sum NUMERIC(6, 2),
    wind_speed_10m_max NUMERIC(5, 2),
    ingested_batch_id UUID,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (city, date)
);

-- 4. Gold Schema (Aggregated Business Intelligence & Analytics)
CREATE TABLE IF NOT EXISTS gold.daily_city_summary (
    city VARCHAR(100) NOT NULL,
    summary_date DATE NOT NULL,
    avg_temp_c NUMERIC(5, 2),
    min_temp_c NUMERIC(5, 2),
    max_temp_c NUMERIC(5, 2),
    temp_range_c NUMERIC(5, 2),
    avg_humidity_pct NUMERIC(5, 2),
    total_precipitation_mm NUMERIC(6, 2),
    max_wind_speed_kmh NUMERIC(5, 2),
    rolling_7d_avg_temp NUMERIC(5, 2),
    comfort_index VARCHAR(30),
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (city, summary_date)
);

CREATE TABLE IF NOT EXISTS gold.weather_anomalies (
    id BIGSERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    anomaly_date DATE NOT NULL,
    anomaly_type VARCHAR(50) NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    metric_value NUMERIC(7, 2) NOT NULL,
    threshold_value NUMERIC(7, 2) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    description TEXT,
    detected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_city_date_anomaly UNIQUE (city, anomaly_date, anomaly_type)
);

-- Indexes for analytical query optimization
CREATE INDEX IF NOT EXISTS idx_silver_hourly_city_time ON silver.weather_hourly(city, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_gold_summary_date ON gold.daily_city_summary(summary_date DESC);

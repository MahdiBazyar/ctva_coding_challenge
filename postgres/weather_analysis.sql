-- Table: ctva.weather_analysis

-- DROP TABLE IF EXISTS ctva.weather_analysis;


CREATE TABLE IF NOT EXISTS ctva.weather_analysis
(
    station_id character varying(20) COLLATE pg_catalog."default" NOT NULL,
    year integer NOT NULL,
    avg_max_temperature_celsius double precision,
    avg_min_temperature_celsius double precision,
    total_precipitation_cm double precision,
    CONSTRAINT weather_analysis_pkey PRIMARY KEY (station_id, year)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS ctva.weather_analysis
    OWNER to postgres;

GRANT ALL ON TABLE ctva.weather_analysis TO postgres;

-- Weather Analysis
INSERT INTO ctva.weather_analysis (station_id, year, avg_max_temperature_celsius, avg_min_temperature_celsius, total_precipitation_cm)
                        SELECT
                            station_id,
                            year,
                            ROUND(AVG(NULLIF(max_temperature, -9999) / 10)::numeric, 2) AS avg_max_temperature_celsius,
                            ROUND(AVG(NULLIF(min_temperature, -9999) / 10)::numeric, 2) AS avg_min_temperature_celsius,
                            ROUND(SUM(NULLIF(precipitation, -9999) / 100)::numeric, 2) AS total_precipitation_cm
                        FROM
                            ctva.weather_data
                        GROUP BY
                            station_id,
                            year
                        ORDER BY
                            station_id,
                            year
                        ON CONFLICT (station_id, year)
                        DO UPDATE SET
                            avg_max_temperature_celsius = EXCLUDED.avg_max_temperature_celsius,
                            avg_min_temperature_celsius = EXCLUDED.avg_min_temperature_celsius,
                            total_precipitation_cm = EXCLUDED.total_precipitation_cm;





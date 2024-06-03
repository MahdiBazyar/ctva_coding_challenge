-- Table: ctva.weather_data

-- DROP TABLE IF EXISTS ctva.weather_data;


CREATE TABLE IF NOT EXISTS ctva.weather_data
(
    station_id character varying(20) COLLATE pg_catalog."default" NOT NULL,
	date integer NOT NULL,
	year integer NOT NULL,
    max_temperature double precision,
    min_temperature double precision,
    precipitation double precision,
    created timestamp with time zone NOT NULL DEFAULT timezone('EDT'::text, CURRENT_TIMESTAMP(3)),
    updated timestamp with time zone NOT NULL DEFAULT timezone('EDT'::text, CURRENT_TIMESTAMP(3)),
    
    CONSTRAINT weather_data_pkey PRIMARY KEY (year, station_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS ctva.weather_data
    OWNER to postgres;

GRANT ALL ON TABLE ctva.weather_data TO postgres;
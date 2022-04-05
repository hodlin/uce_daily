CREATE TABLE public.mart_operative_forecasting_result
(
    site character varying(255),
    latitude numeric,
    longitude numeric,
    region character varying(255),
    cluster integer,
    date date,
    hour integer,
    datetime timestamp without time zone,
    datetime_tz timestamp with time zone,
    yield integer,
    forecast integer,
    error integer,
    error_positive integer,
    error_negative integer,
    error_abs integer,
    error_type character varying(255),
    CONSTRAINT primary_key PRIMARY KEY (site, date, hour)
)
WITH (
    OIDS = FALSE
);

ALTER TABLE IF EXISTS public.mart_operative_forecasting_result
    OWNER to energy_analytics_owner;
-- show stock data alongside weather data

WITH weather_pivot AS (
    SELECT
        date,

        -- New York
        MAX(CASE WHEN city = 'New York' THEN max_temp END) AS ny_max_temp,
        MAX(CASE WHEN city = 'New York' THEN min_temp END) AS ny_min_temp,
        MAX(CASE WHEN city = 'New York' THEN max_wind END) AS ny_max_wind,
        MAX(CASE WHEN city = 'New York' THEN precip END) AS ny_precip,

        -- London
        MAX(CASE WHEN city = 'London' THEN max_temp END) AS london_max_temp,
        MAX(CASE WHEN city = 'London' THEN min_temp END) AS london_min_temp,
        MAX(CASE WHEN city = 'London' THEN max_wind END) AS london_max_wind,
        MAX(CASE WHEN city = 'London' THEN precip END) AS london_precip,

        -- Tokyo
        MAX(CASE WHEN city = 'Tokyo' THEN max_temp END) AS tokyo_max_temp,
        MAX(CASE WHEN city = 'Tokyo' THEN min_temp END) AS tokyo_min_temp,
        MAX(CASE WHEN city = 'Tokyo' THEN max_wind END) AS tokyo_max_wind,
        MAX(CASE WHEN city = 'Tokyo' THEN precip END) AS tokyo_precip

    FROM snowbearair_db.raw.weather_api_evans_e
    GROUP BY date
)

SELECT
    s.symbol,
    CAST(s.datetime AS DATE) AS date,
    s.open,
    s.high,
    s.low,
    s.close,

    w.ny_max_temp,
    w.ny_min_temp,
    w.ny_max_wind,
    w.ny_precip,

    w.london_max_temp,
    w.london_min_temp,
    w.london_max_wind,
    w.london_precip,

    w.tokyo_max_temp,
    w.tokyo_min_temp,
    w.tokyo_max_wind,
    w.tokyo_precip

FROM snowbearair_db.raw.stonks s
LEFT JOIN weather_pivot w
    ON CAST(s.datetime AS DATE) = w.date
;
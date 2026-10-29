-- Singular test: returns the rows that break basic sanity rules.
-- dbt treats any returned row as a failure.
--   * rates must be between 0 and 1
--   * cancelled plus delayed flights cannot exceed total departures

select
    airport_hour_key,
    total_departures,
    cancelled_flights,
    delayed_flights,
    cancellation_rate,
    delay_rate
from {{ ref('mart_weather_flight_impact') }}
where cancellation_rate not between 0 and 1
   or delay_rate not between 0 and 1
   or cancelled_flights + delayed_flights > total_departures

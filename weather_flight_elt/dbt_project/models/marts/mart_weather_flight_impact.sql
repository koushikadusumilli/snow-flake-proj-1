{{ config(materialized='view') }}

/*
    mart_weather_flight_impact
    --------------------------
    Grain: one row per departure airport per UTC hour.

    Joins hourly departure performance to the weather observed at the origin
    airport in that hour, so analysts can answer "how much does weather hurt
    on-time performance?" with a simple GROUP BY, for example:

        select weather_condition,
               sum(total_departures)                                  as departures,
               round(100.0 * sum(delayed_flights)   / sum(total_departures), 1) as delay_pct,
               round(100.0 * sum(cancelled_flights) / sum(total_departures), 2) as cancel_pct
        from marts.mart_weather_flight_impact
        group by 1
        order by delay_pct desc;

    Flights are the spine of the join (left join to weather), so an hour with
    departures but no weather observation still appears, with
    weather_severity = 'unknown'. Flip materialized to 'table' above if query
    speed matters more than freshness.
*/

with flights as (

    select * from {{ ref('stg_flights') }}

),

weather as (

    select * from {{ ref('stg_weather') }}

),

hourly_departures as (

    select
        origin_airport_code                                         as airport_code,
        scheduled_departure_hour_utc                                as departure_hour_utc,
        count(*)                                                    as total_departures,
        count(*) filter (where is_cancelled)                        as cancelled_flights,
        count(*) filter (where is_delayed)                          as delayed_flights,
        count(*) filter (where not is_cancelled and not is_delayed) as on_time_flights,
        round(avg(departure_delay_minutes), 1)                      as avg_departure_delay_minutes,
        max(departure_delay_minutes)                                as max_departure_delay_minutes
    from flights
    group by 1, 2

),

-- stg_weather is already unique per observation. This guard keeps the join
-- from fanning out if a source ever emits two observations in the same hour.
hourly_weather as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by airport_code, observed_hour_utc
                order by observed_at_utc desc
            ) as hour_rank
        from weather
    ) as ranked
    where hour_rank = 1

),

joined as (

    select
        d.airport_code || '_' || to_char(d.departure_hour_utc, 'YYYYMMDDHH24')
                                                                    as airport_hour_key,
        d.airport_code,
        d.departure_hour_utc,
        cast(d.departure_hour_utc as date)                          as departure_date,

        w.weather_condition,
        w.temperature_f,
        w.temperature_c,
        w.humidity_pct,
        w.wind_speed_mph,
        w.wind_gust_mph,
        w.visibility_miles,
        w.precipitation_in,
        w.is_low_visibility,
        w.is_high_wind,
        w.has_precipitation,

        d.total_departures,
        d.cancelled_flights,
        d.delayed_flights,
        d.on_time_flights,
        d.avg_departure_delay_minutes,
        d.max_departure_delay_minutes

    from hourly_departures as d
    left join hourly_weather as w
        on  d.airport_code       = w.airport_code
        and d.departure_hour_utc = w.observed_hour_utc

),

enriched as (

    select
        *,

        case
            when weather_condition is null
                then 'unknown'
            when weather_condition = 'Thunderstorm'
                or coalesce(visibility_miles, 10) < 1
                or coalesce(wind_gust_mph, wind_speed_mph) >= 35
                then 'severe'
            when weather_condition in ('Snow', 'Fog', 'Rain')
                or is_low_visibility
                or is_high_wind
                or has_precipitation
                then 'moderate'
            else 'mild'
        end                                                         as weather_severity,

        round(cast(cancelled_flights as numeric) / nullif(total_departures, 0), 4)
                                                                    as cancellation_rate,
        round(cast(delayed_flights as numeric) / nullif(total_departures, 0), 4)
                                                                    as delay_rate,

        -- Each airport's own overall delay rate across every loaded hour.
        round(
            cast(sum(delayed_flights) over (partition by airport_code) as numeric)
            / nullif(sum(total_departures) over (partition by airport_code), 0),
            4
        )                                                           as airport_baseline_delay_rate

    from joined

)

select
    *,
    -- Positive means this hour was worse than the airport's usual performance.
    round(delay_rate - airport_baseline_delay_rate, 4)              as delay_rate_vs_baseline
from enriched

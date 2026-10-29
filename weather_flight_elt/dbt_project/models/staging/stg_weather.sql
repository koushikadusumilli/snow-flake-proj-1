/*
    stg_weather
    -----------
    Grain: one row per airport per hourly observation.

    What this model does (and nothing more):
      * removes duplicate observations (keeps the most recently loaded copy)
      * standardises codes and labels
      * adds unit conversions and simple boolean flags used by the marts
      * exposes timestamps in UTC, independent of the database session timezone
*/

with source as (

    select * from {{ source('raw', 'weather_observations') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by observation_id
            order by _loaded_at desc
        ) as row_num
    from source

),

cleaned as (

    select
        observation_id,
        upper(trim(airport_code))                                   as airport_code,
        observation_date,
        observed_at_utc,
        date_trunc('hour', observed_at_utc at time zone 'UTC')      as observed_hour_utc,

        temperature_f,
        round((temperature_f - 32) * 5.0 / 9.0, 1)                  as temperature_c,
        humidity_pct,
        wind_speed_mph,
        wind_gust_mph,
        visibility_miles,
        precipitation_in,
        initcap(trim(weather_condition))                            as weather_condition,

        coalesce(visibility_miles < 3, false)                       as is_low_visibility,
        coalesce(wind_gust_mph, wind_speed_mph) >= 25               as is_high_wind,
        coalesce(precipitation_in > 0, false)                       as has_precipitation,

        _loaded_at                                                  as loaded_at

    from deduplicated
    where row_num = 1

)

select * from cleaned

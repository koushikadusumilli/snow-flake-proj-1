/*
    stg_flights
    -----------
    Grain: one row per scheduled flight departure.

    What this model does (and nothing more):
      * removes duplicate flights (keeps the most recently loaded copy)
      * standardises airport codes and the free-text status label
      * exposes timestamps in UTC, independent of the database session timezone
      * adds the delay and cancellation flags used by the marts

    A flight counts as delayed when it leaves 15 or more minutes late, the same
    threshold the US DOT uses. Cancelled flights have no actual times.
*/

with source as (

    select * from {{ source('raw', 'flights') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by flight_id
            order by _loaded_at desc
        ) as row_num
    from source

),

standardized as (

    select
        flight_id,
        flight_date,
        upper(trim(carrier_code))                                   as carrier_code,
        flight_number,
        upper(trim(origin_airport))                                 as origin_airport_code,
        upper(trim(destination_airport))                            as destination_airport_code,

        scheduled_departure_utc at time zone 'UTC'                  as scheduled_departure_at_utc,
        actual_departure_utc    at time zone 'UTC'                  as actual_departure_at_utc,
        scheduled_arrival_utc   at time zone 'UTC'                  as scheduled_arrival_at_utc,
        actual_arrival_utc      at time zone 'UTC'                  as actual_arrival_at_utc,

        departure_delay_minutes,
        arrival_delay_minutes,

        case
            when lower(trim(flight_status)) in ('on time', 'on-time', 'ontime') then 'on_time'
            when lower(trim(flight_status)) = 'delayed'                         then 'delayed'
            when lower(trim(flight_status)) in ('cancelled', 'canceled')        then 'cancelled'
            else 'unknown'
        end                                                         as flight_status,

        aircraft_type,
        distance_miles,
        _loaded_at                                                  as loaded_at

    from deduplicated
    where row_num = 1

),

final as (

    select
        *,
        upper(trim(origin_airport_code)) || '-' || destination_airport_code
                                                                    as route,
        date_trunc('hour', scheduled_departure_at_utc)              as scheduled_departure_hour_utc,
        flight_status = 'cancelled'                                 as is_cancelled,
        coalesce(departure_delay_minutes >= 15, false)              as is_delayed

    from standardized

)

select * from final

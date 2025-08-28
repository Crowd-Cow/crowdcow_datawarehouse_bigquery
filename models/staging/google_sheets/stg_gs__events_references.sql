with

source as ( select * from {{ source('google_sheets', 'events_reporting_reference_events') }} )

,renamed as (
    select
         id as reference_id
        ,start_date as start_date_at_utc
        ,end_date as end_date_at_utc
        ,{{ clean_strings('channel') }} as channel
        ,{{ clean_strings('entry_type') }} as entry_type
        ,{{ clean_strings('event') }} as event
        ,{{ clean_strings('name') }} as name
        ,{{ clean_strings('event_type') }} as event_type
        ,year
        ,is_launch
        ,plan_revenue  as plan_data
        ,date_diff(
            cast(end_date as date),
            cast(start_date as date),
            day
        ) as duration_days
        ,safe_divide(
            plan_revenue,
            date_diff(
                cast(end_date as date),
                cast(start_date as date),
                day
            )
        ) as daily_plan_data
        ,row_number() over ( partition by id, channel, entry_type, name, event, event_type, year) as rn
    from source
)

select * from renamed where rn = 1 





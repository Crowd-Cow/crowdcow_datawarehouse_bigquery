{{ config(
    materialized='incremental',
    unique_key='visit_id',
    partition_by={'field': 'started_at_utc', 'data_type': 'timestamp'},
    cluster_by=['visit_ip']
) }}

with base_visits as (
    select *
    from {{ ref('visit_classification') }}
    {% if is_incremental() %}
      where started_at_utc > (select max(started_at_utc) from {{ this }})
    {% endif %}
),

lagged_visits as (
    select
        *,
        LAG(started_at_utc) OVER (PARTITION BY visit_ip ORDER BY started_at_utc) as prev_started_at
    from base_visits
),

session_start_flags as (
    select
        *,
        case
            when prev_started_at is null or TIMESTAMP_DIFF(started_at_utc, prev_started_at, minute) >= 420 then 1
            else 0
        end as new_session_start_flag
    from lagged_visits
),

ip_sessions as (
    select
        *,
        SUM(new_session_start_flag) OVER (PARTITION BY visit_ip ORDER BY started_at_utc) as ip_session_number
    from session_start_flags
)

select * from ip_sessions
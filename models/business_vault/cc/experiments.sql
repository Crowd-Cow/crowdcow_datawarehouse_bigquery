{{ config(
    materialized='incremental',
    unique_key='event_id',
    partition_by={'field': 'occurred_at_utc', 'data_type': 'timestamp'},
    cluster_by=['visit_id']
) }}

with

events as (
    select
        event_id,
        visit_id,
        occurred_at_utc,
        experiments
    from {{ ref('events') }}
    where event_name = 'EXPERIMENT_ASSIGNED_TO_SESSION'
      and occurred_at_utc >= date_sub(current_timestamp(), interval 30 day)
      {% if is_incremental() %}
        and occurred_at_utc >= (select max(occurred_at_utc) from {{ this }})
      {% endif %}
),

experiment_details as (
    select
        event_id,
        visit_id,
        REPLACE(
          TRANSLATE(
            UPPER(TRIM(TO_JSON_STRING(experiments))),
            '{}\"',
            ''
          ),
          'EXP-CC-',
          ''
        ) AS experiments
    from events
)

select
    event_id,
    visit_id,
    experiments
from experiment_details

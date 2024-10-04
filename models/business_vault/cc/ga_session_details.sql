{{ config(
  enabled=false
) }}
with

ga_session_details as ( select * from {{ ref('stg_ga__session_details') }} )

select * from ga_session_details
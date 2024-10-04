{{ config(
  enabled=false
) }}
with

ga_incoming_traffic as ( select * from {{ ref('stg_ga__incoming_traffic') }} )

select * from ga_incoming_traffic
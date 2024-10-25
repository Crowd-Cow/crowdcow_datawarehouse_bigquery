with

ga4_incoming_traffic as ( select * from {{ ref('stg_ga4__incoming_traffic') }} )

select * from ga4_incoming_traffic
with

ga_incoming_traffic as ( select * from {{ ref('stg_google_analytics__incoming_traffic') }} )

select * from ga_incoming_traffic
with

ga_session_details as ( select * from {{ ref('stg_ga4__session_details') }} )

select * from ga_session_details
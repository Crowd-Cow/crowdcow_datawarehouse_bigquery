{{
  config(
    tags=["staging", "spreadsheet"],
    materialized = "view"
  )
}}

select
   first_name
  ,last_name
  ,email_address as email
  ,admin_link
  ,date_received::date as date_received
from {{ ref('stg_strings__ccpa_requests') }}
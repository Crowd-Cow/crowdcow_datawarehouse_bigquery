
{% set column_names = [
     "first_name"
    ,"last_name"
    ,"email_address"
    ,"admin_link"
    ,"date_received"
  ] 
%}

select
  {{ google_sheets_stg_strings(column_names)}}
from {{ source('google_sheets', 'ccpa_requests') }}

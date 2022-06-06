
{% set column_names = [
     "first_name"
    ,"last_name"
    ,"email_address"
    ,"admin_link"
    ,"date_received"
  ] 
%}

with

format_columns as (
  select
    {{ google_sheets_stg_strings(column_names)}}
  from {{ source('google_sheets', 'ccpa_requests') }}
)

,final as (
  select
    {{ clean_strings('first_name') }} as first_name
    ,{{ clean_strings('last_name') }} as last_name
    ,{{ clean_strings('email_address')}} as email
    ,{{ clean_strings('admin_link') }} as admin_link
    ,to_date(date_received,'mm/dd/yyyy') as date_received
  from format_columns
)

select * from final

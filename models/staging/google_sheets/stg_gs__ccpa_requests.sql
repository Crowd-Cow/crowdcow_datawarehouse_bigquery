with

request as ( select * from {{ source('google_sheets', 'ccpa_requests') }} )

,final as (
  select
    {{ clean_strings('first_name') }} as first_name
    ,{{ clean_strings('last_name') }} as last_name
    ,{{ clean_strings('email_address')}} as email
    ,{{ clean_strings('admin_link') }} as admin_link
    ,CAST(date_received as DATE) as date_received
  from request
  qualify row_number() over(partition by email_address, admin_link order by date_received desc) = 1
)

select * from final

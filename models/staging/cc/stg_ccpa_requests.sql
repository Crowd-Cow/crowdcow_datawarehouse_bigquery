

select
   {{ clean_strings('first_name') }} as first_name
  ,{{ clean_strings('last_name') }} as last_name
  ,{{ clean_strings('email_address')}} as email
  ,{{ clean_strings('admin_link') }} as admin_link
  ,date_received::date as date_received
from {{ ref('stg_strings__ccpa_requests') }}
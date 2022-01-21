
select
   {{ clean_strings('first_name') }} as first_name
  ,{{ clean_strings('last_name') }} as last_name
  ,email_address as email
  ,admin_link
  ,date_received::date as date_received
from {{ ref('stg_strings__ccpa_requests') }}
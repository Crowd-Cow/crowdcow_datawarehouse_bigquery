with source as (

    select * from {{ source('cc', 'ahoy_visits') }}

),

renamed as (

    select
        id as visit_id
        ,{{ clean_strings('browser') }} as visit_browser
        ,updated_at as updated_at_utc
        ,{{ clean_strings('city') }} as visit_city
        ,{{ clean_strings('utm_content') }} as utm_content
        --,properties  this field is 100% null in cc.ahoy_visits
        ,{{ clean_strings('visit_token') }} as visit_token
        ,{{ clean_strings('ip') }} as visit_ip
        ,{{ clean_strings('utm_campaign') }} as utm_campaign
        ,{{ clean_strings('landing_page') }} as visit_landing_page
        ,{{ clean_strings('os') }} as visit_os
        ,{{ clean_strings('utm_term') }} as utm_term
        ,{{ clean_strings('utm_medium') }} as utm_medium
        ,started_at as started_at_utc
        ,{{ clean_strings('referrer') }} as visit_referrer
        ,user_id
        ,{{ clean_strings('country') }} as visit_country
        ,{{ clean_strings('search_keyword') }} as visit_search_keyword
        ,{{ clean_strings('utm_source') }} as utm_source
        ,{{ clean_strings('visitor_token') }} as visitor_token
        ,{{ clean_strings('device_type') }} as visit_device_type
        ,{{ clean_strings('referring_domain') }} as visit_referring_domain
        ,{{ clean_strings('region') }} as visit_region
        ,{{ clean_strings('user_agent') }} as visit_user_agent
        ,wall_displayed as is_wall_displayed

    from source

)

select * from renamed


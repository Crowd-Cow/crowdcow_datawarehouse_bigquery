{{
  config(
    tags=["events"]
  )
}}

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
        ,parse_url({{ clean_strings('landing_page') }}):host::text as visit_landing_page_host
        ,parse_url({{ clean_strings('landing_page') }}):path::text as visit_landing_page_path
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

),

add_flags as (

    select
        visit_id
        ,visit_browser
        ,updated_at_utc
        ,visit_city
        ,utm_content
        ,visit_token
        ,visit_ip
        ,utm_campaign
        ,visit_landing_page
        ,visit_landing_page_path

        ,case
            when visit_landing_page_host = 'WWW.CROWDCOW.COM' 
                and visit_landing_page_path = '' or visit_landing_page_path = 'L' then true
            else false
         end as is_homepage_landing

        ,visit_os
        ,utm_term
        ,utm_medium
        ,started_at_utc
        ,visit_referrer
        ,user_id
        ,visit_country
        ,visit_search_keyword
        ,utm_source
        ,visitor_token
        ,visit_device_type
        ,visit_referring_domain
        ,visit_region
        ,visit_user_agent

        ,case
            when /* dsia.ip_address IS NOT NULL TODO: add suspicious IP logic
                or */ visit_user_agent like '%BOT%'
                or lower(visit_user_agent) like '%CRAWL%'
                or lower(visit_user_agent) like '%LIBRATO%'
                or lower(visit_user_agent) like '%TWILIOPROXY%'
                or lower(visit_user_agent) like '%YAHOOMAILPROXY%'
                or lower(visit_user_agent) like '%SCOUTURLMONITOR%'
                or lower(visit_user_agent) like '%FULLCONTACT%'
                or lower(visit_user_agent) like '%IMGIX%'
                or lower(visit_user_agent) like '%BUCK%'
                or (visit_ip is null and visit_user_agent is null) then true
            else false
         end as is_bot

         ,is_wall_displayed
    from renamed
)

select * from add_flags


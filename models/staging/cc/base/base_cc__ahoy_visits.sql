{{
  config(
    tags=["base","events"]
  )
}}

with 
backup as (select * from  raw.deleted_ahoy_visits where started_at <= '2024-06-30' ) --Visits backup from hard deleted data 
,source as ( select * from {{ source('cc', 'ahoy_visits') }} where started_at > '2024-06-30' )

,renamed as (

    select
        id as visit_id
        ,{{ clean_strings('browser') }} as visit_browser
        ,updated_at as updated_at_utc
        --,clean_strings('city') }} as visit_city
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
        --, clean_strings('country') }} as visit_country
        --, clean_strings('search_keyword') }} as visit_search_keyword
        ,{{ clean_strings('utm_source') }} as utm_source
        ,{{ clean_strings('visitor_token') }} as visitor_token
        ,{{ clean_strings('device_type') }} as visit_device_type
        ,{{ clean_strings('referring_domain') }} as visit_referring_domain
        --, clean_strings('region') }} as visit_region
        ,{{ clean_strings('user_agent') }} as visit_user_agent
        ,wall_displayed as is_wall_displayed

    from source

    union all 

    select 
        id as visit_id
        ,{{ clean_strings('browser') }} as visit_browser
        ,updated_at as updated_at_utc
        --,clean_strings('city') }} as visit_city
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
        --, clean_strings('country') }} as visit_country
        --, clean_strings('search_keyword') }} as visit_search_keyword
        ,{{ clean_strings('utm_source') }} as utm_source
        ,{{ clean_strings('visitor_token') }} as visitor_token
        ,{{ clean_strings('device_type') }} as visit_device_type
        ,{{ clean_strings('referring_domain') }} as visit_referring_domain
        --, clean_strings('region') }} as visit_region
        ,{{ clean_strings('user_agent') }} as visit_user_agent
        ,cast(if(wall_displayed,1,0) as int64) as is_wall_displayed
      from backup

)

,clean_landing_page as (
  select 
    visit_id
    ,visit_browser
    ,updated_at_utc
    --,visit_city
    ,utm_content
    ,visit_token
    ,visit_ip
    ,utm_campaign
    
    /** Modify landing page to get the URL with the most information **/
    ,case
      when visit_landing_page not like '%UTM_%' 
          and visit_referrer like '%CROWDCOW.COM%'
          and  visit_referrer like '%UTM_%' then coalesce(trim(visit_referrer),'')
      else coalesce(trim(visit_landing_page),'')
     end as visit_landing_page

    ,visit_os
    ,utm_term
    ,utm_medium
    ,started_at_utc
    ,visit_referrer
    ,user_id
    --,visit_country
    --,visit_search_keyword
    ,utm_source
    ,visitor_token
    ,visit_device_type
    ,visit_referring_domain
    --,visit_region
    ,visit_user_agent
    ,is_wall_displayed
  from renamed
)

,parse_landing_page as (
  select 
    *
    ,NET.HOST(visit_landing_page) AS visit_landing_page_host
    ,REPLACE(REPLACE(CONCAT('/', REGEXP_EXTRACT(visit_landing_page, r'(?i)https?://[^/]+(/[^?#]*)')), '//', '/'), '/ROBOTS.TXT', '') AS visit_landing_page_path

  from clean_landing_page
)

,identify_bad_urls as (
  select
    *
    ,visit_landing_page_path like any ('%.JS%','%.ICO%','%.PNG%','%.CSS%','%.PHP%','%.TXT%','%GRAPHQL%'
                                       ,'%.XML%','%.SQL%','%.ICS%','%WELL-KNOWN%','%/e/%','%.ENV%','%/WP-%','/CROWDCOW.COM%'
                                       ,'%/WWW.CROWDCOW.COM%.%','%/ADMIN%','%/INGREDIENT-LIST%','%.','%PHPINFO%','%.YML%'
                                       ,'%.HTML%','%.ASP','%XXXSS%','%.RAR','%.AXD%','%.AWS%','%;VAR%') as is_invalid_visit
  from parse_landing_page
)

select * from identify_bad_urls where not is_invalid_visit and visit_landing_page <> ''

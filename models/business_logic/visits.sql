{{
  config(
        materialized = 'incremental',
        unique_key = 'visit_id',
        snowflake_warehouse = 'TRANSFORMING_M'
    )
}}

with

base_visits as ( 
    select * 
    from {{ ref('base_cc__ahoy_visits') }} 
    
    {% if is_incremental() %}
      where started_at_utc >= coalesce((select max(started_at_utc) from {{ this }}), '1900-01-01')
    {% endif %}

)

,ambassadors as ( select * from {{ ref('stg_cc__ambassadors') }} )
,partners as ( select * from {{ ref('stg_cc__partners') }} )

,ambassador_paths as (
    select distinct
        partners.partner_path
    from ambassadors
        inner join partners on ambassadors.partner_id = partners.partner_id
)

,visits as (
    select
        visit_id
        ,user_id
        ,visitor_token
        ,visit_token
        
        /** Modify landing page to get the URL with the most information **/
        ,case
            when visit_landing_page not like '%UTM_%' 
                and visit_referrer like '%CROWDCOW.COM%'
                and  visit_referrer like '%UTM_%' then coalesce(trim(visit_referrer),'')
            else coalesce(trim(visit_landing_page),'')
        end as visit_landing_page
        
        ,visit_referring_domain
        ,visit_referrer
        ,visit_search_keyword
        ,visit_browser
        ,visit_ip

        /** Assign a sequential session number to the same IP address if the visits are within 30 minutes of each other **/
        /** For example: the first visit for IP address 127.0.0.1 gets a session number of 0. If the second visit for the same IP address is within 30 minutes, the session number stays 0. **/
        /** If the next visit for the same IP address is more than 30 minutes from the previous visit, the session number increments to 1 **/
        ,CONDITIONAL_TRUE_EVENT(DATEDIFF(MIN, LAG(started_at_utc) OVER (PARTITION BY visit_ip ORDER BY started_at_utc ASC), started_at_utc) >= 30) OVER (PARTITION BY visit_ip ORDER BY started_at_utc ASC) AS ip_session_number

        ,visit_device_type
        ,visit_user_agent
        ,visit_os
        ,utm_source
        ,utm_medium
        ,utm_campaign
        ,utm_content
        ,utm_term
        ,visit_city
        ,visit_country
        ,visit_region
        ,is_wall_displayed
        ,started_at_utc
        ,updated_at_utc
    from base_visits
)

,extract_url_parts as (
    select
        visits.visit_id
        ,visits.user_id
        ,visits.visitor_token
        ,visits.visit_token
        ,visits.visit_landing_page
        ,parse_url(visits.visit_landing_page):host::text as visit_landing_page_host
        ,replace(replace('/' || parse_url(visits.visit_landing_page):path::text,'//','/'),'/ROBOTS.TXT','') as visit_landing_page_path
        ,visits.visit_referring_domain
        ,visits.visit_referrer
        ,visits.visit_search_keyword
        ,visits.visit_browser
        ,visits.visit_ip

        /** Combine the IP address with the sequential session number to create a unique session ID for that IP address **/
        /** Note: This session ID is not unique per day. For example: On day one, at 11:59 pm, IP address is assigned a session ID of 127.0.0.1-0 **/
        /** On day two, at 1:45 am more than 30 minutes from the previous visit, the session ID is now 127.0.0.1-1 and does not start over at 127.0.0.1-0 **/
        ,visits.visit_ip || '-' || visits.ip_session_number as visitor_ip_session
        ,visits.visit_device_type
        ,visits.visit_user_agent
        ,visits.visit_os
        ,visits.utm_source
        ,visits.utm_medium
        ,visits.utm_campaign
        ,visits.utm_content
        ,visits.utm_term
        ,parse_url(visits.visit_landing_page) as parsed_landing_page
        ,parse_url(visits.visit_landing_page):parameters:UTM_MEDIUM::text as landing_utm_medium
        ,parse_url(visits.visit_landing_page):parameters:UTM_SOURCE::text as landing_utm_source
        ,parse_url(visits.visit_landing_page):parameters:UTM_CAMPAIGN::text as landing_utm_campaign
        ,parse_url(visits.visit_landing_page):parameters:UTM_ADSET::text as landing_utm_adset
        ,ambassador_paths.partner_path as ambassador_path
        ,visits.visit_city
        ,visits.visit_country
        ,visits.visit_region
        ,visits.is_wall_displayed
        ,visits.started_at_utc
        ,visits.updated_at_utc
    from visits
        left join ambassador_paths on parse_url(visits.visit_landing_page):path::text = ambassador_paths.partner_path
)

,combine_elements_extract_user_token as (
    select
        visit_id
        ,user_id
        ,visitor_token
        ,visit_token
        ,visit_landing_page
        ,visit_landing_page_host
        ,coalesce(visit_landing_page_path,'') as visit_landing_page_path

        /** Extract user token from the landing page URL **/
        ,case
            when visit_landing_page_path like '/L_U%' then split_part(visit_landing_page_path,'/',3)
            else coalesce(object_pick(parsed_landing_page:parameters,'C'):C::text,object_pick(parsed_landing_page:parameters,'USER_TOKEN'):USER_TOKEN::text)
         end as visit_landing_page_user_token

        ,coalesce(visit_referring_domain,'') as visit_referring_domain
        ,visit_referrer
        ,coalesce(visit_search_keyword,'') as visit_search_keyword
        ,visit_browser
        ,visit_ip
        ,visitor_ip_session

        /** Adds the row number per visitor_ip_session so that marketing can isolate the first visit for this IP session in order to attribute the source of the visit appropriately **/
        ,row_number() over(partition by visitor_ip_session order by started_at_utc, visit_id) as ip_session_visit_number

        ,visit_device_type
        ,visit_user_agent
        ,visit_os
        ,coalesce(utm_source,landing_utm_source,'') as utm_source
        ,coalesce(utm_medium,landing_utm_medium,'') as utm_medium
        ,coalesce(utm_campaign,landing_utm_campaign,'') as utm_campaign
        ,coalesce(landing_utm_adset,'') as utm_adset
        ,utm_content
        ,utm_term
        ,coalesce(ambassador_path,'') as ambassador_path
        ,visit_city
        ,visit_country
        ,visit_region
        ,is_wall_displayed
        ,started_at_utc
        ,updated_at_utc
    from extract_url_parts
)

,assign_sub_channel as (
    select
        *
        ,case 
            when concat(utm_source,utm_medium,visit_referring_domain) like any ('%INSTAGRAM%','%IGSHOPPING%') then 'INSTAGRAM'
            when concat(utm_source,utm_medium,visit_referring_domain) like '%FACEBOOK%GROUP%' then 'FACEBOOK-GROUP'
            when concat(utm_source,utm_medium,visit_referring_domain) like any ('%FACEBOOK%','%ZEMANTA%') then 'FACEBOOK'
            when concat(utm_source,utm_medium,visit_referring_domain) like '%LINKTR.EE%' then 'LINKTREE'
            when concat(utm_source,utm_medium,visit_referring_domain) like '%YOUTUBE%' then 'YOUTUBE'
            when concat(utm_source,utm_medium,visit_referring_domain) like '%REDDIT%' then 'REDDIT'
            when concat(utm_source,utm_medium,visit_referring_domain) like '%LINKEDIN%' then 'LINKEDIN'
            when concat(utm_source,utm_medium,visit_referring_domain) like '%TWITTER%' then 'TWITTER'
            when concat(utm_source,utm_medium,visit_referring_domain) like '%TIKTOK%' then 'TIKTOK'
            when concat(utm_source,utm_medium,visit_referring_domain) like '%PINTEREST%' then 'PINTEREST'
            when utm_medium like '%PODCAST%' then 'PODCAST'
            when utm_source like '%GEIST%' then 'GEIST'
            when visit_landing_page like '%/L_U%' and visit_landing_page_user_token <> '' then 'USER REFERRAL'
            when utm_medium like '%PARTNER%' then 'PARTNER'
            when utm_medium like '%AFFILIATE%' or utm_source like '%SHAREASALE%' or visit_landing_page like '%/AFFILIATE-GIVEAWAY%' then 'AFFILIATE'
            when utm_medium like '%AMBASSADOR%' or visit_landing_page like '%/AMBASSADOR-GIVEAWAY%' or ambassador_path <> '' then 'AMBASSADOR'
            when utm_medium like '%INFLUENCER%' or visit_referring_domain like '%INFLUENCER%' then 'INFLUENCER'
            when utm_medium like '%SMS%' or utm_source like '%ATTENTIVE%' then 'SMS - MARKETING'
            when utm_medium like '%SMS%' and utm_source not like '%ATTENTIVE%' then 'SMS -TRANSACTIONAL'
            when utm_source like '%TXN%' then 'EMAIL MARKETING - TRANSACTIONAL'
            when (utm_medium like '%EMAIL%' or utm_source like '%ITERABLE%') and utm_source <> 'TXN' and utm_medium <> 'SMS' and utm_campaign like '%_202%' then 'EMAIL MARKETING - MANUAL'
            when (utm_medium like '%EMAIL%' or utm_source like '%ITERABLE%') and utm_source <> 'TXN' and utm_medium <> 'SMS' and utm_campaign not like '%_202%' then 'EMAIL MARKETING - AUTOMATED'
            when (visit_referring_domain like '%GOOGLE.%' or utm_source = 'GOOGLE') then 'GOOGLE'
            when (visit_referring_domain like '%BING.%' or utm_source = 'BING') then 'BING'
            when visit_referring_domain like any ('%YAHOO.%','%DUCKDUCKGO.%') then 'SEO'
            when visit_referring_domain <> '' and visit_referring_domain not like '%CROWDCOW.%' then 'NON-USER REFERRAL'
            when utm_campaign = '' and visit_referring_domain = '' then 'DIRECT'
            else 'OTHER'
         end as sub_channel
    from combine_elements_extract_user_token
)

,assign_paid_social_platform as (
    select
        *
        ,utm_medium in ('OCPM', 'CPC', 'CPCB', 'CPCNB', 'MAXCPA', 'CPM') 
            or utm_source = 'PINTEREST' 
            or utm_source like 'PAID%'
            or utm_medium like 'PAID%'
            or sub_channel in ('GEIST','GOOGLE','BING','USER REFERRAL','NON-USER REFERRAL','PARTNER','AFFILIATE','AMBASSADOR','INFLUENCER') as is_paid_referrer
        ,sub_channel in ('INSTAGRAM','FACEBOOK-GROUP','FACEBOOK','LINKTREE','YOUTUBE','REDDIT','LINKEDIN','TWITTER','TIKTOK','PINTEREST','PODCAST') as is_social_platform_referrer
    from assign_sub_channel
)

,assign_channel as (
    select
        visit_id
        ,user_id
        ,visitor_token
        ,visit_token
        ,visit_landing_page
        ,visit_landing_page_host
        ,visit_landing_page_path
        ,visit_landing_page_user_token
        ,visit_referring_domain
        ,visit_referrer
        ,visit_search_keyword
        ,visit_browser
        ,visit_ip
        ,visitor_ip_session
        ,ip_session_visit_number
        ,visit_device_type
        ,visit_user_agent
        ,visit_os
        ,utm_source
        ,utm_medium
        ,utm_campaign
        ,utm_adset
        ,utm_content
        ,utm_term
        
        ,case
            when is_paid_referrer and is_social_platform_referrer then 'SOCIAL'
            when is_paid_referrer and sub_channel in ('GOOGLE','BING') then 'SEM'
            when is_paid_referrer and sub_channel in ('USER REFERRAL','NON-USER REFERRAL') then 'REFERRAL'
            when is_paid_referrer and sub_channel in ('AMBASSADOR','INFLUENCER') then 'INFLUENCER'
            when is_paid_referrer and sub_channel = 'GEIST' then 'CONTENT'
            when not is_paid_referrer and is_social_platform_referrer then 'ORGANIC SOCIAL'
            when not is_paid_referrer and sub_channel like 'SMS%' then 'SMS'
            when not is_paid_referrer and sub_channel like 'EMAIL%' then 'EMAIL'
            else sub_channel
         end as channel

        ,sub_channel
        ,null::text as visit_attributed_source
        ,ambassador_path
        ,visit_city
        ,visit_country
        ,visit_region
        ,is_paid_referrer
        ,is_social_platform_referrer
        ,is_wall_displayed
        ,started_at_utc
        ,updated_at_utc
    from assign_paid_social_platform
)

select * from assign_channel

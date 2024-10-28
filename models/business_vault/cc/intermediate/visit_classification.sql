{% set partitions_to_replace = [
  'timestamp(current_date)',
  'timestamp(date_sub(current_date, interval 1 day))'
] %}
{{
  config(
        materialized = 'incremental',
        partition_by = {"field": "started_at_utc", "data_type": "timestamp" },
        incremental_strategy = 'insert_overwrite',
        cluster_by = ["visit_id","user_id"],
        partitions = partitions_to_replace
    )
}}

with

base_visits as ( 
    select * 
    from {{ ref('base_cc__ahoy_visits') }} 
    {% if is_incremental() %}
      where timestamp_trunc(started_at_utc, day) in ({{ partitions_to_replace | join(',') }})
    {% endif %}

)

,ambassadors as ( select * from {{ ref('stg_cc__ambassadors') }} )
,partners as ( select * from {{ ref('stg_cc__partners') }} )

,most_current_partner_path as (
    select
        partner_id
        ,partner_path
        ,row_number() over(partition by partner_path order by created_at_utc desc) as rn
    from {{ ref('stg_cc__partners') }}
    qualify rn = 1
)

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
        ,visit_landing_page
        ,visit_landing_page_path
        ,visit_landing_page_host
        ,visit_referring_domain
        ,visit_referrer
        --,visit_search_keyword
        ,visit_browser
        ,visit_ip
        ,visit_device_type
        ,visit_user_agent
        ,visit_os
        ,utm_source
        ,utm_medium
        ,utm_campaign
        ,utm_content
        ,utm_term
        --,visit_city
        --,visit_country
        --,visit_region
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
        ,visits.visit_landing_page_host
        ,visits.visit_landing_page_path
        ,visits.visit_referring_domain
        ,visits.visit_referrer
        --,visits.visit_search_keyword
        ,visits.visit_browser
        ,visits.visit_ip
        ,visits.visit_device_type
        ,visits.visit_user_agent
        ,visits.visit_os
        ,visits.utm_source
        ,visits.utm_medium
        ,visits.utm_campaign
        ,visits.utm_content
        ,visits.utm_term
        ,REGEXP_EXTRACT(visits.visit_landing_page, r'\?(.*)') AS parsed_landing_page
        ,REGEXP_EXTRACT(visits.visit_landing_page, '[?&]UTM_MEDIUM=([^&]*)') AS landing_utm_medium
        ,REGEXP_EXTRACT(visits.visit_landing_page, '[?&]UTM_SOURCE=([^&]*)') AS landing_utm_source
        ,REGEXP_EXTRACT(visits.visit_landing_page, '[?&]UTM_CAMPAIGN=([^&]*)') AS landing_utm_campaign
        ,REGEXP_EXTRACT(visits.visit_landing_page, '[?&]UTM_ADSET=([^&]*)') AS landing_utm_adset
        ,REGEXP_EXTRACT(visits.visit_landing_page, '[?&]GCLID=([^&]*)') AS gclid
        ,ambassador_paths.partner_path as ambassador_path
        ,most_current_partner_path.partner_id
        --,visits.visit_city
        --,visits.visit_country
        --,visits.visit_region
        ,visits.is_wall_displayed
        ,visits.started_at_utc
        ,visits.updated_at_utc
    FROM visits
    LEFT JOIN ambassador_paths 
        ON REGEXP_EXTRACT(visits.visit_landing_page, r'^https?://[^/]+(/[^?#]*)') = ambassador_paths.partner_path
    LEFT JOIN most_current_partner_path 
        ON REGEXP_EXTRACT(visits.visit_landing_page, r'^https?://[^/]+(/[^?#]*)') = most_current_partner_path.partner_path


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
         ,LOWER(
                CASE
                    WHEN visit_landing_page_path LIKE '/L_U%' THEN 
                        REGEXP_EXTRACT(visit_landing_page_path, '/L/([A-Z0-9]+)')
                    ELSE 
                        COALESCE(
                             REGEXP_EXTRACT(visit_landing_page, '[?&]C=([^&]*)')
                            ,REGEXP_EXTRACT(visit_landing_page, '[?&]USER_TOKEN=([^&]*)')
                        )
                END
                ) AS visit_landing_page_user_token


        ,coalesce(visit_referring_domain,'') as visit_referring_domain
        ,visit_referrer
        --,coalesce(visit_search_keyword,'') as visit_search_keyword
        ,visit_browser
        ,visit_ip
        ,visit_device_type
        ,visit_user_agent
        ,visit_os
        ,coalesce(utm_source,landing_utm_source,'') as utm_source
        ,coalesce(utm_medium,landing_utm_medium,'') as utm_medium
        ,coalesce(utm_campaign,landing_utm_campaign,'') as utm_campaign
        ,coalesce(landing_utm_adset,'') as utm_adset
        ,utm_content
        ,utm_term
        ,gclid
        ,coalesce(ambassador_path,'') as ambassador_path
        ,partner_id
        --,visit_city
        --,visit_country
        --,visit_region
        ,is_wall_displayed
        ,started_at_utc
        ,updated_at_utc
    from extract_url_parts
)
  ,meta_subchannel as (
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
        --,visit_search_keyword
        ,visit_browser
        ,visit_ip
        ,visit_device_type
        ,visit_user_agent
        ,visit_os
        ,case when (utm_source is null or utm_source = '' or utm_source = '') and utm_campaign like '%VOLT%' THEN 'FACEBOOK' else utm_source end as utm_source
        ,utm_medium
        ,utm_campaign
        ,utm_adset
        ,utm_content
        ,utm_term
        ,gclid
        ,ambassador_path
        ,partner_id
        --,visit_city
        --,visit_country
        --,visit_region
        ,is_wall_displayed
        ,started_at_utc
        ,updated_at_utc 
    from combine_elements_extract_user_token

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
            when utm_medium = 'FIELD-MARKETING' then 'FIELD-MARKETING'
            when utm_source like '%GEIST%' then 'GEIST'
            when visit_landing_page like '%/L_U%' and visit_landing_page_user_token <> '' then 'USER REFERRAL'
            when utm_medium  like '%PARTNER%' or utm_source  like '%PARTNER%' then 'PARTNER'
            when utm_medium like any ('%AFFILIATE%' , '%HIVEWYRE%')
                or utm_source like '%SHAREASALE%' 
                or (visit_landing_page like any ('%/AFFILIATE-GIVEAWAY%','%/FREE-PRODUCT%') and utm_source not in ('GOOGLE','ITERABLE')) then 'AFFILIATE'
            when utm_medium like '%AMBASSADOR%' or visit_landing_page like '%/AMBASSADOR-GIVEAWAY%' or ambassador_path <> '' then 'AMBASSADOR'
            when utm_medium like '%INFLUENCER%' or utm_source like '%INFLUENCER%' or visit_referring_domain like '%INFLUENCER%' then 'INFLUENCER'
            when utm_medium like '%SMS%' and utm_source not like '%ATTENTIVE%' then 'SMS -TRANSACTIONAL'
            when utm_medium like '%SMS%' or utm_source like '%ATTENTIVE%' then 'SMS - MARKETING'
            when utm_source like '%TXN%' then 'EMAIL MARKETING - TRANSACTIONAL'
            when (utm_medium like '%EMAIL%' or utm_source like '%ITERABLE%') and utm_source <> 'TXN' and utm_medium <> 'SMS' then 'EMAIL MARKETING - CAMPAIGNS'
            when (visit_referring_domain like '%GOOGLE.%' or utm_source = 'GOOGLE' or visit_referrer like '%GOOGLE%') then 'GOOGLE'
            when (visit_referring_domain like '%BING.%' or utm_source = 'BING' or visit_referrer like '%BING%') then 'BING'
            when visit_referring_domain like any ('%YAHOO.%','%DUCKDUCKGO.%') or visit_referrer like any ('%YAHOO%','%DUCKDUCKGO%') then 'OTHER SEARCH'
            when visit_referring_domain <> '' and visit_referring_domain not like '%CROWDCOW.%' then 'NON-USER REFERRAL'
            when utm_campaign = '' and utm_medium = '' and utm_source = '' and (visit_referring_domain = '' or visit_referring_domain like '%CROWDCOW.%')
                or (utm_campaign is null and utm_medium is null and utm_source is null and (visit_referring_domain is null or visit_referring_domain like '%CROWDCOW.%'))
                or (trim(utm_campaign) = '' and trim(utm_medium) = '' and trim(utm_source) = '' and (trim(visit_referring_domain) = '' or visit_referring_domain like '%CROWDCOW.%')) 
                or utm_medium = 'SOCIAL' and utm_source = 'REFERRAL' and visit_referring_domain like '%CROWDCOW.%' then 'DIRECT'
            when utm_medium = 'SOCIAL' and utm_source = 'REFERRAL' and visit_referring_domain not like '%CROWDCOW.%' then 'SOCIAL REFERRAL' 
            else 'OTHER'
         end as sub_channel
    from meta_subchannel
)

,assign_paid_social_platform as (
    select
        *
        ,utm_medium in ('OCPM', 'CPC', 'CPCB', 'CPCNB', 'MAXCPA', 'CPM', 'ADS') 
            or utm_source = 'PINTEREST' 
            or utm_source like 'PAID%'
            or utm_medium like 'PAID%'
            or sub_channel in ('FIELD-MARKETING','GEIST','USER REFERRAL','NON-USER REFERRAL','PARTNER','AFFILIATE','AMBASSADOR','INFLUENCER') as is_paid_referrer 
        ,sub_channel in ('INSTAGRAM','FACEBOOK-GROUP','FACEBOOK','LINKTREE','YOUTUBE','REDDIT','LINKEDIN','TWITTER','TIKTOK','PINTEREST','PODCAST','SOCIAL REFERRAL') as is_social_platform_referrer
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
        --,visit_search_keyword
        ,visit_browser
        ,visit_ip
        ,visit_device_type
        ,visit_user_agent
        ,visit_os
        ,utm_source
        ,utm_medium
        ,utm_campaign
        ,utm_adset
        ,utm_content
        ,utm_term
        ,gclid
        
        ,case
            when is_paid_referrer and is_social_platform_referrer then 'SOCIAL'
            when is_paid_referrer and sub_channel in ('GOOGLE','BING') then 'SEM'
            when not is_paid_referrer and sub_channel in ('GOOGLE','BING','OTHER SEARCH') then 'SEO'
            when is_paid_referrer and sub_channel in ('USER REFERRAL','NON-USER REFERRAL') then 'REFERRAL'
            when is_paid_referrer and sub_channel in ('AMBASSADOR','INFLUENCER') then 'INFLUENCER'
            when is_paid_referrer and sub_channel = 'GEIST' then 'CONTENT'
            when not is_paid_referrer and is_social_platform_referrer then 'ORGANIC SOCIAL'
            when not is_paid_referrer and sub_channel like 'SMS%' then 'SMS'
            when not is_paid_referrer and sub_channel like 'EMAIL%' then 'EMAIL'
            else sub_channel
         end as channel

        ,sub_channel
        ,STRING(null) as visit_attributed_source
        ,ambassador_path
        ,partner_id
        --,visit_city
        --,visit_country
        --,visit_region
        ,is_paid_referrer
        ,is_social_platform_referrer
        ,is_wall_displayed
        ,started_at_utc
        ,updated_at_utc
    from assign_paid_social_platform
)

select * from assign_channel

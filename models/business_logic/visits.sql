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

,visit_classification as (
    select
        *
        ,utm_campaign = '' and visit_referring_domain = '' as direct
        ,visit_search_keyword <> '' or visit_referring_domain like any ('%GOOGLE.%','%BING.%','%YAHOO.%','%DUCKDUCKGO.%') as seo
        ,utm_medium like '%AFFILIATE%' or utm_source like '%SHAREASALE%' or visit_landing_page like '%/AFFILIATE-GIVEAWAY%' as affiliate
        ,utm_medium like '%AMBASSADOR%' or visit_landing_page like '%/AMBASSADOR-GIVEAWAY%' or ambassador_path <> '' as ambassador
        ,utm_medium like '%INFLUENCER%' or visit_referring_domain like '%INFLUENCER%' as influencer
        ,visit_landing_page like '%/L_U%' and visit_landing_page_user_token <> '' as user_referral
        ,visit_referring_domain <> '' and visit_referring_domain not like '%CROWDCOW.%' as non_user_referral
        ,utm_source like '%TXN%' as email_transactional
        ,(utm_medium like '%EMAIL%' or utm_source like '%ITERABLE%') and utm_source <> 'TXN' and utm_medium <> 'SMS' and utm_campaign like '%_202%' as email_marketing_manual
        ,(utm_medium like '%EMAIL%' or utm_source like '%ITERABLE%') and utm_source <> 'TXN' and utm_medium <> 'SMS' and utm_campaign not like '%_202%' as email_marketing_automated
        ,(utm_medium like '%EMAIL%' or utm_source like any ('%EMAIL%','%ONBOARDING%')) and not(utm_source like any ('%ITERABLE%','%TXN%')) as email_other
        ,utm_medium like '%SMS%' or utm_source like '%ATTENTIVE%' as sms_marketing
        ,utm_medium like '%SMS%' and utm_source not like '%ATTENTIVE%' as sms_transactional
        ,concat(utm_source,utm_medium,visit_referring_domain) like any ('%INSTAGRAM%','%IGSHOPPING%') as instagram
        ,concat(utm_source,utm_medium,visit_referring_domain) like '%FACEBOOK%' as facebook
        ,concat(utm_source,utm_medium,visit_referring_domain) like '%LINKTREE%' as linktree
        ,concat(utm_source,utm_medium,visit_referring_domain) like '%YOUTUBE%' as youtube
        ,concat(utm_source,utm_medium,visit_referring_domain) like '%REDDIT%' as reddit
        ,concat(utm_source,utm_medium,visit_referring_domain) like '%LINKEDIN%' as linkedin
        ,concat(utm_source,utm_medium,visit_referring_domain) like '%TWITTER%' as twitter
        ,(utm_source = 'SOCIAL' or utm_medium = 'SOCIAL')
                and not(concat(utm_source,utm_medium,visit_referring_domain,visit_landing_page) like any ('%INSTAGRAM%','%IGSHOPPING%','%FACEBOOK%','%LINKTREE%','%YOUTUBE%'
                                                                                                ,'%REDDIT%','%LINKEDIN%','%PINTEREST%','%TWITTER%')) as social_other
        ,utm_medium in ('OCPM', 'CPC', 'CPCB', 'CPCNB', 'MAXCPA', 'CPM') or utm_source = 'PINTEREST' as paid
        ,utm_medium in ('OCPM', 'CPC', 'CPCB', 'CPCNB', 'MAXCPA', 'CPM') and utm_source = 'PINTEREST' as paid_pinterest
        ,utm_medium in ('OCPM', 'CPC', 'CPCB', 'CPCNB', 'MAXCPA', 'CPM') and utm_source in ('ZEMANTA','FACEBOOK') as paid_facebook
        ,utm_medium in ('OCPM', 'CPC', 'CPCB', 'CPCNB', 'MAXCPA', 'CPM') and utm_source = 'GOOGLE' and utm_campaign like '%WAGYU%' as paid_waygu
        ,utm_medium in ('OCPM', 'CPC', 'CPCB', 'CPCNB', 'MAXCPA', 'CPM') and utm_source = 'GOOGLE' and utm_campaign not like '%WAGYU%' as paid_non_waygu
        ,utm_medium in ('OCPM', 'CPC', 'CPCB', 'CPCNB', 'MAXCPA', 'CPM') and utm_source = 'BING' as paid_other_bing
        ,utm_medium in ('OCPM', 'CPC', 'CPCB', 'CPCNB', 'MAXCPA', 'CPM') and concat(utm_source,utm_medium,visit_referring_domain) like '%YOUTUBE%' as paid_other_youtube
        ,concat(utm_source,utm_medium,visit_referring_domain) like '%TIKTOK%' as paid_other_tiktok
        ,concat(utm_source,utm_medium,visit_referring_domain) like '%PHYSICAL%' as paid_other_physical
        ,concat(utm_source,utm_medium) like '%FIELD-MARKETING%' as paid_other_field_marketing
        ,utm_medium like '%PODCAST%' as paid_other_podcast
        ,utm_medium like '%PARTNER%' as paid_other_partner
        ,visit_landing_page_path = '/VOUCHER' as paid_other_voucher
    from combine_elements_extract_user_token
)

,visit_attribution as (
    select
        *
        ,case
            when paid_pinterest then 'PAID: PINTEREST'
            when paid_facebook then 'PAID: FACEBOOK'
            when paid_waygu then 'PAID: WAYGU'
            when paid_non_waygu then 'PAID: NON-WAYGU'
            when ambassador then 'AMBASSADOR'
            when affiliate then 'AFFILIATES'
            when influencer then 'INFLUENCER'
            when paid_other_bing then 'PAID OTHER: BING'
            when paid_other_youtube then 'PAID OTHER: YOUTUBE'
            when paid_other_tiktok then 'PAID OTHER: TIKTOK'
            when paid_other_physical then 'PAID OTHER: PHYSICAL'
            when paid_other_field_marketing then 'PAID OTHER: FIELD MARKETING'
            when paid_other_podcast then 'PAID OTHER: PODCAST'
            when paid_other_partner then 'PAID OTHER: PARTNER'
            when paid_other_voucher then 'PAID OTHER: VOUCHER'
            when paid then 'PAID OTHER'
            when sms_marketing then 'SMS: MARKETING'
            when sms_transactional then 'SMS: TRANSACTIONAL'
            when email_transactional then 'EMAIL: TRANSACTIONAL'
            when email_marketing_manual then 'EMAIL: MANUAL'
            when email_marketing_automated then 'EMAIL: AUTOMATED'
            when email_other then 'EMAIL: OTHER'
            when user_referral then 'USER REFERRAL'
            when instagram then 'SOCIAL: INSTAGRAM'
            when facebook then 'SOCIAL: FACEBOOK'
            when linktree then 'SOCIAL: LINKTREE'
            when youtube then 'SOCIAL: YOUTUBE'
            when reddit then 'SOCIAL: REDDIT'
            when linkedin then 'SOCIAL: LINKED IN'
            when twitter then 'SOCIAL: TWITTER'
            when social_other then 'SOCIAL: OTHER'
            when seo then 'SEO'
            when non_user_referral then 'NON-USER REFERRAL'
            when direct then 'DIRECT'
            else null
        end as visit_attributed_source
    from visit_classification
)

,channel_sub_channel_rollup as (
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
        ,utm_content
        ,utm_term
        
        ,case
            when visit_attributed_source in ('AFFILIATES','AMBASSADOR','INFLUENCER') then 'AFFILIATES'
            when visit_attributed_source in ('USER REFERRAL','NON-USER REFERRAL') then 'REFERRAL'
            when visit_attributed_source like any ('SMS%','EMAIL%','SOCIAL:%') then 'LIFECYCLE'
            when visit_attributed_source in ('PAID: WAYGU','PAID: NON-WAYGU') then 'PAID: GOOGLE'
            when visit_attributed_source like 'PAID OTHER%' then 'PAID: OTHER'
            else visit_attributed_source
        end as channel

        ,case
            when visit_attributed_source like 'EMAIL:%' then 'EMAIL'
            when visit_attributed_source like 'SMS:%' then 'SMS'
            when visit_attributed_source like 'SOCIAL:%' then 'SOCIAL'
            when visit_attributed_source like 'PAID:%' then 'PAID'
            when visit_attributed_source in ('PAID OTHER: BING','PAID OTHER: YOUTUBE','PAID OTHER: TIKTOK','PAID OTHER: PHYSICAL'
                                                ,'PAID OTHER: FIELD MARKETING','PAID OTHER') then 'PAID'
            when visit_attributed_source = 'PAID OTHER: PODCAST' then 'PODCAST'
            when visit_attributed_source = 'PAID OTHER: PARTNER' then 'PARTNER'
            when visit_attributed_source = 'PAID OTHER: VOUCHER' then 'VOUCHER'
            else visit_attributed_source
        end as sub_channel

        ,visit_attributed_source
        ,ambassador_path
        ,visit_city
        ,visit_country
        ,visit_region
        ,is_wall_displayed
        ,started_at_utc
        ,updated_at_utc
    from visit_attribution
)

select * from channel_sub_channel_rollup

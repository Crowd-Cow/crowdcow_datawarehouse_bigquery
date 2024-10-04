
with 

ccpa as (select * from {{ ref('stg_gs__ccpa_requests') }})
,users as (select * from {{ ref('stg_cc__users') }})

,ccpa_users as (
    select 
        ccpa.first_name
        ,ccpa.last_name
        ,ccpa.email
        
        ,CASE
            WHEN REGEXP_CONTAINS(ccpa.admin_link, r'CROWDCOW.COM/ADMIN/') THEN LOWER(SPLIT(ccpa.admin_link, '/')[OFFSET(4)])
            WHEN ccpa.email IS NOT NULL THEN users.user_token
            ELSE NULL
        END AS user_token
        
        ,ccpa.date_received
    from ccpa
        left join users on ccpa.email = users.user_email
)

select * from ccpa_users
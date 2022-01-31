
with ccpa as (select * from {{ ref('stg_ccpa_requests') }})
,users as (select * from {{ ref('stg_cc__users') }})

,ccpa_users as (
    select ccpa.first_name
           ,ccpa.last_name
           ,ccpa.email
           ,case when ccpa.admin_link like '%CROWDCOW.COM/ADMIN/%' then lower(split_part(admin_link,'/',5))
                 when ccpa.email is not null then users.user_token
                 else null 
            end as user_token
           ,ccpa.date_received
    from ccpa
        left join users on ccpa.email = users.user_email
)

select * from ccpa_users
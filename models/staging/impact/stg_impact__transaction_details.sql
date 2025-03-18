with

source as ( select * from {{ source('impact', 'impact_actions') }} )

,renamed as (
    select
        cast(REPLACE(id, '.', '') as int64) as transaction_id
        ,id
        ,actiontrackerid   as action_tracker_id
        ,actiontrackername  as action_tracker_name
        ,adid              as ad_id
        ,amount            
        ,campaignid        as campaign_id
        ,campaignname      as campaign_name
        ,clientcost        as comission
        ,creationdate      as creation_date
        ,currency          
        ,customerarea      as customer_area
        ,customercity      as customer_city
        ,customercountry   as customer_country
        ,customerid        as user_token
        ,customerregion    as customer_region
        ,if(customerstatus = 'New',1,0)  as customer_status
        ,deltaamount       as delta_amount
        ,deltapayout       as delta_payout
        ,eventdate         as event_date
        ,intendedamount    as intended_amount
        ,intendedpayout    as intended_payout
        ,ipaddress         as ip_address
        ,lockingdate       as locking_date
        ,mediapartnerid    as media_partner_id
        ,mediapartnername  as media_partner_name
        ,oid               as order_token
        ,payout            
        ,{{ clean_strings('promocode') }} as promo_code
        ,referringdate     as referring_date
        ,referringdomain   as referring_domain
        ,referringtype     as referring_type
        ,state             
        ,row_number() over ( partition by id order by creationdate desc) as rn
    from source
)

select * from renamed where rn = 1 

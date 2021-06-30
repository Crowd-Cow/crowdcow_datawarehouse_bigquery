with stage as (

    select * from {{ ref('users_ss') }}

),

base as (

  select 
    user_id 
    , case
        when NULLIF(TRIM(u.roles_for_access), '') IS NOT NULL THEN 'EMPLOYEE'
        when LOWER(u.email) LIKE '%@crowdcow.com%' THEN 'INTERNAL'
        when LOWER(TRIM(u.user_type)) = 'c' THEN 'CUSTOMER'
        when LOWER(TRIM(u.user_type)) = 'p' THEN 'PROSPECT'
        else 'OTHER'
      end as user_type   
    , user_email 
    , user_gender
    , user_last_geocoded_address
    , user_last_geocoded_city 
    , user_last_geocoded_state_code
    , user_last_geocoded_state 
    , user_last_geocoded_postal_code 
    , user_last_geocoded_country_code  
    , user_last_geocoded_country 
    , user_zip  
    , user_last_geocoded_latitude 
    , user_last_geocoded_longitude 
    , user_cow_cash_balance_usd 
    , user_unsubscribed_all_at_utc
    , updated_at_utc
    , referred_by_user_id
    , referrer_url
    , user_roles_for_notifications
    , user_token
    , created_at_utc
    , utm_source
    , user_page_landing_source
    -- , phone_number -- inclined to leave this out for pii
    , user_total_sign_in_count
    , user_email_name
    , user_vip_level
    , user_last_sign_in_at_utc
    , user_test_bucket
    , user_opt_out_list
    , user_is_banned_from_referrals
    , user_is_email_lead
    , user_has_opted_in_to_emails
    , dbt_valid_to 
    , dbt_valid_from
    , dbt_scd_id as user_key
  from stage

)

select * from base
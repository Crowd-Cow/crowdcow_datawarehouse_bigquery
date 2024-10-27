with source as (

    select * from {{  source('cc', 'users') }}  where __deleted is null

),

renamed as (

    select
        id as user_id
        ,{{ clean_strings('user_type') }} as user_type
        ,{{ clean_strings('email') }} as user_email
        ,{{ clean_strings('gender') }} as user_gender
        --, clean_strings('default_card_token') }} as default_card_token
        ,{{ clean_strings('last_geocoded_address') }} as user_last_geocoded_address
        ,{{ clean_strings('last_geocoded_city') }} as user_last_geocoded_city
        ,{{ clean_strings('last_geocoded_state_code') }} as user_last_geocoded_state_code
        ,{{ clean_strings('last_geocoded_state') }} as user_last_geocoded_state
        ,{{ clean_strings('last_geocoded_postal_code') }} as user_last_geocoded_postal_code
        ,{{ clean_strings('last_geocoded_country_code') }} as user_last_geocoded_country_code
        ,{{ clean_strings('last_geocoded_country') }} as user_last_geocoded_country
        ,last_geocoded_latitude as user_last_geocoded_latitude
        ,last_geocoded_longitude as user_last_geocoded_longitude
        ,{{ clean_strings('last_geocoded_ip_address') }} as user_last_geocoded_ip_address
        ,{{ clean_strings('last_sign_in_ip') }} as user_last_sign_in_ip
        ,full_contact_data_updated_at as full_contact_data_updated_at_utc
        ,referrer_token as user_referrer_token
        ,fc_id as full_contact_id
        ,{{ clean_strings('fc_given_name') }} as full_contact_given_name
        ,{{ clean_strings('fc_family_name') }} as full_contact_family_name
        ,{{ clean_strings('fc_full_name') }} as full_contact_full_name
        ,{{ clean_strings('fc_gender') }} as full_contact_gender
        ,{{ clean_strings('fc_company_name') }} as full_contact_company_name
        ,{{ clean_strings('fc_job_title') }} as full_contact_job_title
        ,{{ clean_strings('fc_bio') }} as full_contact_bio
        ,{{ clean_strings('fc_angellist_url') }} as full_contact_angellist_url
        ,fc_angellist_followers as full_contact_angellist_total_followers
        ,{{ clean_strings('fc_linkedin_url') }} as full_contact_linkedin_url
        ,fc_linkedin_followers as full_contact_linkedin_total_followers
        ,{{ clean_strings('fc_instagram_url') }} as full_contact_instagram_url
        ,fc_instagram_followers as full_contact_instagram_total_followers
        ,{{ clean_strings('fc_twitter_url') }} as full_contact_twitter_url
        ,fc_twitter_followers as full_contact_twitter_total_followers
        ,{{ clean_strings('fc_pinterest_url') }} as full_contact_pinterest_url
        ,fc_pinterest_followers as full_contact_pinterest_total_followers
        ,{{ clean_strings('fc_facebook_url') }} as full_contact_facebook_url
        ,{{ clean_strings('fc_image_url') }} as full_contact_image_url
        ,{{ cents_to_usd('cow_cash_balance_cents') }} as user_cow_cash_balance_usd
        ,unsubscribed_all_at as unsubscribed_all_at_utc
        ,{{ clean_strings('utm_campaign') }} as utm_campaign
        ,{{ clean_strings('vendor_access') }} as user_vendor_access
        ,account_breach_checked_at as account_breach_checked_at_utc
        ,remember_created_at as remember_created_at_utc
        ,vacation_ends_at as vacation_ends_at_utc
        ,{{ clean_strings('unlock_token') }} as user_unlock_token
        ,{{ clean_strings('encrypted_password') }} as user_encrypted_password
        ,email_subscribed_often_at as email_subscribed_often_at_utc
        ,updated_at as updated_at_utc
        ,reorder_remind_date as remind_at_utc
        ,referred_by_user_id
        ,{{ clean_strings('affiliate_transaction_id') }} as affiliate_transaction_id
        ,{{ clean_strings('referrer_url') }} as referrer_url
        ,retention_offer_last_accepted_at as retention_offer_last_accepted_at_utc
        ,banned_at as user_banned_at_utc
        ,{{ clean_strings('fb_id') }} as user_fb_id
        ,{{ clean_strings('banned_reason') }} as user_banned_reason 
        ,support_state_updated_at as support_state_updated_at_utc
        ,retention_offers_accepted as total_retention_offers_accepted
        ,utm_time as utm_time_at_utc
        ,{{ clean_strings('utm_content') }} as utm_content
        ,{{ clean_strings('roles_for_notifications') }} as user_roles_for_notifications
        ,token as user_token
        ,created_at created_at_utc
        ,{{ clean_strings('campaign_id') }} as campaign_id
        ,{{ clean_strings('utm_source') }} as utm_source
        ,current_sign_in_at as current_sign_in_at_utc
        ,email_subscribed_weekly_at as email_subscribed_weekly_at_utc
        ,{{ clean_strings('landing_page_source') }} as user_landing_page_source
        ,{{ clean_strings('stripe_customer_token') }} as stripe_customer_token
        ,banned_by_user_id
        ,{{ clean_strings('reset_password_token') }} as user_reset_password_token
        ,{{ clean_strings('unconfirmed_email') }} as user_unconfirmed_email
        ,{{ clean_strings('notes_for_next_order') }} as user_notes_for_next_order
        ,resubscribed_at as user_resubscribed_at_utc
        ,adgroup_id
        ,failed_attempts as user_total_failed_attempts
        ,merged_into_user_id
        ,mailchimp_unsubscribed_all_at as user_mailchimp_unsubscribed_all_at_utc
        ,{{ clean_strings('targetid') }} as user_target_id
        ,email_validated_at as email_validated_at_utc
        ,reset_password_sent_at as reset_password_sent_at_utc
        ,phone_number_id as phone_number_id
        ,emails_paused_until_date as emails_paused_until_at_utc
        ,{{ clean_strings('current_sign_in_ip') }} as user_current_sign_in_ip
        ,sign_in_count as user_total_sign_in_count
        ,{{ clean_strings('email_name') }} as user_email_name
        ,vip_level as user_vip_level
        ,{{ clean_strings('creativeid') }} as user_creative_id
        ,predicted_reorder_date_updated_at as predicted_reorder_updated_at_utc
        ,predicted_reorder_date as predicted_reorder_at_utc
        ,locked_at as user_locked_at_utc
        ,{{ clean_strings('original_url') }} as user_original_url
        ,{{ clean_strings('support_state') }} as user_support_status
        ,{{ clean_strings('excluded_shipping_carriers') }} as user_excluded_shipping_carriers
        ,{{ clean_strings('confirmation_token') }} as user_confirmation_token
        ,{{ clean_strings('utm_medium') }} as utm_medium
        ,active_order_id
        ,account_breach_reset_password_at as account_breach_reset_password_at_utc
        ,{{ cents_to_usd('pay_rate_cents') }} as user_pay_rate_usd
        ,vacation_begins_at as vacation_begins_at_utc
        ,left(replace({{ clean_strings('zip') }},'~',''),5) as user_zip
        ,last_sign_in_at as last_sign_in_at_utc
        ,alternate_phone_number_id
        ,{{ clean_strings('roles_for_access') }} as user_roles_for_access
        ,{{ clean_strings('email_validation_code') }} as user_email_validation_code
        ,test_bucket as user_test_bucket
        ,{{ clean_strings('opt_out_list') }} as user_opt_out_list
        ,banned_from_referrals as user_is_banned_from_referrals
        ,was_email_lead
        ,if(opted_in_to_emails=1,true,false) as has_opted_in_to_emails
        ,null as dbt_valid_to 
        /*,dbt_valid_from
        ,dbt_scd_id as user_key
        ,case
            when dbt_valid_from = first_value(dbt_valid_from) over(partition by id order by dbt_valid_from) then '1970-01-01'
            else dbt_valid_from
        end as adjusted_dbt_valid_from
        ,coalesce(dbt_valid_to,'2999-01-01') as adjusted_dbt_valid_to */
    from source

)

select * from renamed


with source as (

    select * from {{ source('cc', 'identities') }} where __deleted is null

),

renamed as (

    select
        id as identity_id
        ,{{ clean_strings('first_name') }} as first_name
        ,{{ clean_strings('google_profile_url') }} as google_profile_url
        ,timezone_offset
        ,{{ clean_strings('provider') }} as identity_provider
        ,{{ clean_strings('name') }} as full_name
        ,{{ clean_strings('image_url') }} as image_url
        ,user_id
        ,token as identity_token
        ,{{ clean_strings('gender') }} gender
        ,created_at as created_at_utc
        ,{{ clean_strings('last_name') }} as last_name
        ,updated_at as updated_at_utc
        ,{{ clean_strings('facebook_profile_url') }} as facebook_profile_url
        ,uid as identity_uid
        ,{{ clean_strings('email') }} as email
        ,google_email_verified as is_google_email_verified
        ,facebook_email_verified as is_facebook_email_verified

    from source

)

select * from renamed


{{
  config(
    materialized = 'table'
)
}}

with
user_hist as ( select * from {{ source('iterable', 'users') }} )

,renamed as (
    select
        {{ clean_strings('email') }} AS user_email,
        profileupdatedat AS updated_at_utc,
        {{ clean_strings('first_name') }} AS first_name,
        {{ clean_strings('last_name') }} AS last_name,
        phone_number,
        userid AS user_token,
        signupdate AS signup_date_utc,
        {{ clean_strings('signupsource') }} AS signup_source,
        --email_list_ids,
        --additional_properties,
        ROW_NUMBER() OVER(PARTITION BY email ORDER BY profileupdatedat DESC) AS row_num
    from user_hist
)

select * from renamed

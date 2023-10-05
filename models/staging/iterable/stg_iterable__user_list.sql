{{
  config(
    materialized = 'table'
)
}}

with

user_hist as ( select * from {{ source('iterable', 'user_history') }} )

 ,renamed as (

    SELECT 
        distinct
        {{ clean_strings('email') }} as user_email
        ,user_id as user_token
        ,value as list_id
        ,updated_at as updated_at_utc
    FROM user_hist,
    LATERAL FLATTEN(input => email_list_ids) f
)
select * from renamed

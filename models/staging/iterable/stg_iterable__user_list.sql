{{
  config(
    materialized = 'table'
)
}}

with

user_hist as ( select * from {{ source('iterable', 'users') }} )
,user_list as ( select * from {{ source('iterable', 'users_emaillistids') }} )

 ,renamed as (

    SELECT 
        distinct
        {{ clean_strings('email') }} as user_email
        ,userid as user_token
        ,value as list_id
        ,profileupdatedat as updated_at_utc
    FROM user_hist
    left join user_list on user_list.__iterable_users_panoply_id = user_hist.__panoply_id
)
select * from renamed

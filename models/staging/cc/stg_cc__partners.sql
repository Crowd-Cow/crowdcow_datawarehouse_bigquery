with source as (

    select * from {{ ref('partners_ss') }} where not _fivetran_deleted

),

renamed as (

  select
    id as partner_id
    ,dbt_scd_id as partner_key
    ,event_id
    ,created_by_user_id as partner_created_by_user_id
    ,street_team_user_id
    ,{{ clean_strings('redirect_path') }} as partner_redirect_path
    ,{{ clean_strings('campaign_type') }} as partner_campaign_type
    ,{{ clean_strings('promo_description') }} as partner_promo_description
    ,{{ clean_strings('path') }} as partner_path
    ,token as partner_token
    ,{{ clean_strings('page_text') }} as partner_page_text
    ,{{ clean_strings('page_title') }} as partner_page_title
    ,{{ clean_strings('notes') }} as partner_notes
    ,{{ clean_strings('image_url') }} as partner_image_url
    ,{{ clean_strings('utm_medium') }} as partner_utm_medium
    ,{{ clean_strings('url') }} as partner_url
    ,{{ clean_strings('name') }} as partner_name
    ,{{ clean_strings('utm_source') }} as partner_utm_source
    ,requires_redirect as partner_requires_redirect
    ,redeem_on_visit as should_redeem_on_visit
    ,requires_email as partner_requires_email
    ,created_at as created_at_utc
    ,updated_at as updated_at_utc
    ,archived_at as archived_at_utc
    ,dbt_valid_to
    ,dbt_valid_from
    ,case
        when dbt_valid_from = first_value(dbt_valid_from) over(partition by id order by dbt_valid_from) then '1970-01-01'
        else dbt_valid_from
      end as adjusted_dbt_valid_from
    ,coalesce(dbt_valid_to,'2999-01-01') as adjusted_dbt_valid_to

  from source

)

select * from renamed

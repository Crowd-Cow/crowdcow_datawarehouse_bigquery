with

campaign_hist as ( select * from {{ source('iterable', 'campaign_history') }} )

,renamed as (
    select
        id as campaign_id
        ,updated_at as updated_at_utc
        ,template_id
        ,recurring_campaign_id
        ,{{ clean_strings('campaign_state') }} as campaign_state
        ,created_at as created_at_utc
        ,{{ clean_strings('created_by_user_id') }} as created_by_user_id
        ,coalesce(ended_at,created_at) as ended_at_utc
        ,{{ clean_strings('name') }} as campaign_name
        ,send_size
        ,{{ clean_strings('type') }} as campaign_type
    from campaign_hist
    qualify row_number() over(partition by id order by updated_at desc) = 1
)

select * from renamed

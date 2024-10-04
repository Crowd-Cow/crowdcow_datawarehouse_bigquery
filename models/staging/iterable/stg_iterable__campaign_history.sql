with

campaign_hist as ( select * from {{ source('iterable', 'campaigns') }} )

,renamed as (
    select
        id AS campaign_id,
        updatedat AS updated_at_utc,
        templateid AS template_id,
        --recurring_campaign_id,
        {{ clean_strings('campaignstate') }} AS campaign_state,
        createdat AS created_at_utc,
        {{ clean_strings('createdbyuserid') }} AS created_by_user_id,
        COALESCE(endedat, createdat) AS ended_at_utc,
        {{ clean_strings('name') }} AS campaign_name,
        sendsize as send_size,
        {{ clean_strings('type') }} AS campaign_type,
        ROW_NUMBER() OVER(PARTITION BY id ORDER BY updatedat DESC) AS row_num
    from campaign_hist
)

select * from renamed

with

campaign_hist as ( select * from {{ source('iterable', 'campaigns') }} )
,iterable_journeys as ( select * from {{ ref('stg_iterable__journeys') }})


,renamed as (
    select
        id AS campaign_id,
        TIMESTAMP_MILLIS(updatedat) AS updated_at_utc,
        templateid AS template_id,
        --recurring_campaign_id,
        {{ clean_strings('campaignstate') }} AS campaign_state,
        TIMESTAMP_MILLIS(createdat) AS created_at_utc,
        {{ clean_strings('createdbyuserid') }} AS created_by_user_id,
        TIMESTAMP_MILLIS(COALESCE(endedat, createdat)) AS ended_at_utc,
        {{ clean_strings('name') }} AS campaign_name,
        workflowid  as workflow_id,
        sendsize as send_size,
        iterable_journeys.workflow_name,
        {{ clean_strings('type') }} AS campaign_type,
    from campaign_hist
    left join iterable_journeys on campaign_hist.workflowid = iterable_journeys.workflow_id
    qualify row_number() over(partition by id order by updatedat desc) = 1
)

select * from renamed

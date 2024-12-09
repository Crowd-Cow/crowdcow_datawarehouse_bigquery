with

campaign_hist as ( select * from {{ source('iterable', 'campaigns') }} )
,iterable_journeys as ( select * from {{ ref('stg_iterable__journeys') }})
,campaign_lists as ( select * from {{ source('iterable', 'campaigns_listids') }} where value in (4480519,4762588,4762628))
,lists_info as ( select * from {{ source('iterable', 'iterable_lists') }} ) 




,renamed as (
    select
        campaign_hist.id AS campaign_id,
        TIMESTAMP_MILLIS(updatedat) AS updated_at_utc,
        templateid AS template_id,
        --recurring_campaign_id,
        {{ clean_strings('campaignstate') }} AS campaign_state,
        TIMESTAMP_MILLIS(campaign_hist.createdat) AS created_at_utc,
        {{ clean_strings('createdbyuserid') }} AS created_by_user_id,
        TIMESTAMP_MILLIS(COALESCE(endedat, campaign_hist.createdat)) AS ended_at_utc,
        {{ clean_strings('campaign_hist.name') }} AS campaign_name,
        workflowid  as workflow_id,
        sendsize as send_size,
        iterable_journeys.workflow_name,
        {{ clean_strings('type') }} AS campaign_type,
        lists_info.name as list_name
    from campaign_hist
    left join iterable_journeys on campaign_hist.workflowid = iterable_journeys.workflow_id
    left join campaign_lists on campaign_hist.__panoply_id = campaign_lists.__campaigns_panoply_id
    left join lists_info on campaign_lists.value = lists_info.id
    qualify row_number() over(partition by campaign_hist.id order by updatedat desc) = 1
)

select * from renamed

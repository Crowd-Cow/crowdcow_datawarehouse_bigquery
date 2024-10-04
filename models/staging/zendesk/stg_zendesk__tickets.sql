with 

source as ( select * from {{ source('zendesk', 'tickets') }} )
--,tickets_followups as ( select * from {{ source('zendesk', 'tickets_followup_ids') }} )
,tickets_custom_fields as ( select * from {{ ref('stg_zendesk__tickets_custom_fields') }} )


,renamed as (
    select
        cast(id as string) as ticket_id
        ,{{ clean_strings('url') }} as ticket_url
        ,external_id
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(via, '$.channel')") }} as via_channel
        ,JSON_EXTRACT_SCALAR(via, '$.source.id') as via_source_from_id
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(via, '$.source.from.title')" ) }} as via_source_from_title
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(via, '$.source.from.address')" ) }} as via_source_from_email
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(via, '$.source.to.name')" ) }} as via_source_to_name
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(via, '$.source.to.address')" ) }} as via_source_to_email
        ,{{ clean_strings("JSON_EXTRACT_SCALAR(via, '$.source.rel')" ) }} as via_source_rel
        --,merged_ticket_ids
        ,created_at as created_at_utc
        ,updated_at as updated_at_utc
        ,{{ clean_strings('type') }} as ticket_type
        ,{{ clean_strings('source.subject') }} as ticket_subject
        ,{{ clean_strings('source.description') }} as ticket_description
        ,{{ clean_strings('source.priority') }} as ticket_priority
        ,{{ clean_strings('source.status') }} as ticket_status
        ,{{ clean_strings('source.recipient') }} as recipeint_email
        ,requester_id
        ,submitter_id
        ,assignee_id
        ,organization_id
        ,group_id
        --,forum_topic_id
        ,problem_id
        ,has_incidents
        ,is_public
        --,JSON_EXTRACT_SCALAR(via, '$.followup.source.id') as via_followup_source_id
        --,tickets_followups.value as followup_ids
        ,due_at as due_at_utc
        ,ticket_form_id
        ,brand_id
        ,allow_channelback as does_allow_channelback
        --, as system_client
        --,system_raw_email_identifier
        --,system_json_email_identifier
        --,system_message_id
        --,system_ip_address
        ,{{ clean_strings('tickets_custom_fields.order_errors') }} as order_errors
        --,custom_playlist_assigned_at
        ,case when {{ clean_strings('tickets_custom_fields.issue_category') }} is null then {{ clean_strings('tickets_custom_fields.issue_triage') }} else {{ clean_strings('tickets_custom_fields.issue_category') }} end  as issue_category
        ,{{ clean_strings('tickets_custom_fields.cow_cash_reason') }} as cow_cash_reason
        ,{{ clean_strings('tickets_custom_fields.ai_category') }} as ai_category
        ,{{ clean_strings('tickets_custom_fields.filter_for_productivity') }} as filter_for_productivity
        ,tickets_custom_fields.order_id as order_token
        ,tickets_custom_fields.time_spent_last_update_sec as time_spent_last_update_seconds
        ,{{ clean_strings('tickets_custom_fields.automation_type') }} as automation_type
        --,tickets_custom_fields.playlist_assigned_ as playlist_assigned
        --,tickets_custom_fields.playlist_assigned_by as playlist_assigned_by
        ,tickets_custom_fields.total_time_spent_sec as total_time_spent_seconds
        ,tickets_custom_fields.total_credit_refund_given as total_credit_refund_given
        ,{{ clean_strings('tickets_custom_fields.custom_type') }} as custom_type
        --,tickets_custom_fields.playlist_autoplayed_ as playlist_autoplayed
        ,source.__updatetime as _fivetran_synced
        --, as system_location
        --,system_latitude
        --,system_longitude
        --,system_machine_generated as is_machine_generated
        --,system_ccs
        ,tickets_custom_fields.blastable_potential_template as is_potential_blastable_template
        ,{{ clean_strings('tickets_custom_fields.ticket_form_name') }} as ticket_form_name
        ,tickets_custom_fields.ticket_form_order_number as ticket_form_order_number
        ,{{ clean_strings('tickets_custom_fields.ticket_form_reason') }} as ticket_form_reason
        --,system_email_id
        --,system_eml_redacted
        ,tickets_custom_fields.stylo_frustration_options as stylo_frustration_options
        ,tickets_custom_fields.stylo_urgency as stylo_urgency
        ,tickets_custom_fields.stylo_deescalate as stylo_de_escalate
        ,tickets_custom_fields.stylo_p_csat as stylo_p_csat
        ,tickets_custom_fields.stylo_peak_delight as stylo_peak_delight
        ,tickets_custom_fields.stylo_frustration_change as stylo_frustration_change
        ,tickets_custom_fields.stylo_delight as stylo_delight
        ,tickets_custom_fields.stylo_urgency_change as stylo_urgency_change
        ,tickets_custom_fields.stylo_frustration_change_numeric as stylo_frustration_change_numeric
        ,tickets_custom_fields.stylo_peak_agent_frustration as stylo_peak_agent_frustration
        ,tickets_custom_fields.stylo_delight_change_numeric as stylo_delight_change_numeric
        ,tickets_custom_fields.stylo_urgency_change_numeric as stylo_urgency_change_numeric
        ,tickets_custom_fields.stylo_frustration as stylo_frustration
        ,tickets_custom_fields.stylo_peak_urgency as stylo_peak_urgency
        ,tickets_custom_fields.stylo_peak_frustration as stylo_peak_frustration
        ,tickets_custom_fields.stylo_urgency_options as stylo_urgency_options
        ,tickets_custom_fields.askstylo_issue_category as ask_stylo_issue_category
        ,tickets_custom_fields.form_inquiry_category as form_inquiry_category
        ,tickets_custom_fields.form_product_quality_issue_catagories as form_product_quality_issue_catagories
        ,tickets_custom_fields.form_order_issue_catagories as form_order_issue_catagories
        ,tickets_custom_fields.form_delivery_catagories as form_delivery_catagories
        ,tickets_custom_fields.form_website_catagories as form_website_catagories
    from source
    --left join tickets_followups on tickets_followups.__tickets_panoply_id = source.__panoply_id
    left join tickets_custom_fields on tickets_custom_fields.__tickets_panoply_id = source.__panoply_id
)

select * from renamed


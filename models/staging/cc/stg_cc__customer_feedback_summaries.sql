with

source as ( select * from {{ source('cc', 'customer_feedback_summaries') }}  )
,user_vendor_cut_ratings as ( select * from {{ ref('stg_cc__user_vendor_cut_ratings')  }} )
,zendesk_tickets as ( select * from {{ ref('stg_cc__zendesk_tickets')  }} )

,renamed as (
    select
        source.id as customer_feedback_summaries_id
        ,source.sku_vendor_id
        ,source.cut_id
        ,source.lot_id
        ,source.order_id
        ,source.created_at as customer_feedback_created_at_utc
        ,source.feedback_summary_of_id
        ,source.feedback_summary_of_type
        ,cast(case when source.feedback_summary_of_type = 'ZendeskTicket' then source.feedback_summary_of_id else null end as int64) as zendesk_id
        ,cast(case when source.feedback_summary_of_type = 'UserVendorCutRating' then source.feedback_summary_of_id else null end as int64) as rating_id
        ,source.overall_rating
        ,source.arrived_frozen
        ,source.butchering_quality
        ,source.color_appearance
        ,source.delivery_issue
        ,source.likelyness_to_reorder
        ,source.packaging
        ,source.price_and_value
        ,source.product_age
        ,source.product_weight
        ,source.received_wrong_item
        ,source.review_summary
        ,source.smell
        ,source.taste
        ,source.texture
        ,source.website_issue
        ,source.updated_at as customer_feedback_updated_at_utc

    from source
)

select renamed.* 
,case 
    when feedback_summary_of_type = 'ZendeskTicket' then zendesk_tickets.created_at_utc 
    when feedback_summary_of_type = 'UserVendorCutRating' then user_vendor_cut_ratings.created_at_utc 
    else null 
end as created_at_utc
from renamed 
left join user_vendor_cut_ratings on cast(user_vendor_cut_ratings.user_vendor_cut_rating_id as int64) = renamed.rating_id
left join zendesk_tickets on cast(zendesk_tickets.zendesk_id as int64) = renamed.zendesk_id



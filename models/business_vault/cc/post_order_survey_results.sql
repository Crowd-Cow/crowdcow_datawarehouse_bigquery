with

order_survey as ( select * from {{ ref('stg_cc__post_order_survey_results') }} )

select
    post_order_survey_id
    ,order_id
    ,{{ get_order_type('order_survey') }} as order_type
    ,user_id
    ,survey_monkey_response_id
    ,age
    ,gender
    ,income
    ,butcher_name
    ,farm_name
    ,how_first_heard
    ,does_well
    ,does_well_tags
    ,needs_improvement
    ,needs_improvement_tags
    ,rank_of_diet
    ,rank_of_producers
    ,rank_of_convenience
    ,rank_of_meds
    ,rank_of_quality
    ,rank_of_welfare
    ,nps_score
    ,nps_score_normalized
    
    ,case
        when nps_score is null then null
        when nps_score < 7 then 'DETRACTOR'
        when nps_score < 9 then 'NEUTRAL'
        else 'PROMOTER'
     end as nps_score_category
    
    ,flavor_score
    ,tender_score
    ,did_leak
    ,created_at_utc
    ,updated_at_utc
    ,added_as_testimonial_at_utc
    ,solicited_google_review_at_utc
    ,zendesk_notified_at_utc
from order_survey

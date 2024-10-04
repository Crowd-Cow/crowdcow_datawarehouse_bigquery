with

order_survey as ( select * from {{ ref('stg_cc__post_order_survey_results') }} )
,order_details as (select order_id, is_rastellis, is_qvc from {{ ref('stg_cc__orders') }})
,detraction_classifications as (select * from {{ ref('stg_gs__nps_manual_categorization') }})

,survey_order_combined as (
    select
    order_survey.post_order_survey_id
    ,order_survey.order_id
    ,order_details.is_rastellis
    ,order_details.is_qvc
    ,order_survey.user_id
    ,order_survey.survey_monkey_response_id
    ,order_survey.age
    ,order_survey.gender
    ,order_survey.income
    ,order_survey.butcher_name
    ,order_survey.farm_name
    ,detraction_classifications.farm as detraction_farm
    ,detraction_classifications.cut as detraction_cut
    ,detraction_classifications.category as detraction_category
    ,detraction_classifications.manual_classification as detraction_classification
    ,detraction_classifications.lot_number as detraction_lot_number
    ,order_survey.how_first_heard
    ,order_survey.does_well
    ,order_survey.does_well_tags
    ,order_survey.needs_improvement
    ,order_survey.needs_improvement_tags
    ,order_survey.rank_of_diet
    ,order_survey.rank_of_producers
    ,order_survey.rank_of_convenience
    ,order_survey.rank_of_meds
    ,order_survey.rank_of_quality
    ,order_survey.rank_of_welfare
    ,order_survey.nps_score
    ,order_survey.nps_score_normalized
    
    ,case
        when order_survey.nps_score is null then null
        when order_survey.nps_score < 7 then 'DETRACTOR'
        when order_survey.nps_score < 9 then 'NEUTRAL'
        else 'PROMOTER'
     end as nps_score_category
    
    ,order_survey.flavor_score
    ,order_survey.tender_score
    ,order_survey.did_leak
    ,order_survey.created_at_utc
    ,order_survey.updated_at_utc
    ,order_survey.added_as_testimonial_at_utc
    ,order_survey.solicited_google_review_at_utc
    ,order_survey.zendesk_notified_at_utc
from order_survey
    left join order_details on order_survey.order_id = order_details.order_id
    left join detraction_classifications on order_survey.post_order_survey_id = detraction_classifications.post_order_survey_id
    )

    select *
    from survey_order_combined

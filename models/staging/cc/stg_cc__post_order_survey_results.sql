with 

source as ( select * from {{ source('cc', 'post_order_survey_results') }}  )

,renamed as (

    select
        id as post_order_survey_id
        ,created_at as created_at_utc
        ,flavor_score
        ,{{ clean_strings('age') }} as age
        ,order_id
        ,added_as_testimonial_at as added_as_testimonial_at_utc
        ,{{ clean_strings('does_well') }} as does_well
        ,rank_of_diet
        ,{{ clean_strings('needs_improvement_tags') }} as needs_improvement_tags
        ,rank_of_producers
        ,updated_at as updated_at_utc
        ,solicited_google_review_at as solicited_google_review_at_utc
        ,user_id
        ,{{ clean_strings('does_well_tags') }} as does_well_tags
        ,nps_score_normalized
        ,{{ clean_strings('gender') }} as gender
        ,rank_of_convenience
        ,survey_monkey_response_id
        ,zendesk_notified_at as zendesk_notified_at_utc
        ,tender_score
        ,rank_of_meds
        ,rank_of_quality
        ,nps_score
        ,{{ clean_strings('butcher_name') }} as butcher_name
        ,{{ clean_strings('farm_name') }} as farm_name
        ,{{ clean_strings('needs_improvement') }} as needs_improvement
        ,{{ clean_strings('how_first_heard') }} as how_first_heard
        ,rank_of_welfare
        ,{{ clean_strings('income') }} as income
        ,coalesce(leaked = 1,FALSE) as did_leak

    from source
    qualify row_number() over(partition by order_id order by updated_at desc) = 1

)

select * from renamed

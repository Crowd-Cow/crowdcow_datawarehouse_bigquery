with

source as ( select * from {{ source('google_sheets', 'nps_manual_categorization') }} )

,renamed as (
    select
        survey_id as post_order_survey_id
        ,{{ clean_strings('cut') }} as cut
        ,{{ clean_strings('farm') }} as farm
        ,{{ clean_strings('category') }} as category
        ,{{ clean_strings('manual_classification') }} as manual_classification
        ,lot as lot_number
    from source
)

select * from renamed
with 

source as ( select * from {{ source('cc', 'failure_cases') }}  )

,renamed as (

    select
        id as failure_case_id
        ,reported_by_user_id
        ,entered_by_user_id
        ,coalesce(charge_back_amount,0) as charge_back_amount
        ,{{ clean_strings('category') }} as category
        ,{{ clean_strings('specifics') }} as specifics
        ,archived_at as archived_at_utc
        ,order_id
        ,{{ clean_strings('butcher_name') }} as butcher_name
        ,scheduled_fulfillment_date as scheduled_fulfillment_date_utc
        ,{{ clean_strings('reported_via') }} as reported_via
        ,customer_id as user_id
        ,reported_at as reported_at_utc
        ,{{ clean_strings('link_to_zendesk_ticket') }} as link_to_zendesk_ticket
        ,coalesce(refund_amount,0) as refund_amount
        ,failure_occured_at as failure_occured_at_utc
        ,created_at as created_at_utc
        ,shipped_at as shipped_at_utc
        ,{{ clean_strings('notes') }} as notes
        ,coalesce({{ cents_to_usd('credit_amount') }},0) as credit_amount
        ,updated_at as updated_at_utc
        ,{{ clean_strings('farm_name') }} as farm_name
    from source
)

select * from renamed

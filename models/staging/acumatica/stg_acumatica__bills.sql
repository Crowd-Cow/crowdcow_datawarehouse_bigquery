with 

source as ( select * from {{ source('acumatica', 'bills') }} )

,renamed as (
    select
        id as bill_id
        ,date as bill_date_utc
        ,{{ clean_strings('note') }} as notes
        ,amount as bill_amount
        ,tax_total
        ,parse_json(custom) as custom_fields
        ,due_date as due_date_utc
        ,cash_account
        ,{{ clean_strings('description') }} as bill_description
        ,{{ clean_strings('type') }} as bill_type
        ,reference_number
        ,location_id
        ,hold as is_on_hold
        ,balance
        ,{{ clean_strings('terms') }} as terms
        ,vendor
        ,parse_json(details) as details
        ,approved_for_payement as is_approved_for_payment
        ,vendor_ref
        ,{{ clean_strings('currency_id') }} as currency_id
        ,post_period
        ,{{ clean_strings('status') }} as bill_status
    from source
)

select * from renamed


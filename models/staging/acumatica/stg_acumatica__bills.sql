with 

source as ( select * from {{ source('acumatica', 'bills') }} )

,flatten_details as (
    select
        source.*
        ,value:Account:value::int as account_nbr
        ,value:Amount:value::float as line_item_amount
        ,value:Branch:value::text as branch
        ,value:Description:value::text as line_item_description
        ,value:ExtendedCost:value::float as line_item_extended_cost
        ,value:Qty:value::int as line_item_quantity
        ,value:Subaccount:value::int as sub_account
        ,value:TransactionDescription:value::text as transaction_description
        ,value:UnitCost:value::int as unit_cost
        ,value:note::text as line_item_note
        ,value:rowNumber::int as line_number
    from source,
        lateral flatten ( input => parse_json(details) )
)

,renamed as (
    select
        {{ dbt_utils.surrogate_key(['id','line_number']) }} as bill_item_id
        ,id as bill_id
        ,date as bill_date_utc
        ,{{ clean_strings('note') }} as notes
        ,due_date as due_date_utc
        ,cash_account
        ,{{ clean_strings('description') }} as bill_description
        ,{{ clean_strings('type') }} as bill_type
        ,reference_number
        ,location_id
        ,hold as is_on_hold
        ,{{ clean_strings('terms') }} as terms
        ,vendor
        ,approved_for_payement as is_approved_for_payment
        ,vendor_ref
        ,{{ clean_strings('currency_id') }} as currency_id
        ,post_period
        ,{{ clean_strings('status') }} as bill_status
        ,account_nbr
        ,line_item_amount
        ,{{ clean_strings('branch') }} as branch
        ,{{ clean_strings('line_item_description') }} as line_item_description
        ,line_item_extended_cost
        ,line_item_quantity
        ,sub_account
        ,{{ clean_strings('transaction_description') }} as transaction_description
        ,unit_cost
        ,{{ clean_strings('line_item_note') }} as line_item_note
        ,line_number
    from flatten_details
)

select * from renamed


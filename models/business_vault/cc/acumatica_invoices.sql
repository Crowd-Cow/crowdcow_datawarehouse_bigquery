with

invoice as ( select * from {{ ref('stg_acumatica__bills') }} )

select
    bill_item_id
    ,invoice_key
    ,bill_id
    ,reference_number
    ,location_id
    ,vendor
    ,vendor_ref
    ,account_nbr
    ,sub_account
    ,currency_id
    ,notes
    ,cash_account
    ,bill_description
    ,bill_type

     ,case 
        when bill_type = 'DEBIT ADJ.' then -bill_amount
        when bill_type = 'PREPAYMENT' then null
        else bill_amount
     end as bill_amount

    ,terms
    ,post_period
    ,bill_status
    ,line_item_amount
    ,branch
    ,line_item_description
    ,line_item_extended_cost
    ,line_item_quantity
    ,transaction_description
    ,unit_cost
    ,line_item_note
    ,line_number
    ,is_on_hold
    ,is_approved_for_payment
    ,bill_date_utc
    ,due_date_utc
from invoice

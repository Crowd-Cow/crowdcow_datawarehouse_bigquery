with

invoice as ( select * from {{ ref('stg_acumatica__bills') }} )

select
    bill_item_id
    ,invoice_key
    ,bill_id
    ,bill_date_utc
    ,notes
    ,due_date_utc
    ,cash_account
    ,bill_description
    ,regexp_substr(bill_description,'[0-9]{4}') as lot_number
    ,bill_type

     ,case 
        when bill_type = 'BILL' then bill_amount
        when bill_type = 'CREDIT ADJ.' then bill_amount
        when bill_type = 'DEBIT ADJ.' then -bill_amount
        when bill_type = 'PREPAYMENT' then null
     end as bill_amount

    ,reference_number
    ,location_id
    ,hold
    ,terms
    ,vendor
    ,is_approved_for_payment
    ,vendor_ref
    ,currency_id
    ,post_period
    ,bill_status
    ,account_nbr
    ,line_item_amount
    ,branch
    ,line_item_description
    ,line_item_extended_cost
    ,line_item_quantity
    ,sub_account
    ,transaction_description
    ,unit_cost
    ,line_item_note
    ,line_number
from invoice

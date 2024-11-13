with

transactions as (select * from {{ ref('stg_shareasale__transaction_details')}})
,affiliates as ( select * from {{ ref('stg_shareasale__affiliate_list') }} )

,orders as (
    select
        transaction_id,
        transactions.affiliate_id,
        affiliates.affiliate_name,
        transaction_date_utc,
        transaction_amount,
        comission,
        shareasale_comission,
        order_token,
        new_customer_flag,
        is_alacarte_order,
    from transactions
        left join affiliates on affiliates.affiliate_id = transactions.affiliate_id
)

select * from orders

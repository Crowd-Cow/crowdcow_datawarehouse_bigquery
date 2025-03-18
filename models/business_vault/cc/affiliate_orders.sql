with

sas_transactions as (select * from {{ ref('stg_shareasale__transaction_details')}})
,sas_affiliates as ( select * from {{ ref('stg_shareasale__affiliate_list') }} )
,impact_transactions as ( select * from {{ ref('stg_impact__transaction_details') }} )
,orders as (select * from {{ ref('orders') }})

,sas_orders as (
    select
        transaction_id,
        sas_transactions.affiliate_id,
        sas_affiliates.affiliate_name,
        transaction_date_utc,
        transaction_amount,
        comission,
        shareasale_comission,
        order_token,
        new_customer_flag,
        is_alacarte_order,
        "SHARE A SALE" as affiliate_platform
    from sas_transactions
        left join sas_affiliates on sas_affiliates.affiliate_id = sas_transactions.affiliate_id
)

,impact_orders as (
    select
        transaction_id,
        media_partner_id as affiliate_id ,
        media_partner_name as affiliate_name ,
        creation_date as transaction_date_utc,
        amount as transaction_amount,
        comission,
        0 as shareasale_comission,
        impact_transactions.order_token,
        customer_status as new_customer_flag,
        orders.is_ala_carte_order as is_alacarte_order,
        "IMPACT" as affiliate_platform
    from impact_transactions
        left join orders on orders.order_token = impact_transactions.order_token
)

select * from sas_orders
union all 
select * from impact_orders

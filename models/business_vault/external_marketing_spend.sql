with 

bill as ( select * from {{ ref('stg_acumatica__bills') }} )

,get_marketing_accounts as (

    /** Get only the non-digital marketing accounts **/

    select
        *
    from bill
    where account_nbr in (61111,61113,61113,61140)
)

select * from get_marketing_accounts

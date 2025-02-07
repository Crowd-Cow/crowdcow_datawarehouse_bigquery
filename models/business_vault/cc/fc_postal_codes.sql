with

fc_postal_code as ( select * from {{ ref('stg_cc__fc_postal_codes') }} )
,postal_code_ref as ( select distinct postal_code, state_code, state_name, dma_name, city_name from {{ ref('stg_cc__postal_codes') }} )
,fc as ( select * from {{ ref('stg_cc__fcs') }} )

,get_valid_postal_codes as (
    select
        *
    from fc_postal_code
    where postal_code in ( select postal_code from postal_code_ref )
)

,get_fc_key as (
    select
        get_valid_postal_codes.*
        ,fc.fc_key
        ,postal_code_ref.state_code
        ,postal_code_ref.state_name
        ,postal_code_ref.dma_name
        ,postal_code_ref.city_name
    from get_valid_postal_codes
        left join fc on get_valid_postal_codes.fc_id = fc.fc_id
            and get_valid_postal_codes.created_at_utc >= fc.adjusted_dbt_valid_from
            and get_valid_postal_codes.created_at_utc < fc.adjusted_dbt_valid_to
        left join postal_code_ref on get_valid_postal_codes.postal_code = postal_code_ref.postal_code
)

select * from get_fc_key

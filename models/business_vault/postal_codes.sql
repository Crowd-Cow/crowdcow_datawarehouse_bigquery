with

postal_code as ( select * from {{ ref('stg_cc__postal_codes') }} )
,postal_code_lookup as ( select * from {{ ref('stg_cc__postal_code_lookups') }} )

,join_postal_code_lookup as (
    select
        postal_code.*
        ,postal_code_lookup.county_name
    from postal_code
        left join postal_code_lookup on postal_code.postal_code = postal_code_lookup.postal_code
)

select * from join_postal_code_lookup

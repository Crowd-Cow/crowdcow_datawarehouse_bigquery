with

source as ( select * from {{ source('shareasale', 'affiliate_list') }} )

,renamed as (
    select
        userid as affiliate_id, 
        organization as affiliate_name,
        row_number() over (partition by userid order by __createtime desc) as rn
    from source
)

select * from renamed where rn = 1 

with

packing_action as (select * from {{ ref('stg_cc__packing_actions') }} )
,insert_sku as (
        select 
            sku_id
            ,sku_name 
        from {{ ref('skus') }} 
        where dbt_valid_to is null
            and not (sku_weight <= 0.05 and category is null)
            and sku_name <> 'CROWD COW HANDWRITTEN NOTE'
)

,pick_pack as (
    select
        packing_action.order_id
        ,packing_action.user_id
        ,packing_action.sku_id
        ,packing_action.action
        ,packing_action.created_at_utc
        from packing_action
            inner join insert_sku on packing_action.sku_id = insert_sku.sku_id
)

,get_pick_pack_duration as (
    select
        user_id
        ,order_id
        ,action
        ,created_at_utc::date as action_date
        ,min(created_at_utc) as action_started_at_utc
        ,max(created_at_utc) as action_ended_at_utc
        ,count(*) as item_count
        ,count(distinct sku_id) as sku_count
        ,datediff(minute,action_started_at_utc,action_ended_at_utc)/60 as hour_duration
        ,datediff(minute,action_started_at_utc,action_ended_at_utc) as minute_duration
        ,datediff(second,action_started_at_utc,action_ended_at_utc) as second_duration
    from pick_pack
    where action in ('ADD_TO_BOX','REMOVED_FROM_BOX','PACKED_ITEM')
    group by 1,2,3,4
)

select * from get_pick_pack_duration

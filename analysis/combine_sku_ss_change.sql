/**** Getting a complete historical picture of SKU changes, specifically prices, required combining snapshots ****/
/**** from our old Postgres datawarehouse with our new Snowflake warehouse ***/
/**** This script is what was used to combine both snapshots, as well as validations used to verify accuracy of code ****/
/**** Link to full Snowflake query: https://app.snowflake.com/us-east-1/lna65058/w1KrwMstbpvt#query ****/

/**** Create combined snapshot table ****/
create or replace table kdent_analytics_dev.snapshots.combined_skus_ss as (
with

analytics_skus_ss as (
    select
        'analytics_snapshot' as source
        ,id
        ,non_member_promotion_start_at
        ,average_cost_in_cents
        ,promotion_start_at
        ,vendor_funded_discount_name
        ,sku_code
        ,vendor_funded_discount_cents
        ,non_member_promotion_discount
        ,member_only_promotion_discount
        ,member_only_promotion_start_at
        ,updated_at
        ,platform_fee_in_cents
        ,name
        ,fulfillment_fee_in_cents
        ,price_in_cents
        ,marketplace_cost_in_cents
        ,created_at
        ,sku_vendor_id
        ,average_box_quantity
        ,vendor_funded_discount_start_at
        ,vendor_funded_discount_end_at
        ,barcode
        ,payment_processing_fee_in_cents
        ,cut_id
        ,vendor_funded_discount_percent
        ,active_at
        ,promotion_end_at
        ,weight
        ,standard_price_in_cents
        ,promotional_price_in_cents
        ,member_only_promotion_end_at
        ,sku_plan_entry_id
        ,reservation_window_days
        ,non_member_promotion_end_at
        ,_fivetran_deleted
        ,bulk_receivable
        ,is_presellable
        ,virtual_inventory
        ,_fivetran_synced
        ,member_discount_start_at
        ,general_discount_start_at
        ,general_discount_end_at
        ,member_discount_end_at
        ,member_discount_percent
        ,general_discount_percent
        ,non_member_discount_end_at
        ,non_member_discount_percent
        ,non_member_discount_start_at
        ,partial_member_discount_percent
        ,dbt_scd_id
        ,dbt_updated_at
        ,dbt_valid_from
        ,dbt_valid_to
        ,notes
        ,admin_updated_at
    from analytics.snapshots.skus_ss
) 

,datawarehouse_skus_ss as (
    select
        'datawarehouse_snapshot' as source
        ,id
        ,null::timestamp as non_member_promotion_start_at
        ,cost*100 as average_cost_in_cents
        ,promotion_start_at
        ,null::text as vendor_funded_discount_name
        ,null::text as sku_code
        ,null::int as vendor_funded_discount_cents
        ,null::int as non_member_promotion_discount
        ,null::int as member_only_promotion_discount
        ,null::timestamp as member_only_promotion_start_at
        ,dbt_updated_at as updated_at
        ,marketplace_platform_fee*100 as platform_fee_in_cents
        ,null::text as name
        ,marketplace_fulfillment_fee*100 as fulfillment_fee_in_cents
        ,price*100 as price_in_cents    
        ,marketplace_cost*100 as marketplace_cost_in_cents
        ,(select min(created_at) from analytics.snapshots.skus_ss s where s.id = skus_ss.id) as created_at
        ,null::int as sku_vendor_id
        ,null::float as average_box_quantity
        ,null::timestamp as vendor_funded_discount_start_at
        ,null::timestamp as vendor_funded_discount_end_at
        ,null::text as barcode
        ,marketplace_payment_processing_fee*100 as payment_processing_fee_in_cents
        ,null::int as cut_id
        ,null::float as vendor_funded_discount_percent
        ,null::timestamp as active_at
        ,promotion_end_at
        ,null::float as weight
        ,null::int as standard_price_in_cents
        ,promotional_price*100 as promotional_price_in_cents
        ,null::timestamp as member_only_promotion_end_at
        ,null::int as sku_plan_entry_id
        ,null::int as reservation_window_days
        ,null::timestamp as non_member_promotion_end_at
        ,false as _fivetran_deleted
        ,null::boolean as bulk_receivable
        ,null::boolean as is_presellable
        ,null::boolean as virtual_inventory
        ,null::timestamp as _fivetran_synced
        ,null::timestamp as member_discount_start_at
        ,null::timestamp as general_discount_start_at
        ,null::timestamp as general_discount_end_at
        ,null::timestamp as member_discount_end_at
        ,null::float as member_discount_percent
        ,null::float as general_discount_percent
        ,null::timestamp as non_member_discount_end_at
        ,null::float as non_member_discount_percent
        ,null::timestamp non_member_discount_start_at
        ,null::float as partial_member_discount_percent
        ,dbt_scd_id
        ,dbt_updated_at
        ,dbt_valid_from
        ,dbt_valid_to
        ,null::text as notes
        ,null::timestamp as admin_updated_at
    from datawarehouse.bi_snapshots.skus_ss
)

,union_snapshots as (
    select * from analytics_skus_ss
    union all
    select * from datawarehouse_skus_ss
)

,add_rules as (
    select
        *   
        ,source = 'datawarehouse_snapshot' and dbt_valid_from <= '2021-10-28 23:00' as rule_1 --date and time for when the new skus__ss code was merged
        ,source = 'analytics_snapshot' as rule_2 --prefer analytics snapshots since they contain more data
    from union_snapshots
    -- where id = 158720
    -- where id = 128606
    -- where id = 158861
    -- where id = 159078
    -- where id = 154397
    order by source desc,dbt_updated_at
)

,add_previous_next as (
    select 
    *
    ,min(case when source = 'analytics_snapshot' then dbt_valid_from end) over(partition by id order by updated_at rows between unbounded preceding and unbounded following) as first_analytics_snapshot_date
    ,lead(dbt_valid_from,1) over(partition by id order by updated_at) as next_dbt_valid_from
    ,lag(dbt_valid_from,1) over(partition by id order by updated_at) as previous_dbt_valid_from   
    ,lead(dbt_valid_to,1) over(partition by id order by updated_at) as next_dbt_valid_to
    ,lag(dbt_valid_to,1) over(partition by id order by updated_at) as previous_dbt_valid_to
    ,lead(source,1) over(partition by id order by updated_at) as next_snapshot_source
    ,lag(source,1) over(partition by id order by updated_at) as previous_snapshot_source
    from add_rules
    where (rule_1 or rule_2)
)

,add_overlaps as (
    select 
        (previous_dbt_valid_to is not null and previous_dbt_valid_to > dbt_valid_from) or (next_dbt_valid_from is not null and next_dbt_valid_from < dbt_valid_to) as overlaps
        ,last_value(source) over(partition by id order by updated_at) as last_source
        ,*
    from add_previous_next
)

,mind_the_gap as (
    select
        *
    from add_overlaps
    where source = 'analytics_snapshot'
        or (source = 'datawarehouse_snapshot' and (not overlaps or dbt_valid_from < first_analytics_snapshot_date) and source <> last_source)
)

,adjust_dbt_valid_to_dates as (
    select
        *
        ,case
            when overlaps and source = 'datawarehouse_snapshot' then lead(dbt_valid_from,1) over(partition by id order by updated_at)
            else coalesce(dbt_valid_to,lead(dbt_valid_from,1) over(partition by id order by updated_at))
         end as merged_dbt_valid_to
    from mind_the_gap
)

,final_snapshot as (
    select
        id
        ,non_member_promotion_start_at
        ,average_cost_in_cents
        ,promotion_start_at
        ,vendor_funded_discount_name
        ,sku_code
        ,vendor_funded_discount_cents
        ,non_member_promotion_discount
        ,member_only_promotion_discount
        ,member_only_promotion_start_at
        ,updated_at
        ,platform_fee_in_cents
        ,name
        ,fulfillment_fee_in_cents
        ,price_in_cents
        ,marketplace_cost_in_cents
        ,created_at
        ,sku_vendor_id
        ,average_box_quantity
        ,vendor_funded_discount_start_at
        ,vendor_funded_discount_end_at
        ,barcode
        ,payment_processing_fee_in_cents
        ,cut_id
        ,vendor_funded_discount_percent
        ,active_at
        ,promotion_end_at
        ,weight
        ,standard_price_in_cents
        ,promotional_price_in_cents
        ,member_only_promotion_end_at
        ,sku_plan_entry_id
        ,reservation_window_days
        ,non_member_promotion_end_at
        ,_fivetran_deleted
        ,bulk_receivable
        ,is_presellable
        ,virtual_inventory
        ,_fivetran_synced
        ,member_discount_start_at
        ,general_discount_start_at
        ,general_discount_end_at
        ,member_discount_end_at
        ,member_discount_percent
        ,general_discount_percent
        ,non_member_discount_end_at
        ,non_member_discount_percent
        ,non_member_discount_start_at
        ,partial_member_discount_percent
        ,dbt_scd_id
        ,dbt_updated_at
        ,dbt_valid_from
        ,merged_dbt_valid_to as dbt_valid_to
        ,notes
        ,admin_updated_at
    from adjust_dbt_valid_to_dates
)
    
select *
from final_snapshot);

/**** Verify there are no gaps or overlaps in dbt valid from/to dates ****/
with 

overlap_check as (
    select
        id
        ,price_in_cents
        ,updated_at
        ,dbt_valid_from
        ,dbt_valid_to
        ,lead(dbt_valid_from,1) over(partition by id order by updated_at) as next_dbt_valid_from
        ,lag(dbt_valid_from,1) over(partition by id order by updated_at) as previous_dbt_valid_from   
        ,lead(dbt_valid_to,1) over(partition by id order by updated_at) as next_dbt_valid_to
        ,lag(dbt_valid_to,1) over(partition by id order by updated_at) as previous_dbt_valid_to
    from kdent_analytics_dev.snapshots.combined_skus_ss
)

select *
,(previous_dbt_valid_to is not null and previous_dbt_valid_to > dbt_valid_from) or (next_dbt_valid_from is not null and next_dbt_valid_from < dbt_valid_to) as overlaps
,(previous_dbt_valid_to is not null and previous_dbt_valid_to <> dbt_valid_from) or (next_dbt_valid_from is not null and next_dbt_valid_from <> dbt_valid_to) as gaps
from overlap_check
where (previous_dbt_valid_to is not null and previous_dbt_valid_to > dbt_valid_from) or (next_dbt_valid_from is not null and next_dbt_valid_from < dbt_valid_to)
    or (previous_dbt_valid_to is not null and previous_dbt_valid_to <> dbt_valid_from) or (next_dbt_valid_from is not null and next_dbt_valid_from <> dbt_valid_to)
limit 100000;


/**** Swap old snapshot with new snapshot ****/
alter table kdent_analytics_dev.snapshots.skus_ss swap with kdent_analytics_dev.snapshots.combined_skus_ss;

/**** Transfer ownership of new snapshot table to primary owner of database ****/
grant ownership on table kdent_analytics_dev.snapshots.skus_ss to role transformer;

/**** Verify counts in the swapped tables to make sure they were swapped ****/
select count(*) from kdent_analytics_dev.snapshots.combined_skus_ss;
select count(*) from kdent_analytics_dev.snapshots.skus_ss;

/**** Verify counts in amounts between the dev and prod databases to ensure the numbers match ****/
select count(*),sum(order_item_revenue),sum(sku_product_revenue) from kdent_analytics_dev.business_vault.order_item_details limit 100;
select count(*),sum(order_item_revenue),sum(sku_product_revenue) from analytics.business_vault.order_item_details limit 100;

/**** Replace historical SKU cost in the "old" data model snapshots ****/
select 
    count(*) as cnt
    ,count(distinct id) as sku_id_count
from datawarehouse.bi_snapshots.skus_ss
where id in (select sku_id from sandbox.public.sku_fix_upload)
    and (dbt_valid_to >= '2021-12-01'
        or dbt_valid_to is null)
limit 1000;

--783 records should be updated

create or replace table datawarehouse.bi_snapshots.skus_ss_backup_20220119 clone datawarehouse.bi_snapshots.skus_ss;

update datawarehouse.bi_snapshots.skus_ss_backup_20220119
set cost = fix.sku_cost
from sandbox.public.sku_fix_upload fix
where id = fix.sku_id
    and (dbt_valid_to >= '2021-12-01'
        or dbt_valid_to is null);
    
-- updated 783 records

select
   skus_ss.id
   ,skus_ss.cost
   ,sku_fix_upload.sku_id
   ,sku_fix_upload.sku_cost as new_sku_cost
from datawarehouse.bi_snapshots.skus_ss
    inner join sandbox.public.sku_fix_upload on skus_ss.id = sku_fix_upload.sku_id
where skus_ss.cost <> sku_fix_upload.sku_cost
    and (dbt_valid_to >= '2021-12-01'
        or dbt_valid_to is null);
-- Check SKU updates in current table

select
   skus_ss_backup_20220119.id
   ,skus_ss_backup_20220119.cost
   ,sku_fix_upload.sku_id
   ,sku_fix_upload.sku_cost as new_sku_cost
from datawarehouse.bi_snapshots.skus_ss_backup_20220119
    inner join sandbox.public.sku_fix_upload on skus_ss_backup_20220119.id = sku_fix_upload.sku_id
where skus_ss_backup_20220119.cost <> sku_fix_upload.sku_cost
    and (dbt_valid_to >= '2021-12-01'
        or dbt_valid_to is null);
-- Check SKU updates in backup


alter table datawarehouse.bi_snapshots.skus_ss swap with datawarehouse.bi_snapshots.skus_ss_backup_20220119;

grant ownership on table datawarehouse.bi_snapshots.skus_ss to role transformer copy current grants;

/**** Replace historical SKU cost in the "new" data model snapshots ****/
select 
    count(*) as cnt
    ,count(distinct id) as sku_id_count
from analytics.snapshots.skus_ss
where id in (select sku_id from sandbox.public.sku_fix_upload)
    and (dbt_valid_to >= '2021-12-01'
        or dbt_valid_to is null)
limit 1000;

--4,216 should be updated

create or replace table analytics.snapshots.skus_ss_backup_20220119 clone analytics.snapshots.skus_ss;

update analytics.snapshots.skus_ss_backup_20220119
set average_cost_in_cents = fix.sku_cost*100
from sandbox.public.sku_fix_upload fix
where id = fix.sku_id
    and (dbt_valid_to >= '2021-12-01'
        or dbt_valid_to is null);
        
--4,216 were updated

select
   skus_ss.id
   ,skus_ss.average_cost_in_cents
   ,sku_fix_upload.sku_id
   ,sku_fix_upload.sku_cost*100 as new_sku_cost
from analytics.snapshots.skus_ss
    inner join sandbox.public.sku_fix_upload on skus_ss.id = sku_fix_upload.sku_id
where skus_ss.average_cost_in_cents <> sku_fix_upload.sku_cost*100
    and (dbt_valid_to >= '2021-12-01'
        or dbt_valid_to is null);
-- Check SKU updates in current table

select
   skus_ss_backup_20220119.id
   ,skus_ss_backup_20220119.average_cost_in_cents
   ,sku_fix_upload.sku_id
   ,sku_fix_upload.sku_cost*100 as new_sku_cost
from analytics.snapshots.skus_ss_backup_20220119
    inner join sandbox.public.sku_fix_upload on skus_ss_backup_20220119.id = sku_fix_upload.sku_id
where skus_ss_backup_20220119.average_cost_in_cents <> sku_fix_upload.sku_cost*100
    and (dbt_valid_to >= '2021-12-01'
        or dbt_valid_to is null);
-- Check SKU updates in backup

alter table analytics.snapshots.skus_ss swap with analytics.snapshots.skus_ss_backup_20220119;

grant ownership on table analytics.snapshots.skus_ss to role transformer copy current grants;

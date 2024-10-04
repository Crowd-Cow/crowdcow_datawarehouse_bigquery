with 

 source as (select * from  {{ source('cc', 'scan_session_logs') }} )
 ,sku_boxes as (select distinct sku_box_id, lot_id, fc_id from {{ ref('stg_cc__sku_boxes') }} where dbt_valid_to is null )
 ,lots as (select * from {{ ref('lots') }} where dbt_valid_to is null )
 ,scanned_weight as ( select distinct sku_id, sku_box_id, scanned_weight, barcode from {{  ref('int_scanned_weight')  }})


,renamed as (
    select
        id as scan_session_logs_id
        ,sku_boxes.fc_id as fc_id 
        ,lots.lot_number as lot_number
        --,scanned_weight --- scanned out weight
        ,coalesce(scanned_weight.scanned_weight, weight) as scan_weight --first scannedout weight 
        ,created_at as created_at_utc
        ,source.barcode 
        ,scanned_weight.barcode as scanned_weight_barcode
        ,updated_at as updated_at_utc
        ,source.quantity
        ,scan_session_id 
        ,box_id as sku_box_id
        ,source.sku_id
        
    from source    
    left join sku_boxes on sku_boxes.sku_box_id = source.box_id
    left join lots on lots.lot_id = sku_boxes.lot_id
    left join scanned_weight on scanned_weight.sku_box_id = source.box_id  and  scanned_weight.sku_id = source.sku_id and scanned_weight.barcode = source.barcode 


)

select * from renamed

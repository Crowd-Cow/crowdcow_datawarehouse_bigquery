{% snapshot sku_reservations_ss %}

{{
    config(
        target_schema='snapshots',
        unique_key='id',

        strategy='check',
        check_cols=['bid_id','bid_item_id','cost_in_cents','created_at','fc_id','id','manually_changed_at','order_id','original_quantity','price_in_cents','quantity','reservation_state','sku_id','updated_at','pick_list_id','fulfillment_fee_in_cents','payment_processing_fee_in_cents','platform_fee_in_cents','packing_insert','insert_type','__deleted']
    )
}}

select 
  bid_id,
  bid_item_id,
  cost_in_cents,
  created_at,
  fc_id,
  id,
  manually_changed_at,
  order_id,
  original_quantity,
  price_in_cents,
  quantity,
  reservation_state,
  sku_id,
  updated_at,
  pick_list_id,
  fulfillment_fee_in_cents,
  payment_processing_fee_in_cents,
  platform_fee_in_cents,
  packing_insert,
  insert_type,
  __deleted
from {{ source('cc', 'sku_reservations') }}

{% endsnapshot %}

  
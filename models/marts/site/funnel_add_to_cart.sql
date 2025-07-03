-- models/your_model.sql
{{ config(
    materialized='incremental',
    unique_key='visit_id',
    partition_by={'field': 'started_at_utc', 'data_type': 'timestamp', 'granularity': 'day'},
    cluster_by=['visit_id'],
    on_schema_change = 'sync_all_columns'
) }}

{% if is_incremental() %}
-- Pre-calculating the max_loaded_at timestamp is more efficient than a subquery
{% set max_loaded_at = run_query("select max(started_at_utc) from " ~ this) .columns[0].values()[0] %}
{% endif %}

with
visits as (
    select
    visit_id,
    visitor_ip_session,
    started_at_utc
    from {{ ref('visits') }}
    where
    not is_proxy
    and not is_server
    and not is_internal_traffic
    {% if is_incremental() %}
    -- Filter for new visits only
    and started_at_utc >= '{{ max_loaded_at }}'
    {% endif %}
),

events as (
    select
      e.visit_id,
      e.event_sequence_number,
      e.event_name,
      e.on_page_path
    from {{ ref('events') }} e
    where e.event_name in (
      'PAGE_VIEW','VIEWED_PRODUCT','PRODUCT_CARD_VIEWED',
      'PRODUCT_CARD_QUICK_ADD_TO_CART','ORDER_ADD_TO_CART',
      'ORDER_ENTER_ADDRESS','ORDER_ENTER_PAYMENT',
      'ORDER_COMPLETE','ORDER_PAID'
    )
    {% if is_incremental() %}
      -- Filter for events related to the new visits, with a lookback for late data
      and e.occurred_at_utc >= timestamp_sub(timestamp('{{ max_loaded_at }}'), interval 3 day)
    {% endif %}
)

,flags as (
  select
    visit_id,
    max(if(event_sequence_number=1 and event_name='PAGE_VIEW',1,0)) as has_page_view,
    max(if(event_name in ('VIEWED_PRODUCT','PRODUCT_CARD_VIEWED'),1,0)) as viewed_product,
    max(if(event_name in ('PRODUCT_CARD_QUICK_ADD_TO_CART','ORDER_ADD_TO_CART'),1,0)) as added_to_cart,
    max(if(on_page_path like '%/DELIVERY',1,0))                 as on_delivery_page,
    max(if(event_name='ORDER_ENTER_ADDRESS',1,0))              as entered_address,
    max(if(event_name='ORDER_ENTER_PAYMENT',1,0))              as entered_payment,
    max(if(event_name='ORDER_COMPLETE',1,0))                   as completed_order,
    max(if(event_name='ORDER_PAID',1,0))                       as paid
  from events
  group by visit_id
)

select
  v.visit_id,
  v.visitor_ip_session,
  v.started_at_utc,
  flags.has_page_view as session_start,
  (flags.has_page_view = 1 AND flags.viewed_product = 1) as viewed_product,
  (flags.has_page_view = 1 AND flags.viewed_product = 1 AND flags.added_to_cart = 1) as add_to_carts,
  (flags.has_page_view = 1 AND flags.viewed_product = 1 AND flags.added_to_cart = 1 AND flags.on_delivery_page = 1) as initiate_checkout,
  (flags.has_page_view = 1 AND flags.viewed_product = 1 AND flags.added_to_cart = 1 AND flags.on_delivery_page = 1 AND flags.entered_address = 1) as ORDER_ENTER_ADDRESS,
  (flags.has_page_view = 1 AND flags.viewed_product = 1 AND flags.added_to_cart = 1 AND flags.on_delivery_page = 1 AND flags.entered_address = 1 AND flags.entered_payment = 1) as ORDER_ENTER_PAYMENT,
  (flags.has_page_view = 1 AND flags.viewed_product = 1 AND flags.added_to_cart = 1 AND flags.on_delivery_page = 1 AND flags.entered_address = 1 AND flags.entered_payment = 1 AND flags.completed_order = 1) as ORDER_COMPLETE,
  (flags.has_page_view = 1 AND flags.viewed_product = 1 AND flags.added_to_cart = 1 AND flags.on_delivery_page = 1 AND flags.entered_address = 1 AND flags.entered_payment = 1 AND flags.completed_order = 1 AND flags.paid = 1) as ORDER_PAID
from visits v
left join flags on v.visit_id = flags.visit_id

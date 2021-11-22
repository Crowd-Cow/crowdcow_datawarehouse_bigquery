{% snapshot pipeline_orders_ss %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('cc', 'pipeline_orders') }}

{% endsnapshot %}
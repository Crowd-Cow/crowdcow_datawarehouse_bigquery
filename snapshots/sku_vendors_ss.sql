{% snapshot sku_vendors_ss %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('cc', 'sku_vendors') }}

{% endsnapshot %}
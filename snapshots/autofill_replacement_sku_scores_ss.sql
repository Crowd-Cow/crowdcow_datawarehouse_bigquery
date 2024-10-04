{% snapshot autofill_replacement_sku_scores_ss %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',

      strategy='timestamp',
      updated_at='__updatetime',
    )
}}

select * from {{ source('cc', 'autofill_replacement_sku_scores') }}

{% endsnapshot %}
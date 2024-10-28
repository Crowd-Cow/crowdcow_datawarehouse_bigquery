{% snapshot demand_forecast_ss %}

{{
   config(
       target_schema='snapshots',
       unique_key='id',

       strategy='check',
       check_cols = ['id','date', 'fc_id', 'fc_name', 'category', 'sub_category', 'cut_id', 'cut_name', 'inventory_classification', 'predicted_quantity']
   )
}}

select 
    {{ dbt_utils.surrogate_key(['date(date)','fc_id','category','sub_category','cut_id']) }} as id
    ,date
    ,fc_id
    ,fc_name
    ,category
    ,sub_category
    ,cut_id
    ,cut_name
    ,inventory_classification
    ,predicted_quantity
from {{ source('demand_forecast', 'prediction_report') }}

{% endsnapshot %}
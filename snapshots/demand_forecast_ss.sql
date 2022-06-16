{% snapshot demand_forecast_ss %}

{{
   config(
       target_schema='snapshots',
       unique_key='id',

       strategy='check',
       check_cols = 'all'
   )
}}

select 
    {{ dbt_utils.surrogate_key(['date','fc_id','category','sub_category','cut_id']) }} as id
    ,* 
from {{ source('demand_forecast', 'prediction_report') }}

{% endsnapshot %}
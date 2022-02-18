with

site_performance as ( select * from {{ source('google_analytics', 'site_performance') }} )

select
date::date as date
,avg_redirection_time
,dom_latency_metrics_sample
,avg_page_load_time
from site_performance
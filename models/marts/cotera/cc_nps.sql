with

nps as ( select * from {{ ref('post_order_survey_results') }} )

select *
from nps
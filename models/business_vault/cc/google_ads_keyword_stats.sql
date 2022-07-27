with

keyword_stats as ( select * from {{ ref('stg_google_ads__keyword_stats') }} )

select * from keyword_stats
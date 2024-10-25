with

email as ( select * from {{ ref('stg_cc__user_sms_log_entries') }} )

select * from email

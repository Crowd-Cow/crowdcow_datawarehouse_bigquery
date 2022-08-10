with

email as ( select * from {{ ref('stg_cc__user_mail_log_entries') }} )

select * from email

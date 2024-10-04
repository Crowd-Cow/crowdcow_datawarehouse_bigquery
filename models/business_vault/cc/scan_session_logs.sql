with

scan_session_logs as ( select * from {{ ref('stg_cc__scan_session_logs') }} )

select * from scan_session_logs

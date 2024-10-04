with

source as ( select * from {{ source('cc', 'user_mail_log_entries') }}  )

,renamed as (
    select  
        id as user_mail_log_id
        ,created_at as created_at_utc
        ,{{ clean_strings('to_str') }} as to_email
        ,{{ clean_strings('first_clicked_url') }} as first_clicked_url
        ,first_clicked_at as first_clicked_at_utc
        ,to_user_id
        ,token as user_mail_log_token
        ,first_opened_at as first_opened_at_utc
        ,{{ clean_strings('from_str') }} as from_email
        ,event_id
        ,{{ clean_strings('reply_to_email_address') }} as reply_to_email
        ,{{ clean_strings('first_clicked_anchor') }} as first_clicked_anchor
        ,from_user_id
        ,order_id
        ,actually_sent_at as email_sent_at_utc
        ,updated_at as updated_at_utc
        ,{{ clean_strings('key') }} as email_type
        ,{{ clean_strings('subject') }} as email_subject
    from source
)

select * from renamed

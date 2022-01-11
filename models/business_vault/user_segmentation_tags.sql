with

tag as ( select * from {{ ref('stg_cc__tags') }} )
,tag_definition as ( select * from {{ ref('stg_reference__tag_definitions') }} )

select
    tag.tag_id
    ,tag.tag_name
    ,tag_definition.tag_definition
    ,tag.user_id
    ,tag.tag_purpose
    ,tag.created_at_utc
    ,tag.updated_at_utc
from tag
    left join tag_definition on tag.tag_name = tag_definition.tag_name

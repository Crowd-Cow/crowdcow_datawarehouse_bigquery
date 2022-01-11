with

tag_definition as ( select * from {{ source('cc', 'tag_definitions') }} )

select
    {{ clean_strings('tag_name') }} as tag_name
    ,{{ clean_strings('tag_definition') }} as tag_definition
from tag_definition

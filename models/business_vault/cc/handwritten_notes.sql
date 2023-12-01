with

notes as ( select * from {{ ref('stg_cc__handwritten_notes') }} )



select * from notes

with

packing_action as (select * from {{ ref('stg_cc__packing_actions') }} where action = 'ADD_TO_BOX' )


select * from packing_action

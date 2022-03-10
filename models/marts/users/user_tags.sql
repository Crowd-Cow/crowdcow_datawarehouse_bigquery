with

is_employee as (
    {{ generate_tag('users','user_id','employee','user_segment') }}
    where user_type = 'EMPLOYEE'
)

select * from is_employee

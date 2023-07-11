with new_cols as (select
    *,
    date(pickup_datetime) as pu_date
from {{ref('union__green_taxi')}})

select 
    *
from new_cols
where pu_date >= '2021-01-01'


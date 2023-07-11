select 
    pu_date,
    round(sum(fare_amount), 2) as revenue,
from {{ ref('stg__cleaned') }}
group by (1)
order by (1)
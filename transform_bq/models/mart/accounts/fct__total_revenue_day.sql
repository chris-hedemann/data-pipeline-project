select 
    pu_date as pickup_date,
    cast(revenue as STRING FORMAT '$999,999.99') as total_daily_revenue
from {{ ref('int__revenue_day') }}
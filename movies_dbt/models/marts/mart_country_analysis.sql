-- Country analysis mart
-- Answers: Which countries produce most profitable movies?
-- Used for: country analysis visualization

with movies as (
    select * from {{ ref('stg_movies') }}
)

select
    country,
    count(title)                        as movie_count,
    round(avg(budget))                  as avg_budget,
    round(avg(box_office))              as avg_box_office,
    round(avg(roi)::numeric, 2)                  as avg_roi,
    round(sum(box_office))              as total_box_office,
    rank() over (
        order by sum(box_office) desc
    )                                   as revenue_rank
from movies
where box_office is not null
group by country
order by total_box_office desc
-- Genre analysis mart
-- Answers: Which genres have highest revenue, ROI and movie count?
-- Used for: investment recommendations visualization

with movies as (
    select * from {{ ref('stg_movies') }}
),

genres as (
    select * from {{ ref('stg_genres') }}
),

joined as (
    select
        g.genre,
        count(distinct m.title)        as movie_count,
        round(avg(m.budget))           as avg_budget,
        round(avg(m.box_office))       as avg_box_office,
        round(avg(m.roi)::numeric, 2)           as avg_roi,
        round(sum(m.box_office))       as total_box_office
    from genres g
    left join movies m on g.title = m.title
    where m.box_office is not null
    group by g.genre
)

select
    genre,
    movie_count,
    avg_budget,
    avg_box_office,
    avg_roi,
    total_box_office,
    -- Rank genres by total revenue
    rank() over (order by total_box_office desc) as revenue_rank
from joined
order by total_box_office desc
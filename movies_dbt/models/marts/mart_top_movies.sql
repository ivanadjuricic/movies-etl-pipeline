-- Top movies mart
-- Answers: Which movies performed best by ROI and box office?
-- Used for: top movies visualization

with movies as (
    select * from {{ ref('stg_movies') }}
),

genres as (
    select * from {{ ref('stg_genres') }}
),

-- Aggregate genres per movie into a single string
movie_genres_agg as (
    select
        title,
        string_agg(genre, ', ' order by genre) as genres
    from genres
    group by title
),

final as (
    select
        m.title,
        m.release_year,
        m.director,
        m.country,
        m.budget,
        m.box_office,
        m.roi,
        m.budget_category,
        m.roi_category,
        g.genres
    from movies m
    left join movie_genres_agg g on m.title = g.title
)

select * from final
order by box_office desc
-- Staging model for movies table
-- Selects and renames columns from the raw movies table
-- Filters out movies without box_office data for cleaner analytics

with source as (
    select * from {{ source('public', 'movies') }}
),

renamed as (
    select
        title,
        release_year,
        director,
        language,
        country,
        duration,
        budget,
        box_office,
        roi,

        -- Classify movies by budget size
        case
            when budget < 10000000  then 'Low Budget'
            when budget < 50000000  then 'Mid Budget'
            when budget < 150000000 then 'High Budget'
            else 'Blockbuster'
        end as budget_category,

        -- Classify movies by ROI performance
        case
            when roi < 0   then 'Loss'
            when roi < 100 then 'Low Return'
            when roi < 500 then 'Good Return'
            else 'Exceptional Return'
        end as roi_category

    from source
    where box_office is not null  -- exclude movies with unknown box office
)

select * from renamed
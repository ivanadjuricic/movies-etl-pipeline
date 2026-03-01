-- Staging model for movie_genres table
-- Clean pass-through with minor standardization

with source as (
    select * from {{ source('public', 'movie_genres') }}
)

select
    title,
    genre
from source
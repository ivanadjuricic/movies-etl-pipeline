from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

with engine.connect() as conn:

    print("--- Top 5 filmova po ROI ---")
    result = conn.execute(text("""
        SELECT title, release_year, budget, box_office, roi
        FROM movies
        ORDER BY roi DESC
        LIMIT 5;
    """))
    for row in result:
        print(row)

    print("\n--- Broj filmova po žanru (top 10) ---")
    result = conn.execute(text("""
        SELECT genre, COUNT(*) as film_count
        FROM movie_genres
        GROUP BY genre
        ORDER BY film_count DESC
        LIMIT 10;
    """))
    for row in result:
        print(row)

    print("\n--- Ukupan broj redova ---")
    movies_count = conn.execute(
        text("SELECT COUNT(*) FROM movies")
    ).scalar()
    genres_count = conn.execute(
        text("SELECT COUNT(*) FROM movie_genres")
    ).scalar()
    print(f"movies: {movies_count} redova")
    print(f"movie_genres: {genres_count} redova")
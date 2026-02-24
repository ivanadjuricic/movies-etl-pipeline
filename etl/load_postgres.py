from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd
import os

load_dotenv()


def get_engine():
    """
    Creates and returns a SQLAlchemy engine using credentials from .env file.
    The engine is the core connection object — it manages the connection pool
    to PostgreSQL and is reused across all database operations.
    """
    return create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )


def create_tables(engine) -> None:
    """
    Creates the movies and movie_genres tables in PostgreSQL if they don't exist.
    
    We define the schema explicitly here rather than letting pandas infer it,
    because we want proper data types and constraints (PRIMARY KEY, FOREIGN KEY).
    This is the professional approach — always control your schema.
    """
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS movies (
                id          SERIAL PRIMARY KEY,
                title       VARCHAR(300) NOT NULL,
                release_year INTEGER,
                director    VARCHAR(200),
                language    VARCHAR(100),
                country     VARCHAR(100),
                duration    INTEGER,
                budget      BIGINT,
                box_office  BIGINT,
                roi         NUMERIC(10, 2)
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS movie_genres (
                id       SERIAL PRIMARY KEY,
                title    VARCHAR(300) NOT NULL,
                genre    VARCHAR(100) NOT NULL
            );
        """))

        conn.commit()
        print("Tables created successfully (or already exist).")


def load_to_postgres(df_movies: pd.DataFrame,
                     df_movie_genres: pd.DataFrame) -> None:
    """
    Loads cleaned DataFrames into PostgreSQL staging tables.

    if_exists='replace' means:
      - If the table already has data, drop it and reload from scratch
      - This is appropriate for a full-refresh pipeline like ours
      - Alternative would be 'append' for incremental loads

    index=False means:
      - Don't write the pandas DataFrame index (0, 1, 2...) as a column
      - We have our own SERIAL PRIMARY KEY defined in the schema
    """
    engine = get_engine()

    # Step 1: Create tables with proper schema
    create_tables(engine)

    # Step 2: Load movies table
    print(f"Loading {len(df_movies)} rows into movies table...")
    df_movies.to_sql(
        name='movies',
        con=engine,
        if_exists='replace',
        index=False
    )
    print("movies table loaded successfully.")

    # Step 3: Load movie_genres table
    print(f"Loading {len(df_movie_genres)} rows into movie_genres table...")
    df_movie_genres.to_sql(
        name='movie_genres',
        con=engine,
        if_exists='replace',
        index=False
    )
    print("movie_genres table loaded successfully.")

    # Step 4: Verify row counts match what we loaded
    with engine.connect() as conn:
        movies_count = conn.execute(
            text("SELECT COUNT(*) FROM movies")
        ).scalar()

        genres_count = conn.execute(
            text("SELECT COUNT(*) FROM movie_genres")
        ).scalar()

    print(f"\nVerification:")
    print(f"  movies table:       {movies_count} rows")
    print(f"  movie_genres table: {genres_count} rows")


if __name__ == "__main__":
    # Full pipeline test: extract from local CSV → transform → load to PostgreSQL
    from extract import extract_from_s3
    from transform import transform

    print("=== Starting ETL: Extract → Transform → Load ===\n")

    # Extract
    print("--- EXTRACT ---")
    df_raw = extract_from_s3()

    # Transform
    print("\n--- TRANSFORM ---")
    df_movies, df_movie_genres = transform(df_raw)

    # Load
    print("\n--- LOAD ---")
    load_to_postgres(df_movies, df_movie_genres)

    print("\n=== ETL Complete ===")
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# NOTE: Snowflake integration is prepared but requires an active Snowflake
# account. For portfolio demonstration, the pipeline loads data into
# PostgreSQL (staging) and transforms it with dbt. Snowflake would be
# the final data warehouse destination in a production environment.


def get_snowflake_engine():
    """
    Creates and returns a SQLAlchemy engine for Snowflake.

    Snowflake connection requires:
    - account: your_org-your_account (e.g. xy12345.eu-central-1)
    - warehouse: compute cluster that runs queries (MOVIES_WH)
    - database: logical container for schemas and tables (MOVIES_DB)
    - schema: namespace within database (PUBLIC)
    """
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'MOVIES_WH')
    database = os.getenv('SNOWFLAKE_DATABASE', 'MOVIES_DB')
    schema = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')

    connection_string = (
        f"snowflake://{user}:{password}@{account}/"
        f"{database}/{schema}?warehouse={warehouse}"
    )

    return create_engine(connection_string)


def create_snowflake_tables(engine) -> None:
    """
    Creates staging tables in Snowflake if they don't exist.

    Snowflake uses the same SQL syntax as PostgreSQL for CREATE TABLE,
    but with Snowflake-specific data types like VARCHAR(unlimited).
    AUTOINCREMENT replaces PostgreSQL's SERIAL.
    """
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS movies (
                id          INTEGER AUTOINCREMENT PRIMARY KEY,
                title       VARCHAR NOT NULL,
                release_year INTEGER,
                director    VARCHAR,
                language    VARCHAR,
                country     VARCHAR,
                duration    INTEGER,
                budget      BIGINT,
                box_office  BIGINT,
                roi         NUMERIC(10, 2)
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS movie_genres (
                id      INTEGER AUTOINCREMENT PRIMARY KEY,
                title   VARCHAR NOT NULL,
                genre   VARCHAR NOT NULL
            );
        """))

        print("Snowflake tables created successfully (or already exist).")


def load_to_snowflake(df_movies: pd.DataFrame,
                      df_movie_genres: pd.DataFrame) -> None:
    """
    Loads transformed DataFrames into Snowflake staging tables.

    The process is identical to PostgreSQL loading:
    1. Create tables with proper schema
    2. TRUNCATE existing data
    3. Load new data with pandas to_sql
    4. Verify row counts

    Key difference from PostgreSQL:
    - Snowflake automatically handles scaling and compression
    - Queries run on a separate compute cluster (warehouse)
    - Data is stored in Snowflake's columnar format for fast analytics
    """
    engine = get_snowflake_engine()

    # Step 1: Create tables
    create_snowflake_tables(engine)

    # Step 2: Load movies table
    print(f"Loading {len(df_movies)} rows into Snowflake movies table...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE movies"))
    df_movies.to_sql(
        name='movies',
        con=engine,
        if_exists='append',
        index=False
    )
    print("Snowflake movies table loaded successfully.")

    # Step 3: Load movie_genres table
    print(f"Loading {len(df_movie_genres)} rows into Snowflake movie_genres table...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE movie_genres"))
    df_movie_genres.to_sql(
        name='movie_genres',
        con=engine,
        if_exists='append',
        index=False
    )
    print("Snowflake movie_genres table loaded successfully.")

    # Step 4: Verify row counts
    with engine.begin() as conn:
        movies_count = conn.execute(
            text("SELECT COUNT(*) FROM movies")
        ).scalar()
        genres_count = conn.execute(
            text("SELECT COUNT(*) FROM movie_genres")
        ).scalar()

    print(f"\nVerification:")
    print(f"  Snowflake movies table:       {movies_count} rows")
    print(f"  Snowflake movie_genres table: {genres_count} rows")


if __name__ == "__main__":
    # NOTE: Requires active Snowflake account and credentials in .env
    # This script demonstrates the Snowflake loading pattern.
    # In production, this would be triggered by Airflow after dbt models run.
    from extract import extract_from_s3
    from transform import transform

    print("=== Starting Snowflake Load ===\n")

    df_raw = extract_from_s3()
    df_movies, df_movie_genres = transform(df_raw)
    load_to_snowflake(df_movies, df_movie_genres)

    print("\n=== Snowflake Load Complete ===")
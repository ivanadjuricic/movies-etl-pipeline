import pandas as pd
import numpy as np


# Mapping of typos and non-standard genre names to standardized values
GENRE_CORRECTIONS = {
    'Horor': 'Horror',
    'Mistery': 'Mystery',
    'Misterija': 'Mystery',
    'Krimi': 'Crime',
    'Komedija': 'Comedy',
    'Akcija': 'Action',
    'Romantic Drama': 'Drama',
    'Comedy-Drama': 'Drama',
    'Crime Drama': 'Crime',
    'Sports Drama': 'Drama',
    'War Drama': 'Drama',
    'Historical Drama': 'Drama',
    'Thriller': 'Thriller',
    'Avant-Garde': 'Documentary'
}


def fix_genre(genre: str) -> str:
    """
    Corrects a single genre string using the GENRE_CORRECTIONS mapping.
    Returns the corrected genre, or the original if no correction exists.
    """
    genre = genre.strip()
    return GENRE_CORRECTIONS.get(genre, genre)


def clean_genres(genre_string: str) -> list:
    """
    Splits a comma-separated genre string into a list of cleaned genre names.
    Applies typo correction and removes duplicates while preserving order.
    Example: "Action, Horor, Mistery" → ["Action", "Horror", "Mystery"]
    """
    if pd.isna(genre_string):
        return []
    genres = [fix_genre(g) for g in genre_string.split(',')]
    seen = set()
    unique_genres = []
    for g in genres:
        if g not in seen:
            seen.add(g)
            unique_genres.append(g)
    return unique_genres


def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Main transformation function.

    Accepts raw DataFrame loaded from S3.
    Returns two DataFrames:
      - df_movies: cleaned movies table (one row per movie, no genre column)
      - df_movie_genres: normalized genres table (one row per movie-genre pair)
    """
    print("Starting transformation...")
    print(f"Input rows: {len(df)}")

    # Work on a copy to avoid modifying the original DataFrame in memory
    df = df.copy()

    # Replace string "unknown" with NaN so it's treated as a true null value
    df['box_office'] = df['box_office'].replace('unknown', np.nan)

    # Convert numeric columns from string/object type to proper numeric types
    # errors='coerce' turns any unparseable values into NaN instead of raising errors
    df['budget'] = pd.to_numeric(df['budget'], errors='coerce')
    df['box_office'] = pd.to_numeric(df['box_office'], errors='coerce')
    df['duration'] = pd.to_numeric(df['duration'], errors='coerce')
    df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce')

    # Strip leading/trailing whitespace from all text columns
    df['title'] = df['title'].str.strip()
    df['director'] = df['director'].str.strip()
    df['language'] = df['language'].str.strip()
    df['country'] = df['country'].str.strip()

    # Calculate ROI (Return on Investment) as a percentage
    # Formula: (box_office - budget) / budget * 100
    # This derived metric will be used in visualizations and analytics
    df['roi'] = ((df['box_office'] - df['budget']) / df['budget'] * 100).round(2)

    # Create the movies table by dropping the genre column
    # Genres are stored separately in a normalized table (df_movie_genres)
    df_movies = df.drop(columns=['genre']).copy()

    # Normalize genres: split "Action, Drama, Sci-Fi" into separate rows
    # Result: one row per movie-genre combination
    # Before: Avatar | "Action, Adventure, Sci-Fi"
    # After:  Avatar | Action
    #         Avatar | Adventure
    #         Avatar | Sci-Fi
    rows = []
    for idx, row in df.iterrows():
        genres = clean_genres(row['genre'])
        for genre in genres:
            rows.append({
                'title': row['title'],
                'genre': genre
            })

    df_movie_genres = pd.DataFrame(rows)

    # Print transformation summary for logging/debugging
    print(f"Movies after cleaning: {len(df_movies)}")
    print(f"Rows with NULL box_office: {df_movies['box_office'].isna().sum()}")
    print(f"Rows with NULL budget: {df_movies['budget'].isna().sum()}")
    print(f"Total genre rows: {len(df_movie_genres)}")
    print(f"Unique genres: {df_movie_genres['genre'].nunique()}")
    print("Transformation complete!")

    return df_movies, df_movie_genres


if __name__ == "__main__":
    # Quick local test using the CSV file directly (bypasses S3)
    df_raw = pd.read_csv('data/movies.csv')
    df_movies, df_movie_genres = transform(df_raw)

    print("\n--- First 5 movies ---")
    print(df_movies[['title', 'release_year', 'budget', 'box_office', 'roi']].head())

    print("\n--- First 10 genre rows ---")
    print(df_movie_genres.head(10))

    print("\n--- All unique genres ---")
    print(sorted(df_movie_genres['genre'].unique()))
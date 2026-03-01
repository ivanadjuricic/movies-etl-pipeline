import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd
import os

load_dotenv()

# mart tables from dbt (data build tool) used for visualization.


def get_engine():
    """Creates SQLAlchemy engine using credentials from .env file."""
    return create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )


def load_data(engine) -> dict:
    """
    Loads all three mart tables from PostgreSQL into DataFrames.
    Returns a dictionary with three DataFrames ready for visualization.
    """
    with engine.connect() as conn:

        df_genres = pd.read_sql(text("""
            SELECT genre, movie_count, avg_budget, avg_box_office,
                   avg_roi, total_box_office, revenue_rank
            FROM analytics.mart_genre_analysis
            ORDER BY revenue_rank
        """), conn)

        df_countries = pd.read_sql(text("""
            SELECT country, movie_count, avg_budget, avg_box_office,
                   avg_roi, total_box_office, revenue_rank
            FROM analytics.mart_country_analysis
            ORDER BY revenue_rank
        """), conn)

        df_movies = pd.read_sql(text("""
            SELECT title, release_year, director, country,
                   budget, box_office, roi, budget_category,
                   roi_category, genres
            FROM analytics.mart_top_movies
            WHERE box_office IS NOT NULL
            ORDER BY box_office DESC
        """), conn)

    print(f"Loaded: {len(df_genres)} genres, "
          f"{len(df_countries)} countries, "
          f"{len(df_movies)} movies")

    return {
        'genres': df_genres,
        'countries': df_countries,
        'movies': df_movies
    }


def plot_genre_revenue(df: pd.DataFrame) -> go.Figure:
    """
    Bar chart: Total box office revenue by genre (top 10).
    Answers: Which genres make the most money overall?
    """
    df_top10 = df.head(10).copy()
    df_top10['total_box_office_b'] = (
        df_top10['total_box_office'] / 1_000_000_000
    ).round(2)

    fig = px.bar(
        df_top10,
        x='genre',
        y='total_box_office_b',
        color='avg_roi',
        color_continuous_scale='viridis',
        title='Top 10 Genres by Total Box Office Revenue',
        labels={
            'genre': 'Genre',
            'total_box_office_b': 'Total Revenue (Billions $)',
            'avg_roi': 'Average ROI (%)'
        },
        text='total_box_office_b'
    )

    fig.update_traces(texttemplate='$%{text}B', textposition='outside')
    fig.update_layout(
        plot_bgcolor='white',
        showlegend=True,
        height=500
    )

    return fig


def plot_budget_vs_revenue(df: pd.DataFrame) -> go.Figure:
    """
    Scatter plot: Budget vs Box office with ROI as color.
    Answers: Is there a correlation between budget and revenue?
    This is the refactored version of the original MySQL analysis.
    """
    # size must be positive — use absolute ROI value
    df = df.copy()
    df['roi_abs'] = df['roi'].abs()
    
    fig = px.scatter(
        df,
        x='budget',
        y='box_office',
        color='roi_category',
        size='roi_abs',
        size_max=30,
        hover_name='title',
        hover_data=['director', 'release_year', 'country'],
        title='Budget vs Box Office Revenue by Film',
        labels={
            'budget': 'Budget ($)',
            'box_office': 'Box Office Revenue ($)',
            'roi_abs': 'ROI Absolute (%)'
        },
        color_discrete_map={
            'Loss': '#d62728',
            'Low Return': '#ff7f0e',
            'Good Return': '#2ca02c',
            'Exceptional Return': '#1f77b4'
        }
    )

    fig.update_layout(
        plot_bgcolor='white',
        height=600
    )

    return fig


def plot_top_movies(df: pd.DataFrame) -> go.Figure:
    """
    Horizontal bar chart: Top 10 movies by box office.
    Answers: Which movies made the most money?
    """
    df_top10 = df.head(10).copy()
    df_top10['box_office_b'] = (
        df_top10['box_office'] / 1_000_000_000
    ).round(2)

    fig = px.bar(
        df_top10,
        x='box_office_b',
        y='title',
        orientation='h',
        color='budget_category',
        title='Top 10 Movies by Box Office Revenue',
        labels={
            'box_office_b': 'Box Office Revenue (Billions $)',
            'title': 'Movie Title',
            'budget_category': 'Budget Category'
        },
        text='box_office_b'
    )

    fig.update_traces(texttemplate='$%{text}B', textposition='outside')
    fig.update_yaxes(autorange='reversed')
    fig.update_layout(
        plot_bgcolor='white',
        height=500
    )

    return fig


def plot_country_analysis(df: pd.DataFrame) -> go.Figure:
    """
    Grouped bar chart: Top 10 countries by average ROI and movie count.
    Answers: Which countries produce the most profitable movies?
    Filtered to countries with more than 3 movies to avoid misleading averages.
    """
    df_filtered = df[df['movie_count'] > 3].head(10).copy()
    df_filtered['avg_roi'] = df_filtered['avg_roi'].astype(float)
    df_filtered['avg_box_office_m'] = (
        df_filtered['avg_box_office'] / 1_000_000
    ).round(1)

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=(
            'Average ROI by Country (%)',
            'Average Box Office per Film (Millions $)'
        )
    )

    # Left chart — Average ROI
    fig.add_trace(
        go.Bar(
            x=df_filtered['country'],
            y=df_filtered['avg_roi'],
            name='Avg ROI (%)',
            marker_color='steelblue',
            text=df_filtered['avg_roi'].round(1),
            texttemplate='%{text}%',
            textposition='outside'
        ),
        row=1, col=1
    )

    # Right chart — Average box office per film
    fig.add_trace(
        go.Bar(
            x=df_filtered['country'],
            y=df_filtered['avg_box_office_m'],
            name='Avg Box Office (M$)',
            marker_color='seagreen',
            text=df_filtered['avg_box_office_m'],
            texttemplate='$%{text}M',
            textposition='outside'
        ),
        row=1, col=2
    )

    fig.update_layout(
        title='Top Countries: ROI and Average Box Office per Film',
        plot_bgcolor='white',
        height=500,
        showlegend=False
    )

    return fig


def plot_roi_by_genre(df: pd.DataFrame) -> go.Figure:
    """
    Bar chart: Average ROI by genre.
    Answers: Which genres give the best return on investment?
    """
    df_sorted = df.sort_values('avg_roi', ascending=True).copy()
    df_sorted['avg_roi'] = df_sorted['avg_roi'].astype(float)

    fig = px.bar(
        df_sorted,
        x='avg_roi',
        y='genre',
        orientation='h',
        color='avg_roi',
        color_continuous_scale='RdYlGn',
        title='Average ROI by Genre',
        labels={
            'avg_roi': 'Average ROI (%)',
            'genre': 'Genre'
        },
        text='avg_roi'
    )

    fig.update_traces(
        texttemplate='%{text:.1f}%',
        textposition='outside'
    )
    fig.update_layout(
        plot_bgcolor='white',
        height=700,
        showlegend=False
    )

    return fig

def export_all_html(data: dict, output_dir: str = 'docs') -> None:
    """
    Exports all figures as standalone HTML files.
    Each file contains the full interactive Plotly chart.
    Saved to docs/ folder for GitHub Pages hosting.
    """
    import os
    os.makedirs(output_dir, exist_ok=True)

    charts = {
        '01_genre_revenue': plot_genre_revenue(data['genres']),
        '02_budget_vs_revenue': plot_budget_vs_revenue(data['movies']),
        '03_top_movies': plot_top_movies(data['movies']),
        '04_country_analysis': plot_country_analysis(data['countries']),
        '05_roi_by_genre': plot_roi_by_genre(data['genres'])
    }

    for filename, fig in charts.items():
        filepath = os.path.join(output_dir, f'{filename}.html')
        fig.write_html(filepath)
        print(f"Exported: {filepath}")

    print(f"\nAll charts exported to {output_dir}/")


if __name__ == "__main__":
    engine = get_engine()
    data = load_data(engine)

    print("\nGenerating visualizations...")

    fig1 = plot_genre_revenue(data['genres'])
    fig1.show()

    fig2 = plot_budget_vs_revenue(data['movies'])
    fig2.show()

    fig3 = plot_top_movies(data['movies'])
    fig3.show()

    fig4 = plot_country_analysis(data['countries'])
    fig4.show()

    fig5 = plot_roi_by_genre(data['genres'])
    fig5.show()

    print("\nExporting HTML files...")
    export_all_html(data)

    print("\nAll visualizations generated and exported successfully!")
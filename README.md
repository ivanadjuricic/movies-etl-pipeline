# 🎬 Movies ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.13-blue?logo=python)
![Airflow](https://img.shields.io/badge/Airflow-2.8.1-017CEE?logo=apache-airflow)
![dbt](https://img.shields.io/badge/dbt-1.7.0-FF694B?logo=dbt)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![AWS](https://img.shields.io/badge/AWS-S3-FF9900?logo=amazon-aws)
![License](https://img.shields.io/badge/License-MIT-green)
![CI](https://github.com/ivanadjuricic/movies-etl-pipeline/actions/workflows/ci.yml/badge.svg)

End-to-end data engineering portfolio project — from raw CSV on AWS S3 to interactive analytics dashboard.

## 📊 Live Dashboard

👉 **[View Interactive Analytics Dashboard](https://ivanadjuricic.github.io/movies-etl-pipeline/)**

---

## 📌 Project Overview

A production-style ETL pipeline that extracts 543 films from AWS S3, transforms and cleans the data, loads it into PostgreSQL, runs dbt models for analytics, and visualizes insights through an interactive Plotly dashboard hosted on GitHub Pages.

The pipeline is fully orchestrated with Apache Airflow and includes CI/CD via GitHub Actions.

---

## 🏗️ Pipeline Architecture

```
AWS S3 (movies.csv)
        │
        ▼
   extract.py (boto3)
        │
        ▼
  transform.py (pandas)
        │
        ▼
load_postgres.py (SQLAlchemy)
        │
        ▼
PostgreSQL — staging tables
        │
        ▼
   dbt run (staging + marts)
        │
        ▼
  Snowflake (data warehouse)
        │
        ▼
 Plotly → GitHub Pages
```

**Apache Airflow orchestrates the entire pipeline** with monthly scheduling and retry logic.

---

## 🛠️ Tech Stack

| Tool | Role |
|------|------|
| **Python 3.13** | ETL logic — extract, transform, load |
| **AWS S3** | Raw data storage (movies.csv) |
| **Docker** | Containerization — PostgreSQL + Airflow |
| **PostgreSQL** | Staging database |
| **SQLAlchemy** | Database connection layer |
| **dbt** | SQL transformations — staging + marts |
| **Apache Airflow** | Pipeline orchestration and scheduling |
| **Snowflake** | Data warehouse (integration prepared) |
| **Plotly** | Interactive visualizations |
| **GitHub Pages** | Dashboard hosting |
| **GitHub Actions** | CI/CD — syntax checks on every push |

---

## 📁 Project Structure

```
movies-etl-pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml                  ← GitHub Actions CI
├── airflow/
│   └── dags/
│       └── movies_pipeline.py      ← Airflow DAG
├── etl/
│   ├── extract.py                  ← S3 → DataFrame
│   ├── transform.py                ← cleaning + normalization
│   ├── load_postgres.py            ← PostgreSQL staging tables
│   └── load_snowflake.py           ← Snowflake integration
├── movies_dbt/
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── stg_movies.sql
│       │   └── stg_genres.sql
│       └── marts/
│           ├── mart_genre_analysis.sql
│           ├── mart_top_movies.sql
│           └── mart_country_analysis.sql
├── analysis/
│   └── visualizations.py           ← Plotly charts
├── scripts/
│   ├── test_connection.py          ← PostgreSQL connection test
│   └── verify_postgres.py          ← data verification
├── docs/                           ← GitHub Pages
│   ├── index.html
│   ├── 01_genre_revenue.html
│   ├── 02_budget_vs_revenue.html
│   ├── 03_top_movies.html
│   ├── 04_country_analysis.html
│   ├── 05_roi_by_genre.html
│   └── screenshots/                ← 29 process screenshots
├── docker-compose.yml              ← PostgreSQL + Airflow
├── .env.example                    ← credentials template
└── requirements.txt
```

---

## 🔧 dbt Models

dbt transforms raw staging tables into analytics-ready mart tables using a two-layer architecture:

**Staging layer** — minimal cleaning, 1:1 with source tables:
- `stg_movies.sql` — cleans and standardizes movies data (nulls, data types, ROI calculation)
- `stg_genres.sql` — normalizes genres from a single column into one row per genre per film

**Marts layer** — business-ready analytics tables:
- `mart_genre_analysis.sql` — total box office revenue and average ROI per genre
- `mart_top_movies.sql` — top grossing films with budget category classification
- `mart_country_analysis.sql` — ROI and average box office revenue per country

Staging models are materialized as **views** (fast to create, no data copy), while mart models are materialized as **tables** (faster to query, used by Plotly visualizations).

---

## ❄️ Snowflake Integration

Snowflake is prepared as the final data warehouse destination. The `etl/load_snowflake.py` module implements the full load logic — creating tables, truncating existing data, and loading transformed DataFrames using the same SQLAlchemy pattern as PostgreSQL.

To activate Snowflake integration, add credentials to `.env` (see `.env.example`) and trigger the load:

```bash
python etl/load_snowflake.py
```

Snowflake was chosen as the target DW because it is the industry standard for cloud-native data warehousing — scalable, SQL-based, and widely used in Data Engineering roles.

---

## 📊 Dataset

- **Source:** AWS S3 bucket `movies-etl-pipeline-raw`
- **File:** `movies.csv`
- **Size:** 543 films, 1925–2023
- **Coverage:** 25 unique genres, 29 countries

---

## 🔍 Key Findings

**Top 10 Movies by Box Office Revenue**
- Avatar leads with $2.85B box office revenue
- Avengers: Endgame second with $2.8B
- All top 10 films are Blockbuster budget category
- Top 5: Avatar, Avengers: Endgame, Avatar: The Way of Water ($2.32B), Titanic ($2.19B), Star Wars: The Force Awakens ($2.07B)

**Top 10 Genres by Total Box Office Revenue**
- Adventure leads with $48.03B total revenue
- Action follows with $44.51B
- Drama third with $36.6B
- Thriller shows highest average ROI (~775%) despite lower total revenue ($6.28B)

**Budget vs Box Office**
- Films with lower budgets frequently achieve Exceptional Return
- Higher budgets do not guarantee proportionally higher revenue
- Most losses concentrated in mid-range budgets ($50M–$150M)

**ROI by Genre**
- Family: 1,275% ROI — broad audience + merchandising
- Horror: 995% ROI — low budget, high profit model
- Historical: -31% ROI — niche audience, high production cost

**Country Analysis**
- UK leads ROI at 898%
- New Zealand highest average box office per film at $757M
- USA dominates total revenue at $380M average per film

---

## 🚀 How to Run Locally

### Prerequisites
- Docker Desktop
- Python 3.10+
- AWS account with S3 access

### 1. Clone the repository
```bash
git clone https://github.com/ivanadjuricic/movies-etl-pipeline.git
cd movies-etl-pipeline
```

### 2. Set up environment
```bash
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 3. Configure credentials
```bash
cp .env.example .env
# Fill in your AWS and database credentials in .env
```

### 4. Start Docker services
```bash
docker-compose up -d
```

### 5. Access Airflow UI
```
http://localhost:8080
Username: admin
Password: admin
```

### 6. Trigger the pipeline
In Airflow UI, find `movies_etl_pipeline` and click the ▶ play button.

---

## 🔄 Airflow DAG

The pipeline runs on a `@monthly` schedule with the following tasks:

```
extract_from_s3 → transform_data → load_to_postgres → run_dbt_models
```

Each task has retry logic (1 retry, 5 minute delay) to handle transient failures.

---

## 🧪 CI/CD

GitHub Actions runs on every push to `main`:
- ✅ Python syntax check — `extract.py`, `transform.py`, `load_postgres.py`, `load_snowflake.py`
- ✅ dbt project structure validation

---

## 📸 Process Screenshots

Full documentation of the setup process is available in [`docs/screenshots/`](docs/screenshots/) — 29 screenshots covering Docker setup, AWS configuration, Airflow DAG execution, dbt models, and GitHub Actions CI.

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

## 👩‍💻 Author

**Ivana Đuričić**

[GitHub](https://github.com/ivanadjuricic) | [LinkedIn](https://www.linkedin.com/in/ivanadjuricicvisualdesign772025/)
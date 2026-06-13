# NewsFlow AI — Automated News Intelligence Pipeline

A production-grade data engineering pipeline that automatically fetches real-time AI news headlines, classifies sentiment using OpenAI GPT-4o-mini, and stores results in PostgreSQL — orchestrated hourly with Apache Airflow.

## Tech Stack
- Apache Airflow 3.x — pipeline orchestration & scheduling
- Python — pipeline logic
- OpenAI GPT-4o-mini — AI sentiment classification
- NewsAPI — real-time headline ingestion
- PostgreSQL 15 — structured data storage
- psycopg2 — database connector

## Pipeline Architecture
NewsAPI → fetch_headlines → GPT-4o-mini sentiment analysis → PostgreSQL

## DAG Tasks
1. create_table — ensures DB schema exists
2. fetch_headlines — pulls 10 latest AI headlines from NewsAPI
3. analyse_sentiment — classifies each as Positive / Negative / Neutral
4. store_results — persists all results to PostgreSQL

## Sample Output
| Headline | Sentiment |
|----------|-----------|
| Elon Musk becomes world's first trillionaire as SpaceX shares jump | Positive |
| AOL cofounder Steve Case on AI — major upside, real risk | Neutral |
| Oracle vs. IBM: 1 Legacy Tech Giant Is Winning the AI Race | Positive |

## Setup
export OPENAI_API_KEY=your_key
export NEWS_API_KEY=your_key
airflow dags trigger news_sentiment_pipeline

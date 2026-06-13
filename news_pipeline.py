import os
import sys
import requests
import psycopg2
from datetime import datetime, timedelta
from openai import OpenAI
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# ── Setup ──────────────────────────────────────────────
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
DB_URL = "dbname=newsflow user=apple host=localhost"

# ── Task 1: Create table if it doesn't exist ──────────
def create_table():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS headlines (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            source TEXT,
            url TEXT,
            sentiment TEXT,
            fetched_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Table ready.")

# ── Task 2: Fetch headlines ───────────────────────────
def fetch_headlines(**context):
    response = requests.get("https://newsapi.org/v2/everything", params={
        "q": "artificial intelligence",
        "language": "en",
        "pageSize": 10,
        "sortBy": "publishedAt",
        "apiKey": NEWS_API_KEY
    })
    articles = response.json().get("articles", [])
    headlines = [
        {"title": a["title"], "source": a["source"]["name"], "url": a["url"]}
        for a in articles
        if a.get("title") and a["title"] != "[Removed]"
    ]
    context["ti"].xcom_push(key="headlines", value=headlines)
    print(f"Fetched {len(headlines)} headlines.")

# ── Task 3: Analyse sentiment ─────────────────────────
def analyse_sentiment(**context):
    headlines = context["ti"].xcom_pull(key="headlines", task_ids="fetch_headlines")
    results = []
    for h in headlines:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Respond with exactly one word: Positive, Negative, or Neutral."},
                {"role": "user", "content": f"Sentiment of: {h['title']}"}
            ]
        )
        sentiment = response.choices[0].message.content.strip()
        results.append({**h, "sentiment": sentiment})
        print(f"{sentiment}: {h['title'][:50]}")
    context["ti"].xcom_push(key="results", value=results)

# ── Task 4: Store in PostgreSQL ───────────────────────
def store_results(**context):
    results = context["ti"].xcom_pull(key="results", task_ids="analyse_sentiment")
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    for r in results:
        cur.execute("""
            INSERT INTO headlines (title, source, url, sentiment)
            VALUES (%s, %s, %s, %s)
        """, (r["title"], r["source"], r["url"], r["sentiment"]))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Stored {len(results)} records to PostgreSQL.")

# ── DAG Definition ────────────────────────────────────
default_args = {
    "owner": "tom",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="news_sentiment_pipeline",
    default_args=default_args,
    description="Fetch AI news, analyse sentiment, store in PostgreSQL",
    schedule="@hourly",
    start_date=datetime(2026, 6, 13),
    catchup=False,
    tags=["ai", "nlp", "data-engineering"],
) as dag:

    t1 = PythonOperator(task_id="create_table", python_callable=create_table)
    t2 = PythonOperator(task_id="fetch_headlines", python_callable=fetch_headlines)
    t3 = PythonOperator(task_id="analyse_sentiment", python_callable=analyse_sentiment)
    t4 = PythonOperator(task_id="store_results", python_callable=store_results)

    t1 >> t2 >> t3 >> t4
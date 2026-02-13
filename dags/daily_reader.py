import hashlib
import pendulum
import pandas as pd
import requests
import newspaper
import time
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# 1. SETUP & TIMEZONE
LOCAL_TZ = "America/Chicago"

# 2. SOURCE LIST
SOURCES_LIST = ['abc-news-au', 'al-jazeera-english',
                'associated-press', 'bbc-news', 
                'breitbart-news', 
                'business-insider', 'buzzfeed', 'cbc-news', 'cbs-news', 'financial-post', 'fortune', 
                'fox-news', 'fox-sports', 'hacker-news', 
                'nbc-news', 
                 'rte',  'techradar', 
                'the-times-of-india', 
                'the-verge', 'usa-today']

def get_browser_config():
    config = newspaper.Config()
    config.browser_user_agent = (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
    )
    config.request_timeout = 15
    config.fetch_images = False
    config.memoize_articles = False
    return config

def fetch_and_store_news(**context):
    # Retrieve configuration using BaseHook
    api_key = Variable.get("newsroom_api_key")
    db_conn_id = 'my_postgres_conn'
    
    conn = BaseHook.get_connection(db_conn_id)
    db_uri = conn.get_uri().replace("postgres://", "postgresql://", 1)
    engine = create_engine(db_uri)
    
    # Calculate target date (yesterday)
    target_date = pendulum.now(LOCAL_TZ).subtract(days=1).to_date_string()
    user_config = get_browser_config()

    for source in SOURCES_LIST:
        print(f"--- Processing Source: {source} ---")
        
        api_url = "https://newsapi.org/v2/everything"
        params = {
            'sources': source,
            'from': target_date,
            'to': target_date,
            'sortBy': 'popularity',
            'pageSize': 100,
            'apiKey': api_key
        }
        
        try:
            response = requests.get(api_url, params=params)
            res = response.json()
            if res.get('status') != 'ok':
                print(f"Error for {source}: {res.get('message')}")
                continue
                
            articles = res.get('articles', [])
            batch_data = []

            for art in articles:
                print(f'Reading article: {art["title"]}')
                article_url = art.get('url')
                if not article_url: continue

                try:
                    article_id = hashlib.md5(str(article_url).encode()).hexdigest()[:16]
                    source_name = art.get('source', {}).get('name', 'unknown')
                    safe_source_name = str(source_name).strip().lower()
                    source_id_hash = hashlib.md5(safe_source_name.encode()).hexdigest()[:16]

                    time.sleep(random.uniform(1.0, 2.0)) # Stealth delay
                    
                    scraper = newspaper.Article(article_url, config=user_config)
                    scraper.download()
                    scraper.parse()
                    
                    full_text = scraper.text or ""

                    if full_text and "consent" in full_text.lower() and len(full_text) < 500:
                        full_text = "Blocked by Consent Wall"

                    batch_data.append({
                        'article_id': article_id,
                        'source_id': source_id_hash,
                        'source_name': source_name,
                        'author': ", ".join(scraper.authors) if scraper.authors else art.get('author'),
                        'title': art['title'],
                        'url': article_url,
                        'full_content': full_text,
                        'publish_date': art.get('publishedAt')
                    })
                    
                except Exception as e:
                    print(f"  !! Skipping {article_url}: {e}")
                    continue

            if batch_data:
                df = pd.DataFrame(batch_data)
                df['publish_date'] = pd.to_datetime(df['publish_date']).dt.date
                
                with engine.begin() as sql_conn:
                    df.to_sql('stg_news_articles', sql_conn, if_exists='replace', index=False)
                    sql_conn.execute(text("""
                        INSERT INTO article_data (article_id, source_id, source_name, author, title, url, full_content, publish_date)
                        SELECT article_id, source_id, source_name, author, title, url, full_content, publish_date
                        FROM stg_news_articles
                        ON CONFLICT DO NOTHING;
                    """))
                    sql_conn.execute(text("DROP TABLE stg_news_articles;"))
                print(f"--- Saved {len(batch_data)} articles for {source} ---")

        except Exception as e:
            print(f"Critical API Error for {source}: {e}")

# --- DAG DEFINITION (Aligned with your working example) ---
with DAG(
    dag_id='news_ingestion_v2', 
    start_date=datetime(2024, 1, 1), 
    schedule=None, 
    catchup=False,
) as dag:

    fetch_news = PythonOperator(
        task_id='fetch_stratified_news', 
        python_callable=fetch_and_store_news,
        execution_timeout=timedelta(hours=6)
    )
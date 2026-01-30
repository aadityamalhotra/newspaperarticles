import requests
import pandas as pd
import hashlib
import urllib.parse
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import Variable
from airflow.sdk.bases.hook import BaseHook
from newspaper import Article, Config
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# --- CONFIGURATION ---
DB_CONN_ID = 'my_postgres_conn'

# MAPPING: Country Code -> Search Query
# We use explicit country names to search the 'everything' endpoint
# This guarantees data even if the 'top-headlines' feed for that region is empty.
COUNTRY_QUERIES = {
    'cn': 'China',
    'ru': 'Russia',
    'in': 'India',
    'gb': 'United Kingdom',
    'de': 'Germany',
    'fr': 'France',
    'jp': 'Japan',
    'sa': 'Saudi Arabia',
    'ua': 'Ukraine',
    'il': 'Israel'
}

def fetch_stratified_news(**context):
    """
    Fetches news using a Robust Strategy:
    1. Global Headlines (The "Front Page" of the world)
    2. Global Business (Economic shifts)
    3. Country Deep-Dives (Using 'everything' endpoint to guarantee coverage)
    """
    
    api_key = Variable.get("newsroom_api_key")
    conn = BaseHook.get_connection(DB_CONN_ID)
    db_uri = conn.get_uri().replace("postgres://", "postgresql://", 1)
    
    # Newspaper3k Config
    user_config = Config()
    user_config.browser_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
    user_config.request_timeout = 10
    
    # Calculate Date Range (Last 48 hours to keep it relevant)
    from_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    
    fetch_targets = []
    
    # --- BUCKET 1: Global Headlines (High Quality, Low Volume) ---
    categories = ['general', 'business', 'technology']
    for cat in categories:
        fetch_targets.append({
            "url": f"https://newsapi.org/v2/top-headlines?category={cat}&language=en&pageSize=100&page=1&apiKey={api_key}",
            "type": f"Global {cat.capitalize()} Headlines"
        })

    # --- BUCKET 2: Country Specific (High Volume, 'Everything' Endpoint) ---
    # We search for the country name + 'politics' to filter noise
    for code, country_name in COUNTRY_QUERIES.items():
        # Query: (China AND (politics OR economy OR military))
        query = f'"{country_name}" AND (politics OR economy OR military)'
        encoded_query = urllib.parse.quote(query)
        
        fetch_targets.append({
            "url": (
                f"https://newsapi.org/v2/everything?"
                f"q={encoded_query}&"
                f"language=en&"
                f"from={from_date}&"
                f"sortBy=publishedAt&" # Get the newest stuff
                f"pageSize=50&"        # Limit to 50 to save processing time
                f"apiKey={api_key}"
            ),
            "type": f"Deep Dive: {country_name}"
        })

    print(f"--- PLAN GENERATED: {len(fetch_targets)} API Requests queued. ---")

    # --- EXECUTION ---
    all_articles = []
    total_requests = len(fetch_targets)

    for i, target in enumerate(fetch_targets):
        print(f"\n[Request {i+1}/{total_requests}] Fetching {target['type']}...")
        
        try:
            response = requests.get(target['url'])
            data = response.json()
            
            if data.get('status') == 'error':
                print(f"  !! API Error: {data.get('message')}")
                continue
                
            articles = data.get('articles', [])
            if not articles:
                print("  -> No articles returned for this target.")
                continue
            
            print(f"  -> API returned {len(articles)} articles. Processing...")

            new_count = 0
            dup_count = 0
            
            for art in articles:
                article_url = art.get('url')
                if not article_url: continue

                # Generate ID
                article_id = hashlib.md5(str(article_url).encode()).hexdigest()[:16]

                # Check Duplicates (In-Memory)
                if any(x['article_id'] == article_id for x in all_articles):
                    dup_count += 1
                    continue

                # Scrape Content
                article_scraper = Article(article_url, config=user_config)
                try:
                    article_scraper.download()
                    article_scraper.parse()
                    full_text = article_scraper.text
                    
                    if "consent" in full_text.lower() and len(full_text) < 500:
                        full_text = "Blocked by Consent Wall"
                except:
                    full_text = "Could not retrieve content"

                all_articles.append({
                    'article_id': article_id,
                    'source_name': art.get('source', {}).get('name'),
                    'author': art.get('author'),
                    'title': art.get('title'),
                    'url': article_url,
                    'full_content': full_text,
                    'published_at': art.get('publishedAt'),
                    'content_snippet': art.get('description'),
                    'extracted_at': datetime.now()
                })
                new_count += 1
            
            print(f"  -> Scraped {new_count} new articles. (Skipped {dup_count} duplicates)")

        except Exception as e:
            print(f"  !! Critical Request Failure: {e}")

    # --- DATABASE UPLOAD ---
    if all_articles:
        print(f"\n--- PROCESSING COMPLETE. Preparing {len(all_articles)} articles for DB upload... ---")
        
        df = pd.DataFrame(all_articles)
        df['published_at'] = pd.to_datetime(df['published_at'])
        df['extracted_at'] = pd.to_datetime(df['extracted_at'])
        df = df.drop_duplicates(subset=['article_id'])
        
        engine = create_engine(db_uri)

        setup_query = text("""
            CREATE TABLE IF NOT EXISTS news_articles (
                article_id VARCHAR(16) PRIMARY KEY,
                source_name TEXT,
                author TEXT,
                title TEXT,
                url TEXT,
                full_content TEXT,
                published_at TIMESTAMP,
                content_snippet TEXT,
                extracted_at TIMESTAMP
            );
        """)

        try:
            with engine.begin() as conn:
                conn.execute(setup_query)
                df.to_sql('stg_news_articles', engine, if_exists='replace', index=False, method='multi', chunksize=100)
                
                upsert_query = text("""
                    INSERT INTO news_articles 
                    SELECT * FROM stg_news_articles
                    ON CONFLICT (article_id) DO NOTHING;
                """)
                conn.execute(upsert_query)
                conn.execute(text("DROP TABLE stg_news_articles;"))
                
            print(f"--- SUCCESS: Ingestion complete. DB is up to date. ---")
            
        except Exception as e:
            print(f"!!! DATABASE ERROR: {e}")
            raise
    else:
        print("No articles were found or scraped. Database upload skipped.")


# --- DAG DEFINITION ---
with DAG(
    dag_id='news_collector_v1',
    start_date=datetime(2024, 1, 1),
    schedule=None, 
    catchup=False
) as dag:

    fetch_news = PythonOperator(
        task_id='fetch_stratified_news',
        python_callable=fetch_stratified_news
    )

    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_refinery',
        trigger_dag_id='data_refinery_v1',
        wait_for_completion=False 
    )

    fetch_news >> trigger_silver
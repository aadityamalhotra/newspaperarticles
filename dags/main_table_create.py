import requests
import pandas as pd
import hashlib
import urllib.parse
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
COUNTRY_QUERIES = {
    'cn': 'China', 'ru': 'Russia', 'in': 'India', 'gb': 'United Kingdom', 'de': 'Germany',
    'fr': 'France', 'jp': 'Japan', 'sa': 'Saudi Arabia', 'ua': 'Ukraine', 'il': 'Israel',
    'ir': 'Iran', 'tr': 'Turkey', 'br': 'Brazil', 'kr': 'South Korea', 'tw': 'Taiwan'
}

GLOBAL_TOPICS = ['Energy', 'Defense', 'Intelligence']

def fetch_stratified_news(**context):
    api_key = Variable.get("newsroom_api_key")
    conn = BaseHook.get_connection(DB_CONN_ID)
    db_uri = conn.get_uri().replace("postgres://", "postgresql://", 1)
    
    # Newspaper3k Config
    user_config = Config()
    user_config.browser_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
    user_config.request_timeout = 10
    
    # Date Range: Last 48 hours
    from_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    
    fetch_targets = []
    
    # 1. Official Categories
    categories = ['general', 'business', 'technology', 'science', 'health']
    for cat in categories:
        fetch_targets.append({
            "url": f"https://newsapi.org/v2/top-headlines?category={cat}&language=en&pageSize=100&page=1&apiKey={api_key}",
            "type": f"Global {cat.capitalize()}"
        })

    # 2. Custom Topics
    for topic in GLOBAL_TOPICS:
        query = urllib.parse.quote(f'"{topic}" AND (politics OR economy OR policy)')
        fetch_targets.append({
             "url": f"https://newsapi.org/v2/everything?q={query}&language=en&from={from_date}&sortBy=publishedAt&pageSize=50&apiKey={api_key}",
            "type": f"Topic: {topic}"
        })

    # 3. Country Deep-Dives
    for code, country_name in COUNTRY_QUERIES.items():
        query = urllib.parse.quote(f'"{country_name}" AND (politics OR economy OR military)')
        fetch_targets.append({
            "url": f"https://newsapi.org/v2/everything?q={query}&language=en&from={from_date}&sortBy=publishedAt&pageSize=50&apiKey={api_key}",
            "type": f"Country: {country_name}"
        })

    print(f"--- PLAN GENERATED: {len(fetch_targets)} API Requests queued. ---")

    all_articles = []
    
    for i, target in enumerate(fetch_targets):
        try:
            response = requests.get(target['url'])
            data = response.json()
            articles = data.get('articles', [])
            
            if not articles: continue

            print(f"  -> {target['type']}: Found {len(articles)} articles.")

            for art in articles:
                article_url = art.get('url')
                source_name = art.get('source', {}).get('name')
                
                if not article_url: continue

                # --- ID GENERATION (Consistent Hashing) ---
                # 1. Article ID: MD5 of URL
                article_id = hashlib.md5(str(article_url).encode()).hexdigest()[:16]

                # 2. Source ID: MD5 of Source Name
                # Handle None/Empty source names by defaulting to 'unknown'
                safe_source_name = str(source_name).strip().lower() if source_name else "unknown"
                source_id = hashlib.md5(safe_source_name.encode()).hexdigest()[:16]

                # Deduplication check
                if any(x['article_id'] == article_id for x in all_articles): continue

                # Scrape Content
                full_text = "Could not retrieve content"
                try:
                    article_scraper = Article(article_url, config=user_config)
                    article_scraper.download()
                    article_scraper.parse()
                    full_text = article_scraper.text
                    if "consent" in full_text.lower() and len(full_text) < 500:
                        full_text = "Blocked by Consent Wall"
                except:
                    pass

                all_articles.append({
                    'article_id': article_id,
                    'source_id': source_id,  # <--- NEW COLUMN
                    'source_name': source_name,
                    'author': art.get('author'),
                    'title': art.get('title'),
                    'url': article_url,
                    'full_content': full_text,
                    'published_at': art.get('publishedAt')
                    # REMOVED: extracted_at, content_snippet
                })

        except Exception as e:
            print(f"  !! Request Failed: {e}")

    # --- DATABASE UPLOAD ---
    if all_articles:
        print(f"\n--- UPLOADING {len(all_articles)} ARTICLES ---")
        
        df = pd.DataFrame(all_articles)
        df['published_at'] = pd.to_datetime(df['published_at'])
        df = df.drop_duplicates(subset=['article_id'])
        
        engine = create_engine(db_uri)

        # Updated Schema Definition
        setup_query = text("""
            CREATE TABLE IF NOT EXISTS main_table (
                article_id VARCHAR(16) PRIMARY KEY,
                source_id VARCHAR(16),
                source_name TEXT,
                author TEXT,
                title TEXT,
                url TEXT,
                full_content TEXT,
                published_at TIMESTAMP
            );
        """)

        with engine.begin() as conn:
            conn.execute(setup_query)
            df.to_sql('stg_news_articles', engine, if_exists='replace', index=False, method='multi', chunksize=100)
            
            # Upsert Logic
            conn.execute(text("""
                INSERT INTO main_table (article_id, source_id, source_name, author, title, url, full_content, published_at)
                SELECT article_id, source_id, source_name, author, title, url, full_content, published_at
                FROM stg_news_articles
                ON CONFLICT (article_id) DO NOTHING;
            """))
            conn.execute(text("DROP TABLE stg_news_articles;"))
            
        print("--- SUCCESS ---")

# --- DAG DEFINITION ---
with DAG(dag_id='create_main_table', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    fetch_news = PythonOperator(task_id='fetch_stratified_news', python_callable=fetch_stratified_news)
    trigger_silver = TriggerDagRunOperator(task_id='trigger_silver', trigger_dag_id='data_refinery_v1')
    fetch_news >> trigger_silver
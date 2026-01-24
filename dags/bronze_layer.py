# importing necessary libraries
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import Variable
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timedelta
from newspaper import Article, Config
import hashlib
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# specifications
DB_CONN_ID = 'my_postgres_conn'
SOURCES = "bbc-news"

# function that loads and stores data
def fetch_news_with_pagination(**context):

    # api connection, database connection
    api_key = Variable.get("newsroom_api_key")
    conn = BaseHook.get_connection(DB_CONN_ID)
    db_uri = conn.get_uri().replace("postgres://", "postgresql://", 1)

    # get a range of the last 10 days
    # TODO: need to find a way to automatically get the last 10 days
    start_dt = datetime(2026, 1, 14)
    end_dt = datetime.now()
    date_list = [(start_dt + timedelta(days=x)).strftime('%Y-%m-%d') 
                 for x in range((end_dt - start_dt).days + 1)]

    # list to store all articles
    all_articles = []

    # scraper configuration
    user_config = Config()
    user_config.browser_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
    user_config.request_timeout = 10

    # loop for each date
    for target_date in date_list:
        print(f"--- Fetching news for date: {target_date} ---")
        
        # url for exact day
        url = (
            f"https://newsapi.org/v2/everything?"
            f"sources={SOURCES}&"
            f"from={target_date}&"
            f"to={target_date}&"
            f"pageSize=100&apiKey={api_key}"
        )
        
        # get the articles via web response
        response = requests.get(url)
        data = response.json()
        articles = data.get('articles', [])

        # handling empty articles
        if not articles:
            print(f"No articles found for {target_date}. Skipping...")
            continue
            
        # looping over articles
        for art in articles:
            article_url = art.get('url')

            # generate unique article id from article url
            article_id = hashlib.md5(str(article_url).encode()).hexdigest()[:16]
            article_scraper = Article(article_url, config=user_config)
            
            try:
                article_scraper.download()
                article_scraper.parse()

                # scraping article text
                full_text = article_scraper.text

                # checking if article blocked
                if "consent" in full_text.lower() and len(full_text) < 500:
                    full_text = "Blocked by Consent Wall"

            # failed to retrieve article
            except:
                full_text = "Could not retrieve content"

            # TODO: ensure that table created has primary key

            # appending article data in json format
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

    # loading the articles
    if all_articles:

        # convert into df
        df = pd.DataFrame(all_articles)

        # ensuring columns are actual datatime type
        df['published_at'] = pd.to_datetime(df['published_at'])
        df['extracted_at'] = pd.to_datetime(df['extracted_at'])

        # dropping duplicate articles
        df = df.drop_duplicates(subset=['article_id'])
        
        # conencting to db
        engine = create_engine(db_uri)
        
        # loading data into temporary staging table
        df.to_sql('stg_news_articles', engine, if_exists='replace', index=False)
        
        # moving data into main news_articles table ignoring duplicates
        upsert_query = text("""
            INSERT INTO news_articles 
            SELECT * FROM stg_news_articles
            ON CONFLICT (article_id) DO NOTHING;
        """)
        
        # executing the query
        with engine.begin() as conn:
            conn.execute(upsert_query)
            
            # dropping the staging table
            conn.execute(text("DROP TABLE stg_news_articles;"))
            
        print(f"Ingestion complete. Handled {len(df)} potential articles.")


# DAG DEFINITION
with DAG(
    dag_id='news_collector_v1',
    start_date=datetime(2024, 1, 1),
    schedule='@daily', 
    catchup=False
) as dag:

    fetch_news = PythonOperator(
        task_id='fetch_news_pages',
        python_callable=fetch_news_with_pagination
    )

    # triggering the silver layer code
    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_refinery',
        trigger_dag_id='data_refinery_v1',
        wait_for_completion=False
    )


    fetch_news >> trigger_silver
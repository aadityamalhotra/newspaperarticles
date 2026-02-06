import time
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable 

# --- CONFIGURATION ---
DB_CONN_ID = 'moltbook_conn'
SUBMOLT = "general"
POSTS_TABLE = f"data_{SUBMOLT}_posts"
COMMENTS_TABLE = f"data_{SUBMOLT}_comments"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def flatten_comments(comment_list, post_id, parent_id="POST"):
    """
    Recursively drills into the Moltbook nested JSON comment tree.
    Extracts author name and the newly added 'content' field.
    """
    flat_list = []
    for c in comment_list:
        author_data = c.get('author', {})
        if isinstance(author_data, dict):
            author_name = author_data.get('name', 'anonymous')
        else:
            author_name = str(author_data)

        # Structure: (comment_id, post_id, upvotes, downvotes, poster_name, parent_id, content)
        flat_list.append((
            c.get('id'), 
            post_id, 
            c.get('upvotes', 0), 
            c.get('downvotes', 0), 
            author_name, 
            parent_id,
            c.get('content', '') # Added content extraction
        ))
        
        if c.get('replies') and isinstance(c['replies'], list):
            flat_list.extend(flatten_comments(c['replies'], post_id, c['id']))
            
    return flat_list

with DAG(
    dag_id=f'moltbook_{SUBMOLT}_pipeline',
    default_args=default_args,
    description=f'Full depth ETL pipeline for Moltbook {SUBMOLT}',
    schedule=None, # Updated from schedule_interval
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['moltbook', 'production']
) as dag:

    @task()
    def initialize_database():
        pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
        pg_hook.run(f"""
            CREATE TABLE IF NOT EXISTS {POSTS_TABLE} (
                post_id TEXT PRIMARY KEY,
                title TEXT,
                content TEXT,
                upvotes INTEGER,
                downvotes INTEGER,
                comment_count INTEGER,
                poster_name TEXT
            );
            CREATE TABLE IF NOT EXISTS {COMMENTS_TABLE} (
                comment_id TEXT PRIMARY KEY,
                post_id TEXT REFERENCES {POSTS_TABLE}(post_id) ON DELETE CASCADE,
                upvotes INTEGER,
                downvotes INTEGER,
                poster_name TEXT,
                parent_id TEXT,
                content TEXT
            );
        """)

    @task()
    def run_ingestion():
        api_key = Variable.get("moltbook_api_key")
        pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
        headers = {"Authorization": f"Bearer {api_key}"}
        
        offset = 0
        limit = 100
        keep_running = True

        

        while keep_running:
            # 1. Fetch posts with pagination (offset logic)
            url = f"https://www.moltbook.com/api/v1/posts?submolt={SUBMOLT}&limit={limit}&offset={offset}"
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            posts = resp.json().get('posts', [])

            # If no posts are returned, we have hit the end of the submolt
            if not posts:
                print(f"No more posts found. Total posts processed: {offset}")
                keep_running = False
                break

            for p in posts:
                # Extract author name
                author_obj = p.get('author', {})
                author_name = author_obj.get('name', 'anonymous') if isinstance(author_obj, dict) else str(author_obj)

                # Upsert Post
                upsert_post_sql = f"""
                    INSERT INTO {POSTS_TABLE} (post_id, title, content, upvotes, downvotes, comment_count, poster_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (post_id) DO UPDATE SET
                        upvotes = EXCLUDED.upvotes,
                        downvotes = EXCLUDED.downvotes,
                        comment_count = EXCLUDED.comment_count;
                """
                pg_hook.run(upsert_post_sql, parameters=(
                    p['id'], p['title'], p['content'], p['upvotes'], p['downvotes'], p['comment_count'], author_name
                ))

                # 2. Fetch Comments for each post
                c_url = f"https://www.moltbook.com/api/v1/posts/{p['id']}/comments"
                c_resp = requests.get(c_url, headers=headers)
                
                if c_resp.status_code == 200:
                    tree = c_resp.json().get('comments', [])
                    flat_comments = flatten_comments(tree, p['id'])
                    
                    for c_vals in flat_comments:
                        # Upsert Comment (including the new content column)
                        upsert_comm_sql = f"""
                            INSERT INTO {COMMENTS_TABLE} (comment_id, post_id, upvotes, downvotes, poster_name, parent_id, content)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (comment_id) DO UPDATE SET
                                upvotes = EXCLUDED.upvotes,
                                downvotes = EXCLUDED.downvotes,
                                content = EXCLUDED.content;
                        """
                        pg_hook.run(upsert_comm_sql, parameters=c_vals)
                
                time.sleep(0.6) # Respect rate limits

            # Increment offset for the next batch of 100
            offset += limit
            print(f"Batch complete. Moving to offset {offset}...")

    initialize_database() >> run_ingestion()
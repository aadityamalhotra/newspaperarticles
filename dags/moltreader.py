import time
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable 

# --- CONFIGURATION ---
DB_CONN_ID = 'moltbook_conn'
# Add any submolts here; the code will create tables for each automatically
SUBMOLTS_LIST = ["general", "tech", "finance"] 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def flatten_comments(comment_list, post_id, parent_id="POST"):
    flat_list = []
    for c in comment_list:
        author_data = c.get('author', {})
        author_name = author_data.get('name', 'anonymous') if isinstance(author_data, dict) else str(author_data)

        flat_list.append((
            c.get('id'), post_id, c.get('upvotes', 0), c.get('downvotes', 0), 
            author_name, parent_id, c.get('content', '')
        ))
        if c.get('replies') and isinstance(c['replies'], list):
            flat_list.extend(flatten_comments(c['replies'], post_id, c['id']))
    return flat_list

with DAG(
    dag_id='moltbook_multi_submolt_final',
    default_args=default_args,
    description='Fixed ETL with explicit string casting for offsets',
    schedule=None,
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['moltbook', 'production']
) as dag:

    @task()
    def initialize_database(submolt_name):
        pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
        post_table = f"data_{submolt_name}_posts"
        comm_table = f"data_{submolt_name}_comments"
        
        pg_hook.run(f"""
            CREATE TABLE IF NOT EXISTS {post_table} (
                post_id TEXT PRIMARY KEY, title TEXT, content TEXT, 
                upvotes INTEGER, downvotes INTEGER, comment_count INTEGER, poster_name TEXT
            );
            CREATE TABLE IF NOT EXISTS {comm_table} (
                comment_id TEXT PRIMARY KEY, 
                post_id TEXT REFERENCES {post_table}(post_id) ON DELETE CASCADE,
                upvotes INTEGER, downvotes INTEGER, poster_name TEXT, parent_id TEXT, content TEXT
            );
        """)
        return submolt_name

    @task()
    def run_ingestion(submolt_name):
        api_key = Variable.get("moltbook_api_key")
        
        # 1. Get current offset as string, then convert to int for math
        # We wrap the getter in str() just in case the DB has a weird type stored
        raw_val = Variable.get(f"moltbook_offset_{submolt_name}", default_var="0")
        current_offset = int(str(raw_val))
        
        pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
        headers = {"Authorization": f"Bearer {api_key}"}
        limit = 100
        
        post_table = f"data_{submolt_name}_posts"
        comm_table = f"data_{submolt_name}_comments"

        print(f"--- STARTING {submolt_name} AT OFFSET {current_offset} ---")

        while True:
            url = f"https://www.moltbook.com/api/v1/posts?submolt={submolt_name}&limit={limit}&offset={current_offset}"
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            
            posts = resp.json().get('posts', [])

            if not posts:
                print(f"Finished submolt: {submolt_name}")
                break

            for p in posts:
                author_obj = p.get('author', {})
                author_name = author_obj.get('name', 'anonymous') if isinstance(author_obj, dict) else str(author_obj)

                # Upsert Post
                pg_hook.run(f"""
                    INSERT INTO {post_table} (post_id, title, content, upvotes, downvotes, comment_count, poster_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (post_id) DO UPDATE SET 
                    upvotes = EXCLUDED.upvotes, downvotes = EXCLUDED.downvotes, comment_count = EXCLUDED.comment_count;
                """, parameters=(p['id'], p['title'], p['content'], p['upvotes'], p['downvotes'], p['comment_count'], author_name))

                # Fetch and Upsert Comments
                c_url = f"https://www.moltbook.com/api/v1/posts/{p['id']}/comments"
                c_resp = requests.get(c_url, headers=headers)
                if c_resp.status_code == 200:
                    tree = c_resp.json().get('comments', [])
                    flat_comments = flatten_comments(tree, p['id'])
                    for c_vals in flat_comments:
                        pg_hook.run(f"""
                            INSERT INTO {comm_table} (comment_id, post_id, upvotes, downvotes, poster_name, parent_id, content)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (comment_id) DO UPDATE SET 
                            upvotes = EXCLUDED.upvotes, downvotes = EXCLUDED.downvotes, content = EXCLUDED.content;
                        """, parameters=c_vals)
                
                time.sleep(0.5) 

            # Update offset for next iteration
            current_offset += limit
            
            # 2. SAVE PROGRESS: Explicitly force the value to a string
            Variable.set(f"moltbook_offset_{submolt_name}", str(current_offset))
            print(f"Offset for {submolt_name} updated to {current_offset}")

    # Orchestration
    for sub in SUBMOLTS_LIST:
        init_db = initialize_database(sub)
        ingest_data = run_ingestion(sub)
        init_db >> ingest_data
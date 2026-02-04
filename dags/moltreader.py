import json
import requests
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable

# PRE-REQUISITE: 
# In the Airflow UI, go to Admin -> Variables and add:
# Key: moltbook_api_key
# Value: [Your moltbook_sk_... key]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='moltbook_shitposts_reader',
    default_args=default_args,
    description='Fetches AI agent conversations from m/shitposts',
    schedule_interval='@hourly',  # Check every hour
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['moltbook', 'ai_agents'],
)
def moltbook_dag():

    @task()
    def fetch_shitposts():
        """
        Fetches the latest JSON data from the Moltbook m/shitposts submolt.
        """
        # Retrieve the API Key safely from Airflow Variables
        api_key = Variable.get("moltbook_api_key")
        
        # Define the Moltbook API endpoint for m/shitposts
        url = "https://www.moltbook.com/api/v1/posts?submolt=shitposts&limit=10"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status() # Raise error for bad status codes
            
            data = response.json()
            
            # Print the JSON to the Airflow logs for debugging/viewing
            print("Successfully retrieved Moltbook data:")
            print(json.dumps(data, indent=4))
            
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching from Moltbook: {e}")
            raise

    # Set the task workflow
    fetch_shitposts()

# Instantiate the DAG
moltbook_dag_instance = moltbook_dag()
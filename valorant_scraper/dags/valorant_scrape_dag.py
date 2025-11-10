import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scrapers.tournaments_scraper import scrape_tournaments_data
from scrapers.tournaments_detail_scraper import scrape_tournaments_detail_data
from scrapers.tournaments_match_scraper import scrape_tournaments_match_data
from scrapers.tournaments_game_scraper import scrape_tournaments_game_data
from scrapers.tournaments_game_overview_scraper import scrape_tournaments_game_overview_data
from scrapers.tournaments_game_round_scraper import scrape_tournaments_game_round_data
from scrapers.tournaments_game_head2head_scraper import scrape_tournaments_game_head2head_data
from scrapers.tournaments_game_performance_scraper import scrape_tournaments_game_performance_data
from processors.aggregator import aggregate_all_data
from processors.feature_engineering import engineer_features

default_args = {
    'owner': 'valorant_analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'valorant_match_prediction_scraper',
    default_args=default_args,
    description='Scrape Valorant match data for prediction modeling',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['valorant', 'esports', 'scraping'],
)

# Task 1: Scrape tournaments data
scrape_tournaments_task = PythonOperator(
    task_id='scrape_tournaments_data',
    python_callable=scrape_tournaments_data,
    dag=dag,
)

# Task 2: Scrape tournaments detail data
scrape_tournaments_detail_task = PythonOperator(
    task_id='scrape_tournaments_detail_data',
    python_callable=scrape_tournaments_detail_data,
    dag=dag,
)

# Task 3: Scrape tournaments match summary data
scrape_tournaments_match_task = PythonOperator(
    task_id='scrape_tournaments_match_data',
    python_callable=scrape_tournaments_match_data,
    dag=dag,
)

# Task 4: Scrape tournaments game summary data
scrape_tournaments_game_task = PythonOperator(
    task_id='scrape_tournaments_game_data',
    python_callable=scrape_tournaments_game_data,
    dag=dag,
)

# Task 5: Scrape tournaments game overview data
scrape_tournaments_game_overview_task = PythonOperator(
    task_id='scrape_tournaments_game_overview_data',
    python_callable=scrape_tournaments_game_overview_data,
    dag=dag,
)

# Task 6: Scrape tournaments game round data
scrape_tournaments_game_round_task = PythonOperator(
    task_id='scrape_tournaments_game_round_data',
    python_callable=scrape_tournaments_game_round_data,
    dag=dag,
)

# Task 7: Scrape tournaments player head 2 head data
scrape_tournaments_game_head2head_task = PythonOperator(
    task_id='scrape_tournaments_game_head2head_data',
    python_callable=scrape_tournaments_game_head2head_data,
    dag=dag,
)

# Task 8: Scrape tournaments player performance data
scrape_tournaments_game_performance_task = PythonOperator(
    task_id='scrape_tournaments_game_performance_data',
    python_callable=scrape_tournaments_game_performance_data,
    dag=dag,
)

# Task 4: Aggregate all raw data
aggregate_data = PythonOperator(
    task_id='aggregate_raw_data',
    python_callable=aggregate_all_data,
    dag=dag,
)

# Task 5: Engineer features (decay, opponent-adj, etc.)
engineer_feat = PythonOperator(
    task_id='engineer_features',
    python_callable=engineer_features,
    dag=dag,
)

# Define dependencies
scrape_tournaments_task >> [scrape_tournaments_detail_task, scrape_tournaments_match_task] >> scrape_tournaments_game_task >> [scrape_tournaments_game_overview_task, scrape_tournaments_game_round_task, scrape_tournaments_game_head2head_task, scrape_tournaments_game_performance_task] >> aggregate_data >> engineer_feat
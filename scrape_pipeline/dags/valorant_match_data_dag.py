"""
Valorant Match Data Collection DAG
Handles comprehensive match data scraping including scores, player statistics,
map performance, round-by-round data, and all battle-tested predictive metrics.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

# Import our custom components
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from config.settings import settings
from config.scraping_config import scraping_config, DataType
from config.rate_limits import RateLimitedRequest
from plugins.operators.valorant_scraper_operator import ValorantScraperOperator
from plugins.operators.data_quality_operator import DataQualityOperator
from plugins.utils.scrapers.vlr_scraper import VLRScraper
from plugins.utils.scrapers.thespike_scraper import TheSpikeScraper
from plugins.utils.processors.match_processor import MatchProcessor
from plugins.utils.storage.database_manager import DatabaseManager
from plugins.utils.validators.data_quality import DataQualityValidator

# Set up logging
logger = logging.getLogger(__name__)

# DAG configuration
DAG_ID = 'valorant_match_data_pipeline'
SCHEDULE_INTERVAL = settings.pipeline.MATCH_DATA_SCHEDULE  # Every 2 hours
MAX_ACTIVE_RUNS = 3  # Allow multiple concurrent runs for live matches
CATCHUP = False

# Default arguments
default_args = {
    'owner': 'valorant-data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': settings.EMAIL_NOTIFICATIONS_ENABLED,
    'email_on_retry': False,
    'retries': settings.pipeline.DEFAULT_RETRIES,
    'retry_delay': timedelta(minutes=2),  # Shorter retry delay for match data
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=15),
    'execution_timeout': settings.pipeline.SCRAPING_TASK_TIMEOUT,
    'sla': timedelta(hours=2),  # Match data should be fresh within 2 hours
}

# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Valorant match data collection and processing pipeline',
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=MAX_ACTIVE_RUNS,
    catchup=CATCHUP,
    tags=['valorant', 'matches', 'live-data', 'stats'],
    doc_md=__doc__,
)


def identify_target_matches(**context) -> List[Dict[str, Any]]:
    """
    Identify matches to scrape based on collection plan and current priorities
    """
    logger.info("Identifying target matches for data collection...")
    
    # Get collection plan from DAG run configuration or XCom
    dag_run_conf = context.get('dag_run', {}).conf or {}
    collection_plan = dag_run_conf.get('collection_plan', {})
    
    target_matches = []
    
    try:
        db_manager = DatabaseManager()
        
        # Priority 1: Live and ongoing matches
        live_matches_query = """
            SELECT 
                m.match_id,
                m.vlr_match_id,
                m.thespike_match_id,
                m.event_name,
                m.team1_name,
                m.team2_name,
                m.match_date,
                m.match_status,
                m.bo_format,
                m.last_updated,
                EXTRACT(EPOCH FROM (NOW() - m.last_updated))/60 as minutes_since_update
            FROM matches m
            WHERE m.match_status IN ('live', 'ongoing', 'scheduled')
              AND m.match_date BETWEEN NOW() - INTERVAL '2 hours' AND NOW() + INTERVAL '12 hours'
            ORDER BY 
                CASE 
                    WHEN m.match_status = 'live' THEN 1
                    WHEN m.match_status = 'ongoing' THEN 2
                    ELSE 3
                END,
                m.match_date ASC
            LIMIT 20
        """
        
        # Priority 2: Recently completed matches needing updates
        recent_matches_query = """
            SELECT 
                m.match_id,
                m.vlr_match_id,
                m.thespike_match_id,
                m.event_name,
                m.team1_name,
                m.team2_name,
                m.match_date,
                m.match_status,
                m.bo_format,
                m.last_updated,
                EXTRACT(EPOCH FROM (NOW() - m.last_updated))/60 as minutes_since_update
            FROM matches m
            WHERE m.match_status = 'completed'
              AND m.match_date >= NOW() - INTERVAL '24 hours'
              AND (
                  m.last_updated IS NULL 
                  OR m.last_updated < NOW() - INTERVAL '2 hours'
                  OR NOT EXISTS (
                      SELECT 1 FROM match_maps mm WHERE mm.match_id = m.match_id
                  )
              )
            ORDER BY m.match_date DESC
            LIMIT 30
        """
        
        # Priority 3: High-profile matches from collection plan
        priority_matches = []
        if collection_plan and 'immediate_tasks' in collection_plan:
            for task in collection_plan['immediate_tasks']:
                if task['task_type'] in ['live_matches', 'recent_matches']:
                    # These are already covered by above queries
                    pass
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                # Get live matches
                cursor.execute(live_matches_query)
                live_results = cursor.fetchall()
                
                for row in live_results:
                    match_data = dict(row)
                    match_data['priority'] = 'high'
                    match_data['scraping_sources'] = ['vlr', 'thespike'] if settings.ENABLE_VLR_SCRAPING and settings.ENABLE_SPIKE_SCRAPING else ['vlr']
                    target_matches.append(match_data)
                
                # Get recent matches
                cursor.execute(recent_matches_query)
                recent_results = cursor.fetchall()
                
                for row in recent_results:
                    match_data = dict(row)
                    match_data['priority'] = 'medium'
                    match_data['scraping_sources'] = ['vlr', 'thespike'] if settings.ENABLE_VLR_SCRAPING and settings.ENABLE_SPIKE_SCRAPING else ['vlr']
                    target_matches.append(match_data)
        
        logger.info(f"Identified {len(target_matches)} matches for data collection:")
        logger.info(f"  - {len(live_results)} live/ongoing matches")
        logger.info(f"  - {len(recent_results)} recent completed matches")
        
        # Sort by priority and recency
        target_matches.sort(key=lambda x: (
            0 if x['priority'] == 'high' else 1,
            -x['match_date'].timestamp() if x['match_date'] else 0
        ))
        
    except Exception as e:
        logger.error(f"Error identifying target matches: {e}")
        # Return empty list on error - pipeline will skip gracefully
        target_matches = []
    
    return target_matches


def scrape_vlr_match_data(match_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Scrape detailed match data from VLR.gg
    """
    if not settings.ENABLE_VLR_SCRAPING or not match_info.get('vlr_match_id'):
        logger.info(f"Skipping VLR scraping for match {match_info.get('match_id')}")
        return None
    
    logger.info(f"Scraping VLR data for match: {match_info['team1_name']} vs {match_info['team2_name']}")
    
    try:
        scraper = VLRScraper()
        
        with RateLimitedRequest('vlr', timeout=30):
            # Build match URL
            match_url = scraping_config.build_url(
                'vlr', 
                DataType.MATCH, 
                match_id=match_info['vlr_match_id'],
                match_slug=f"{match_info['team1_name'].lower()}-vs-{match_info['team2_name'].lower()}"
            )
            
            if not match_url:
                logger.error(f"Could not build VLR URL for match {match_info['match_id']}")
                return None
            
            # Scrape the match page
            match_data = scraper.scrape_match_data(match_url, match_info['match_id'])
            
            if match_data:
                # Extract battle-tested metrics
                enhanced_data = scraper.extract_predictive_metrics(match_data)
                enhanced_data['source'] = 'vlr'
                enhanced_data['scraped_at'] = datetime.now()
                
                logger.info(f"Successfully scraped VLR data for match {match_info['match_id']}")
                return enhanced_data
            else:
                logger.warning(f"No data returned from VLR for match {match_info['match_id']}")
                return None
                
    except Exception as e:
        logger.error(f"VLR scraping failed for match {match_info['match_id']}: {e}")
        return None


def scrape_thespike_match_data(match_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Scrape match data from TheSpike.gg
    """
    if not settings.ENABLE_SPIKE_SCRAPING or not match_info.get('thespike_match_id'):
        logger.info(f"Skipping TheSpike scraping for match {match_info.get('match_id')}")
        return None
    
    logger.info(f"Scraping TheSpike data for match: {match_info['team1_name']} vs {match_info['team2_name']}")
    
    try:
        scraper = TheSpikeScraper()
        
        with RateLimitedRequest('thespike', timeout=30):
            # Build API URL
            api_url = scraping_config.build_url(
                'thespike',
                DataType.MATCH,
                match_id=match_info['thespike_match_id']
            )
            
            if not api_url:
                logger.error(f"Could not build TheSpike URL for match {match_info['match_id']}")
                return None
            
            # Scrape the match data
            match_data = scraper.scrape_match_data(api_url, match_info['match_id'])
            
            if match_data:
                match_data['source'] = 'thespike'
                match_data['scraped_at'] = datetime.now()
                
                logger.info(f"Successfully scraped TheSpike data for match {match_info['match_id']}")
                return match_data
            else:
                logger.warning(f"No data returned from TheSpike for match {match_info['match_id']}")
                return None
                
    except Exception as e:
        logger.error(f"TheSpike scraping failed for match {match_info['match_id']}: {e}")
        return None


def process_and_merge_match_data(**context) -> Dict[str, Any]:
    """
    Process and merge match data from different sources
    """
    task_instance = context['task_instance']
    
    # Get scraped data from upstream tasks
    vlr_data = task_instance.xcom_pull(task_ids='scraping_group.scrape_vlr_matches') or []
    thespike_data = task_instance.xcom_pull(task_ids='scraping_group.scrape_thespike_matches') or []
    
    if not vlr_data and not thespike_data:
        logger.warning("No match data to process")
        return {'processed_matches': 0, 'failed_matches': 0}
    
    logger.info(f"Processing {len(vlr_data)} VLR matches and {len(thespike_data)} TheSpike matches")
    
    processor = MatchProcessor()
    processing_results = {
        'processed_matches': 0,
        'failed_matches': 0,
        'updated_matches': 0,
        'new_matches': 0,
        'quality_issues': []
    }
    
    try:
        # Process VLR data
        for match_data in vlr_data:
            if not match_data:
                continue
                
            try:
                processed_data = processor.process_match_data(match_data, source='vlr')
                
                # Validate data quality
                validator = DataQualityValidator()
                quality_check = validator.validate_match_data(processed_data)
                
                if quality_check['is_valid']:
                    # Store in database
                    success = processor.store_match_data(processed_data)
                    if success:
                        processing_results['processed_matches'] += 1
                        if processed_data.get('is_new_record'):
                            processing_results['new_matches'] += 1
                        else:
                            processing_results['updated_matches'] += 1
                    else:
                        processing_results['failed_matches'] += 1
                else:
                    processing_results['quality_issues'].extend(quality_check['issues'])
                    processing_results['failed_matches'] += 1
                    logger.warning(f"Match data quality issues: {quality_check['issues']}")
                    
            except Exception as e:
                logger.error(f"Error processing VLR match data: {e}")
                processing_results['failed_matches'] += 1
        
        # Process TheSpike data
        for match_data in thespike_data:
            if not match_data:
                continue
                
            try:
                processed_data = processor.process_match_data(match_data, source='thespike')
                
                # Check if we already processed this match from VLR
                existing_match = processor.find_existing_match(processed_data['match_id'])
                
                if existing_match:
                    # Merge with existing data
                    merged_data = processor.merge_match_sources(existing_match, processed_data)
                    success = processor.store_match_data(merged_data)
                    if success:
                        processing_results['updated_matches'] += 1
                else:
                    # Validate and store new match
                    validator = DataQualityValidator()
                    quality_check = validator.validate_match_data(processed_data)
                    
                    if quality_check['is_valid']:
                        success = processor.store_match_data(processed_data)
                        if success:
                            processing_results['processed_matches'] += 1
                            processing_results['new_matches'] += 1
                        else:
                            processing_results['failed_matches'] += 1
                    else:
                        processing_results['quality_issues'].extend(quality_check['issues'])
                        processing_results['failed_matches'] += 1
                        
            except Exception as e:
                logger.error(f"Error processing TheSpike match data: {e}")
                processing_results['failed_matches'] += 1
        
        logger.info(f"Match processing complete: {processing_results}")
        
    except Exception as e:
        logger.error(f"Match data processing failed: {e}")
        processing_results['failed_matches'] = len(vlr_data) + len(thespike_data)
    
    return processing_results


def extract_battle_tested_metrics(**context) -> Dict[str, Any]:
    """
    Extract and calculate battle-tested predictive metrics from match data
    """
    logger.info("Extracting battle-tested predictive metrics...")
    
    processor = MatchProcessor()
    metrics_results = {
        'matches_analyzed': 0,
        'metrics_calculated': {},
        'quality_flags': []
    }
    
    try:
        db_manager = DatabaseManager()
        
        # Get recent matches that need metrics calculation
        recent_matches_query = """
            SELECT 
                m.match_id,
                m.team1_name,
                m.team2_name,
                m.match_date,
                m.bo_format,
                COUNT(mm.map_id) as map_count,
                COUNT(ps.player_id) as player_stats_count
            FROM matches m
            LEFT JOIN match_maps mm ON m.match_id = mm.match_id
            LEFT JOIN player_stats ps ON m.match_id = ps.match_id
            WHERE m.match_date >= NOW() - INTERVAL '48 hours'
              AND m.match_status = 'completed'
              AND (m.metrics_calculated = false OR m.metrics_calculated IS NULL)
            GROUP BY m.match_id, m.team1_name, m.team2_name, m.match_date, m.bo_format
            HAVING COUNT(mm.map_id) > 0 AND COUNT(ps.player_id) >= 10
            ORDER BY m.match_date DESC
            LIMIT 50
        """
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(recent_matches_query)
                matches_to_analyze = cursor.fetchall()
        
        for match in matches_to_analyze:
            try:
                match_id = match['match_id']
                logger.info(f"Calculating metrics for match {match_id}: {match['team1_name']} vs {match['team2_name']}")
                
                # Extract comprehensive metrics
                metrics = processor.calculate_predictive_metrics(match_id)
                
                if metrics:
                    # Store calculated metrics
                    success = processor.store_match_metrics(match_id, metrics)
                    if success:
                        metrics_results['matches_analyzed'] += 1
                        
                        # Track which metrics were calculated
                        for metric_category in metrics.keys():
                            if metric_category not in metrics_results['metrics_calculated']:
                                metrics_results['metrics_calculated'][metric_category] = 0
                            metrics_results['metrics_calculated'][metric_category] += 1
                    else:
                        metrics_results['quality_flags'].append(f"Failed to store metrics for match {match_id}")
                else:
                    metrics_results['quality_flags'].append(f"No metrics calculated for match {match_id}")
                    
            except Exception as e:
                logger.error(f"Error calculating metrics for match {match['match_id']}: {e}")
                metrics_results['quality_flags'].append(f"Error processing match {match['match_id']}: {str(e)}")
        
        logger.info(f"Metrics extraction complete: {metrics_results}")
        
    except Exception as e:
        logger.error(f"Battle-tested metrics extraction failed: {e}")
        metrics_results['quality_flags'].append(f"Overall processing error: {str(e)}")
    
    return metrics_results


def update_team_performance_cache(**context) -> Dict[str, Any]:
    """
    Update cached team performance metrics based on recent match data
    """
    logger.info("Updating team performance cache...")
    
    processor = MatchProcessor()
    cache_results = {
        'teams_updated': 0,
        'metrics_refreshed': [],
        'errors': []
    }
    
    try:
        # Get teams that participated in recent matches
        db_manager = DatabaseManager()
        
        teams_query = """
            SELECT DISTINCT t.team_id, t.team_name
            FROM teams t
            WHERE EXISTS (
                SELECT 1 FROM matches m 
                WHERE (m.team1_name = t.team_name OR m.team2_name = t.team_name)
                  AND m.match_date >= NOW() - INTERVAL '48 hours'
                  AND m.match_status = 'completed'
            )
        """
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(teams_query)
                teams_to_update = cursor.fetchall()
        
        for team in teams_to_update:
            try:
                team_id = team['team_id']
                team_name = team['team_name']
                
                # Calculate recent form metrics (last 10-20 maps)
                recent_form = processor.calculate_recent_form(team_name, maps_count=20)
                
                # Calculate map-specific performance
                map_performance = processor.calculate_map_performance(team_name)
                
                # Calculate side-specific win rates
                side_performance = processor.calculate_side_performance(team_name)
                
                # Update team cache
                cache_data = {
                    'team_id': team_id,
                    'recent_form': recent_form,
                    'map_performance': map_performance,
                    'side_performance': side_performance,
                    'last_updated': datetime.now()
                }
                
                success = processor.update_team_cache(team_id, cache_data)
                if success:
                    cache_results['teams_updated'] += 1
                    cache_results['metrics_refreshed'].extend(['recent_form', 'map_performance', 'side_performance'])
                else:
                    cache_results['errors'].append(f"Failed to update cache for team {team_name}")
                    
            except Exception as e:
                logger.error(f"Error updating cache for team {team['team_name']}: {e}")
                cache_results['errors'].append(f"Error processing team {team['team_name']}: {str(e)}")
        
        logger.info(f"Team cache update complete: {cache_results}")
        
    except Exception as e:
        logger.error(f"Team performance cache update failed: {e}")
        cache_results['errors'].append(f"Overall cache update error: {str(e)}")
    
    return cache_results


# Task definitions
start_task = EmptyOperator(task_id="start_match_pipeline")

# Identify target matches
identify_matches_task = PythonOperator(
    task_id='identify_target_matches',
    python_callable=identify_target_matches,
    dag=dag,
)

# Data scraping task group
with TaskGroup('scraping_group', dag=dag) as scraping_group:
    
    # VLR scraping - dynamic task generation based on identified matches
    scrape_vlr_task = PythonOperator(
        task_id='scrape_vlr_matches',
        python_callable=lambda **context: [
            scrape_vlr_match_data(match) 
            for match in context['task_instance'].xcom_pull(task_ids='identify_target_matches')
            if match and 'vlr' in match.get('scraping_sources', [])
        ],
        dag=dag,
    )
    
    # TheSpike scraping
    scrape_thespike_task = PythonOperator(
        task_id='scrape_thespike_matches',
        python_callable=lambda **context: [
            scrape_thespike_match_data(match) 
            for match in context['task_instance'].xcom_pull(task_ids='identify_target_matches')
            if match and 'thespike' in match.get('scraping_sources', [])
        ],
        dag=dag,
    )
    
    # Both scraping tasks can run in parallel
    [scrape_vlr_task, scrape_thespike_task]

# Data processing task group
with TaskGroup('processing_group', dag=dag) as processing_group:
    
    # Process and merge scraped data
    process_matches_task = PythonOperator(
        task_id='process_match_data',
        python_callable=process_and_merge_match_data,
        dag=dag,
    )
    
    # Extract battle-tested metrics
    extract_metrics_task = PythonOperator(
        task_id='extract_predictive_metrics',
        python_callable=extract_battle_tested_metrics,
        dag=dag,
    )
    
    # Update team performance cache
    update_cache_task = PythonOperator(
        task_id='update_team_cache',
        python_callable=update_team_performance_cache,
        dag=dag,
    )
    
    process_matches_task >> [extract_metrics_task, update_cache_task]

# Data quality checks
quality_check_task = DataQualityOperator(
    task_id='match_data_quality_check',
    tables_to_check=['matches', 'match_maps', 'player_stats'],
    data_quality_checks=[
        {
            'check_sql': '''
                SELECT COUNT(*) FROM matches m 
                WHERE m.match_date >= NOW() - INTERVAL '24 hours'
                  AND m.last_updated >= NOW() - INTERVAL '4 hours'
            ''',
            'expected_result': 'greater_than',
            'expected_value': 0,
            'description': 'Recent matches should have fresh updates'
        },
        {
            'check_sql': '''
                SELECT AVG(CASE WHEN player_count >= 10 THEN 1 ELSE 0 END) 
                FROM (
                    SELECT match_id, COUNT(player_id) as player_count
                    FROM player_stats 
                    WHERE match_date >= NOW() - INTERVAL '24 hours'
                    GROUP BY match_id
                ) t
            ''',
            'expected_result': 'greater_than',
            'expected_value': 0.8,
            'description': 'At least 80% of matches should have complete player data'
        }
    ],
    dag=dag,
)

end_task = EmptyOperator(
    task_id="end_match_pipeline",
    trigger_rule=TriggerRule.ALL_DONE,
)

# Define task dependencies
start_task >> identify_matches_task >> scraping_group
scraping_group >> processing_group >> quality_check_task >> end_task
"""
Valorant Team Data Collection DAG
Handles team-level statistics, roster tracking, recent form analysis,
map performance, and all battle-tested team predictive metrics.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging
import json
import statistics
import numpy as np

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
from plugins.utils.processors.team_processor import TeamProcessor
from plugins.utils.storage.database_manager import DatabaseManager
from plugins.utils.validators.data_quality import DataQualityValidator

# Set up logging
logger = logging.getLogger(__name__)

# DAG configuration
DAG_ID = 'valorant_team_data_pipeline'
SCHEDULE_INTERVAL = settings.pipeline.TEAM_DATA_SCHEDULE  # Daily at 8 AM
MAX_ACTIVE_RUNS = 2
CATCHUP = False

# Default arguments
default_args = {
    'owner': 'valorant-data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': settings.EMAIL_NOTIFICATIONS_ENABLED,
    'email_on_retry': False,
    'retries': settings.pipeline.DEFAULT_RETRIES,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': settings.pipeline.DATA_PROCESSING_TIMEOUT,
    'sla': timedelta(hours=6),  # Team data should be updated within 6 hours
}

# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Valorant team statistics and performance tracking pipeline',
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=MAX_ACTIVE_RUNS,
    catchup=CATCHUP,
    tags=['valorant', 'teams', 'roster', 'performance'],
    doc_md=__doc__,
)


def identify_teams_for_update(**context) -> List[Dict[str, Any]]:
    """
    Identify teams that need data updates based on recent activity and freshness
    """
    logger.info("Identifying teams for data update...")
    
    # Get collection plan from DAG run configuration
    dag_run_conf = context.get('dag_run', {}).conf or {}
    collection_plan = dag_run_conf.get('collection_plan', {})
    
    target_teams = []
    
    try:
        db_manager = DatabaseManager()
        
        # Priority 1: Teams with recent match activity
        active_teams_query = """
            WITH recent_team_activity AS (
                SELECT 
                    t.team_id,
                    t.team_name,
                    t.vlr_team_id,
                    t.thespike_team_id,
                    t.region,
                    t.last_updated,
                    t.roster_last_updated,
                    COUNT(DISTINCT m.match_id) as recent_matches,
                    MAX(m.match_date) as last_match_date,
                    AVG(CASE 
                        WHEN m.team1_name = t.team_name AND m.team1_score > m.team2_score THEN 1
                        WHEN m.team2_name = t.team_name AND m.team2_score > m.team1_score THEN 1
                        ELSE 0 
                    END) as recent_win_rate,
                    COUNT(DISTINCT CASE WHEN m.match_date >= NOW() - INTERVAL '7 days' THEN m.match_id END) as matches_last_week,
                    EXTRACT(EPOCH FROM (NOW() - t.last_updated))/3600 as hours_since_update
                FROM teams t
                INNER JOIN matches m ON (m.team1_name = t.team_name OR m.team2_name = t.team_name)
                WHERE m.match_date >= NOW() - INTERVAL '21 days'
                  AND t.is_active = true
                  AND m.match_status = 'completed'
                GROUP BY t.team_id, t.team_name, t.vlr_team_id, t.thespike_team_id, 
                         t.region, t.last_updated, t.roster_last_updated
            )
            SELECT 
                *,
                'high' as priority
            FROM recent_team_activity
            WHERE hours_since_update > 12  -- Data older than 12 hours
               OR roster_last_updated < NOW() - INTERVAL '48 hours'  -- Roster data older than 48 hours
               OR matches_last_week > 0  -- Teams with recent activity
            ORDER BY matches_last_week DESC, recent_matches DESC, hours_since_update DESC
            LIMIT 30
        """
        
        # Priority 2: Teams with stale data but still competitive relevance
        stale_teams_query = """
            SELECT 
                t.team_id,
                t.team_name,
                t.vlr_team_id,
                t.thespike_team_id,
                t.region,
                t.last_updated,
                t.roster_last_updated,
                0 as recent_matches,
                NULL as last_match_date,
                0 as recent_win_rate,
                0 as matches_last_week,
                EXTRACT(EPOCH FROM (NOW() - t.last_updated))/3600 as hours_since_update,
                'medium' as priority
            FROM teams t
            WHERE t.is_active = true
              AND t.last_updated < NOW() - INTERVAL '72 hours'  -- Very stale data
              AND EXISTS (
                  -- Teams that have played in the last 3 months
                  SELECT 1 FROM matches m 
                  WHERE (m.team1_name = t.team_name OR m.team2_name = t.team_name)
                    AND m.match_date >= NOW() - INTERVAL '90 days'
              )
            ORDER BY t.last_updated ASC NULLS FIRST
            LIMIT 20
        """
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                # Get active teams
                cursor.execute(active_teams_query)
                active_results = cursor.fetchall()
                
                for row in active_results:
                    team_data = dict(row)
                    team_data['scraping_sources'] = []
                    
                    if settings.ENABLE_VLR_SCRAPING and team_data.get('vlr_team_id'):
                        team_data['scraping_sources'].append('vlr')
                    if settings.ENABLE_SPIKE_SCRAPING and team_data.get('thespike_team_id'):
                        team_data['scraping_sources'].append('thespike')
                    
                    if team_data['scraping_sources']:
                        target_teams.append(team_data)
                
                # Get stale teams
                cursor.execute(stale_teams_query)
                stale_results = cursor.fetchall()
                
                for row in stale_results:
                    team_data = dict(row)
                    team_data['scraping_sources'] = []
                    
                    if settings.ENABLE_VLR_SCRAPING and team_data.get('vlr_team_id'):
                        team_data['scraping_sources'].append('vlr')
                    if settings.ENABLE_SPIKE_SCRAPING and team_data.get('thespike_team_id'):
                        team_data['scraping_sources'].append('thespike')
                    
                    if team_data['scraping_sources']:
                        target_teams.append(team_data)
        
        # Remove duplicates and sort by priority
        seen_teams = set()
        unique_teams = []
        
        for team in target_teams:
            team_key = team['team_id']
            if team_key not in seen_teams:
                seen_teams.add(team_key)
                unique_teams.append(team)
        
        # Sort by priority and recent activity
        unique_teams.sort(key=lambda x: (
            0 if x['priority'] == 'high' else 1,
            -x['recent_matches'],
            -x['hours_since_update']
        ))
        
        logger.info(f"Identified {len(unique_teams)} teams for update:")
        logger.info(f"  - {sum(1 for t in unique_teams if t['priority'] == 'high')} high priority")
        logger.info(f"  - {sum(1 for t in unique_teams if t['priority'] == 'medium')} medium priority")
        
    except Exception as e:
        logger.error(f"Error identifying target teams: {e}")
        target_teams = []
    
    return unique_teams


def scrape_vlr_team_data(team_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Scrape detailed team data from VLR.gg
    """
    if not settings.ENABLE_VLR_SCRAPING or not team_info.get('vlr_team_id'):
        return None
    
    logger.info(f"Scraping VLR data for team: {team_info['team_name']} ({team_info['region']})")
    
    try:
        scraper = VLRScraper()
        
        with RateLimitedRequest('vlr', timeout=60):
            # Build team URL
            team_url = scraping_config.build_url(
                'vlr',
                DataType.TEAM,
                team_id=team_info['vlr_team_id'],
                team_slug=team_info['team_name'].lower().replace(' ', '-').replace('.', '')
            )
            
            if not team_url:
                logger.error(f"Could not build VLR URL for team {team_info['team_id']}")
                return None
            
            # Scrape comprehensive team data
            team_data = scraper.scrape_team_profile(team_url, team_info['team_id'])
            
            if team_data:
                # Extract battle-tested team metrics
                enhanced_data = scraper.extract_team_battle_metrics(team_data)
                enhanced_data['source'] = 'vlr'
                enhanced_data['scraped_at'] = datetime.now()
                
                logger.info(f"Successfully scraped VLR data for team {team_info['team_name']}")
                return enhanced_data
            else:
                logger.warning(f"No data returned from VLR for team {team_info['team_name']}")
                return None
                
    except Exception as e:
        logger.error(f"VLR scraping failed for team {team_info['team_name']}: {e}")
        return None


def scrape_thespike_team_data(team_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Scrape team data from TheSpike.gg
    """
    if not settings.ENABLE_SPIKE_SCRAPING or not team_info.get('thespike_team_id'):
        return None
    
    logger.info(f"Scraping TheSpike data for team: {team_info['team_name']}")
    
    try:
        scraper = TheSpikeScraper()
        
        with RateLimitedRequest('thespike', timeout=45):
            # Build API URL
            api_url = scraping_config.build_url(
                'thespike',
                DataType.TEAM,
                team_id=team_info['thespike_team_id']
            )
            
            if not api_url:
                logger.error(f"Could not build TheSpike URL for team {team_info['team_id']}")
                return None
            
            # Scrape team data
            team_data = scraper.scrape_team_data(api_url, team_info['team_id'])
            
            if team_data:
                team_data['source'] = 'thespike'
                team_data['scraped_at'] = datetime.now()
                
                logger.info(f"Successfully scraped TheSpike data for team {team_info['team_name']}")
                return team_data
            else:
                logger.warning(f"No data returned from TheSpike for team {team_info['team_name']}")
                return None
                
    except Exception as e:
        logger.error(f"TheSpike scraping failed for team {team_info['team_name']}: {e}")
        return None


def process_team_data_and_rosters(**context) -> Dict[str, Any]:
    """
    Process and merge team data from different sources, track roster changes
    """
    task_instance = context['task_instance']
    
    # Get scraped data from upstream tasks
    vlr_data = task_instance.xcom_pull(task_ids='scraping_group.scrape_vlr_teams') or []
    thespike_data = task_instance.xcom_pull(task_ids='scraping_group.scrape_thespike_teams') or []
    
    if not vlr_data and not thespike_data:
        logger.warning("No team data to process")
        return {'processed_teams': 0, 'failed_teams': 0}
    
    logger.info(f"Processing {len(vlr_data)} VLR teams and {len(thespike_data)} TheSpike teams")
    
    processor = TeamProcessor()
    processing_results = {
        'processed_teams': 0,
        'failed_teams': 0,
        'updated_teams': 0,
        'new_teams': 0,
        'roster_changes_detected': 0,
        'quality_issues': []
    }
    
    try:
        # Process VLR data
        for team_data in vlr_data:
            if not team_data:
                continue
                
            try:
                processed_data = processor.process_team_data(team_data, source='vlr')
                
                # Detect roster changes
                roster_changes = processor.detect_roster_changes(processed_data)
                if roster_changes:
                    processing_results['roster_changes_detected'] += len(roster_changes)
                    processor.store_roster_changes(processed_data['team_id'], roster_changes)
                
                # Validate data quality
                validator = DataQualityValidator()
                quality_check = validator.validate_team_data(processed_data)
                
                if quality_check['is_valid']:
                    # Store in database
                    success = processor.store_team_data(processed_data)
                    if success:
                        processing_results['processed_teams'] += 1
                        if processed_data.get('is_new_record'):
                            processing_results['new_teams'] += 1
                        else:
                            processing_results['updated_teams'] += 1
                    else:
                        processing_results['failed_teams'] += 1
                else:
                    processing_results['quality_issues'].extend(quality_check['issues'])
                    processing_results['failed_teams'] += 1
                    logger.warning(f"Team data quality issues: {quality_check['issues']}")
                    
            except Exception as e:
                logger.error(f"Error processing VLR team data: {e}")
                processing_results['failed_teams'] += 1
        
        # Process TheSpike data
        for team_data in thespike_data:
            if not team_data:
                continue
                
            try:
                processed_data = processor.process_team_data(team_data, source='thespike')
                
                # Check if we already processed this team from VLR
                existing_team = processor.find_existing_team(processed_data['team_id'])
                
                if existing_team:
                    # Merge with existing data
                    merged_data = processor.merge_team_sources(existing_team, processed_data)
                    success = processor.store_team_data(merged_data)
                    if success:
                        processing_results['updated_teams'] += 1
                else:
                    # Validate and store new team
                    validator = DataQualityValidator()
                    quality_check = validator.validate_team_data(processed_data)
                    
                    if quality_check['is_valid']:
                        success = processor.store_team_data(processed_data)
                        if success:
                            processing_results['processed_teams'] += 1
                            processing_results['new_teams'] += 1
                        else:
                            processing_results['failed_teams'] += 1
                    else:
                        processing_results['quality_issues'].extend(quality_check['issues'])
                        processing_results['failed_teams'] += 1
                        
            except Exception as e:
                logger.error(f"Error processing TheSpike team data: {e}")
                processing_results['failed_teams'] += 1
        
        logger.info(f"Team processing complete: {processing_results}")
        
    except Exception as e:
        logger.error(f"Team data processing failed: {e}")
        processing_results['failed_teams'] = len(vlr_data) + len(thespike_data)
    
    return processing_results


def calculate_team_battle_metrics(**context) -> Dict[str, Any]:
    """
    Calculate battle-tested predictive metrics for teams
    """
    logger.info("Calculating battle-tested team metrics...")
    
    processor = TeamProcessor()
    metrics_results = {
        'teams_analyzed': 0,
        'metrics_calculated': {},
        'quality_flags': []
    }
    
    try:
        db_manager = DatabaseManager()
        
        # Get teams that need metrics calculation
        teams_for_metrics_query = """
            WITH team_recent_activity AS (
                SELECT 
                    t.team_id,
                    t.team_name,
                    t.region,
                    COUNT(DISTINCT m.match_id) as recent_matches,
                    COUNT(DISTINCT mm.map_name) as maps_played,
                    MAX(m.match_date) as last_match_date,
                    MIN(m.match_date) as first_recent_match
                FROM teams t
                INNER JOIN matches m ON (m.team1_name = t.team_name OR m.team2_name = t.team_name)
                LEFT JOIN match_maps mm ON m.match_id = mm.match_id
                WHERE m.match_date >= NOW() - INTERVAL '60 days'
                  AND t.is_active = true
                  AND m.match_status = 'completed'
                  AND (t.metrics_calculated = false OR t.metrics_calculated IS NULL OR t.last_metrics_update < NOW() - INTERVAL '24 hours')
                GROUP BY t.team_id, t.team_name, t.region
                HAVING COUNT(DISTINCT m.match_id) >= 5  -- Minimum sample size
                   AND COUNT(DISTINCT mm.map_name) >= 3  -- Played on multiple maps
            )
            SELECT * FROM team_recent_activity
            ORDER BY recent_matches DESC, last_match_date DESC
            LIMIT 50
        """
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(teams_for_metrics_query)
                teams_to_analyze = cursor.fetchall()
        
        for team in teams_to_analyze:
            try:
                team_id = team['team_id']
                team_name = team['team_name']
                
                logger.info(f"Calculating metrics for team {team_name}")
                
                # Calculate comprehensive battle-tested metrics
                metrics = processor.calculate_battle_tested_team_metrics(team_id)
                
                if metrics:
                    # Store calculated metrics
                    success = processor.store_team_metrics(team_id, metrics)
                    if success:
                        metrics_results['teams_analyzed'] += 1
                        
                        # Track which metrics were calculated
                        for metric_category in metrics.keys():
                            if metric_category not in metrics_results['metrics_calculated']:
                                metrics_results['metrics_calculated'][metric_category] = 0
                            metrics_results['metrics_calculated'][metric_category] += 1
                    else:
                        metrics_results['quality_flags'].append(f"Failed to store metrics for team {team_name}")
                else:
                    metrics_results['quality_flags'].append(f"No metrics calculated for team {team_name}")
                    
            except Exception as e:
                logger.error(f"Error calculating metrics for team {team['team_name']}: {e}")
                metrics_results['quality_flags'].append(f"Error processing team {team['team_name']}: {str(e)}")
        
        logger.info(f"Team metrics calculation complete: {metrics_results}")
        
    except Exception as e:
        logger.error(f"Battle-tested team metrics calculation failed: {e}")
        metrics_results['quality_flags'].append(f"Overall processing error: {str(e)}")
    
    return metrics_results


def calculate_recent_form_and_momentum(**context) -> Dict[str, Any]:
    """
    Calculate recent form with exponential decay and momentum indicators
    """
    logger.info("Calculating recent form and momentum metrics...")
    
    processor = TeamProcessor()
    form_results = {
        'teams_analyzed': 0,
        'form_trends_calculated': 0,
        'momentum_scores_calculated': 0,
        'streak_analysis_completed': 0
    }
    
    try:
        db_manager = DatabaseManager()
        
        # Get teams with sufficient recent data
        form_analysis_query = """
            SELECT DISTINCT
                t.team_id,
                t.team_name,
                t.region
            FROM teams t
            INNER JOIN matches m ON (m.team1_name = t.team_name OR m.team2_name = t.team_name)
            WHERE m.match_date >= NOW() - INTERVAL '45 days'
              AND t.is_active = true
              AND m.match_status = 'completed'
            GROUP BY t.team_id, t.team_name, t.region
            HAVING COUNT(DISTINCT m.match_id) >= 8  -- Need reasonable sample for form analysis
            ORDER BY COUNT(DISTINCT m.match_id) DESC
            LIMIT 100
        """
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(form_analysis_query)
                teams_for_form = cursor.fetchall()
        
        for team in teams_for_form:
            try:
                team_id = team['team_id']
                team_name = team['team_name']
                
                logger.info(f"Calculating form for team {team_name}")
                
                # Calculate recent form (last 10-20 maps with exponential decay)
                recent_form = processor.calculate_recent_form_exponential_decay(
                    team_name, maps_count=20, decay_factor=0.95
                )
                
                # Calculate momentum indicators
                momentum_metrics = processor.calculate_team_momentum(team_name)
                
                # Analyze win/loss streaks
                streak_analysis = processor.analyze_team_streaks(team_name)
                
                # Calculate patch-era form (performance since last major patch)
                patch_era_form = processor.calculate_patch_era_performance(team_name)
                
                # Store form analysis
                form_data = {
                    'team_id': team_id,
                    'recent_form': recent_form,
                    'momentum_metrics': momentum_metrics,
                    'streak_analysis': streak_analysis,
                    'patch_era_form': patch_era_form,
                    'analysis_window_days': 45,
                    'calculated_at': datetime.now()
                }
                
                success = processor.store_team_form_analysis(team_id, form_data)
                if success:
                    form_results['teams_analyzed'] += 1
                    form_results['form_trends_calculated'] += 1
                    form_results['momentum_scores_calculated'] += 1
                    form_results['streak_analysis_completed'] += 1
                
            except Exception as e:
                logger.error(f"Error calculating form for team {team['team_name']}: {e}")
                continue
        
        logger.info(f"Form and momentum analysis complete: {form_results}")
        
    except Exception as e:
        logger.error(f"Form and momentum analysis failed: {e}")
        form_results['error'] = str(e)
    
    return form_results


def calculate_map_side_performance(**context) -> Dict[str, Any]:
    """
    Calculate map-specific and side-specific performance metrics
    """
    logger.info("Calculating map and side performance metrics...")
    
    processor = TeamProcessor()
    map_results = {
        'teams_analyzed': 0,
        'map_profiles_updated': 0,
        'side_preferences_calculated': 0
    }
    
    try:
        db_manager = DatabaseManager()
        
        # Get team map performance data
        map_performance_query = """
            WITH team_map_stats AS (
                SELECT 
                    t.team_id,
                    t.team_name,
                    mm.map_name,
                    COUNT(*) as games_played,
                    -- Calculate wins when team is attacking
                    COUNT(CASE 
                        WHEN (mm.attacking_team = t.team_name AND mm.attacking_score > mm.defending_score)
                          OR (mm.defending_team = t.team_name AND mm.defending_score > mm.attacking_score)
                        THEN 1 
                    END) as wins,
                    -- Attack side performance
                    AVG(CASE 
                        WHEN mm.attacking_team = t.team_name 
                        THEN mm.attacking_score::float / (mm.attacking_score + mm.defending_score)
                        WHEN mm.defending_team = t.team_name 
                        THEN mm.defending_score::float / (mm.attacking_score + mm.defending_score)
                    END) as avg_rounds_won_rate,
                    -- Attack vs Defense breakdown
                    AVG(CASE 
                        WHEN mm.attacking_team = t.team_name 
                        THEN mm.attacking_score::float / GREATEST(mm.attacking_score + mm.defending_score, 1)
                        ELSE NULL
                    END) as attack_round_win_rate,
                    AVG(CASE 
                        WHEN mm.defending_team = t.team_name 
                        THEN mm.defending_score::float / GREATEST(mm.attacking_score + mm.defending_score, 1)
                        ELSE NULL
                    END) as defense_round_win_rate,
                    COUNT(CASE WHEN mm.attacking_team = t.team_name THEN 1 END) as games_attacking,
                    COUNT(CASE WHEN mm.defending_team = t.team_name THEN 1 END) as games_defending
                FROM teams t
                INNER JOIN matches m ON (m.team1_name = t.team_name OR m.team2_name = t.team_name)
                INNER JOIN match_maps mm ON m.match_id = mm.match_id
                WHERE m.match_date >= NOW() - INTERVAL '90 days'
                  AND t.is_active = true
                  AND m.match_status = 'completed'
                  AND mm.attacking_score + mm.defending_score > 0  -- Valid scores
                GROUP BY t.team_id, t.team_name, mm.map_name
                HAVING COUNT(*) >= 3  -- Minimum games per map
            )
            SELECT 
                team_id,
                team_name,
                COUNT(DISTINCT map_name) as maps_played,
                JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'map_name', map_name,
                        'games_played', games_played,
                        'wins', wins,
                        'win_rate', wins::float / games_played,
                        'avg_rounds_won_rate', avg_rounds_won_rate,
                        'attack_round_win_rate', attack_round_win_rate,
                        'defense_round_win_rate', defense_round_win_rate,
                        'games_attacking', games_attacking,
                        'games_defending', games_defending
                    )
                ) as map_performance_data
            FROM team_map_stats
            GROUP BY team_id, team_name
            ORDER BY maps_played DESC
        """
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(map_performance_query)
                team_map_data = cursor.fetchall()
        
        for team_data in team_map_data:
            try:
                team_id = team_data['team_id']
                team_name = team_data['team_name']
                map_data = team_data['map_performance_data']
                
                logger.info(f"Analyzing map performance for {team_name}: {len(map_data)} maps")
                
                # Calculate comprehensive map analysis
                map_analysis = processor.analyze_team_map_performance(team_id, map_data)
                
                # Calculate side preferences
                side_analysis = processor.calculate_side_preferences(team_id, map_data)
                
                # Identify map strengths and weaknesses
                map_strengths = processor.identify_map_strengths_weaknesses(map_data)
                
                # Calculate veto predictions
                veto_predictions = processor.calculate_veto_tendencies(team_id, map_data)
                
                # Store map performance analysis
                map_performance_data = {
                    'team_id': team_id,
                    'maps_analyzed': len(map_data),
                    'map_statistics': map_data,
                    'map_analysis': map_analysis,
                    'side_analysis': side_analysis,
                    'map_strengths': map_strengths,
                    'veto_predictions': veto_predictions,
                    'last_updated': datetime.now()
                }
                
                success = processor.store_map_performance_analysis(team_id, map_performance_data)
                if success:
                    map_results['teams_analyzed'] += 1
                    map_results['map_profiles_updated'] += 1
                    map_results['side_preferences_calculated'] += 1
                
            except Exception as e:
                logger.error(f"Error analyzing map performance for team {team_data['team_name']}: {e}")
                continue
        
        logger.info(f"Map and side performance analysis complete: {map_results}")
        
    except Exception as e:
        logger.error(f"Map and side performance analysis failed: {e}")
        map_results['error'] = str(e)
    
    return map_results


def calculate_roster_stability_impact(**context) -> Dict[str, Any]:
    """
    Calculate roster stability and its impact on team performance
    """
    logger.info("Calculating roster stability and impact metrics...")
    
    processor = TeamProcessor()
    stability_results = {
        'teams_analyzed': 0,
        'roster_changes_tracked': 0,
        'stability_scores_calculated': 0,
        'synergy_metrics_updated': 0
    }
    
    try:
        db_manager = DatabaseManager()
        
        # Get roster change data for teams
        roster_stability_query = """
            WITH team_roster_changes AS (
                SELECT 
                    t.team_id,
                    t.team_name,
                    COUNT(rc.change_id) as total_changes_6_months,
                    COUNT(CASE WHEN rc.change_date >= NOW() - INTERVAL '90 days' THEN 1 END) as changes_last_3_months,
                    COUNT(CASE WHEN rc.change_date >= NOW() - INTERVAL '30 days' THEN 1 END) as changes_last_month,
                    MAX(rc.change_date) as last_roster_change,
                    MIN(rc.change_date) as first_tracked_change,
                    AVG(EXTRACT(EPOCH FROM (NOW() - rc.change_date))/86400) as avg_days_since_changes
                FROM teams t
                LEFT JOIN roster_changes rc ON t.team_id = rc.team_id 
                    AND rc.change_date >= NOW() - INTERVAL '180 days'
                WHERE t.is_active = true
                GROUP BY t.team_id, t.team_name
            ),
            current_roster_tenure AS (
                SELECT 
                    t.team_id,
                    COUNT(DISTINCT p.player_id) as current_roster_size,
                    AVG(EXTRACT(EPOCH FROM (NOW() - p.team_join_date))/86400) as avg_player_tenure_days,
                    MIN(EXTRACT(EPOCH FROM (NOW() - p.team_join_date))/86400) as newest_player_tenure_days,
                    COUNT(CASE WHEN p.team_join_date >= NOW() - INTERVAL '30 days' THEN 1 END) as new_players_last_month
                FROM teams t
                INNER JOIN players p ON p.team_name = t.team_name AND p.is_active = true
                WHERE t.is_active = true
                GROUP BY t.team_id
            )
            SELECT 
                trc.*,
                crt.current_roster_size,
                crt.avg_player_tenure_days,
                crt.newest_player_tenure_days,
                crt.new_players_last_month
            FROM team_roster_changes trc
            LEFT JOIN current_roster_tenure crt ON trc.team_id = crt.team_id
            WHERE crt.current_roster_size >= 4  -- Teams with reasonable roster size
            ORDER BY total_changes_6_months DESC, changes_last_3_months DESC
        """
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(roster_stability_query)
                roster_data = cursor.fetchall()
        
        for team_roster in roster_data:
            try:
                team_id = team_roster['team_id']
                team_name = team_roster['team_name']
                
                logger.info(f"Calculating roster stability for {team_name}")
                
                # Calculate roster stability score
                stability_score = processor.calculate_roster_stability_score(team_roster)
                
                # Calculate synergy development metrics
                synergy_metrics = processor.calculate_team_synergy_metrics(team_id)
                
                # Analyze performance impact of roster changes
                change_impact = processor.analyze_roster_change_impact(team_id)
                
                # Calculate maps played with current roster
                current_roster_experience = processor.calculate_current_roster_maps(team_id)
                
                # Store stability analysis
                stability_data = {
                    'team_id': team_id,
                    'roster_change_data': dict(team_roster),
                    'stability_score': stability_score,
                    'synergy_metrics': synergy_metrics,
                    'change_impact_analysis': change_impact,
                    'current_roster_experience': current_roster_experience,
                    'calculated_at': datetime.now()
                }
                
                success = processor.store_roster_stability_analysis(team_id, stability_data)
                if success:
                    stability_results['teams_analyzed'] += 1
                    stability_results['roster_changes_tracked'] += team_roster['total_changes_6_months']
                    stability_results['stability_scores_calculated'] += 1
                    stability_results['synergy_metrics_updated'] += 1
                
            except Exception as e:
                logger.error(f"Error calculating roster stability for team {team_roster['team_name']}: {e}")
                continue
        
        logger.info(f"Roster stability analysis complete: {stability_results}")
        
    except Exception as e:
        logger.error(f"Roster stability analysis failed: {e}")
        stability_results['error'] = str(e)
    
    return stability_results


def calculate_tactical_metrics(**context) -> Dict[str, Any]:
    """
    Calculate tactical and strategic performance metrics
    """
    logger.info("Calculating tactical and strategic metrics...")
    
    processor = TeamProcessor()
    tactical_results = {
        'teams_analyzed': 0,
        'pistol_analysis_completed': 0,
        'eco_analysis_completed': 0,
        'clutch_analysis_completed': 0,
        'timeout_impact_calculated': 0
    }
    
    try:
        db_manager = DatabaseManager()
        
        # Get teams with sufficient round-level data
        tactical_data_query = """
            SELECT 
                t.team_id,
                t.team_name,
                COUNT(DISTINCT m.match_id) as matches_with_round_data,
                COUNT(DISTINCT mm.map_id) as maps_with_round_data
            FROM teams t
            INNER JOIN matches m ON (m.team1_name = t.team_name OR m.team2_name = t.team_name)
            INNER JOIN match_maps mm ON m.match_id = mm.match_id
            WHERE m.match_date >= NOW() - INTERVAL '60 days'
              AND t.is_active = true
              AND m.match_status = 'completed'
              AND EXISTS (
                  -- Check if we have round-level data
                  SELECT 1 FROM round_results rr 
                  WHERE rr.match_id = m.match_id AND rr.map_id = mm.map_id
              )
            GROUP BY t.team_id, t.team_name
            HAVING COUNT(DISTINCT m.match_id) >= 5
            ORDER BY matches_with_round_data DESC
            LIMIT 75
        """
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(tactical_data_query)
                teams_with_data = cursor.fetchall()
        
        for team_data in teams_with_data:
            try:
                team_id = team_data['team_id']
                team_name = team_data['team_name']
                
                logger.info(f"Calculating tactical metrics for {team_name}")
                
                # Calculate pistol round performance
                pistol_analysis = processor.calculate_pistol_round_performance(team_id)
                
                # Calculate eco/anti-eco/bonus round performance
                economy_analysis = processor.calculate_economy_round_performance(team_id)
                
                # Calculate clutch situation performance
                clutch_analysis = processor.calculate_team_clutch_performance(team_id)
                
                # Calculate timeout and tactical pause impact
                timeout_impact = processor.calculate_timeout_impact(team_id)
                
                # Calculate opening duel success rates
                opening_duel_stats = processor.calculate_opening_duel_efficiency(team_id)
                
                # Calculate trade efficiency
                trade_efficiency = processor.calculate_trade_efficiency(team_id)
                
                # Store tactical analysis
                tactical_data = {
                    'team_id': team_id,
                    'pistol_analysis': pistol_analysis,
                    'economy_analysis': economy_analysis,
                    'clutch_analysis': clutch_analysis,
                    'timeout_impact': timeout_impact,
                    'opening_duel_stats': opening_duel_stats,
                    'trade_efficiency': trade_efficiency,
                    'sample_matches': team_data['matches_with_round_data'],
                    'calculated_at': datetime.now()
                }
                
                success = processor.store_tactical_analysis(team_id, tactical_data)
                if success:
                    tactical_results['teams_analyzed'] += 1
                    tactical_results['pistol_analysis_completed'] += 1
                    tactical_results['eco_analysis_completed'] += 1
                    tactical_results['clutch_analysis_completed'] += 1
                    tactical_results['timeout_impact_calculated'] += 1
                
            except Exception as e:
                logger.error(f"Error calculating tactical metrics for team {team_data['team_name']}: {e}")
                continue
        
        logger.info(f"Tactical metrics calculation complete: {tactical_results}")
        
    except Exception as e:
        logger.error(f"Tactical metrics calculation failed: {e}")
        tactical_results['error'] = str(e)
    
    return tactical_results


def update_strength_of_schedule_ratings(**context) -> Dict[str, Any]:
    """
    Update strength of schedule adjusted ratings and opponent quality metrics
    """
    logger.info("Updating strength of schedule and opponent-adjusted ratings...")
    
    processor = TeamProcessor()
    sos_results = {
        'teams_updated': 0,
        'elo_ratings_calculated': 0,
        'sos_adjustments_made': 0
    }
    
    try:
        db_manager = DatabaseManager()
        
        # Get all active teams for SoS calculation
        teams_query = """
            SELECT 
                t.team_id,
                t.team_name,
                t.region,
                t.current_rating,
                COUNT(DISTINCT m.match_id) as recent_matches
            FROM teams t
            INNER JOIN matches m ON (m.team1_name = t.team_name OR m.team2_name = t.team_name)
            WHERE m.match_date >= NOW() - INTERVAL '90 days'
              AND t.is_active = true
              AND m.match_status = 'completed'
            GROUP BY t.team_id, t.team_name, t.region, t.current_rating
            HAVING COUNT(DISTINCT m.match_id) >= 3
            ORDER BY recent_matches DESC
        """
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(teams_query)
                teams_for_rating = cursor.fetchall()
        
        # Calculate opponent-adjusted ratings using Glicko-2 or ELO system
        team_ratings = {}
        
        for team in teams_for_rating:
            try:
                team_id = team['team_id']
                team_name = team['team_name']
                
                logger.info(f"Calculating SoS rating for {team_name}")
                
                # Get opponent data and match results
                opponent_data = processor.get_team_opponent_history(team_id, days=90)
                
                # Calculate strength of schedule
                sos_metrics = processor.calculate_strength_of_schedule(team_id, opponent_data)
                
                # Calculate opponent-adjusted ELO/Glicko rating
                adjusted_rating = processor.calculate_opponent_adjusted_rating(
                    team_id, opponent_data, current_rating=team['current_rating']
                )
                
                # Calculate regional strength adjustment
                regional_adjustment = processor.calculate_regional_strength_factor(
                    team['region'], opponent_data
                )
                
                team_ratings[team_id] = {
                    'team_name': team_name,
                    'sos_metrics': sos_metrics,
                    'adjusted_rating': adjusted_rating,
                    'regional_adjustment': regional_adjustment,
                    'opponent_quality_avg': sos_metrics.get('avg_opponent_rating', 1500)
                }
                
            except Exception as e:
                logger.error(f"Error calculating SoS for team {team['team_name']}: {e}")
                continue
        
        # Store updated ratings and SoS data
        for team_id, rating_data in team_ratings.items():
            try:
                success = processor.store_sos_adjusted_rating(team_id, rating_data)
                if success:
                    sos_results['teams_updated'] += 1
                    sos_results['elo_ratings_calculated'] += 1
                    sos_results['sos_adjustments_made'] += 1
                
            except Exception as e:
                logger.error(f"Error storing SoS data for team {rating_data['team_name']}: {e}")
                continue
        
        logger.info(f"Strength of schedule analysis complete: {sos_results}")
        
    except Exception as e:
        logger.error(f"Strength of schedule analysis failed: {e}")
        sos_results['error'] = str(e)
    
    return sos_results


# Task definitions
start_task = EmptyOperator(task_id='start_team_pipeline')

# Identify target teams
identify_teams_task = PythonOperator(
    task_id='identify_teams_for_update',
    python_callable=identify_teams_for_update,
    dag=dag,
)

# Data scraping task group
with TaskGroup('scraping_group', dag=dag) as scraping_group:
    
    # VLR team scraping
    scrape_vlr_task = PythonOperator(
        task_id='scrape_vlr_teams',
        python_callable=lambda **context: [
            scrape_vlr_team_data(team) 
            for team in context['task_instance'].xcom_pull(task_ids='identify_teams_for_update')
            if team and 'vlr' in team.get('scraping_sources', [])
        ],
        dag=dag,
    )
    
    # TheSpike team scraping
    scrape_thespike_task = PythonOperator(
        task_id='scrape_thespike_teams',
        python_callable=lambda **context: [
            scrape_thespike_team_data(team) 
            for team in context['task_instance'].xcom_pull(task_ids='identify_teams_for_update')
            if team and 'thespike' in team.get('scraping_sources', [])
        ],
        dag=dag,
    )
    
    # Both scraping tasks can run in parallel
    [scrape_vlr_task, scrape_thespike_task]

# Data processing task group
with TaskGroup('processing_group', dag=dag) as processing_group:
    
    # Process basic team data and rosters
    process_teams_task = PythonOperator(
        task_id='process_team_data_and_rosters',
        python_callable=process_team_data_and_rosters,
        dag=dag,
    )
    
    # Calculate battle-tested metrics
    calculate_battle_metrics_task = PythonOperator(
        task_id='calculate_team_battle_metrics',
        python_callable=calculate_team_battle_metrics,
        dag=dag,
    )
    
    process_teams_task >> calculate_battle_metrics_task

# Advanced analytics task group
with TaskGroup('analytics_group', dag=dag) as analytics_group:
    
    # Recent form and momentum
    form_momentum_task = PythonOperator(
        task_id='calculate_recent_form_and_momentum',
        python_callable=calculate_recent_form_and_momentum,
        dag=dag,
    )
    
    # Map and side performance
    map_performance_task = PythonOperator(
        task_id='calculate_map_side_performance',
        python_callable=calculate_map_side_performance,
        dag=dag,
    )
    
    # Roster stability analysis
    roster_stability_task = PythonOperator(
        task_id='calculate_roster_stability_impact',
        python_callable=calculate_roster_stability_impact,
        dag=dag,
    )
    
    # Tactical metrics
    tactical_metrics_task = PythonOperator(
        task_id='calculate_tactical_metrics',
        python_callable=calculate_tactical_metrics,
        dag=dag,
    )
    
    # Strength of schedule
    sos_ratings_task = PythonOperator(
        task_id='update_strength_of_schedule_ratings',
        python_callable=update_strength_of_schedule_ratings,
        dag=dag,
    )
    
    # These can run in parallel after basic processing
    [form_momentum_task, map_performance_task, roster_stability_task, tactical_metrics_task, sos_ratings_task]

# Data quality checks
quality_check_task = DataQualityOperator(
    task_id='team_data_quality_check',
    tables_to_check=['teams', 'team_stats', 'roster_changes', 'team_form_analysis'],
    data_quality_checks=[
        {
            'check_sql': '''
                SELECT COUNT(*) FROM teams t
                WHERE t.is_active = true
                  AND t.last_updated >= NOW() - INTERVAL '48 hours'
            ''',
            'expected_result': 'greater_than',
            'expected_value': 20,
            'description': 'At least 20 active teams should have recent updates'
        },
        {
            'check_sql': '''
                SELECT AVG(
                    CASE WHEN recent_form_score BETWEEN 0 AND 1 THEN 1 ELSE 0 END
                ) FROM team_form_analysis
                WHERE calculated_at >= NOW() - INTERVAL '48 hours'
            ''',
            'expected_result': 'greater_than',
            'expected_value': 0.9,
            'description': 'Team form scores should be within valid range (0-1)'
        },
        {
            'check_sql': '''
                SELECT COUNT(*) FROM team_stats ts
                INNER JOIN teams t ON ts.team_id = t.team_id
                WHERE t.is_active = true
                  AND ts.pistol_win_rate IS NOT NULL
                  AND ts.map_win_rate IS NOT NULL
                  AND ts.last_updated >= NOW() - INTERVAL '72 hours'
            ''',
            'expected_result': 'greater_than',
            'expected_value': 15,
            'description': 'Teams should have complete tactical statistics'
        }
    ],
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end_team_pipeline',
    trigger_rule=TriggerRule.ALL_DONE,
)

# Define task dependencies
start_task >> identify_teams_task >> scraping_group
scraping_group >> processing_group >> analytics_group
analytics_group >> quality_check_task >> end_task
"""
Valorant Player Data Collection DAG
Handles individual player statistics, performance metrics, agent pools,
role assignments, and all battle-tested player-level predictive features.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging
import json
import statistics

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
from plugins.utils.processors.player_processor import PlayerProcessor
from plugins.utils.storage.database_manager import DatabaseManager
from plugins.utils.validators.data_quality import DataQualityValidator

# Set up logging
logger = logging.getLogger(__name__)

# DAG configuration
DAG_ID = 'valorant_player_data_pipeline'
SCHEDULE_INTERVAL = settings.pipeline.PLAYER_DATA_SCHEDULE  # Daily at 6 AM
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
    'sla': timedelta(hours=4),  # Player data should be updated within 4 hours
}

# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Valorant player statistics and performance tracking pipeline',
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=MAX_ACTIVE_RUNS,
    catchup=CATCHUP,
    tags=['valorant', 'players', 'statistics', 'performance'],
    doc_md=__doc__,
)


def identify_players_for_update(**context) -> List[Dict[str, Any]]:
    """
    Identify players that need statistics updates based on recent activity and data freshness
    """
    logger.info("Identifying players for statistics update...")
    
    # Get collection plan from DAG run configuration
    dag_run_conf = context.get('dag_run', {}).conf or {}
    collection_plan = dag_run_conf.get('collection_plan', {})
    
    target_players = []
    
    try:
        db_manager = DatabaseManager()
        
        # Priority 1: Active players from recent matches
        active_players_query = """
            WITH recent_matches AS (
                SELECT DISTINCT match_id 
                FROM matches 
                WHERE match_date >= NOW() - INTERVAL '48 hours'
                  AND match_status = 'completed'
            ),
            active_players AS (
                SELECT 
                    p.player_id,
                    p.player_name,
                    p.team_name,
                    p.vlr_player_id,
                    p.thespike_player_id,
                    p.role,
                    p.last_updated,
                    p.stats_last_updated,
                    COUNT(ps.match_id) as recent_matches_played,
                    AVG(ps.rating) as recent_avg_rating,
                    EXTRACT(EPOCH FROM (NOW() - p.stats_last_updated))/3600 as hours_since_stats_update
                FROM players p
                INNER JOIN player_stats ps ON p.player_id = ps.player_id
                INNER JOIN recent_matches rm ON ps.match_id = rm.match_id
                WHERE p.is_active = true
                GROUP BY p.player_id, p.player_name, p.team_name, p.vlr_player_id, 
                         p.thespike_player_id, p.role, p.last_updated, p.stats_last_updated
            )
            SELECT 
                player_id,
                player_name,
                team_name,
                vlr_player_id,
                thespike_player_id,
                role,
                last_updated,
                stats_last_updated,
                recent_matches_played,
                recent_avg_rating,
                hours_since_stats_update,
                'high' as priority
            FROM active_players
            WHERE hours_since_stats_update > 6  -- Stats older than 6 hours
            ORDER BY recent_matches_played DESC, hours_since_stats_update DESC
            LIMIT 100
        """
        
        # Priority 2: Players with stale comprehensive data
        stale_players_query = """
            SELECT 
                p.player_id,
                p.player_name,
                p.team_name,
                p.vlr_player_id,
                p.thespike_player_id,
                p.role,
                p.last_updated,
                p.stats_last_updated,
                0 as recent_matches_played,
                0 as recent_avg_rating,
                EXTRACT(EPOCH FROM (NOW() - p.last_updated))/3600 as hours_since_stats_update,
                'medium' as priority
            FROM players p
            WHERE p.is_active = true
              AND (
                  p.last_updated < NOW() - INTERVAL '24 hours'
                  OR p.agent_pool_last_updated < NOW() - INTERVAL '72 hours'
                  OR p.stats_last_updated IS NULL
              )
            ORDER BY p.last_updated ASC NULLS FIRST
            LIMIT 50
        """
        
        # Priority 3: High-profile players (based on collection plan)
        priority_players_from_plan = []
        if collection_plan and 'low_priority_tasks' in collection_plan:
            for task in collection_plan['low_priority_tasks']:
                if task['task_type'] == 'player_updates':
                    # These are already covered by the above queries
                    pass
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                # Get active players
                cursor.execute(active_players_query)
                active_results = cursor.fetchall()
                
                for row in active_results:
                    player_data = dict(row)
                    player_data['scraping_sources'] = []
                    
                    if settings.ENABLE_VLR_SCRAPING and player_data.get('vlr_player_id'):
                        player_data['scraping_sources'].append('vlr')
                    if settings.ENABLE_SPIKE_SCRAPING and player_data.get('thespike_player_id'):
                        player_data['scraping_sources'].append('thespike')
                    
                    if player_data['scraping_sources']:
                        target_players.append(player_data)
                
                # Get stale players
                cursor.execute(stale_players_query)
                stale_results = cursor.fetchall()
                
                for row in stale_results:
                    player_data = dict(row)
                    player_data['scraping_sources'] = []
                    
                    if settings.ENABLE_VLR_SCRAPING and player_data.get('vlr_player_id'):
                        player_data['scraping_sources'].append('vlr')
                    if settings.ENABLE_SPIKE_SCRAPING and player_data.get('thespike_player_id'):
                        player_data['scraping_sources'].append('thespike')
                    
                    if player_data['scraping_sources']:
                        target_players.append(player_data)
        
        # Remove duplicates and sort by priority
        seen_players = set()
        unique_players = []
        
        for player in target_players:
            player_key = player['player_id']
            if player_key not in seen_players:
                seen_players.add(player_key)
                unique_players.append(player)
        
        # Sort by priority and recent activity
        unique_players.sort(key=lambda x: (
            0 if x['priority'] == 'high' else 1,
            -x['recent_matches_played'],
            -x['hours_since_stats_update']
        ))
        
        logger.info(f"Identified {len(unique_players)} players for update:")
        logger.info(f"  - {sum(1 for p in unique_players if p['priority'] == 'high')} high priority")
        logger.info(f"  - {sum(1 for p in unique_players if p['priority'] == 'medium')} medium priority")
        
    except Exception as e:
        logger.error(f"Error identifying target players: {e}")
        target_players = []
    
    return unique_players


def scrape_vlr_player_data(player_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Scrape detailed player data from VLR.gg
    """
    if not settings.ENABLE_VLR_SCRAPING or not player_info.get('vlr_player_id'):
        return None
    
    logger.info(f"Scraping VLR data for player: {player_info['player_name']} ({player_info['team_name']})")
    
    try:
        scraper = VLRScraper()
        
        with RateLimitedRequest('vlr', timeout=45):
            # Build player URL
            player_url = scraping_config.build_url(
                'vlr',
                DataType.PLAYER,
                player_id=player_info['vlr_player_id'],
                player_slug=player_info['player_name'].lower().replace(' ', '-')
            )
            
            if not player_url:
                logger.error(f"Could not build VLR URL for player {player_info['player_id']}")
                return None
            
            # Scrape comprehensive player data
            player_data = scraper.scrape_player_profile(player_url, player_info['player_id'])
            
            if player_data:
                # Extract battle-tested player metrics
                enhanced_data = scraper.extract_player_battle_metrics(player_data)
                enhanced_data['source'] = 'vlr'
                enhanced_data['scraped_at'] = datetime.now()
                
                logger.info(f"Successfully scraped VLR data for player {player_info['player_name']}")
                return enhanced_data
            else:
                logger.warning(f"No data returned from VLR for player {player_info['player_name']}")
                return None
                
    except Exception as e:
        logger.error(f"VLR scraping failed for player {player_info['player_name']}: {e}")
        return None


def scrape_thespike_player_data(player_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Scrape player data from TheSpike.gg
    """
    if not settings.ENABLE_SPIKE_SCRAPING or not player_info.get('thespike_player_id'):
        return None
    
    logger.info(f"Scraping TheSpike data for player: {player_info['player_name']}")
    
    try:
        scraper = TheSpikeScraper()
        
        with RateLimitedRequest('thespike', timeout=30):
            # Build API URL
            api_url = scraping_config.build_url(
                'thespike',
                DataType.PLAYER,
                player_id=player_info['thespike_player_id']
            )
            
            if not api_url:
                logger.error(f"Could not build TheSpike URL for player {player_info['player_id']}")
                return None
            
            # Scrape player data
            player_data = scraper.scrape_player_data(api_url, player_info['player_id'])
            
            if player_data:
                player_data['source'] = 'thespike'
                player_data['scraped_at'] = datetime.now()
                
                logger.info(f"Successfully scraped TheSpike data for player {player_info['player_name']}")
                return player_data
            else:
                logger.warning(f"No data returned from TheSpike for player {player_info['player_name']}")
                return None
                
    except Exception as e:
        logger.error(f"TheSpike scraping failed for player {player_info['player_name']}: {e}")
        return None


def process_player_statistics(**context) -> Dict[str, Any]:
    """
    Process and merge player data from different sources, calculate advanced metrics
    """
    task_instance = context['task_instance']
    
    # Get scraped data from upstream tasks
    vlr_data = task_instance.xcom_pull(task_ids='scraping_group.scrape_vlr_players') or []
    thespike_data = task_instance.xcom_pull(task_ids='scraping_group.scrape_thespike_players') or []
    
    if not vlr_data and not thespike_data:
        logger.warning("No player data to process")
        return {'processed_players': 0, 'failed_players': 0}
    
    logger.info(f"Processing {len(vlr_data)} VLR players and {len(thespike_data)} TheSpike players")
    
    processor = PlayerProcessor()
    processing_results = {
        'processed_players': 0,
        'failed_players': 0,
        'updated_players': 0,
        'new_players': 0,
        'quality_issues': []
    }
    
    try:
        # Process VLR data
        for player_data in vlr_data:
            if not player_data:
                continue
                
            try:
                processed_data = processor.process_player_data(player_data, source='vlr')
                
                # Calculate advanced statistics
                enhanced_data = processor.calculate_advanced_player_metrics(processed_data)
                
                # Validate data quality
                validator = DataQualityValidator()
                quality_check = validator.validate_player_data(enhanced_data)
                
                if quality_check['is_valid']:
                    # Store in database
                    success = processor.store_player_data(enhanced_data)
                    if success:
                        processing_results['processed_players'] += 1
                        if enhanced_data.get('is_new_record'):
                            processing_results['new_players'] += 1
                        else:
                            processing_results['updated_players'] += 1
                    else:
                        processing_results['failed_players'] += 1
                else:
                    processing_results['quality_issues'].extend(quality_check['issues'])
                    processing_results['failed_players'] += 1
                    logger.warning(f"Player data quality issues: {quality_check['issues']}")
                    
            except Exception as e:
                logger.error(f"Error processing VLR player data: {e}")
                processing_results['failed_players'] += 1
        
        # Process TheSpike data
        for player_data in thespike_data:
            if not player_data:
                continue
                
            try:
                processed_data = processor.process_player_data(player_data, source='thespike')
                
                # Check if we already processed this player from VLR
                existing_player = processor.find_existing_player(processed_data['player_id'])
                
                if existing_player:
                    # Merge with existing data
                    merged_data = processor.merge_player_sources(existing_player, processed_data)
                    enhanced_data = processor.calculate_advanced_player_metrics(merged_data)
                    success = processor.store_player_data(enhanced_data)
                    if success:
                        processing_results['updated_players'] += 1
                else:
                    # Validate and store new player
                    enhanced_data = processor.calculate_advanced_player_metrics(processed_data)
                    validator = DataQualityValidator()
                    quality_check = validator.validate_player_data(enhanced_data)
                    
                    if quality_check['is_valid']:
                        success = processor.store_player_data(enhanced_data)
                        if success:
                            processing_results['processed_players'] += 1
                            processing_results['new_players'] += 1
                        else:
                            processing_results['failed_players'] += 1
                    else:
                        processing_results['quality_issues'].extend(quality_check['issues'])
                        processing_results['failed_players'] += 1
                        
            except Exception as e:
                logger.error(f"Error processing TheSpike player data: {e}")
                processing_results['failed_players'] += 1
        
        logger.info(f"Player processing complete: {processing_results}")
        
    except Exception as e:
        logger.error(f"Player data processing failed: {e}")
        processing_results['failed_players'] = len(vlr_data) + len(thespike_data)
    
    return processing_results


def calculate_battle_tested_player_metrics(**context) -> Dict[str, Any]:
    """
    Calculate battle-tested predictive metrics for players
    """
    logger.info("Calculating battle-tested player metrics...")
    
    processor = PlayerProcessor()
    metrics_results = {
        'players_analyzed': 0,
        'metrics_calculated': {},
        'quality_flags': []
    }
    
    try:
        db_manager = DatabaseManager()
        
        # Get players who need metrics calculation
        players_for_metrics_query = """
            WITH player_recent_activity AS (
                SELECT 
                    p.player_id,
                    p.player_name,
                    p.team_name,
                    p.role,
                    COUNT(ps.match_id) as recent_matches,
                    AVG(ps.rating) as avg_rating,
                    AVG(ps.acs) as avg_acs,
                    AVG(ps.adr) as avg_adr,
                    AVG(ps.kills::float / NULLIF(ps.deaths, 0)) as avg_kd,
                    MAX(ps.match_date) as last_match_date
                FROM players p
                INNER JOIN player_stats ps ON p.player_id = ps.player_id
                WHERE ps.match_date >= NOW() - INTERVAL '30 days'
                  AND p.is_active = true
                  AND (p.metrics_calculated = false OR p.metrics_calculated IS NULL)
                GROUP BY p.player_id, p.player_name, p.team_name, p.role
                HAVING COUNT(ps.match_id) >= 10  -- Minimum sample size
            )
            SELECT * FROM player_recent_activity
            ORDER BY recent_matches DESC, last_match_date DESC
            LIMIT 100
        """
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(players_for_metrics_query)
                players_to_analyze = cursor.fetchall()
        
        for player in players_to_analyze:
            try:
                player_id = player['player_id']
                logger.info(f"Calculating metrics for player {player['player_name']} ({player['team_name']})")
                
                # Calculate comprehensive battle-tested metrics
                metrics = processor.calculate_battle_tested_metrics(player_id)
                
                if metrics:
                    # Store calculated metrics
                    success = processor.store_player_metrics(player_id, metrics)
                    if success:
                        metrics_results['players_analyzed'] += 1
                        
                        # Track which metrics were calculated
                        for metric_category in metrics.keys():
                            if metric_category not in metrics_results['metrics_calculated']:
                                metrics_results['metrics_calculated'][metric_category] = 0
                            metrics_results['metrics_calculated'][metric_category] += 1
                    else:
                        metrics_results['quality_flags'].append(f"Failed to store metrics for player {player['player_name']}")
                else:
                    metrics_results['quality_flags'].append(f"No metrics calculated for player {player['player_name']}")
                    
            except Exception as e:
                logger.error(f"Error calculating metrics for player {player['player_name']}: {e}")
                metrics_results['quality_flags'].append(f"Error processing player {player['player_name']}: {str(e)}")
        
        logger.info(f"Player metrics calculation complete: {metrics_results}")
        
    except Exception as e:
        logger.error(f"Battle-tested player metrics calculation failed: {e}")
        metrics_results['quality_flags'].append(f"Overall processing error: {str(e)}")
    
    return metrics_results


def update_agent_pool_analysis(**context) -> Dict[str, Any]:
    """
    Update player agent pools and comfort picks analysis
    """
    logger.info("Updating player agent pool analysis...")
    
    processor = PlayerProcessor()
    agent_results = {
        'players_updated': 0,
        'agent_pools_analyzed': 0,
        'comfort_picks_identified': 0,
        'flexibility_scores_calculated': 0
    }
    
    try:
        db_manager = DatabaseManager()
        
        # Get players with recent agent picks data
        agent_analysis_query = """
            WITH player_agent_stats AS (
                SELECT 
                    p.player_id,
                    p.player_name,
                    p.team_name,
                    ap.agent_name,
                    COUNT(*) as games_played,
                    AVG(ps.rating) as avg_rating_on_agent,
                    AVG(ps.acs) as avg_acs_on_agent,
                    AVG(ps.kills::float / NULLIF(ps.deaths, 0)) as avg_kd_on_agent,
                    AVG(CASE WHEN m.winner_team = p.team_name THEN 1 ELSE 0 END) as win_rate_on_agent,
                    MIN(ps.match_date) as first_played,
                    MAX(ps.match_date) as last_played
                FROM players p
                INNER JOIN player_stats ps ON p.player_id = ps.player_id
                INNER JOIN agent_picks ap ON ps.match_id = ap.match_id AND ps.player_id = ap.player_id
                INNER JOIN matches m ON ps.match_id = m.match_id
                WHERE ps.match_date >= NOW() - INTERVAL '90 days'
                  AND p.is_active = true
                  AND m.match_status = 'completed'
                GROUP BY p.player_id, p.player_name, p.team_name, ap.agent_name
                HAVING COUNT(*) >= 3  -- Minimum games on agent
            ),
            player_agent_pools AS (
                SELECT 
                    player_id,
                    player_name,
                    team_name,
                    COUNT(DISTINCT agent_name) as agent_pool_size,
                    ARRAY_AGG(
                        JSON_BUILD_OBJECT(
                            'agent', agent_name,
                            'games', games_played,
                            'avg_rating', avg_rating_on_agent,
                            'avg_acs', avg_acs_on_agent,
                            'avg_kd', avg_kd_on_agent,
                            'win_rate', win_rate_on_agent,
                            'first_played', first_played,
                            'last_played', last_played
                        ) ORDER BY games_played DESC
                    ) as agent_data
                FROM player_agent_stats
                GROUP BY player_id, player_name, team_name
            )
            SELECT * FROM player_agent_pools
            ORDER BY agent_pool_size DESC
        """
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(agent_analysis_query)
                player_agent_data = cursor.fetchall()
        
        for player_data in player_agent_data:
            try:
                player_id = player_data['player_id']
                player_name = player_data['player_name']
                agent_data = player_data['agent_data']
                
                logger.info(f"Analyzing agent pool for {player_name}: {len(agent_data)} agents")
                
                # Calculate agent pool metrics
                pool_analysis = processor.analyze_agent_pool(player_id, agent_data)
                
                # Identify comfort picks (agents with high performance)
                comfort_picks = processor.identify_comfort_picks(agent_data)
                
                # Calculate flexibility score
                flexibility_score = processor.calculate_player_flexibility(agent_data)
                
                # Analyze role adherence
                role_analysis = processor.analyze_role_adherence(player_id, agent_data)
                
                # Store agent pool analysis
                agent_pool_data = {
                    'player_id': player_id,
                    'agent_pool_size': len(agent_data),
                    'agent_statistics': agent_data,
                    'comfort_picks': comfort_picks,
                    'flexibility_score': flexibility_score,
                    'role_adherence': role_analysis,
                    'pool_analysis': pool_analysis,
                    'last_updated': datetime.now()
                }
                
                success = processor.store_agent_pool_analysis(player_id, agent_pool_data)
                if success:
                    agent_results['players_updated'] += 1
                    agent_results['agent_pools_analyzed'] += 1
                    agent_results['comfort_picks_identified'] += len(comfort_picks)
                    agent_results['flexibility_scores_calculated'] += 1
                
            except Exception as e:
                logger.error(f"Error analyzing agent pool for player {player_data['player_name']}: {e}")
                continue
        
        logger.info(f"Agent pool analysis complete: {agent_results}")
        
    except Exception as e:
        logger.error(f"Agent pool analysis failed: {e}")
        agent_results['error'] = str(e)
    
    return agent_results


def calculate_form_and_volatility(**context) -> Dict[str, Any]:
    """
    Calculate player form trends and performance volatility
    """
    logger.info("Calculating player form and volatility metrics...")
    
    processor = PlayerProcessor()
    form_results = {
        'players_analyzed': 0,
        'form_trends_calculated': 0,
        'volatility_scores_calculated': 0,
        'streak_analysis_completed': 0
    }
    
    try:
        db_manager = DatabaseManager()
        
        # Get players with sufficient recent data for form analysis
        form_analysis_query = """
            SELECT 
                p.player_id,
                p.player_name,
                p.team_name,
                p.role,
                COUNT(ps.match_id) as recent_matches
            FROM players p
            INNER JOIN player_stats ps ON p.player_id = ps.player_id
            WHERE ps.match_date >= NOW() - INTERVAL '30 days'
              AND p.is_active = true
            GROUP BY p.player_id, p.player_name, p.team_name, p.role
            HAVING COUNT(ps.match_id) >= 15  -- Need sufficient data for form analysis
            ORDER BY recent_matches DESC
            LIMIT 150
        """
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(form_analysis_query)
                players_for_form = cursor.fetchall()
        
        for player in players_for_form:
            try:
                player_id = player['player_id']
                player_name = player['player_name']
                
                logger.info(f"Calculating form metrics for {player_name}")
                
                # Get detailed match history for form analysis
                match_history = processor.get_player_match_history(player_id, days=30)
                
                if len(match_history) >= 15:
                    # Calculate form trend (exponential weighted moving average)
                    form_trend = processor.calculate_form_trend(match_history)
                    
                    # Calculate performance volatility
                    volatility_metrics = processor.calculate_performance_volatility(match_history)
                    
                    # Analyze performance streaks
                    streak_analysis = processor.analyze_performance_streaks(match_history)
                    
                    # Calculate clutch performance trends
                    clutch_trends = processor.calculate_clutch_performance_trends(player_id, days=30)
                    
                    # Store form analysis
                    form_data = {
                        'player_id': player_id,
                        'form_trend': form_trend,
                        'volatility_metrics': volatility_metrics,
                        'streak_analysis': streak_analysis,
                        'clutch_trends': clutch_trends,
                        'analysis_period_days': 30,
                        'sample_size': len(match_history),
                        'calculated_at': datetime.now()
                    }
                    
                    success = processor.store_form_analysis(player_id, form_data)
                    if success:
                        form_results['players_analyzed'] += 1
                        form_results['form_trends_calculated'] += 1
                        form_results['volatility_scores_calculated'] += 1
                        form_results['streak_analysis_completed'] += 1
                
            except Exception as e:
                logger.error(f"Error calculating form for player {player['player_name']}: {e}")
                continue
        
        logger.info(f"Form and volatility analysis complete: {form_results}")
        
    except Exception as e:
        logger.error(f"Form and volatility analysis failed: {e}")
        form_results['error'] = str(e)
    
    return form_results


def update_role_performance_analysis(**context) -> Dict[str, Any]:
    """
    Update role-specific performance analysis and comparisons
    """
    logger.info("Updating role-specific performance analysis...")
    
    processor = PlayerProcessor()
    role_results = {
        'roles_analyzed': 0,
        'players_benchmarked': 0,
        'role_rankings_updated': 0
    }
    
    try:
        # Analyze performance by role
        roles = ['Duelist', 'Controller', 'Initiator', 'Sentinel', 'IGL', 'Flex']
        
        for role in roles:
            try:
                logger.info(f"Analyzing {role} performance metrics...")
                
                # Get role-specific benchmarks
                role_benchmarks = processor.calculate_role_benchmarks(role)
                
                # Get players in this role
                role_players = processor.get_players_by_role(role)
                
                # Calculate role-specific performance metrics
                for player_id, player_name in role_players:
                    role_performance = processor.calculate_role_specific_performance(
                        player_id, role, role_benchmarks
                    )
                    
                    if role_performance:
                        success = processor.store_role_performance(player_id, role, role_performance)
                        if success:
                            role_results['players_benchmarked'] += 1
                
                # Update role rankings
                role_rankings = processor.calculate_role_rankings(role, role_benchmarks)
                success = processor.store_role_rankings(role, role_rankings)
                if success:
                    role_results['role_rankings_updated'] += 1
                
                role_results['roles_analyzed'] += 1
                logger.info(f"Completed {role} analysis: {len(role_players)} players benchmarked")
                
            except Exception as e:
                logger.error(f"Error analyzing {role} performance: {e}")
                continue
        
        logger.info(f"Role performance analysis complete: {role_results}")
        
    except Exception as e:
        logger.error(f"Role performance analysis failed: {e}")
        role_results['error'] = str(e)
    
    return role_results


# Task definitions
start_task = EmptyOperator(task_id='start_player_pipeline')

# Identify target players
identify_players_task = PythonOperator(
    task_id='identify_players_for_update',
    python_callable=identify_players_for_update,
    dag=dag,
)

# Data scraping task group
with TaskGroup('scraping_group', dag=dag) as scraping_group:
    
    # VLR player scraping
    scrape_vlr_task = PythonOperator(
        task_id='scrape_vlr_players',
        python_callable=lambda **context: [
            scrape_vlr_player_data(player) 
            for player in context['task_instance'].xcom_pull(task_ids='identify_players_for_update')
            if player and 'vlr' in player.get('scraping_sources', [])
        ],
        dag=dag,
    )
    
    # TheSpike player scraping
    scrape_thespike_task = PythonOperator(
        task_id='scrape_thespike_players',
        python_callable=lambda **context: [
            scrape_thespike_player_data(player) 
            for player in context['task_instance'].xcom_pull(task_ids='identify_players_for_update')
            if player and 'thespike' in player.get('scraping_sources', [])
        ],
        dag=dag,
    )
    
    # Both scraping tasks can run in parallel
    [scrape_vlr_task, scrape_thespike_task]

# Data processing task group
with TaskGroup('processing_group', dag=dag) as processing_group:
    
    # Process basic player statistics
    process_stats_task = PythonOperator(
        task_id='process_player_statistics',
        python_callable=process_player_statistics,
        dag=dag,
    )
    
    # Calculate battle-tested metrics
    calculate_metrics_task = PythonOperator(
        task_id='calculate_battle_tested_metrics',
        python_callable=calculate_battle_tested_player_metrics,
        dag=dag,
    )
    
    process_stats_task >> calculate_metrics_task

# Advanced analytics task group
with TaskGroup('analytics_group', dag=dag) as analytics_group:
    
    # Agent pool analysis
    agent_pool_task = PythonOperator(
        task_id='update_agent_pool_analysis',
        python_callable=update_agent_pool_analysis,
        dag=dag,
    )
    
    # Form and volatility analysis
    form_volatility_task = PythonOperator(
        task_id='calculate_form_and_volatility',
        python_callable=calculate_form_and_volatility,
        dag=dag,
    )
    
    # Role performance analysis
    role_performance_task = PythonOperator(
        task_id='update_role_performance_analysis',
        python_callable=update_role_performance_analysis,
        dag=dag,
    )
    
    # These can run in parallel after basic processing
    [agent_pool_task, form_volatility_task, role_performance_task]

# Data quality checks
quality_check_task = DataQualityOperator(
    task_id='player_data_quality_check',
    tables_to_check=['players', 'player_stats', 'agent_pools', 'player_form_analysis'],
    data_quality_checks=[
        {
            'check_sql': '''
                SELECT COUNT(*) FROM players p
                WHERE p.is_active = true
                  AND p.stats_last_updated >= NOW() - INTERVAL '24 hours'
            ''',
            'expected_result': 'greater_than',
            'expected_value': 50,
            'description': 'At least 50 active players should have recent stat updates'
        },
        {
            'check_sql': '''
                SELECT AVG(
                    CASE WHEN agent_pool_size >= 3 THEN 1 ELSE 0 END
                ) FROM agent_pools ap
                INNER JOIN players p ON ap.player_id = p.player_id
                WHERE p.is_active = true
                  AND ap.last_updated >= NOW() - INTERVAL '72 hours'
            ''',
            'expected_result': 'greater_than',
            'expected_value': 0.7,
            'description': 'At least 70% of players should have reasonable agent pools (3+ agents)'
        },
        {
            'check_sql': '''
                SELECT COUNT(*) FROM player_stats ps
                WHERE ps.match_date >= NOW() - INTERVAL '24 hours'
                  AND ps.rating IS NOT NULL
                  AND ps.acs > 0
                  AND ps.kills >= 0
                  AND ps.deaths > 0
            ''',
            'expected_result': 'greater_than',
            'expected_value': 0,
            'description': 'Recent player stats should have valid core metrics'
        },
        {
            'check_sql': '''
                SELECT AVG(
                    CASE WHEN volatility_score BETWEEN 0 AND 2 THEN 1 ELSE 0 END
                ) FROM player_form_analysis
                WHERE calculated_at >= NOW() - INTERVAL '48 hours'
            ''',
            'expected_result': 'greater_than',
            'expected_value': 0.8,
            'description': 'Player volatility scores should be within reasonable range'
        }
    ],
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end_player_pipeline',
    trigger_rule=TriggerRule.ALL_DONE,
)

# Define task dependencies
start_task >> identify_players_task >> scraping_group
scraping_group >> processing_group >> analytics_group
analytics_group >> quality_check_task >> end_task
"""
Valorant Patch & Meta Data Collection DAG
Tracks game version changes, agent/map updates, balance patches, and meta shifts
that affect the predictive value of historical data.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging
import json
import re

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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
from plugins.utils.scrapers.patch_scraper import PatchScraper
from plugins.utils.processors.meta_processor import MetaProcessor
from plugins.utils.storage.database_manager import DatabaseManager
from plugins.utils.validators.data_quality import DataQualityValidator

# Set up logging
logger = logging.getLogger(__name__)

# DAG configuration
DAG_ID = 'valorant_patch_data_pipeline'
SCHEDULE_INTERVAL = settings.pipeline.PATCH_DATA_SCHEDULE  # Weekly on Monday at noon
MAX_ACTIVE_RUNS = 1
CATCHUP = True  # Important for patch data to catch historical patches

# Default arguments
default_args = {
    'owner': 'valorant-data-team',
    'depends_on_past': False,
    'start_date': days_ago(30),  # Start from 30 days ago to catch recent patches
    'email_on_failure': settings.EMAIL_NOTIFICATIONS_ENABLED,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(hours=1),
    'execution_timeout': timedelta(hours=2),
    'sla': timedelta(days=1),  # Patch data is less time-sensitive
}

# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Valorant patch and meta data collection pipeline',
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=MAX_ACTIVE_RUNS,
    catchup=CATCHUP,
    tags=['valorant', 'patches', 'meta', 'balance'],
    doc_md=__doc__,
)


def check_for_new_patches(**context) -> List[Dict[str, Any]]:
    """
    Check for new Valorant patches and updates that need processing
    """
    logger.info("Checking for new Valorant patches...")
    
    new_patches = []
    
    try:
        # Check official Riot patch notes
        patch_scraper = PatchScraper()
        
        # Get the last processed patch from database
        db_manager = DatabaseManager()
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT patch_version, release_date 
                    FROM patches 
                    ORDER BY release_date DESC 
                    LIMIT 1
                """)
                last_patch = cursor.fetchone()
                
                last_patch_date = last_patch['release_date'] if last_patch else datetime(2020, 1, 1)
        
        # Scrape recent patch information
        with RateLimitedRequest('riot_api', timeout=60):
            recent_patches = patch_scraper.get_recent_patches(since_date=last_patch_date)
        
        for patch in recent_patches:
            if patch['release_date'] > last_patch_date:
                logger.info(f"Found new patch: {patch['version']} released on {patch['release_date']}")
                new_patches.append(patch)
        
        logger.info(f"Found {len(new_patches)} new patches to process")
        
    except Exception as e:
        logger.error(f"Error checking for new patches: {e}")
        # Return empty list to allow graceful continuation
    
    return new_patches


def scrape_patch_notes(**context) -> List[Dict[str, Any]]:
    """
    Scrape detailed patch notes and extract relevant changes
    """
    patches_to_process = context['task_instance'].xcom_pull(task_ids='check_new_patches')
    
    if not patches_to_process:
        logger.info("No new patches to process")
        raise AirflowSkipException("No new patches found")
    
    logger.info(f"Scraping patch notes for {len(patches_to_process)} patches")
    
    processed_patches = []
    patch_scraper = PatchScraper()
    
    for patch_info in patches_to_process:
        try:
            logger.info(f"Processing patch {patch_info['version']}")
            
            with RateLimitedRequest('riot_api', timeout=120):
                # Scrape detailed patch notes
                patch_details = patch_scraper.scrape_patch_details(patch_info['patch_url'])
                
                if patch_details:
                    # Extract structured information
                    structured_patch = {
                        'version': patch_info['version'],
                        'release_date': patch_info['release_date'],
                        'patch_type': patch_scraper.classify_patch_type(patch_details),
                        'agent_changes': patch_scraper.extract_agent_changes(patch_details),
                        'map_changes': patch_scraper.extract_map_changes(patch_details),
                        'weapon_changes': patch_scraper.extract_weapon_changes(patch_details),
                        'economy_changes': patch_scraper.extract_economy_changes(patch_details),
                        'bug_fixes': patch_scraper.extract_bug_fixes(patch_details),
                        'new_content': patch_scraper.extract_new_content(patch_details),
                        'meta_impact_score': patch_scraper.calculate_meta_impact(patch_details),
                        'raw_notes': patch_details['raw_content'],
                        'scraped_at': datetime.now()
                    }
                    
                    processed_patches.append(structured_patch)
                    logger.info(f"Successfully processed patch {patch_info['version']}")
                else:
                    logger.warning(f"Could not scrape details for patch {patch_info['version']}")
                    
        except Exception as e:
            logger.error(f"Error processing patch {patch_info.get('version', 'unknown')}: {e}")
            # Continue with other patches
            continue
    
    return processed_patches


def analyze_meta_shifts(**context) -> Dict[str, Any]:
    """
    Analyze meta shifts and their impact on data validity windows
    """
    logger.info("Analyzing meta shifts and data validity windows...")
    
    processed_patches = context['task_instance'].xcom_pull(task_ids='scraping_group.scrape_patch_notes')
    
    if not processed_patches:
        logger.info("No patch data to analyze")
        return {'meta_shifts': [], 'data_windows': []}
    
    meta_processor = MetaProcessor()
    analysis_results = {
        'meta_shifts': [],
        'data_windows': [],
        'agent_tier_changes': {},
        'map_rotation_updates': [],
        'balance_impact_scores': {}
    }
    
    try:
        # Analyze each patch for meta impact
        for patch in processed_patches:
            logger.info(f"Analyzing meta impact for patch {patch['version']}")
            
            # Calculate meta shift severity
            meta_impact = meta_processor.analyze_patch_impact(patch)
            
            if meta_impact['severity'] >= 0.3:  # Significant meta shift
                shift_info = {
                    'patch_version': patch['version'],
                    'release_date': patch['release_date'],
                    'severity': meta_impact['severity'],
                    'affected_agents': meta_impact['affected_agents'],
                    'affected_maps': meta_impact['affected_maps'],
                    'impact_categories': meta_impact['categories'],
                    'data_validity_cutoff': patch['release_date'],
                    'description': meta_impact['description']
                }
                analysis_results['meta_shifts'].append(shift_info)
                logger.info(f"Detected significant meta shift in patch {patch['version']} (severity: {meta_impact['severity']:.2f})")
            
            # Track agent tier changes
            agent_changes = meta_processor.analyze_agent_balance_changes(patch)
            if agent_changes:
                analysis_results['agent_tier_changes'][patch['version']] = agent_changes
            
            # Track map changes
            map_changes = meta_processor.analyze_map_changes(patch)
            if map_changes:
                analysis_results['map_rotation_updates'].extend(map_changes)
            
            # Store balance impact scores
            analysis_results['balance_impact_scores'][patch['version']] = meta_impact['severity']
        
        # Calculate data validity windows
        data_windows = meta_processor.calculate_data_validity_windows(analysis_results['meta_shifts'])
        analysis_results['data_windows'] = data_windows
        
        logger.info(f"Meta analysis complete: {len(analysis_results['meta_shifts'])} significant shifts detected")
        
    except Exception as e:
        logger.error(f"Meta shift analysis failed: {e}")
        # Return partial results
    
    return analysis_results


def update_agent_meta_tracking(**context) -> Dict[str, Any]:
    """
    Update agent pick rates, win rates, and meta tier tracking
    """
    logger.info("Updating agent meta tracking...")
    
    meta_processor = MetaProcessor()
    tracking_results = {
        'agents_updated': 0,
        'pick_rate_changes': [],
        'tier_movements': [],
        'meta_snapshots': []
    }
    
    try:
        db_manager = DatabaseManager()
        
        # Get recent match data for meta analysis
        recent_matches_query = """
            SELECT 
                ap.agent_name,
                ap.map_name,
                ap.team_side,
                COUNT(*) as pick_count,
                AVG(CASE WHEN m.winner_team = ap.team_name THEN 1 ELSE 0 END) as win_rate,
                m.patch_version
            FROM agent_picks ap
            JOIN matches m ON ap.match_id = m.match_id
            WHERE m.match_date >= NOW() - INTERVAL '30 days'
              AND m.match_status = 'completed'
              AND m.patch_version IS NOT NULL
            GROUP BY ap.agent_name, ap.map_name, ap.team_side, m.patch_version
            HAVING COUNT(*) >= 10  -- Minimum sample size
            ORDER BY m.patch_version DESC, pick_count DESC
        """
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(recent_matches_query)
                agent_stats = cursor.fetchall()
        
        # Process agent statistics by patch
        current_patch = None
        patch_agent_data = {}
        
        for stat in agent_stats:
            patch_version = stat['patch_version']
            agent_name = stat['agent_name']
            
            if patch_version != current_patch:
                if current_patch and patch_agent_data:
                    # Process completed patch data
                    meta_snapshot = meta_processor.create_meta_snapshot(current_patch, patch_agent_data)
                    tracking_results['meta_snapshots'].append(meta_snapshot)
                
                current_patch = patch_version
                patch_agent_data = {}
            
            # Aggregate agent data for this patch
            if agent_name not in patch_agent_data:
                patch_agent_data[agent_name] = {
                    'total_picks': 0,
                    'total_wins': 0,
                    'map_performance': {},
                    'side_performance': {'attack': {'picks': 0, 'wins': 0}, 'defense': {'picks': 0, 'wins': 0}}
                }
            
            agent_data = patch_agent_data[agent_name]
            agent_data['total_picks'] += stat['pick_count']
            agent_data['total_wins'] += stat['pick_count'] * stat['win_rate']
            
            # Map-specific performance
            map_name = stat['map_name']
            if map_name not in agent_data['map_performance']:
                agent_data['map_performance'][map_name] = {'picks': 0, 'wins': 0}
            
            agent_data['map_performance'][map_name]['picks'] += stat['pick_count']
            agent_data['map_performance'][map_name]['wins'] += stat['pick_count'] * stat['win_rate']
            
            # Side-specific performance
            side = stat['team_side'].lower()
            if side in agent_data['side_performance']:
                agent_data['side_performance'][side]['picks'] += stat['pick_count']
                agent_data['side_performance'][side]['wins'] += stat['pick_count'] * stat['win_rate']
        
        # Process the last patch
        if current_patch and patch_agent_data:
            meta_snapshot = meta_processor.create_meta_snapshot(current_patch, patch_agent_data)
            tracking_results['meta_snapshots'].append(meta_snapshot)
        
        # Calculate tier movements and pick rate changes
        if len(tracking_results['meta_snapshots']) >= 2:
            current_meta = tracking_results['meta_snapshots'][0]  # Most recent
            previous_meta = tracking_results['meta_snapshots'][1]  # Previous patch
            
            tier_changes = meta_processor.calculate_tier_movements(previous_meta, current_meta)
            tracking_results['tier_movements'] = tier_changes
            
            pick_rate_changes = meta_processor.calculate_pick_rate_changes(previous_meta, current_meta)
            tracking_results['pick_rate_changes'] = pick_rate_changes
        
        # Store updated meta tracking data
        for snapshot in tracking_results['meta_snapshots']:
            success = meta_processor.store_meta_snapshot(snapshot)
            if success:
                tracking_results['agents_updated'] += len(snapshot.get('agent_data', {}))
        
        logger.info(f"Agent meta tracking updated: {tracking_results['agents_updated']} agent entries processed")
        
    except Exception as e:
        logger.error(f"Agent meta tracking update failed: {e}")
        tracking_results['error'] = str(e)
    
    return tracking_results


def update_map_meta_tracking(**context) -> Dict[str, Any]:
    """
    Update map pick rates, side win rates, and rotation tracking
    """
    logger.info("Updating map meta tracking...")
    
    meta_processor = MetaProcessor()
    map_tracking_results = {
        'maps_updated': 0,
        'side_balance_changes': [],
        'rotation_updates': [],
        'competitive_viability': {}
    }
    
    try:
        db_manager = DatabaseManager()
        
        # Get map performance data
        map_stats_query = """
            SELECT 
                mm.map_name,
                mm.attacking_team,
                mm.defending_team,
                mm.attacking_score,
                mm.defending_score,
                m.patch_version,
                COUNT(*) as games_played,
                AVG(mm.attacking_score::float / (mm.attacking_score + mm.defending_score)) as avg_attack_win_rate
            FROM match_maps mm
            JOIN matches m ON mm.match_id = m.match_id
            WHERE m.match_date >= NOW() - INTERVAL '60 days'
              AND m.match_status = 'completed'
              AND mm.attacking_score + mm.defending_score > 0
            GROUP BY mm.map_name, mm.attacking_team, mm.defending_team, 
                     mm.attacking_score, mm.defending_score, m.patch_version
            HAVING COUNT(*) >= 5
            ORDER BY m.patch_version DESC, mm.map_name
        """
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(map_stats_query)
                map_stats = cursor.fetchall()
        
        # Aggregate map statistics by patch
        map_data_by_patch = {}
        
        for stat in map_stats:
            patch_version = stat['patch_version']
            map_name = stat['map_name']
            
            if patch_version not in map_data_by_patch:
                map_data_by_patch[patch_version] = {}
            
            if map_name not in map_data_by_patch[patch_version]:
                map_data_by_patch[patch_version][map_name] = {
                    'total_games': 0,
                    'attack_wins': 0,
                    'defense_wins': 0,
                    'pick_count': 0
                }
            
            map_data = map_data_by_patch[patch_version][map_name]
            
            # Calculate wins based on scores
            attacking_score = stat['attacking_score']
            defending_score = stat['defending_score']
            games_count = stat['games_played']
            
            if attacking_score > defending_score:
                map_data['attack_wins'] += games_count
            else:
                map_data['defense_wins'] += games_count
            
            map_data['total_games'] += games_count
            map_data['pick_count'] += games_count
        
        # Process each patch's map meta
        for patch_version, patch_maps in map_data_by_patch.items():
            try:
                # Calculate side balance for each map
                side_balance_data = {}
                
                for map_name, map_stats in patch_maps.items():
                    if map_stats['total_games'] >= 20:  # Minimum sample size
                        attack_win_rate = map_stats['attack_wins'] / map_stats['total_games']
                        defense_win_rate = map_stats['defense_wins'] / map_stats['total_games']
                        
                        side_balance_data[map_name] = {
                            'attack_win_rate': attack_win_rate,
                            'defense_win_rate': defense_win_rate,
                            'balance_score': abs(0.5 - attack_win_rate),  # Closer to 0 = more balanced
                            'total_games': map_stats['total_games'],
                            'pick_rate': map_stats['pick_count'] / sum(m['pick_count'] for m in patch_maps.values())
                        }
                
                # Store map meta snapshot
                map_snapshot = {
                    'patch_version': patch_version,
                    'timestamp': datetime.now(),
                    'map_data': side_balance_data,
                    'total_games': sum(m['total_games'] for m in patch_maps.values())
                }
                
                success = meta_processor.store_map_meta_snapshot(map_snapshot)
                if success:
                    map_tracking_results['maps_updated'] += len(side_balance_data)
                
                # Identify significant balance changes
                for map_name, balance_data in side_balance_data.items():
                    if balance_data['balance_score'] > 0.15:  # More than 15% imbalance
                        map_tracking_results['side_balance_changes'].append({
                            'patch_version': patch_version,
                            'map_name': map_name,
                            'attack_win_rate': balance_data['attack_win_rate'],
                            'imbalance_severity': balance_data['balance_score'],
                            'sample_size': balance_data['total_games']
                        })
                
                logger.info(f"Processed map meta for patch {patch_version}: {len(side_balance_data)} maps")
                
            except Exception as e:
                logger.error(f"Error processing map meta for patch {patch_version}: {e}")
                continue
        
        logger.info(f"Map meta tracking updated: {map_tracking_results}")
        
    except Exception as e:
        logger.error(f"Map meta tracking update failed: {e}")
        map_tracking_results['error'] = str(e)
    
    return map_tracking_results


def invalidate_stale_data(**context) -> Dict[str, Any]:
    """
    Mark data as stale when significant meta shifts occur
    """
    logger.info("Identifying and marking stale data due to meta shifts...")
    
    meta_analysis = context['task_instance'].xcom_pull(task_ids='processing_group.analyze_meta_shifts')
    
    if not meta_analysis or not meta_analysis.get('meta_shifts'):
        logger.info("No significant meta shifts detected - no data invalidation needed")
        return {'invalidated_records': 0, 'affected_patches': []}
    
    invalidation_results = {
        'invalidated_records': 0,
        'affected_patches': [],
        'data_windows_updated': 0
    }
    
    try:
        db_manager = DatabaseManager()
        meta_processor = MetaProcessor()
        
        for meta_shift in meta_analysis['meta_shifts']:
            patch_version = meta_shift['patch_version']
            cutoff_date = meta_shift['data_validity_cutoff']
            severity = meta_shift['severity']
            
            logger.info(f"Processing meta shift for patch {patch_version} (severity: {severity:.2f})")
            
            # Update data validity flags based on severity
            if severity >= 0.7:  # Major meta shift
                validity_window = timedelta(days=7)  # Only last 7 days of data remain valid
            elif severity >= 0.5:  # Moderate meta shift
                validity_window = timedelta(days=14)  # Last 2 weeks remain valid
            else:  # Minor meta shift
                validity_window = timedelta(days=30)  # Last month remains valid
            
            validity_cutoff = cutoff_date - validity_window
            
            with db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Mark matches as potentially stale
                    match_update_query = """
                        UPDATE matches 
                        SET data_validity_flag = 'stale_meta',
                            meta_shift_patch = %s,
                            meta_shift_severity = %s
                        WHERE match_date < %s 
                          AND (data_validity_flag IS NULL OR data_validity_flag = 'valid')
                          AND patch_version != %s
                    """
                    
                    cursor.execute(match_update_query, (
                        patch_version, severity, validity_cutoff, patch_version
                    ))
                    invalidated_matches = cursor.rowcount
                    
                    # Mark player stats as potentially stale
                    stats_update_query = """
                        UPDATE player_stats 
                        SET data_validity_flag = 'stale_meta',
                            meta_shift_patch = %s
                        WHERE match_date < %s
                          AND (data_validity_flag IS NULL OR data_validity_flag = 'valid')
                    """
                    
                    cursor.execute(stats_update_query, (patch_version, validity_cutoff))
                    invalidated_stats = cursor.rowcount
                    
                    # Update team performance cache validity
                    team_cache_query = """
                        UPDATE team_stats 
                        SET data_validity_flag = 'stale_meta',
                            needs_recalculation = true
                        WHERE last_calculated < %s
                    """
                    
                    cursor.execute(team_cache_query, (validity_cutoff,))
                    invalidated_team_stats = cursor.rowcount
                    
                    conn.commit()
                    
                    total_invalidated = invalidated_matches + invalidated_stats + invalidated_team_stats
                    invalidation_results['invalidated_records'] += total_invalidated
                    
                    logger.info(f"Patch {patch_version}: invalidated {total_invalidated} records")
            
            # Store data validity window information
            validity_window_data = {
                'patch_version': patch_version,
                'meta_shift_date': cutoff_date,
                'severity': severity,
                'validity_window_days': validity_window.days,
                'affected_categories': meta_shift['impact_categories']
            }
            
            success = meta_processor.store_data_validity_window(validity_window_data)
            if success:
                invalidation_results['data_windows_updated'] += 1
            
            invalidation_results['affected_patches'].append(patch_version)
        
        logger.info(f"Data invalidation complete: {invalidation_results}")
        
    except Exception as e:
        logger.error(f"Data invalidation failed: {e}")
        invalidation_results['error'] = str(e)
    
    return invalidation_results


# Task definitions
start_task = EmptyOperator(task_id='start_patch_pipeline')

# Check for new patches
check_patches_task = PythonOperator(
    task_id='check_new_patches',
    python_callable=check_for_new_patches,
    dag=dag,
)

# Scraping task group
with TaskGroup('scraping_group', dag=dag) as scraping_group:
    
    # Scrape patch notes and details
    scrape_patch_notes_task = PythonOperator(
        task_id='scrape_patch_notes',
        python_callable=scrape_patch_notes,
        dag=dag,
    )

# Processing task group
with TaskGroup('processing_group', dag=dag) as processing_group:
    
    # Analyze meta shifts
    analyze_meta_task = PythonOperator(
        task_id='analyze_meta_shifts',
        python_callable=analyze_meta_shifts,
        dag=dag,
    )
    
    # Update agent meta tracking
    update_agent_meta_task = PythonOperator(
        task_id='update_agent_meta',
        python_callable=update_agent_meta_tracking,
        dag=dag,
    )
    
    # Update map meta tracking
    update_map_meta_task = PythonOperator(
        task_id='update_map_meta',
        python_callable=update_map_meta_tracking,
        dag=dag,
    )
    
    analyze_meta_task >> [update_agent_meta_task, update_map_meta_task]

# Data invalidation
invalidate_data_task = PythonOperator(
    task_id='invalidate_stale_data',
    python_callable=invalidate_stale_data,
    dag=dag,
)

# Data quality checks
quality_check_task = DataQualityOperator(
    task_id='patch_data_quality_check',
    tables_to_check=['patches', 'agent_meta_snapshots', 'map_meta_snapshots'],
    data_quality_checks=[
        {
            'check_sql': 'SELECT COUNT(*) FROM patches WHERE release_date >= NOW() - INTERVAL \'90 days\'',
            'expected_result': 'greater_than',
            'expected_value': 0,
            'description': 'Should have patch data from last 90 days'
        },
        {
            'check_sql': '''
                SELECT COUNT(DISTINCT patch_version) 
                FROM agent_meta_snapshots 
                WHERE created_at >= NOW() - INTERVAL '30 days'
            ''',
            'expected_result': 'greater_than',
            'expected_value': 0,
            'description': 'Should have recent agent meta snapshots'
        }
    ],
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end_patch_pipeline',
    trigger_rule=TriggerRule.ALL_DONE,
)

# Define task dependencies
start_task >> check_patches_task >> scraping_group
scraping_group >> processing_group >> invalidate_data_task
invalidate_data_task >> quality_check_task >> end_task
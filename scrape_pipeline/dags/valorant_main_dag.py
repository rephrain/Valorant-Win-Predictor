"""
Main Valorant data pipeline DAG that orchestrates all data collection and processing tasks.
This is the master scheduler that coordinates match data, player stats, team info, and meta analysis.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

# Import our custom operators and hooks
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from config.settings import settings
from plugins.operators.data_quality_operator import DataQualityOperator
from plugins.operators.notification_operator import NotificationOperator
from plugins.utils.storage.database_manager import DatabaseManager
from plugins.utils.validators.data_quality import DataQualityValidator

# Set up logging
logger = logging.getLogger(__name__)

# DAG configuration
DAG_ID = 'valorant_main_pipeline'
SCHEDULE_INTERVAL = settings.pipeline.MAIN_DAG_SCHEDULE  # Every 6 hours
MAX_ACTIVE_RUNS = 1
CATCHUP = False

# Default arguments for all tasks
default_args = {
    'owner': 'valorant-data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': settings.EMAIL_NOTIFICATIONS_ENABLED,
    'email_on_retry': False,
    'retries': settings.pipeline.DEFAULT_RETRIES,
    'retry_delay': settings.pipeline.RETRY_DELAY,
    'retry_exponential_backoff': settings.pipeline.RETRY_EXPONENTIAL_BACKOFF,
    'max_retry_delay': timedelta(hours=1),
    'execution_timeout': settings.pipeline.DEFAULT_TASK_TIMEOUT,
    'sla': timedelta(hours=8),  # SLA for the entire pipeline
}

# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Main Valorant data collection and processing pipeline',
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=MAX_ACTIVE_RUNS,
    catchup=CATCHUP,
    tags=['valorant', 'esports', 'main-pipeline', 'data-collection'],
    doc_md=__doc__,
)


def check_pipeline_prerequisites(**context) -> bool:
    """
    Check if all prerequisites are met before starting the pipeline
    """
    logger.info("Checking pipeline prerequisites...")
    
    issues = []
    
    # Check database connectivity
    try:
        from config.database import test_database_connection
        if not test_database_connection():
            issues.append("Database connection failed")
    except Exception as e:
        issues.append(f"Database check error: {str(e)}")
    
    # Check data source availability
    data_sources = settings.get_data_sources()
    enabled_sources = [source for source, enabled in data_sources.items() if enabled]
    
    if not enabled_sources:
        issues.append("No data sources enabled")
    else:
        logger.info(f"Enabled data sources: {enabled_sources}")
    
    # Check disk space
    try:
        import shutil
        disk_usage = shutil.disk_usage('/')
        free_gb = disk_usage.free / (1024**3)
        if free_gb < 10:  # Less than 10GB free
            issues.append(f"Low disk space: {free_gb:.1f}GB free")
    except Exception as e:
        logger.warning(f"Could not check disk space: {e}")
    
    # Check rate limiting status
    try:
        from config.rate_limits import get_rate_limiting_stats
        stats = get_rate_limiting_stats()
        
        # Check for any sources with high error rates
        for source, source_stats in stats.items():
            error_rate = source_stats.get('errors', 0) / max(source_stats.get('total_requests', 1), 1)
            if error_rate > 0.1:  # More than 10% error rate
                issues.append(f"High error rate for {source}: {error_rate:.1%}")
    except Exception as e:
        logger.warning(f"Could not check rate limiting stats: {e}")
    
    if issues:
        logger.error(f"Pipeline prerequisites failed: {issues}")
        # Store issues in XCom for downstream tasks
        context['task_instance'].xcom_push(key='prerequisite_issues', value=issues)
        return False
    
    logger.info("All prerequisites satisfied")
    return True


def prioritize_data_collection(**context) -> Dict[str, Any]:
    """
    Determine collection priorities based on current events and data freshness
    """
    logger.info("Determining data collection priorities...")
    
    priorities = {
        'live_matches': [],
        'recent_matches': [],
        'upcoming_matches': [],
        'stale_team_data': [],
        'stale_player_data': [],
        'priority_events': [],
    }
    
    try:
        db_manager = DatabaseManager()
        
        # Find live and recent matches that need updates
        live_matches_query = """
            SELECT match_id, event_name, team1_name, team2_name, last_updated
            FROM matches 
            WHERE match_status IN ('live', 'ongoing') 
               OR (match_status = 'scheduled' AND match_date <= NOW() + INTERVAL '1 hour')
            ORDER BY match_date ASC
            LIMIT 50
        """
        
        recent_matches_query = """
            SELECT match_id, event_name, team1_name, team2_name, last_updated
            FROM matches 
            WHERE match_date >= NOW() - INTERVAL '24 hours'
              AND (last_updated IS NULL OR last_updated < NOW() - INTERVAL '2 hours')
              AND match_status = 'completed'
            ORDER BY match_date DESC
            LIMIT 100
        """
        
        # Find teams with stale data
        stale_teams_query = """
            SELECT team_id, team_name, last_updated
            FROM teams 
            WHERE last_updated < NOW() - INTERVAL '12 hours'
               OR roster_last_updated < NOW() - INTERVAL '24 hours'
            ORDER BY last_updated ASC NULLS FIRST
            LIMIT 50
        """
        
        # Find players with stale data
        stale_players_query = """
            SELECT p.player_id, p.player_name, p.team_name, p.last_updated
            FROM players p
            WHERE p.last_updated < NOW() - INTERVAL '24 hours'
               OR p.stats_last_updated < NOW() - INTERVAL '6 hours'
            ORDER BY p.last_updated ASC NULLS FIRST
            LIMIT 100
        """
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                # Get live matches
                cursor.execute(live_matches_query)
                priorities['live_matches'] = [dict(row) for row in cursor.fetchall()]
                
                # Get recent matches
                cursor.execute(recent_matches_query)
                priorities['recent_matches'] = [dict(row) for row in cursor.fetchall()]
                
                # Get stale teams
                cursor.execute(stale_teams_query)
                priorities['stale_team_data'] = [dict(row) for row in cursor.fetchall()]
                
                # Get stale players
                cursor.execute(stale_players_query)
                priorities['stale_player_data'] = [dict(row) for row in cursor.fetchall()]
        
        # Log priorities
        logger.info(f"Found {len(priorities['live_matches'])} live matches")
        logger.info(f"Found {len(priorities['recent_matches'])} recent matches to update")
        logger.info(f"Found {len(priorities['stale_team_data'])} teams with stale data")
        logger.info(f"Found {len(priorities['stale_player_data'])} players with stale data")
        
    except Exception as e:
        logger.error(f"Error determining priorities: {e}")
        # Return empty priorities if there's an error
        priorities = {key: [] for key in priorities.keys()}
    
    return priorities


def check_data_freshness(**context) -> Dict[str, Any]:
    """
    Check data freshness across all major data types
    """
    logger.info("Checking data freshness...")
    
    freshness_report = {
        'matches': {'fresh': 0, 'stale': 0, 'missing': 0},
        'teams': {'fresh': 0, 'stale': 0, 'missing': 0},
        'players': {'fresh': 0, 'stale': 0, 'missing': 0},
        'events': {'fresh': 0, 'stale': 0, 'missing': 0},
        'overall_health': 'unknown'
    }
    
    try:
        db_manager = DatabaseManager()
        
        freshness_queries = {
            'matches': """
                SELECT 
                    COUNT(CASE WHEN last_updated >= NOW() - INTERVAL '2 hours' THEN 1 END) as fresh,
                    COUNT(CASE WHEN last_updated < NOW() - INTERVAL '2 hours' THEN 1 END) as stale,
                    COUNT(CASE WHEN last_updated IS NULL THEN 1 END) as missing
                FROM matches 
                WHERE match_date >= NOW() - INTERVAL '7 days'
            """,
            'teams': """
                SELECT 
                    COUNT(CASE WHEN last_updated >= NOW() - INTERVAL '12 hours' THEN 1 END) as fresh,
                    COUNT(CASE WHEN last_updated < NOW() - INTERVAL '12 hours' THEN 1 END) as stale,
                    COUNT(CASE WHEN last_updated IS NULL THEN 1 END) as missing
                FROM teams 
                WHERE is_active = true
            """,
            'players': """
                SELECT 
                    COUNT(CASE WHEN last_updated >= NOW() - INTERVAL '24 hours' THEN 1 END) as fresh,
                    COUNT(CASE WHEN last_updated < NOW() - INTERVAL '24 hours' THEN 1 END) as stale,
                    COUNT(CASE WHEN last_updated IS NULL THEN 1 END) as missing
                FROM players 
                WHERE is_active = true
            """,
            'events': """
                SELECT 
                    COUNT(CASE WHEN last_updated >= NOW() - INTERVAL '24 hours' THEN 1 END) as fresh,
                    COUNT(CASE WHEN last_updated < NOW() - INTERVAL '24 hours' THEN 1 END) as stale,
                    COUNT(CASE WHEN last_updated IS NULL THEN 1 END) as missing
                FROM events 
                WHERE end_date >= NOW() - INTERVAL '30 days'
            """
        }
        
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                for data_type, query in freshness_queries.items():
                    cursor.execute(query)
                    result = cursor.fetchone()
                    freshness_report[data_type] = {
                        'fresh': result[0] or 0,
                        'stale': result[1] or 0,
                        'missing': result[2] or 0
                    }
        
        # Calculate overall health
        total_records = sum(
            sum(counts.values()) for counts in freshness_report.values() 
            if isinstance(counts, dict) and 'fresh' in counts
        )
        
        total_fresh = sum(
            counts['fresh'] for counts in freshness_report.values() 
            if isinstance(counts, dict) and 'fresh' in counts
        )
        
        if total_records > 0:
            freshness_ratio = total_fresh / total_records
            if freshness_ratio >= 0.8:
                freshness_report['overall_health'] = 'excellent'
            elif freshness_ratio >= 0.6:
                freshness_report['overall_health'] = 'good'
            elif freshness_ratio >= 0.4:
                freshness_report['overall_health'] = 'fair'
            else:
                freshness_report['overall_health'] = 'poor'
        
        logger.info(f"Data freshness check complete. Overall health: {freshness_report['overall_health']}")
        
    except Exception as e:
        logger.error(f"Data freshness check failed: {e}")
        freshness_report['overall_health'] = 'error'
    
    return freshness_report


def generate_collection_plan(**context) -> Dict[str, Any]:
    """
    Generate the optimal data collection plan based on priorities and resources
    """
    logger.info("Generating data collection plan...")
    
    # Get priorities from previous task
    priorities = context['task_instance'].xcom_pull(task_ids='prioritize_data_collection')
    freshness = context['task_instance'].xcom_pull(task_ids='check_data_freshness')
    
    collection_plan = {
        'immediate_tasks': [],
        'scheduled_tasks': [],
        'low_priority_tasks': [],
        'estimated_duration': 0,
        'resource_allocation': {
            'match_data_workers': 2,
            'team_data_workers': 1,
            'player_data_workers': 1,
            'meta_data_workers': 1,
        }
    }
    
    try:
        # Immediate tasks (high priority)
        if priorities.get('live_matches'):
            collection_plan['immediate_tasks'].append({
                'task_type': 'live_matches',
                'count': len(priorities['live_matches']),
                'estimated_minutes': len(priorities['live_matches']) * 2
            })
        
        if priorities.get('recent_matches'):
            collection_plan['immediate_tasks'].append({
                'task_type': 'recent_matches', 
                'count': min(20, len(priorities['recent_matches'])),  # Limit to top 20
                'estimated_minutes': min(20, len(priorities['recent_matches'])) * 3
            })
        
        # Scheduled tasks (medium priority)
        if priorities.get('stale_team_data'):
            collection_plan['scheduled_tasks'].append({
                'task_type': 'team_updates',
                'count': min(10, len(priorities['stale_team_data'])),  # Limit to top 10
                'estimated_minutes': min(10, len(priorities['stale_team_data'])) * 5
            })
        
        # Low priority tasks
        if priorities.get('stale_player_data'):
            collection_plan['low_priority_tasks'].append({
                'task_type': 'player_updates',
                'count': min(25, len(priorities['stale_player_data'])),  # Limit to top 25
                'estimated_minutes': min(25, len(priorities['stale_player_data'])) * 2
            })
        
        # Calculate total estimated duration
        all_tasks = (collection_plan['immediate_tasks'] + 
                    collection_plan['scheduled_tasks'] + 
                    collection_plan['low_priority_tasks'])
        
        collection_plan['estimated_duration'] = sum(task.get('estimated_minutes', 0) for task in all_tasks)
        
        # Adjust resource allocation based on workload
        total_match_tasks = sum(
            task['count'] for task in all_tasks 
            if task['task_type'] in ['live_matches', 'recent_matches']
        )
        
        if total_match_tasks > 30:
            collection_plan['resource_allocation']['match_data_workers'] = 3
        elif total_match_tasks > 50:
            collection_plan['resource_allocation']['match_data_workers'] = 4
        
        logger.info(f"Collection plan generated: {len(all_tasks)} task types, "
                   f"~{collection_plan['estimated_duration']} minutes estimated")
        
    except Exception as e:
        logger.error(f"Error generating collection plan: {e}")
    
    return collection_plan


def cleanup_old_data(**context) -> Dict[str, int]:
    """
    Clean up old data based on retention policies
    """
    logger.info("Starting data cleanup process...")
    
    try:
        from config.database import db
        
        # Define retention policies (in days)
        retention_policies = {
            'matches': settings.retention.MATCH_DATA_RETENTION_DAYS,
            'match_maps': settings.retention.MATCH_DATA_RETENTION_DAYS,
            'player_stats': settings.retention.PLAYER_STATS_RETENTION_DAYS,
            'team_stats': settings.retention.PLAYER_STATS_RETENTION_DAYS,
            'scraping_logs': settings.retention.LOG_RETENTION_DAYS,
        }
        
        deletion_counts = db.cleanup_old_data(retention_policies)
        
        total_deleted = sum(deletion_counts.values())
        logger.info(f"Cleanup complete: {total_deleted} records deleted across {len(deletion_counts)} tables")
        
        return deletion_counts
        
    except Exception as e:
        logger.error(f"Data cleanup failed: {e}")
        raise


# Task definitions
start_task = EmptyOperator(task_id="start_pipeline")

# Health checks and prerequisites
check_prerequisites_task = PythonOperator(
    task_id='check_prerequisites',
    python_callable=check_pipeline_prerequisites,
    dag=dag,
)

# Planning tasks
with TaskGroup('planning_phase', dag=dag) as planning_group:
    
    prioritize_task = PythonOperator(
        task_id='prioritize_data_collection',
        python_callable=prioritize_data_collection,
        dag=dag,
    )
    
    freshness_check_task = PythonOperator(
        task_id='check_data_freshness',
        python_callable=check_data_freshness,
        dag=dag,
    )
    
    generate_plan_task = PythonOperator(
        task_id='generate_collection_plan',
        python_callable=generate_collection_plan,
        dag=dag,
    )
    
    [prioritize_task, freshness_check_task] >> generate_plan_task


# Data collection orchestration
with TaskGroup('data_collection', dag=dag) as collection_group:
    
    # Trigger match data DAG
    trigger_match_dag = TriggerDagRunOperator(
        task_id='trigger_match_data_dag',
        trigger_dag_id='valorant_match_data_pipeline',
        conf={'collection_plan': '{{ ti.xcom_pull(task_ids="planning_phase.generate_collection_plan") }}'},
        wait_for_completion=False,  # Don't block the main pipeline
        dag=dag,
    )
    
    # Trigger team data DAG
    trigger_team_dag = TriggerDagRunOperator(
        task_id='trigger_team_data_dag',
        trigger_dag_id='valorant_team_data_pipeline',
        conf={'collection_plan': '{{ ti.xcom_pull(task_ids="planning_phase.generate_collection_plan") }}'},
        wait_for_completion=False,
        dag=dag,
    )
    
    # Trigger player data DAG
    trigger_player_dag = TriggerDagRunOperator(
        task_id='trigger_player_data_dag',
        trigger_dag_id='valorant_player_data_pipeline',
        conf={'collection_plan': '{{ ti.xcom_pull(task_ids="planning_phase.generate_collection_plan") }}'},
        wait_for_completion=False,
        dag=dag,
    )
    
    # These can run in parallel
    [trigger_match_dag, trigger_team_dag, trigger_player_dag]


# Data quality and maintenance
with TaskGroup('maintenance', dag=dag) as maintenance_group:
    
    # Data quality checks
    quality_check_task = DataQualityOperator(
        task_id='data_quality_check',
        tables_to_check=['matches', 'teams', 'players', 'events'],
        data_quality_checks=[
            {
                'check_sql': 'SELECT COUNT(*) FROM matches WHERE match_date > NOW() - INTERVAL \'24 hours\'',
                'expected_result': 'greater_than',
                'expected_value': 0,
                'description': 'Recent matches should exist'
            },
            {
                'check_sql': 'SELECT COUNT(*) FROM teams WHERE last_updated IS NULL',
                'expected_result': 'less_than',
                'expected_value': 10,
                'description': 'Most teams should have recent updates'
            }
        ],
        dag=dag,
    )
    
    # Data cleanup
    cleanup_task = PythonOperator(
        task_id='cleanup_old_data',
        python_callable=cleanup_old_data,
        dag=dag,
    )
    
    # Database maintenance
    maintenance_task = BashOperator(
        task_id='database_maintenance',
        bash_command='echo "Running database maintenance..." && python -c "from config.database import db; db.vacuum_analyze()"',
        dag=dag,
    )
    
    quality_check_task >> [cleanup_task, maintenance_task]


# Monitoring and notifications
monitor_task = PythonOperator(
    task_id='monitor_pipeline_health',
    python_callable=lambda **context: logger.info("Pipeline monitoring complete"),
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE,  # Run regardless of upstream task status
)

# Success notification
success_notification = NotificationOperator(
    task_id='send_success_notification',
    message='Valorant data pipeline completed successfully',
    notification_type='success',
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)

# Failure notification  
failure_notification = NotificationOperator(
    task_id='send_failure_notification',
    message='Valorant data pipeline failed - check logs for details',
    notification_type='failure',
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED,
)

end_task = EmptyOperator(
    task_id="end_pipeline",
    trigger_rule=TriggerRule.ALL_DONE,
)

# Define task dependencies
start_task >> check_prerequisites_task >> planning_group
planning_group >> collection_group
collection_group >> maintenance_group
maintenance_group >> monitor_task
monitor_task >> [success_notification, failure_notification] >> end_task
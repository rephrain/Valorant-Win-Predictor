team_dag = DAG(
    'valorant_team_data',
    default_args=default_args,
    description='Team-level statistics and performance data',
    schedule_interval=None,  # Triggered by main DAG
    max_active_runs=1,
    tags=['valorant', 'teams'],
)
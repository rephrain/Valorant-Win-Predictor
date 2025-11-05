from utils.csv_handler import load_from_csv, save_to_csv
import pandas as pd

def aggregate_all_data():
    """
    Combine data from all sources:
    - Join team stats with roster info
    - Align matches with patch versions
    - Calculate roster stability metrics
    """
    
    print("Starting data aggregation...")
    
    try:
        # Load raw data
        team_stats = load_from_csv('data/raw/vlr_team_stats.csv')
        player_stats = load_from_csv('data/raw/vlr_player_stats.csv')
        match_history = load_from_csv('data/raw/vlr_match_history.csv')
        rosters = load_from_csv('data/raw/liquipedia_rosters.csv')
        events = load_from_csv('data/raw/liquipedia_events.csv')
        patches = load_from_csv('data/raw/riot_patches.csv')
        
        if team_stats.empty or match_history.empty:
            print("No data to aggregate")
            return
        
        # Convert date columns
        team_stats['match_date'] = pd.to_datetime(team_stats['match_date'], errors='coerce')
        player_stats['match_date'] = pd.to_datetime(player_stats['match_date'], errors='coerce')
        match_history['match_date'] = pd.to_datetime(match_history['match_date'], errors='coerce')
        
        if not patches.empty:
            patches['release_date'] = pd.to_datetime(patches['release_date'], errors='coerce')
            patches = patches.sort_values('release_date')
        
        if not rosters.empty:
            rosters['change_date'] = pd.to_datetime(rosters['change_date'], errors='coerce')
        
        # Merge patches with matches
        team_stats['patch_version'] = None
        if not patches.empty:
            for idx, row in team_stats.iterrows():
                match_date = row['match_date']
                # Find the patch that was active at match time
                active_patch = patches[patches['release_date'] <= match_date]
                if not active_patch.empty:
                    team_stats.at[idx, 'patch_version'] = active_patch.iloc[-1]['patch_version']
        
        # Calculate roster stability for each team
        team_stats['roster_stability_days'] = 0
        team_stats['recent_roster_changes'] = 0
        
        if not rosters.empty:
            for idx, row in team_stats.iterrows():
                team_name = row['team_name']
                match_date = row['match_date']
                
                # Find most recent roster change before this match
                team_changes = rosters[rosters['team_name'] == team_name]
                team_changes = team_changes[team_changes['change_date'] <= match_date]
                
                if not team_changes.empty:
                    most_recent = team_changes.sort_values('change_date').iloc[-1]
                    days_stable = (match_date - most_recent['change_date']).days
                    team_stats.at[idx, 'roster_stability_days'] = days_stable
                    
                    # Count changes in last 90 days
                    recent = team_changes[team_changes['change_date'] >= match_date - pd.Timedelta(days=90)]
                    team_stats.at[idx, 'recent_roster_changes'] = len(recent)
        
        # Merge with event info if event name can be inferred
        # This would require additional logic to match teams to events
        
        # Calculate team aggregates per map
        team_map_stats = team_stats.groupby(['team_name', 'map_name']).agg({
            'rounds_won': 'sum',
            'rounds_lost': 'sum',
            'win': ['sum', 'count'],
            'attack_rounds_won': 'sum',
            'defense_rounds_won': 'sum',
        }).reset_index()
        
        team_map_stats.columns = ['_'.join(col).strip('_') for col in team_map_stats.columns.values]
        team_map_stats['map_winrate'] = team_map_stats['win_sum'] / team_map_stats['win_count']
        team_map_stats['total_rounds'] = team_map_stats['rounds_won_sum'] + team_map_stats['rounds_lost_sum']
        team_map_stats['attack_winrate'] = team_map_stats['attack_rounds_won_sum'] / (team_map_stats['total_rounds'] / 2)
        team_map_stats['defense_winrate'] = team_map_stats['defense_rounds_won_sum'] / (team_map_stats['total_rounds'] / 2)
        
        # Calculate player aggregates
        player_agg = player_stats.groupby(['player_name', 'team_name']).agg({
            'acs': 'mean',
            'adr': 'mean',
            'kills': 'sum',
            'deaths': 'sum',
            'assists': 'sum',
            'kd_ratio': 'mean',
        }).reset_index()
        
        player_agg['total_games'] = player_stats.groupby(['player_name', 'team_name']).size().values
        
        # Save aggregated data
        save_to_csv(team_stats, 'data/processed/team_stats_with_context.csv')
        save_to_csv(team_map_stats, 'data/processed/team_map_aggregates.csv')
        save_to_csv(player_agg, 'data/processed/player_aggregates.csv')
        save_to_csv(match_history, 'data/processed/match_history.csv')
        
        # Create a combined dataset for modeling
        aggregated = team_stats.copy()
        
        save_to_csv(aggregated, 'data/processed/aggregated_data.csv')
        
        print(f"Aggregation completed: {len(aggregated)} records")
        
    except Exception as e:
        print(f"Error in aggregation: {e}")
        import traceback
        traceback.print_exc()


def calculate_team_strength_of_schedule(team_stats, elo_ratings):
    """
    Calculate strength of schedule for each team based on opponent ELO ratings.
    
    Args:
        team_stats: DataFrame with team match history
        elo_ratings: Dict of team ELO ratings
    
    Returns:
        DataFrame with SOS metrics added
    """
    team_stats['opponent_avg_elo'] = 0.0
    team_stats['sos_adjusted_winrate'] = 0.0
    
    for team in team_stats['team_name'].unique():
        if pd.isna(team):
            continue
        
        team_matches = team_stats[team_stats['team_name'] == team]
        
        # This would require opponent info - placeholder for now
        # In real implementation, track opponents from match_history
        avg_opponent_elo = 1500  # Default
        team_stats.loc[team_stats['team_name'] == team, 'opponent_avg_elo'] = avg_opponent_elo
    
    return team_stats


def merge_player_stats_to_team(team_stats, player_stats):
    """
    Aggregate player-level stats and merge to team-level data.
    
    Args:
        team_stats: Team-level DataFrame
        player_stats: Player-level DataFrame
    
    Returns:
        Merged DataFrame with team and player aggregates
    """
    # Calculate team-level aggregates from player stats
    team_player_agg = player_stats.groupby(['team_name', 'match_date', 'map_name']).agg({
        'acs': ['mean', 'std', 'max', 'min'],
        'adr': ['mean', 'std'],
        'kills': 'sum',
        'deaths': 'sum',
        'assists': 'sum',
        'kd_ratio': ['mean', 'max', 'min'],
    }).reset_index()
    
    team_player_agg.columns = ['_'.join(col).strip('_') for col in team_player_agg.columns.values]
    
    # Merge with team stats
    merged = team_stats.merge(
        team_player_agg,
        on=['team_name', 'match_date', 'map_name'],
        how='left'
    )
    
    return merged


def calculate_map_pool_diversity(team_stats):
    """
    Calculate map pool diversity metrics for each team.
    
    Args:
        team_stats: DataFrame with team match history
    
    Returns:
        DataFrame with map diversity metrics
    """
    map_diversity = team_stats.groupby('team_name').agg({
        'map_name': lambda x: x.nunique(),
        'win': 'mean'
    }).reset_index()
    
    map_diversity.columns = ['team_name', 'unique_maps_played', 'overall_winrate']
    
    return map_diversity


def identify_veto_patterns(match_history, team_stats):
    """
    Analyze veto patterns and map preferences per team.
    
    Args:
        match_history: DataFrame with match results
        team_stats: DataFrame with team statistics
    
    Returns:
        DataFrame with veto insights
    """
    veto_patterns = {}
    
    # Calculate each team's best and worst maps
    for team in team_stats['team_name'].unique():
        if pd.isna(team):
            continue
        
        team_maps = team_stats[team_stats['team_name'] == team]
        
        if not team_maps.empty:
            map_performance = team_maps.groupby('map_name').agg({
                'win': ['sum', 'count', 'mean']
            }).reset_index()
            
            map_performance.columns = ['_'.join(col).strip('_') for col in map_performance.columns.values]
            
            if not map_performance.empty:
                best_map = map_performance.loc[map_performance['win_mean'].idxmax(), 'map_name']
                worst_map = map_performance.loc[map_performance['win_mean'].idxmin(), 'map_name']
                
                veto_patterns[team] = {
                    'best_map': best_map,
                    'worst_map': worst_map,
                    'map_pool_depth': len(map_performance)
                }
    
    return pd.DataFrame.from_dict(veto_patterns, orient='index').reset_index()


def calculate_clutch_metrics(player_stats):
    """
    Calculate clutch performance metrics from player data.
    (Placeholder - requires round-by-round data)
    
    Args:
        player_stats: DataFrame with player statistics
    
    Returns:
        DataFrame with clutch metrics
    """
    # This would require detailed round data
    # For now, use K/D ratio as proxy for clutch ability
    clutch_proxy = player_stats.groupby(['player_name', 'team_name']).agg({
        'kd_ratio': 'mean',
        'kills': 'sum',
        'deaths': 'sum'
    }).reset_index()
    
    clutch_proxy['clutch_score'] = clutch_proxy['kd_ratio'] * (clutch_proxy['kills'] / (clutch_proxy['deaths'] + 1))
    
    return clutch_proxy
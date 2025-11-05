import pandas as pd
import numpy as np
from utils.csv_handler import load_from_csv, save_to_csv
from config.settings import ELO_K_FACTOR, RECENT_MAPS_WINDOW, DECAY_FACTOR

def engineer_features():
    """
    Create prediction features:
    - Exponential decay win rates (last 10-20 maps)
    - Map-specific side splits
    - Opponent-adjusted ELO/ratings
    - Rolling player form (ACS std dev)
    - Roster stability metrics
    """
    
    print("Starting feature engineering...")
    
    try:
        data = load_from_csv('data/processed/aggregated_data.csv')
        team_map_stats = load_from_csv('data/processed/team_map_aggregates.csv')
        player_agg = load_from_csv('data/processed/player_aggregates.csv')
        match_history = load_from_csv('data/processed/match_history.csv')
        
        if data.empty:
            print("No data to process")
            return
        
        data['match_date'] = pd.to_datetime(data['match_date'], errors='coerce')
        data = data.sort_values(['team_name', 'match_date'])
        
        # Initialize ELO ratings
        elo_ratings = {}
        K_FACTOR = ELO_K_FACTOR
        
        def calculate_expected_score(rating_a, rating_b):
            return 1 / (1 + 10**((rating_b - rating_a) / 400))
        
        def update_elo(rating, expected, actual, k=K_FACTOR):
            return rating + k * (actual - expected)
        
        # Calculate exponential decay win rate for each team
        decay_winrates = {}
        
        for team in data['team_name'].unique():
            if pd.isna(team):
                continue
                
            team_data = data[data['team_name'] == team].copy()
            team_data = team_data.sort_values('match_date')
            
            # Initialize ELO
            if team not in elo_ratings:
                elo_ratings[team] = 1500
            
            # Calculate rolling metrics
            recent_wins = []
            recent_rounds = []
            elo_history = []
            
            for idx, row in team_data.iterrows():
                # Exponential decay win rate (last 20 matches)
                recent_wins.append(row['win'])
                if len(recent_wins) > RECENT_MAPS_WINDOW:
                    recent_wins.pop(0)
                
                # Calculate decay weighted win rate
                if recent_wins:
                    weights = [DECAY_FACTOR ** i for i in range(len(recent_wins) - 1, -1, -1)]
                    decay_wr = np.average(recent_wins, weights=weights)
                else:
                    decay_wr = 0.5
                
                data.at[idx, 'exp_decay_winrate'] = decay_wr
                data.at[idx, 'recent_form_games'] = len(recent_wins)
                
                # Update ELO
                elo_history.append(elo_ratings[team])
                data.at[idx, 'elo_rating'] = elo_ratings[team]
                
                # Round performance
                total_rounds = row['rounds_won'] + row['rounds_lost']
                if total_rounds > 0:
                    round_winrate = row['rounds_won'] / total_rounds
                    recent_rounds.append(round_winrate)
                    if len(recent_rounds) > 10:
                        recent_rounds.pop(0)
                
                if recent_rounds:
                    data.at[idx, 'avg_round_winrate'] = np.mean(recent_rounds)
                    data.at[idx, 'round_winrate_volatility'] = np.std(recent_rounds)
        
        # Calculate head-to-head records
        if not match_history.empty:
            match_history['match_date'] = pd.to_datetime(match_history['match_date'], errors='coerce')
            
            h2h_records = {}
            
            for idx, match in match_history.iterrows():
                team1, team2 = match['team1'], match['team2']
                winner = match['winner']
                
                # Create bidirectional keys
                key_12 = f"{team1}_vs_{team2}"
                key_21 = f"{team2}_vs_{team1}"
                
                if key_12 not in h2h_records:
                    h2h_records[key_12] = {'wins': 0, 'losses': 0, 'maps': 0}
                if key_21 not in h2h_records:
                    h2h_records[key_21] = {'wins': 0, 'losses': 0, 'maps': 0}
                
                h2h_records[key_12]['maps'] += 1
                h2h_records[key_21]['maps'] += 1
                
                if winner == team1:
                    h2h_records[key_12]['wins'] += 1
                    h2h_records[key_21]['losses'] += 1
                else:
                    h2h_records[key_12]['losses'] += 1
                    h2h_records[key_21]['wins'] += 1
        
        # Merge team-level map statistics
        if not team_map_stats.empty:
            data = data.merge(
                team_map_stats[['team_name', 'map_name', 'map_winrate', 'attack_winrate', 'defense_winrate']],
                on=['team_name', 'map_name'],
                how='left',
                suffixes=('', '_overall')
            )
        
        # Calculate player-level features per team
        if not player_agg.empty:
            team_player_stats = player_agg.groupby('team_name').agg({
                'acs': ['mean', 'std', 'max'],
                'adr': ['mean', 'std'],
                'kd_ratio': ['mean', 'min', 'max'],
            }).reset_index()
            
            team_player_stats.columns = ['_'.join(col).strip('_') for col in team_player_stats.columns.values]
            
            data = data.merge(
                team_player_stats,
                left_on='team_name',
                right_on='team_name',
                how='left'
            )
        
        # Feature: Momentum (recent performance trend)
        data['momentum'] = 0.0
        
        for team in data['team_name'].unique():
            if pd.isna(team):
                continue
                
            team_data = data[data['team_name'] == team].copy()
            team_data = team_data.sort_values('match_date')
            
            for i, idx in enumerate(team_data.index):
                if i < 5:
                    data.at[idx, 'momentum'] = 0
                else:
                    recent_5 = team_data.iloc[i-5:i]['win'].values
                    data.at[idx, 'momentum'] = np.mean(recent_5) - 0.5  # Centered around 0
        
        # Feature: Side preference strength
        data['side_preference'] = 0.0
        if 'attack_winrate' in data.columns and 'defense_winrate' in data.columns:
            data['side_preference'] = abs(data['attack_winrate'].fillna(0.5) - data['defense_winrate'].fillna(0.5))
        
        # Feature: Roster stability score (0-1)
        if 'roster_stability_days' in data.columns:
            # Sigmoid transformation: more stable after 90 days
            data['roster_stability_score'] = 1 / (1 + np.exp(-(data['roster_stability_days'] - 90) / 30))
        
        # Feature: Patch familiarity (days since patch release)
        data['patch_familiarity'] = 0
        
        # Fill missing values with reasonable defaults
        data['exp_decay_winrate'] = data['exp_decay_winrate'].fillna(0.5)
        data['elo_rating'] = data['elo_rating'].fillna(1500)
        data['map_winrate'] = data['map_winrate'].fillna(0.5)
        data['attack_winrate'] = data['attack_winrate'].fillna(0.5)
        data['defense_winrate'] = data['defense_winrate'].fillna(0.5)
        data['momentum'] = data['momentum'].fillna(0)
        data['roster_stability_score'] = data['roster_stability_score'].fillna(0.5)
        
        # Select key features for modeling
        feature_columns = [
            'team_name', 'match_date', 'map_name', 'patch_version',
            'rounds_won', 'rounds_lost', 'win',
            'exp_decay_winrate', 'elo_rating', 'momentum',
            'map_winrate', 'attack_winrate', 'defense_winrate', 'side_preference',
            'roster_stability_days', 'roster_stability_score', 'recent_roster_changes',
            'avg_round_winrate', 'round_winrate_volatility',
            'recent_form_games'
        ]
        
        # Add player stats if available
        player_cols = [col for col in data.columns if col.startswith(('acs_', 'adr_', 'kd_ratio_'))]
        feature_columns.extend(player_cols)
        
        # Filter to available columns
        available_cols = [col for col in feature_columns if col in data.columns]
        final_features = data[available_cols].copy()
        
        # Save final feature set
        save_to_csv(final_features, 'data/final/features_for_modeling.csv')
        
        # Create summary statistics
        summary = {
            'total_matches': len(final_features),
            'unique_teams': final_features['team_name'].nunique(),
            'unique_maps': final_features['map_name'].nunique() if 'map_name' in final_features.columns else 0,
            'date_range': f"{final_features['match_date'].min()} to {final_features['match_date'].max()}",
            'avg_elo': final_features['elo_rating'].mean() if 'elo_rating' in final_features.columns else 0,
            'features_count': len(available_cols)
        }
        
        summary_df = pd.DataFrame([summary])
        save_to_csv(summary_df, 'data/final/feature_summary.csv')
        
        print(f"Feature engineering completed: {len(final_features)} records with {len(available_cols)} features")
        print(f"Summary: {summary}")
        
    except Exception as e:
        print(f"Error in feature engineering: {e}")
        import traceback
        traceback.print_exc()


def calculate_exponential_decay_metric(values, decay_factor=0.95):
    """
    Calculate exponentially decayed average of a metric.
    Recent values weighted more heavily.
    
    Args:
        values: List or array of values (oldest to newest)
        decay_factor: Decay factor (0-1), higher = slower decay
    
    Returns:
        Weighted average
    """
    if len(values) == 0:
        return 0.0
    
    weights = [decay_factor ** i for i in range(len(values) - 1, -1, -1)]
    return np.average(values, weights=weights)


def calculate_form_volatility(values, window=10):
    """
    Calculate performance volatility over recent window.
    Higher values indicate inconsistent performance.
    
    Args:
        values: List of performance metrics
        window: Rolling window size
    
    Returns:
        Standard deviation of recent performance
    """
    if len(values) < 2:
        return 0.0
    
    recent = values[-window:] if len(values) > window else values
    return np.std(recent)


def calculate_momentum_indicator(win_results, window=5):
    """
    Calculate momentum based on recent win rate vs overall.
    Positive = improving, negative = declining.
    
    Args:
        win_results: List of binary win results (1/0)
        window: Recent window for comparison
    
    Returns:
        Momentum score (-1 to 1)
    """
    if len(win_results) < window:
        return 0.0
    
    recent_wr = np.mean(win_results[-window:])
    overall_wr = np.mean(win_results)
    
    return recent_wr - overall_wr


def normalize_elo_rating(elo, min_elo=1000, max_elo=2000):
    """
    Normalize ELO rating to 0-1 scale for modeling.
    
    Args:
        elo: Raw ELO rating
        min_elo: Minimum expected ELO
        max_elo: Maximum expected ELO
    
    Returns:
        Normalized ELO (0-1)
    """
    return (elo - min_elo) / (max_elo - min_elo)


def calculate_opponent_adjusted_metric(metric_values, opponent_strengths):
    """
    Adjust a metric based on opponent strength.
    Better performance vs stronger opponents weighted more.
    
    Args:
        metric_values: List of performance values
        opponent_strengths: List of opponent ratings (e.g., ELO)
    
    Returns:
        Opponent-adjusted average
    """
    if len(metric_values) != len(opponent_strengths):
        return np.mean(metric_values) if len(metric_values) > 0 else 0.0
    
    if len(metric_values) == 0:
        return 0.0
    
    # Weight by opponent strength (normalized)
    strengths_normalized = np.array(opponent_strengths) / 1500  # Assuming 1500 is baseline
    weighted_avg = np.average(metric_values, weights=strengths_normalized)
    
    return weighted_avg


def create_interaction_features(df, feature_pairs):
    """
    Create interaction features between specified pairs.
    
    Args:
        df: DataFrame with features
        feature_pairs: List of tuples (feature1, feature2)
    
    Returns:
        DataFrame with added interaction features
    """
    for feat1, feat2 in feature_pairs:
        if feat1 in df.columns and feat2 in df.columns:
            interaction_name = f"{feat1}_x_{feat2}"
            df[interaction_name] = df[feat1] * df[feat2]
    
    return df


def calculate_recency_weight(days_ago, half_life=30):
    """
    Calculate exponential decay weight based on days ago.
    Used for time-weighted aggregations.
    
    Args:
        days_ago: Number of days in the past
        half_life: Days until weight is halved
    
    Returns:
        Weight factor (0-1)
    """
    return 0.5 ** (days_ago / half_life)


def engineer_side_split_features(df):
    """
    Engineer features related to attack/defense performance splits.
    
    Args:
        df: DataFrame with attack_winrate and defense_winrate
    
    Returns:
        DataFrame with additional side-related features
    """
    if 'attack_winrate' in df.columns and 'defense_winrate' in df.columns:
        # Side preference strength (how imbalanced)
        df['side_imbalance'] = abs(df['attack_winrate'] - df['defense_winrate'])
        
        # Dominant side
        df['dominant_side'] = df.apply(
            lambda row: 'attack' if row['attack_winrate'] > row['defense_winrate'] else 'defense',
            axis=1
        )
        
        # Overall side performance (combined)
        df['combined_side_performance'] = (df['attack_winrate'] + df['defense_winrate']) / 2
    
    return df


def engineer_roster_features(df, roster_df):
    """
    Engineer features related to roster stability and composition.
    
    Args:
        df: Main DataFrame
        roster_df: DataFrame with roster change information
    
    Returns:
        DataFrame with roster features
    """
    if 'roster_stability_days' in df.columns:
        # Categorize stability
        df['roster_status'] = pd.cut(
            df['roster_stability_days'],
            bins=[-1, 30, 90, 180, float('inf')],
            labels=['new', 'settling', 'stable', 'veteran']
        )
        
        # Stability score (sigmoid)
        df['stability_score'] = 1 / (1 + np.exp(-(df['roster_stability_days'] - 90) / 30))
    
    return df


def create_rolling_aggregates(df, group_cols, value_col, windows=[5, 10, 20]):
    """
    Create rolling window aggregates for a metric.
    
    Args:
        df: DataFrame sorted by time
        group_cols: Columns to group by (e.g., ['team_name'])
        value_col: Column to aggregate
        windows: List of window sizes
    
    Returns:
        DataFrame with rolling features added
    """
    df = df.sort_values(group_cols + ['match_date'])
    
    for window in windows:
        col_name = f"{value_col}_rolling_{window}"
        df[col_name] = df.groupby(group_cols)[value_col].transform(
            lambda x: x.rolling(window=window, min_periods=1).mean()
        )
        
        # Also add rolling std
        col_name_std = f"{value_col}_rolling_std_{window}"
        df[col_name_std] = df.groupby(group_cols)[value_col].transform(
            lambda x: x.rolling(window=window, min_periods=2).std().fillna(0)
        )
    
    return df


def calculate_map_specific_elo(df, team_map_pairs):
    """
    Calculate map-specific ELO ratings for each team.
    
    Args:
        df: DataFrame with match results
        team_map_pairs: Dict to store map-specific ELOs
    
    Returns:
        DataFrame with map ELO ratings
    """
    map_elos = {}
    K_FACTOR = 32
    
    def expected_score(rating_a, rating_b):
        return 1 / (1 + 10**((rating_b - rating_a) / 400))
    
    for idx, row in df.iterrows():
        team = row['team_name']
        map_name = row['map_name']
        key = f"{team}_{map_name}"
        
        if key not in map_elos:
            map_elos[key] = 1500
        
        df.at[idx, 'map_specific_elo'] = map_elos[key]
    
    return df
"""
Data models for Valorant scraping pipeline.
Defines the structure for all scraped data based on predictive features.
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional, List, Dict, Any
from enum import Enum


class MatchType(Enum):
    BO1 = "BO1"
    BO3 = "BO3"  
    BO5 = "BO5"


class EventTier(Enum):
    TIER1 = "Tier1"
    TIER2 = "Tier2"
    TIER3 = "Tier3"
    QUALIFIER = "Qualifier"
    REGIONAL = "Regional"
    INTERNATIONAL = "International"


class MapSide(Enum):
    ATTACK = "Attack"
    DEFENSE = "Defense"


class Region(Enum):
    NA = "North America"
    EU = "Europe"
    APAC = "Asia Pacific"
    LATAM = "Latin America"
    BR = "Brazil"
    KR = "Korea"
    JP = "Japan"


@dataclass
class Player:
    """Individual player data model"""
    player_id: str
    name: str
    team_id: Optional[str] = None
    region: Optional[Region] = None
    primary_agent: Optional[str] = None
    role: Optional[str] = None  # Duelist, Controller, Initiator, Sentinel
    
    # Performance metrics (recent form)
    recent_acs: Optional[float] = None  # Average Combat Score
    recent_adr: Optional[float] = None  # Average Damage per Round
    recent_kd: Optional[float] = None
    recent_kast: Optional[float] = None  # Kill/Assist/Survive/Trade %
    recent_hs_percent: Optional[float] = None  # Headshot %
    recent_fba: Optional[float] = None  # First bullet accuracy
    
    # Entry performance
    entry_success_rate: Optional[float] = None  # As attacker
    entry_survival_rate: Optional[float] = None  # As defender
    
    # Impact metrics
    multikill_rate: Optional[float] = None  # 2K+ rounds %
    clutch_1v1_rate: Optional[float] = None
    clutch_1v2_rate: Optional[float] = None
    
    # Agent pool
    agent_pool: List[str] = field(default_factory=list)
    agent_comfort_map: Dict[str, List[str]] = field(default_factory=dict)  # Map -> Preferred agents
    
    # Volatility
    acs_volatility: Optional[float] = None  # Standard deviation of recent ACS
    
    # Meta info
    maps_played_current_patch: Optional[int] = None
    last_updated: datetime = field(default_factory=datetime.now)


@dataclass  
class Team:
    """Team-level data model"""
    team_id: str
    name: str
    region: Region
    current_roster: List[str] = field(default_factory=list)  # Player IDs
    coach: Optional[str] = None
    
    # Recent form metrics
    recent_win_rate: Optional[float] = None  # Last 10-20 maps
    exponential_decay_rating: Optional[float] = None
    elo_rating: Optional[float] = None
    strength_of_schedule_rating: Optional[float] = None
    
    # Map performance by side
    map_attack_rates: Dict[str, float] = field(default_factory=dict)  # Map -> Attack win %
    map_defense_rates: Dict[str, float] = field(default_factory=dict)  # Map -> Defense win %
    
    # Economic performance
    pistol_win_rate: Optional[float] = None
    pistol_conversion_rate: Optional[float] = None  # Pistol -> anti-eco/bonus
    save_round_rate: Optional[float] = None
    
    # Tactical metrics
    opening_duel_rate: Optional[float] = None  # First kill %
    first_death_rate: Optional[float] = None
    entry_differential: Optional[float] = None  # FK% - FD%
    trade_efficiency: Optional[float] = None  # % deaths traded within 5s
    
    # Clutch performance
    clutch_1vx_rate: Optional[float] = None
    clutch_2vx_rate: Optional[float] = None
    
    # Roster stability
    roster_stability_days: Optional[int] = None  # Days since last change
    maps_with_current_roster: Optional[int] = None
    
    # Tactical diversity
    unique_comps_per_map: Dict[str, int] = field(default_factory=dict)
    role_redundancy_score: Optional[float] = None
    
    # Performance context
    lan_win_rate: Optional[float] = None
    online_win_rate: Optional[float] = None
    timeout_impact_rate: Optional[float] = None  # Win% after tactical timeout
    
    # Meta info
    last_roster_change: Optional[date] = None
    maps_played_current_patch: Optional[int] = None
    last_updated: datetime = field(default_factory=datetime.now)


@dataclass
class Match:
    """Individual match data model"""
    match_id: str
    event_id: str
    team1_id: str
    team2_id: str
    match_date: datetime
    match_type: MatchType
    
    # Context
    event_tier: EventTier
    event_stage: Optional[str] = None  # "Groups", "Playoffs", "Lower Bracket"
    venue: Optional[str] = None  # "Online" or venue name
    region: Region
    
    # Pre-match info
    patch_version: Optional[str] = None
    days_since_last_match_team1: Optional[int] = None
    days_since_last_match_team2: Optional[int] = None
    timezone_delta_team1: Optional[int] = None  # Hours from home timezone
    timezone_delta_team2: Optional[int] = None
    
    # Map pool and vetoes
    available_maps: List[str] = field(default_factory=list)
    veto_order: List[Dict[str, Any]] = field(default_factory=list)  # [{type: "ban/pick", team: "team1/team2", map: "Bind"}]
    map_pool: List[str] = field(default_factory=list)  # Final maps to be played
    
    # Results (populated after match)
    winner_team_id: Optional[str] = None
    final_score: Optional[str] = None  # "2-1", "2-0", etc.
    map_results: List[Dict[str, Any]] = field(default_factory=list)
    
    # Technical
    server_location: Optional[str] = None
    average_ping_team1: Optional[float] = None
    average_ping_team2: Optional[float] = None
    
    last_updated: datetime = field(default_factory=datetime.now)


@dataclass
class MapResult:
    """Individual map result within a match"""
    match_id: str
    map_name: str
    map_order: int  # 1st, 2nd, 3rd map in series
    
    winner_team_id: str
    team1_attack_rounds: Optional[int] = None
    team1_defense_rounds: Optional[int] = None
    team2_attack_rounds: Optional[int] = None  
    team2_defense_rounds: Optional[int] = None
    
    # Side selection
    team1_starting_side: Optional[MapSide] = None
    
    # Round-by-round data
    total_rounds: Optional[int] = None
    overtime_rounds: Optional[int] = None
    
    # Player performances (aggregated)
    player_stats: List[Dict[str, Any]] = field(default_factory=list)
    
    last_updated: datetime = field(default_factory=datetime.now)


@dataclass
class Event:
    """Tournament/Event data model"""
    event_id: str
    name: str
    tier: EventTier
    region: Region
    start_date: date
    end_date: date
    
    # Context
    prize_pool: Optional[float] = None
    teams: List[str] = field(default_factory=list)  # Team IDs
    format: Optional[str] = None  # "Single Elimination", "Double Elimination", etc.
    
    # Map pool for this event
    map_pool: List[str] = field(default_factory=list)
    patch_version: Optional[str] = None
    
    last_updated: datetime = field(default_factory=datetime.now)


@dataclass
class Patch:
    """Game patch/version data model"""
    patch_id: str
    version: str  # "7.12", "8.01", etc.
    release_date: date
    
    # Changes that affect prediction models
    agent_changes: List[Dict[str, Any]] = field(default_factory=list)
    map_changes: List[Dict[str, Any]] = field(default_factory=list)
    weapon_changes: List[Dict[str, Any]] = field(default_factory=list)
    economic_changes: List[Dict[str, Any]] = field(default_factory=list)
    
    # Meta impact
    meta_impact_score: Optional[float] = None  # How much this patch changed the game
    
    last_updated: datetime = field(default_factory=datetime.now)


@dataclass
class HeadToHead:
    """Head-to-head matchup history between teams"""
    team1_id: str
    team2_id: str
    
    # Recent performance (roster-matched only)
    recent_matches: List[str] = field(default_factory=list)  # Match IDs
    team1_wins: Optional[int] = None
    team2_wins: Optional[int] = None
    
    # Map-specific H2H
    map_performance: Dict[str, Dict[str, int]] = field(default_factory=dict)  # Map -> {team1_wins, team2_wins}
    
    # Stylistic matchup notes
    playstyle_notes: Optional[str] = None
    
    last_updated: datetime = field(default_factory=datetime.now)


# Utility functions for data validation and processing
def validate_team_data(team: Team) -> bool:
    """Validate team data completeness for prediction"""
    required_fields = [
        'recent_win_rate', 'pistol_win_rate', 'opening_duel_rate', 
        'trade_efficiency', 'maps_with_current_roster'
    ]
    return all(getattr(team, field) is not None for field in required_fields)


def validate_player_data(player: Player) -> bool:
    """Validate player data completeness for prediction"""
    required_fields = ['recent_acs', 'recent_kd', 'recent_kast', 'entry_success_rate']
    return all(getattr(player, field) is not None for field in required_fields)


def calculate_roster_stability_score(team: Team) -> float:
    """Calculate roster stability impact on performance"""
    if not team.roster_stability_days or not team.maps_with_current_roster:
        return 0.0
    
    # Stability decreases impact of recent changes
    days_factor = min(team.roster_stability_days / 30.0, 1.0)  # Max impact after 30 days
    maps_factor = min(team.maps_with_current_roster / 20.0, 1.0)  # Max impact after 20 maps
    
    return (days_factor + maps_factor) / 2.0
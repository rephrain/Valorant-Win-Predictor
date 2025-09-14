"""
Scraping configuration for different data sources.
Defines endpoints, CSS selectors, data mappings, and extraction rules.
"""

from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import re


class DataType(Enum):
    """Types of data we can scrape"""
    MATCH = "match"
    TEAM = "team"
    PLAYER = "player"
    EVENT = "event"
    STATS = "stats"
    MAP = "map"
    AGENT = "agent"
    PATCH = "patch"


class ScrapingMethod(Enum):
    """Different methods for scraping data"""
    HTML_PARSING = "html_parsing"
    API_REQUEST = "api_request"
    SELENIUM = "selenium"
    PLAYWRIGHT = "playwright"


@dataclass
class FieldMapping:
    """Configuration for extracting a specific field"""
    selector: str  # CSS selector or XPath
    attribute: Optional[str] = None  # HTML attribute to extract (default: text)
    transform: Optional[str] = None  # Transformation function name
    required: bool = True  # Whether this field is required
    default_value: Any = None  # Default value if extraction fails
    regex: Optional[str] = None  # Regex pattern to apply
    multiple: bool = False  # Whether to extract multiple elements


@dataclass
class EndpointConfig:
    """Configuration for a specific endpoint"""
    url_template: str  # URL with placeholders
    method: ScrapingMethod = ScrapingMethod.HTML_PARSING
    headers: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)
    rate_limit_key: str = "default"  # Key for rate limiting
    retry_count: int = 3
    timeout: int = 30
    use_session: bool = True
    cache_duration: Optional[int] = None  # Cache duration in seconds


@dataclass
class DataSourceConfig:
    """Complete configuration for a data source"""
    name: str
    base_url: str
    endpoints: Dict[DataType, EndpointConfig]
    field_mappings: Dict[DataType, Dict[str, FieldMapping]]
    headers: Dict[str, str] = field(default_factory=dict)
    authentication: Optional[Dict[str, Any]] = None
    rate_limit_key: str = "default"


class ScrapingConfig:
    """Main configuration class for all scraping sources"""
    
    def __init__(self):
        self._init_vlr_config()
        self._init_thespike_config()
        self._init_liquipedia_config()
        self._init_riot_api_config()
        
        # Combine all sources
        self.sources = {
            "vlr": self.vlr_config,
            "thespike": self.thespike_config,
            "liquipedia": self.liquipedia_config,
            "riot_api": self.riot_api_config,
        }
    
    def _init_vlr_config(self):
        """Initialize VLR.gg configuration"""
        
        # Common headers to mimic browser
        common_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        
        # Endpoint configurations
        endpoints = {
            DataType.MATCH: EndpointConfig(
                url_template="{base_url}/matches/{match_id}/{match_slug}",
                rate_limit_key="vlr",
                cache_duration=300  # 5 minutes for live matches
            ),
            DataType.TEAM: EndpointConfig(
                url_template="{base_url}/team/{team_id}/{team_slug}",
                rate_limit_key="vlr",
                cache_duration=3600  # 1 hour for team pages
            ),
            DataType.PLAYER: EndpointConfig(
                url_template="{base_url}/player/{player_id}/{player_slug}",
                rate_limit_key="vlr",
                cache_duration=1800  # 30 minutes for player pages
            ),
            DataType.EVENT: EndpointConfig(
                url_template="{base_url}/event/{event_id}/{event_slug}",
                rate_limit_key="vlr",
                cache_duration=1800
            ),
            DataType.STATS: EndpointConfig(
                url_template="{base_url}/stats/?event_group_id=all&event_id={event_id}&region=all&country=all&min_rounds=100&min_rating=1550&agent=all&map_id=all&timespan={timespan}",
                rate_limit_key="vlr",
                cache_duration=3600
            ),
        }
        
        # Field mappings for different data types
        field_mappings = {
            DataType.MATCH: {
                # Match metadata
                "match_id": FieldMapping("div[data-match-id]", "data-match-id"),
                "event_name": FieldMapping(".match-header-event .event-name", required=True),
                "match_date": FieldMapping(".moment-tz-convert", "data-utc-ts", transform="timestamp_to_datetime"),
                "match_status": FieldMapping(".match-header-vs-score .js-spoiler", transform="extract_match_status"),
                "bo_format": FieldMapping(".match-header-vs-note", transform="extract_bo_format"),
                
                # Team information
                "team1_name": FieldMapping(".match-header-link-name .wf-title", multiple=False),
                "team2_name": FieldMapping(".match-header-link-name .wf-title:last-child", multiple=False),
                "team1_score": FieldMapping(".match-header-vs-score .js-spoiler .match-header-vs-score-winner", transform="extract_score"),
                "team2_score": FieldMapping(".match-header-vs-score .js-spoiler .match-header-vs-score-loser", transform="extract_score"),
                
                # Map data
                "maps_data": FieldMapping(".vm-stats-gamesnav-item", multiple=True, transform="extract_map_data"),
                "veto_data": FieldMapping(".match-header-note", transform="extract_veto_info"),
                
                # Player statistics (per map)
                "player_stats": FieldMapping(".vm-stats-game table tbody tr", multiple=True, transform="extract_player_stats"),
            },
            
            DataType.TEAM: {
                "team_id": FieldMapping("div[data-team-id]", "data-team-id"),
                "team_name": FieldMapping(".wf-title"),
                "team_logo": FieldMapping(".team-header-logo img", "src"),
                "region": FieldMapping(".team-header-country img", "alt"),
                "rank": FieldMapping(".team-header-ranking .rating-txt", transform="extract_rating"),
                
                # Recent matches
                "recent_matches": FieldMapping(".wf-card", multiple=True, transform="extract_recent_matches"),
                
                # Current roster
                "roster": FieldMapping(".team-roster-item", multiple=True, transform="extract_roster"),
                
                # Team stats
                "win_rate": FieldMapping(".team-summary-container", transform="extract_team_winrate"),
            },
            
            DataType.PLAYER: {
                "player_id": FieldMapping("div[data-player-id]", "data-player-id"),
                "player_name": FieldMapping(".player-header .wf-title"),
                "real_name": FieldMapping(".player-real-name"),
                "country": FieldMapping(".player-header-country .flag", "alt"),
                "team": FieldMapping(".player-header-team .team-name"),
                "role": FieldMapping(".player-summary-container", transform="extract_player_role"),
                
                # Statistics
                "rating": FieldMapping(".rating-item .rating-item-rating", transform="float"),
                "acs": FieldMapping(".rating-item", transform="extract_acs"),
                "kd_ratio": FieldMapping(".rating-item", transform="extract_kd"),
                "adr": FieldMapping(".rating-item", transform="extract_adr"),
                "kast": FieldMapping(".rating-item", transform="extract_kast"),
                
                # Agent pool
                "agent_pool": FieldMapping(".player-summary-container", transform="extract_agent_pool"),
            },
            
            DataType.EVENT: {
                "event_id": FieldMapping("div[data-event-id]", "data-event-id"),
                "event_name": FieldMapping(".wf-title"),
                "start_date": FieldMapping(".event-desc-item", transform="extract_event_dates"),
                "end_date": FieldMapping(".event-desc-item", transform="extract_event_dates"),
                "prize_pool": FieldMapping(".event-desc-item", transform="extract_prize_pool"),
                "region": FieldMapping(".event-desc-item", transform="extract_region"),
                "teams": FieldMapping(".event-teams-container .team-name", multiple=True),
                "brackets": FieldMapping(".bracket-container", transform="extract_brackets"),
            },
        }
        
        self.vlr_config = DataSourceConfig(
            name="VLR.gg",
            base_url="https://www.vlr.gg",
            endpoints=endpoints,
            field_mappings=field_mappings,
            headers=common_headers,
            rate_limit_key="vlr"
        )
    
    def _init_thespike_config(self):
        """Initialize TheSpike.gg configuration"""
        
        common_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
        }
        
        endpoints = {
            DataType.MATCH: EndpointConfig(
                url_template="{base_url}/match/{match_id}",
                rate_limit_key="thespike",
                cache_duration=300
            ),
            DataType.TEAM: EndpointConfig(
                url_template="{base_url}/team/{team_id}",
                rate_limit_key="thespike",
                cache_duration=1800
            ),
            DataType.PLAYER: EndpointConfig(
                url_template="{base_url}/player/{player_id}",
                rate_limit_key="thespike",
                cache_duration=1800
            ),
            DataType.STATS: EndpointConfig(
                url_template="{base_url}/stats/players?event={event_id}&timespan={timespan}",
                rate_limit_key="thespike",
                cache_duration=3600
            ),
        }
        
        # TheSpike often uses JSON APIs, so field mappings are different
        field_mappings = {
            DataType.MATCH: {
                "match_id": FieldMapping("$.id"),
                "event_name": FieldMapping("$.tournament.name"),
                "match_date": FieldMapping("$.scheduled_at", transform="iso_to_datetime"),
                "team1_name": FieldMapping("$.teams[0].name"),
                "team2_name": FieldMapping("$.teams[1].name"),
                "team1_score": FieldMapping("$.teams[0].score", transform="int"),
                "team2_score": FieldMapping("$.teams[1].score", transform="int"),
                "maps": FieldMapping("$.maps", multiple=True, transform="extract_spike_maps"),
                "bo_format": FieldMapping("$.format", transform="extract_spike_format"),
            },
            
            DataType.TEAM: {
                "team_id": FieldMapping("$.id"),
                "team_name": FieldMapping("$.name"),
                "region": FieldMapping("$.region.name"),
                "logo_url": FieldMapping("$.logo_url"),
                "players": FieldMapping("$.players", multiple=True, transform="extract_spike_players"),
                "recent_matches": FieldMapping("$.recent_matches", multiple=True),
            },
            
            DataType.PLAYER: {
                "player_id": FieldMapping("$.id"),
                "player_name": FieldMapping("$.username"),
                "real_name": FieldMapping("$.name"),
                "team_name": FieldMapping("$.team.name"),
                "role": FieldMapping("$.role"),
                "country": FieldMapping("$.country.code"),
                "stats": FieldMapping("$.statistics", transform="extract_spike_player_stats"),
            },
        }
        
        self.thespike_config = DataSourceConfig(
            name="TheSpike.gg",
            base_url="https://www.thespike.gg/api",
            endpoints=endpoints,
            field_mappings=field_mappings,
            headers=common_headers,
            rate_limit_key="thespike"
        )
    
    def _init_liquipedia_config(self):
        """Initialize Liquipedia configuration"""
        
        common_headers = {
            "User-Agent": "ValorantDataPipeline/1.0 (https://github.com/yourproject) Python/3.x",
            "Accept": "application/json",
        }
        
        endpoints = {
            DataType.MATCH: EndpointConfig(
                url_template="{base_url}/api.php?action=parse&page={page_title}&format=json&prop=wikitext",
                rate_limit_key="liquipedia",
                cache_duration=600
            ),
            DataType.EVENT: EndpointConfig(
                url_template="{base_url}/api.php?action=parse&page={event_name}&format=json&prop=wikitext",
                rate_limit_key="liquipedia",
                cache_duration=1800
            ),
            DataType.TEAM: EndpointConfig(
                url_template="{base_url}/api.php?action=parse&page={team_name}&format=json&prop=wikitext",
                rate_limit_key="liquipedia",
                cache_duration=3600
            ),
        }
        
        # Liquipedia uses MediaWiki API and wikitext parsing
        field_mappings = {
            DataType.MATCH: {
                "match_info": FieldMapping("$.parse.wikitext.*", transform="parse_match_wikitext"),
            },
            DataType.EVENT: {
                "event_info": FieldMapping("$.parse.wikitext.*", transform="parse_event_wikitext"),
                "teams": FieldMapping("$.parse.wikitext.*", transform="extract_event_teams"),
                "brackets": FieldMapping("$.parse.wikitext.*", transform="extract_event_brackets"),
            },
            DataType.TEAM: {
                "team_info": FieldMapping("$.parse.wikitext.*", transform="parse_team_wikitext"),
                "roster_history": FieldMapping("$.parse.wikitext.*", transform="extract_roster_history"),
            },
        }
        
        self.liquipedia_config = DataSourceConfig(
            name="Liquipedia",
            base_url="https://liquipedia.net/valorant",
            endpoints=endpoints,
            field_mappings=field_mappings,
            headers=common_headers,
            rate_limit_key="liquipedia"
        )
    
    def _init_riot_api_config(self):
        """Initialize Riot API configuration (if available)"""
        
        endpoints = {
            DataType.MATCH: EndpointConfig(
                url_template="{base_url}/val/match/v1/matches/{match_id}",
                method=ScrapingMethod.API_REQUEST,
                rate_limit_key="riot_api",
                cache_duration=3600,
                headers={"X-Riot-Token": "{api_key}"}
            ),
            DataType.PLAYER: EndpointConfig(
                url_template="{base_url}/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}",
                method=ScrapingMethod.API_REQUEST,
                rate_limit_key="riot_api",
                cache_duration=86400  # 24 hours
            ),
        }
        
        field_mappings = {
            DataType.MATCH: {
                "match_id": FieldMapping("$.matchInfo.matchId"),
                "game_version": FieldMapping("$.matchInfo.gameVersion"),
                "game_start": FieldMapping("$.matchInfo.gameStartMillis", transform="millis_to_datetime"),
                "game_length": FieldMapping("$.matchInfo.gameLengthMillis", transform="millis_to_duration"),
                "map": FieldMapping("$.matchInfo.mapId", transform="map_id_to_name"),
                "mode": FieldMapping("$.matchInfo.queueId", transform="queue_id_to_mode"),
                "rounds": FieldMapping("$.roundResults", multiple=True, transform="extract_riot_rounds"),
                "players": FieldMapping("$.players", multiple=True, transform="extract_riot_players"),
            },
        }
        
        self.riot_api_config = DataSourceConfig(
            name="Riot API",
            base_url="https://americas.api.riotgames.com",
            endpoints=endpoints,
            field_mappings=field_mappings,
            rate_limit_key="riot_api",
            authentication={"type": "api_key", "header": "X-Riot-Token"}
        )
    
    def get_source_config(self, source_name: str) -> Optional[DataSourceConfig]:
        """Get configuration for a specific source"""
        return self.sources.get(source_name.lower())
    
    def get_endpoint_config(self, source: str, data_type: DataType) -> Optional[EndpointConfig]:
        """Get endpoint configuration for a source and data type"""
        source_config = self.get_source_config(source)
        if source_config:
            return source_config.endpoints.get(data_type)
        return None
    
    def get_field_mappings(self, source: str, data_type: DataType) -> Dict[str, FieldMapping]:
        """Get field mappings for a source and data type"""
        source_config = self.get_source_config(source)
        if source_config:
            return source_config.field_mappings.get(data_type, {})
        return {}
    
    def build_url(self, source: str, data_type: DataType, **kwargs) -> Optional[str]:
        """Build URL for a specific endpoint"""
        endpoint_config = self.get_endpoint_config(source, data_type)
        source_config = self.get_source_config(source)
        
        if not endpoint_config or not source_config:
            return None
        
        # Add base_url to kwargs
        kwargs["base_url"] = source_config.base_url
        
        try:
            return endpoint_config.url_template.format(**kwargs)
        except KeyError as e:
            raise ValueError(f"Missing required parameter for URL building: {e}")


# Transformation functions for data processing
class DataTransformations:
    """Collection of transformation functions for extracted data"""
    
    @staticmethod
    def timestamp_to_datetime(value: str) -> Optional[datetime]:
        """Convert timestamp to datetime"""
        try:
            return datetime.fromtimestamp(int(value))
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def iso_to_datetime(value: str) -> Optional[datetime]:
        """Convert ISO string to datetime"""
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def millis_to_datetime(value: Union[str, int]) -> Optional[datetime]:
        """Convert milliseconds to datetime"""
        try:
            return datetime.fromtimestamp(int(value) / 1000)
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def extract_match_status(element) -> str:
        """Extract match status from VLR element"""
        # Implementation would parse the specific element structure
        # This is a placeholder for the actual implementation
        return "completed"
    
    @staticmethod
    def extract_bo_format(value: str) -> str:
        """Extract Best-of format from match description"""
        if not value:
            return "Bo1"
        
        bo_match = re.search(r'bo(\d+)', value.lower())
        return f"Bo{bo_match.group(1)}" if bo_match else "Bo1"
    
    @staticmethod
    def extract_score(element) -> int:
        """Extract numerical score from score element"""
        # Placeholder for actual implementation
        return 0
    
    @staticmethod
    def float_conversion(value: str) -> Optional[float]:
        """Convert string to float"""
        try:
            return float(value.strip())
        except (ValueError, AttributeError):
            return None
    
    @staticmethod
    def int_conversion(value: str) -> Optional[int]:
        """Convert string to integer"""
        try:
            return int(value.strip())
        except (ValueError, AttributeError):
            return None


# Global configuration instance
scraping_config = ScrapingConfig()


# Priority mapping for data freshness requirements
DATA_FRESHNESS_PRIORITY = {
    DataType.MATCH: timedelta(minutes=5),   # Live matches need frequent updates
    DataType.STATS: timedelta(hours=1),     # Player stats update hourly
    DataType.TEAM: timedelta(hours=6),      # Team data updates less frequently
    DataType.PLAYER: timedelta(hours=12),   # Player profiles change rarely
    DataType.EVENT: timedelta(hours=24),    # Event data is mostly static
    DataType.PATCH: timedelta(days=7),      # Patch data changes weekly at most
}


# Required fields for data quality validation
REQUIRED_FIELDS = {
    DataType.MATCH: ["match_id", "team1_name", "team2_name", "match_date"],
    DataType.TEAM: ["team_id", "team_name", "region"],
    DataType.PLAYER: ["player_id", "player_name", "team"],
    DataType.EVENT: ["event_id", "event_name", "start_date"],
    DataType.STATS: ["player_id", "rating", "matches_played"],
}
"""
Core settings and configuration for Valorant data scraping pipeline.
Centralizes all environment variables and application settings.
"""

import os
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import timedelta

# Base paths
BASE_DIR = Path(__file__).parent.parent
PROJECT_ROOT = BASE_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"
CACHE_DIR = PROJECT_ROOT / "cache"

# Ensure directories exist
for directory in [DATA_DIR, LOGS_DIR, CACHE_DIR]:
    directory.mkdir(exist_ok=True)


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", 5432))
    database: str = os.getenv("POSTGRES_DB", "valorant_data")
    username: str = os.getenv("POSTGRES_USER", "postgres")
    password: str = os.getenv("POSTGRES_PASSWORD", "Pwd&6w9RfK")
    
    @property
    def connection_string(self) -> str:
        """Returns PostgreSQL connection string"""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class RedisConfig:
    """Redis cache configuration"""
    host: str = os.getenv("REDIS_HOST", "localhost")
    port: int = int(os.getenv("REDIS_PORT", 6379))
    db: int = int(os.getenv("REDIS_DB", 0))
    password: str = os.getenv("REDIS_PASSWORD", "")
    
    @property
    def connection_string(self) -> str:
        """Returns Redis connection string"""
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


@dataclass
class ScrapingTargets:
    """Configuration for different scraping sources"""
    
    # VLR.gg endpoints
    VLR_BASE_URL: str = "https://www.vlr.gg"
    VLR_MATCHES_URL: str = f"{VLR_BASE_URL}/matches"
    VLR_STATS_URL: str = f"{VLR_BASE_URL}/stats"
    VLR_TEAMS_URL: str = f"{VLR_BASE_URL}/teams"
    VLR_EVENTS_URL: str = f"{VLR_BASE_URL}/events"
    
    # TheSpike.gg endpoints
    SPIKE_BASE_URL: str = "https://www.thespike.gg"
    SPIKE_MATCHES_URL: str = f"{SPIKE_BASE_URL}/matches"
    SPIKE_STATS_URL: str = f"{SPIKE_BASE_URL}/stats"
    
    # Liquipedia endpoints
    LIQUIPEDIA_BASE_URL: str = "https://liquipedia.net/valorant"
    LIQUIPEDIA_API_URL: str = "https://liquipedia.net/valorant/api.php"
    
    # Riot API (if available)
    RIOT_API_BASE: str = "https://americas.api.riotgames.com"
    RIOT_API_KEY: Optional[str] = os.getenv("RIOT_API_KEY")


@dataclass
class DataRetentionConfig:
    """Configuration for data retention and cleanup"""
    
    # Match data retention (days)
    MATCH_DATA_RETENTION_DAYS: int = 730  # 2 years
    
    # Player stats retention (days)
    PLAYER_STATS_RETENTION_DAYS: int = 365  # 1 year
    
    # Cache retention (hours)
    CACHE_RETENTION_HOURS: int = 24
    
    # Log retention (days)
    LOG_RETENTION_DAYS: int = 30
    
    # Raw scraping data retention (days)
    RAW_DATA_RETENTION_DAYS: int = 90


@dataclass
class PipelineConfig:
    """Configuration for pipeline scheduling and execution"""
    
    # DAG scheduling
    MAIN_DAG_SCHEDULE: str = "0 */6 * * *"  # Every 6 hours
    MATCH_DATA_SCHEDULE: str = "0 */2 * * *"  # Every 2 hours
    PLAYER_DATA_SCHEDULE: str = "0 6 * * *"   # Daily at 6 AM
    TEAM_DATA_SCHEDULE: str = "0 8 * * *"     # Daily at 8 AM
    PATCH_DATA_SCHEDULE: str = "0 12 * * 1"   # Weekly on Monday at noon
    
    # Task timeouts
    DEFAULT_TASK_TIMEOUT: timedelta = timedelta(minutes=30)
    SCRAPING_TASK_TIMEOUT: timedelta = timedelta(hours=2)
    DATA_PROCESSING_TIMEOUT: timedelta = timedelta(hours=1)
    
    # Retry configuration
    DEFAULT_RETRIES: int = 3
    RETRY_DELAY: timedelta = timedelta(minutes=5)
    RETRY_EXPONENTIAL_BACKOFF: bool = True
    
    # Concurrency limits
    MAX_CONCURRENT_SCRAPING_TASKS: int = 3
    MAX_CONCURRENT_PROCESSING_TASKS: int = 5


class Settings:
    """Main settings class that consolidates all configuration"""
    
    # Environment
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    
    # Application info
    APP_NAME: str = "Valorant Data Pipeline"
    VERSION: str = "1.0.0"
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Configuration instances
    database = DatabaseConfig()
    redis = RedisConfig()
    scraping = ScrapingTargets()
    retention = DataRetentionConfig()
    pipeline = PipelineConfig()
    
    # Feature flags for different data sources
    ENABLE_VLR_SCRAPING: bool = os.getenv("ENABLE_VLR_SCRAPING", "true").lower() == "true"
    ENABLE_SPIKE_SCRAPING: bool = os.getenv("ENABLE_SPIKE_SCRAPING", "true").lower() == "true"
    ENABLE_LIQUIPEDIA_SCRAPING: bool = os.getenv("ENABLE_LIQUIPEDIA_SCRAPING", "true").lower() == "true"
    ENABLE_RIOT_API: bool = os.getenv("ENABLE_RIOT_API", "false").lower() == "true"
    
    # Data quality thresholds
    MIN_MATCH_DATA_COMPLETENESS: float = 0.8  # 80% of required fields
    MIN_PLAYER_STATS_COMPLETENESS: float = 0.7  # 70% of stats fields
    MAX_ALLOWED_DATA_AGE_HOURS: int = 48  # Maximum age for "recent" data
    
    # Rate limiting (requests per minute)
    DEFAULT_RATE_LIMIT: int = 60
    VLR_RATE_LIMIT: int = 30
    SPIKE_RATE_LIMIT: int = 60
    LIQUIPEDIA_RATE_LIMIT: int = 120
    
    # User agents for scraping
    USER_AGENTS: List[str] = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    
    # Notification settings
    SLACK_WEBHOOK_URL: Optional[str] = os.getenv("SLACK_WEBHOOK_URL")
    EMAIL_NOTIFICATIONS_ENABLED: bool = os.getenv("EMAIL_NOTIFICATIONS", "false").lower() == "true"
    NOTIFICATION_EMAIL: Optional[str] = os.getenv("NOTIFICATION_EMAIL")
    
    # Data validation thresholds
    OUTLIER_DETECTION_THRESHOLD: float = 3.0  # Z-score threshold
    MINIMUM_MATCHES_FOR_TEAM_STATS: int = 5
    MINIMUM_MAPS_FOR_PLAYER_STATS: int = 10
    
    @classmethod
    def get_data_sources(cls) -> Dict[str, bool]:
        """Returns enabled data sources"""
        return {
            "vlr": cls.ENABLE_VLR_SCRAPING,
            "thespike": cls.ENABLE_SPIKE_SCRAPING,
            "liquipedia": cls.ENABLE_LIQUIPEDIA_SCRAPING,
            "riot_api": cls.ENABLE_RIOT_API,
        }
    
    @classmethod
    def is_production(cls) -> bool:
        """Check if running in production environment"""
        return cls.ENVIRONMENT.lower() == "production"
    
    @classmethod
    def validate_config(cls) -> bool:
        """Validate critical configuration settings"""
        required_settings = [
            cls.database.host,
            cls.database.database,
            cls.database.username,
        ]
        
        missing_settings = [setting for setting in required_settings if not setting]
        
        if missing_settings:
            raise ValueError(f"Missing required configuration: {missing_settings}")
        
        if cls.ENABLE_RIOT_API and not cls.scraping.RIOT_API_KEY:
            raise ValueError("Riot API enabled but no API key provided")
        
        return True


# Global settings instance
settings = Settings()

# Validate configuration on import
if __name__ != "__main__":
    settings.validate_config()
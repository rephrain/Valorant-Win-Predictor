
"""
Utility module for CSV handling and rate limiting.
"""

from .csv_handler import save_to_csv, load_from_csv
from .rate_limiter import rate_limit

__all__ = [
    'save_to_csv',
    'load_from_csv',
    'rate_limit'
]

# Scraping configuration
VLR_BASE_URL = "https://www.vlr.gg"
LIQUIPEDIA_BASE_URL = "https://liquipedia.net/valorant"
RIOT_PATCHES_URL = "https://playvalorant.com/en-us/news/game-updates/"

# Rate limiting
SCRAPE_DELAY = 2  # seconds between requests
MAX_RETRIES = 3

# Feature engineering
RECENT_MAPS_WINDOW = 20
DECAY_FACTOR = 0.95
ELO_K_FACTOR = 32
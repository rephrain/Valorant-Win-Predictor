"""
Scrapers module for Valorant match prediction data collection.
Includes scrapers for VLR.gg, Liquipedia, and Riot patch notes.
"""

from .tournaments_scraper import scrape_tournaments_data
from .tournaments_detail_scraper import scrape_tournaments_detail_data
from .tournaments_match_scraper import scrape_tournaments_match_data
from .tournaments_match_overview_scraper import scrape_tournaments_match_overview_data
from .tournaments_match_round_scraper import scrape_tournaments_match_round_data

__all__ = [
    'scrape_tournaments_data',
    'scrape_tournaments_detail_data',
    'scrape_tournaments_match_data',
    'scrape_tournaments_match_overview_data',
    'scrape_tournaments_match_round_data'
]

__version__ = '1.0.0'
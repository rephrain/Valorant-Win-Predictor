"""
Scrapers module for Valorant match prediction data collection.
Includes scrapers for VLR.gg, Liquipedia, and Riot patch notes.
"""

from .tournaments_scraper import scrape_tournaments_data
from .tournaments_detail_scraper import scrape_tournaments_detail_data
from .tournaments_match_scraper import scrape_tournaments_match_data
from .tournaments_game_scraper import scrape_tournaments_game_data
from .tournaments_game_overview_scraper import scrape_tournaments_game_overview_data
from .tournaments_game_round_scraper import scrape_tournaments_game_round_data
from .tournaments_game_head2head_scraper import scrape_tournaments_game_head2head_data
from .tournaments_game_performance_scraper import scrape_tournaments_game_performance_data
from .tournaments_game_economy_team_scraper import scrape_tournaments_game_economy_team_data
from .tournaments_game_economy_round_scraper import scrape_tournaments_game_economy_round_data

__all__ = [
    'scrape_tournaments_data',
    'scrape_tournaments_detail_data',
    'scrape_tournaments_match_data',
    'scrape_tournaments_game_data',
    'scrape_tournaments_game_overview_data',
    'scrape_tournaments_game_round_data',
    'scrape_tournaments_game_head2head_data',
    'scrape_tournaments_game_performance_data',
    'scrape_tournaments_game_economy_team_data',
    'scrape_tournaments_game_economy_round_data'
]

__version__ = '1.0.0'
"""
Configuration module containing URLs, parameters, and settings.
"""

from .settings import (
    VLR_BASE_URL,
    LIQUIPEDIA_BASE_URL,
    RIOT_PATCHES_URL,
    SCRAPE_DELAY,
    MAX_RETRIES,
    RECENT_MAPS_WINDOW,
    DECAY_FACTOR,
    ELO_K_FACTOR
)

__all__ = [
    'VLR_BASE_URL',
    'LIQUIPEDIA_BASE_URL',
    'RIOT_PATCHES_URL',
    'SCRAPE_DELAY',
    'MAX_RETRIES',
    'RECENT_MAPS_WINDOW',
    'DECAY_FACTOR',
    'ELO_K_FACTOR'
]
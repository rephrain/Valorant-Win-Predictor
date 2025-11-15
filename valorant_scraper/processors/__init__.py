"""
Processors module for data aggregation and feature engineering.
Transforms raw scraped data into model-ready features.
"""

from .aggregator import aggregate_all_data
from .feature_engineering import engineer_features

__all__ = [
    'aggregate_all_data',
    'engineer_features'
]

__version__ = '1.0.0'
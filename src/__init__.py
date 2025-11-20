"""
PostgreSQL Performance Analyser - Core Package
"""
from .db_connector import DatabaseConnector
from .query_parser import QueryParser
from .recommender import IndexRecommender, IndexRecommendation

__all__ = [
    'DatabaseConnector',
    'QueryParser',
    'IndexRecommender',
    'IndexRecommendation',
]

__version__ = '1.0.0'

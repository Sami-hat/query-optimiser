"""
PostgreSQL Performance Analyser - Core Package
"""
from .db_connector import DatabaseConnector
from .query_parser import QueryParser
from .recommender import IndexRecommender, IndexRecommendation
from .batch_analyser import (
    BatchAnalyser,
    BatchAnalysisReport,
    QueryStats,
    AnalysisResult
)

__all__ = [
    'DatabaseConnector',
    'QueryParser',
    'IndexRecommender',
    'IndexRecommendation',
    'BatchAnalyser',
    'BatchAnalysisReport',
    'QueryStats',
    'AnalysisResult',
]

__version__ = '1.0.0'

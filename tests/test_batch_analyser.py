"""
Unit tests for BatchAnalyser module
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from src.batch_analyser import (
    BatchAnalyser,
    BatchAnalysisReport,
    QueryStats,
    AnalysisResult
)
from src.recommender import IndexRecommendation


class TestQueryStats:
    """Tests for QueryStats dataclass"""

    def test_cache_hit_ratio_all_hits(self):
        """Cache hit ratio should be 1.0 when all blocks are hits"""
        stats = QueryStats(
            query="SELECT 1",
            shared_blks_hit=100,
            shared_blks_read=0
        )
        assert stats.cache_hit_ratio == 1.0

    def test_cache_hit_ratio_no_hits(self):
        """Cache hit ratio should be 0.0 when all blocks are reads"""
        stats = QueryStats(
            query="SELECT 1",
            shared_blks_hit=0,
            shared_blks_read=100
        )
        assert stats.cache_hit_ratio == 0.0

    def test_cache_hit_ratio_mixed(self):
        """Cache hit ratio should calculate correctly for mixed access"""
        stats = QueryStats(
            query="SELECT 1",
            shared_blks_hit=75,
            shared_blks_read=25
        )
        assert stats.cache_hit_ratio == 0.75

    def test_cache_hit_ratio_no_blocks(self):
        """Cache hit ratio should be 1.0 when no blocks accessed"""
        stats = QueryStats(
            query="SELECT 1",
            shared_blks_hit=0,
            shared_blks_read=0
        )
        assert stats.cache_hit_ratio == 1.0


class TestAnalysisResult:
    """Tests for AnalysisResult dataclass"""

    def test_to_dict_basic(self):
        """Test basic conversion to dictionary"""
        result = AnalysisResult(
            query="SELECT * FROM users",
            execution_time_ms=100.5,
            planning_time_ms=1.2,
            total_cost=500.0
        )

        d = result.to_dict()

        assert d['query'] == "SELECT * FROM users"
        assert d['execution_time_ms'] == 100.5
        assert d['planning_time_ms'] == 1.2
        assert d['total_cost'] == 500.0
        assert d['error'] is None
        assert d['seq_scans'] == []
        assert d['recommendations'] == []

    def test_to_dict_with_recommendations(self):
        """Test conversion with recommendations"""
        rec = IndexRecommendation(
            table_name='users',
            columns=['email'],
            reason='Sequential scan',
            expected_improvement_pct=50.0,
            current_cost=1000.0,
            estimated_cost=500.0,
            priority=100
        )

        result = AnalysisResult(
            query="SELECT * FROM users WHERE email = 'test'",
            recommendations=[rec]
        )

        d = result.to_dict()

        assert len(d['recommendations']) == 1
        assert d['recommendations'][0]['table'] == 'users'
        assert d['recommendations'][0]['columns'] == ['email']
        assert d['recommendations'][0]['ddl'] == 'CREATE INDEX idx_users_email ON users (email);'

    def test_to_dict_with_error(self):
        """Test conversion with error"""
        result = AnalysisResult(
            query="INVALID SQL",
            error="Syntax error"
        )

        d = result.to_dict()

        assert d['error'] == "Syntax error"


class TestBatchAnalysisReport:
    """Tests for BatchAnalysisReport dataclass"""

    def test_to_json(self):
        """Test JSON serialisation"""
        report = BatchAnalysisReport(
            timestamp="2024-01-01T00:00:00",
            total_queries=10,
            analysed_queries=8,
            failed_queries=2
        )

        json_str = report.to_json()

        assert '"total_queries": 10' in json_str
        assert '"analysed_queries": 8' in json_str
        assert '"failed_queries": 2' in json_str

    def test_get_summary(self):
        """Test human-readable summary generation"""
        report = BatchAnalysisReport(
            timestamp="2024-01-01T00:00:00",
            total_queries=100,
            analysed_queries=95,
            failed_queries=5,
            total_seq_scans=50,
            seq_scans_with_recommendations=45,
            unique_recommendations=10,
            tables_affected=['users', 'orders'],
            total_current_cost=10000.0,
            total_estimated_cost=3000.0,
            estimated_improvement_pct=70.0,
            analysis_duration_seconds=15.5
        )

        summary = report.get_summary()

        assert "BATCH ANALYSIS REPORT" in summary
        assert "Total queries: 100" in summary
        assert "Successfully analysed: 95" in summary
        assert "Failed: 5" in summary
        assert "Total sequential scans found: 50" in summary
        assert "Unique index recommendations: 10" in summary
        assert "Estimated improvement: 70.0%" in summary


class TestBatchAnalyser:
    """Tests for BatchAnalyser class"""

    @pytest.fixture
    def mock_db_connector(self):
        """Create a mock database connector"""
        mock = Mock()
        mock.get_connection.return_value = MagicMock()
        mock.return_connection.return_value = None
        mock.get_explain_plan.return_value = {
            'Plan': {
                'Node Type': 'Seq Scan',
                'Relation Name': 'users',
                'Total Cost': 1000.0,
                'Actual Total Time': 50.0
            },
            'Execution Time': 50.0,
            'Planning Time': 1.0
        }
        mock.extract_execution_metrics.return_value = {
            'execution_time': 50.0,
            'planning_time': 1.0,
            'total_cost': 1000.0
        }
        mock.detect_sequential_scans.return_value = [
            {
                'table_name': 'users',
                'total_cost': 1000.0,
                'scan_time': 50.0,
                'rows_scanned': 500000,
                'rows_removed_by_filter': 499999
            }
        ]
        return mock

    def test_init(self, mock_db_connector):
        """Test BatchAnalyser initialisation"""
        analyser = BatchAnalyser(
            mock_db_connector,
            max_workers=5,
            min_calls=20,
            min_mean_time_ms=50.0
        )

        assert analyser.max_workers == 5
        assert analyser.min_calls == 20
        assert analyser.min_mean_time_ms == 50.0

    def test_analyse_single_query(self, mock_db_connector):
        """Test single query analysis"""
        analyser = BatchAnalyser(mock_db_connector)

        result = analyser.analyse_single_query(
            "SELECT * FROM users WHERE email = 'test@example.com'"
        )

        assert result.error is None
        assert result.execution_time_ms == 50.0
        assert result.planning_time_ms == 1.0
        assert result.total_cost == 1000.0
        assert len(result.seq_scans) == 1
        assert result.seq_scans[0]['table_name'] == 'users'

    def test_analyse_single_query_with_placeholder(self, mock_db_connector):
        """Test query with $1 placeholders gets processed"""
        analyser = BatchAnalyser(mock_db_connector)

        result = analyser.analyse_single_query(
            "SELECT * FROM users WHERE id = $1"
        )

        # Should not error - placeholders replaced
        assert result.error is None
        # Verify the query was modified
        mock_db_connector.get_explain_plan.assert_called()

    def test_analyse_single_query_error(self, mock_db_connector):
        """Test error handling in single query analysis"""
        mock_db_connector.get_explain_plan.side_effect = Exception("Connection failed")

        analyser = BatchAnalyser(mock_db_connector)
        result = analyser.analyse_single_query("SELECT 1")

        assert result.error == "Connection failed"

    def test_analyse_queries_parallel(self, mock_db_connector):
        """Test parallel query analysis"""
        analyser = BatchAnalyser(mock_db_connector, max_workers=3)

        queries = [
            "SELECT * FROM users WHERE email = 'a@b.com'",
            "SELECT * FROM orders WHERE status = 'pending'",
            "SELECT * FROM products WHERE price > 100"
        ]

        report = analyser.analyse_queries(queries)

        assert report.total_queries == 3
        assert report.analysed_queries == 3
        assert report.failed_queries == 0

    def test_analyse_queries_with_progress(self, mock_db_connector):
        """Test progress callback is called"""
        analyser = BatchAnalyser(mock_db_connector, max_workers=1)

        progress_calls = []

        def progress_callback(current, total):
            progress_calls.append((current, total))

        queries = ["SELECT 1", "SELECT 2", "SELECT 3"]
        analyser.analyse_queries(queries, progress_callback=progress_callback)

        # Should have 3 progress calls
        assert len(progress_calls) == 3
        # All should have total=3
        assert all(total == 3 for _, total in progress_calls)
        # Should have called with 1, 2, 3 (order may vary due to parallelism)
        currents = sorted([c for c, _ in progress_calls])
        assert currents == [1, 2, 3]

    def test_analyse_queries_aggregation(self, mock_db_connector):
        """Test result aggregation"""
        analyser = BatchAnalyser(mock_db_connector)

        queries = [
            "SELECT * FROM users WHERE email = 'test@example.com'"
        ]

        report = analyser.analyse_queries(queries)

        assert report.total_seq_scans >= 1
        assert len(report.tables_affected) >= 1
        assert 'users' in report.tables_affected

    def test_replace_placeholders(self, mock_db_connector):
        """Test placeholder replacement"""
        analyser = BatchAnalyser(mock_db_connector)

        query = "SELECT * FROM users WHERE id = $1 AND status = $2"
        replaced = analyser._replace_placeholders(query)

        assert '$1' not in replaced
        assert '$2' not in replaced
        assert "'placeholder'" in replaced

    def test_replace_placeholders_high_numbers(self, mock_db_connector):
        """Test placeholder replacement with high numbers"""
        analyser = BatchAnalyser(mock_db_connector)

        query = "SELECT * FROM t WHERE a = $1 AND b = $10 AND c = $2"
        replaced = analyser._replace_placeholders(query)

        assert '$1' not in replaced
        assert '$2' not in replaced
        assert '$10' not in replaced


class TestBatchAnalyserIntegration:
    """Integration tests that require a real database connection"""

    @pytest.fixture
    def db_connector(self):
        """Create a real database connector if available"""
        try:
            from src.db_connector import DatabaseConnector
            connector = DatabaseConnector()
            if connector.test_connection():
                yield connector
                connector.close()
            else:
                pytest.skip("Database connection not available")
        except Exception:
            pytest.skip("Database connection not available")

    def test_analyse_real_query(self, db_connector):
        """Test analysis with real database"""
        analyser = BatchAnalyser(db_connector)

        result = analyser.analyse_single_query(
            "SELECT * FROM users WHERE email = 'user1@example.com'"
        )

        assert result.error is None
        assert result.total_cost > 0

    def test_get_existing_indexes(self, db_connector):
        """Test fetching existing indexes"""
        analyser = BatchAnalyser(db_connector)

        indexes = analyser.get_existing_indexes()

        # Should return a list
        assert isinstance(indexes, list)
        # Each index should have required keys
        for idx in indexes:
            assert 'table' in idx
            assert 'index_name' in idx
            assert 'definition' in idx

    def test_get_table_statistics(self, db_connector):
        """Test fetching table statistics"""
        analyser = BatchAnalyser(db_connector)

        stats = analyser.get_table_statistics()

        # Should return a list
        assert isinstance(stats, list)

        # Find users table
        users_stats = next((s for s in stats if s['table_name'] == 'users'), None)
        if users_stats:
            # Note: row_count may be 0 if ANALYZE hasn't been run
            assert 'row_count' in users_stats
            assert 'write_ratio' in users_stats

    def test_batch_analyse_multiple_queries(self, db_connector):
        """Test batch analysis with multiple queries"""
        analyser = BatchAnalyser(db_connector, max_workers=3)

        queries = [
            "SELECT * FROM users WHERE email = 'user100@example.com'",
            "SELECT * FROM users WHERE name LIKE 'User1%'",
            "SELECT * FROM orders WHERE user_id = 1000"
        ]

        report = analyser.analyse_queries(queries)

        assert report.total_queries == 3
        assert report.analysed_queries > 0
        assert report.analysis_duration_seconds > 0

        # Should have some recommendations for unindexed columns
        # (This depends on actual database state)
        summary = report.get_summary()
        assert "BATCH ANALYSIS REPORT" in summary


class TestFilterRecommendations:
    """Tests for filtering recommendations by existing indexes"""

    @pytest.fixture
    def mock_db_connector(self):
        """Create a mock database connector"""
        mock = Mock()
        mock.get_connection.return_value = MagicMock()
        mock.return_connection.return_value = None
        return mock

    def test_filter_already_indexed(self, mock_db_connector):
        """Filter out recommendations for already-indexed columns"""
        analyser = BatchAnalyser(mock_db_connector)

        # Mock get_existing_indexes
        analyser.get_existing_indexes = Mock(return_value=[
            {
                'schema': 'public',
                'table': 'users',
                'index_name': 'idx_users_email',
                'definition': 'CREATE INDEX idx_users_email ON users (email)'
            }
        ])

        recommendations = [
            IndexRecommendation(
                table_name='users',
                columns=['email'],
                reason='Test'
            ),
            IndexRecommendation(
                table_name='users',
                columns=['name'],
                reason='Test'
            )
        ]

        filtered = analyser.filter_recommendations_by_existing_indexes(recommendations)

        # Email should be filtered out, name should remain
        assert len(filtered) == 1
        assert filtered[0].columns == ['name']

    def test_filter_keeps_unindexed(self, mock_db_connector):
        """Keep recommendations for columns not yet indexed"""
        analyser = BatchAnalyser(mock_db_connector)

        analyser.get_existing_indexes = Mock(return_value=[])

        recommendations = [
            IndexRecommendation(
                table_name='users',
                columns=['email'],
                reason='Test'
            )
        ]

        filtered = analyser.filter_recommendations_by_existing_indexes(recommendations)

        assert len(filtered) == 1

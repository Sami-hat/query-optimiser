"""
Tests for DatabaseConnector
"""
import pytest
import os
from unittest.mock import Mock, patch, MagicMock
from src.db_connector import DatabaseConnector


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set up mock environment variables for testing."""
    monkeypatch.setenv('DB_HOST', 'localhost')
    monkeypatch.setenv('DB_PORT', '5432')
    monkeypatch.setenv('DB_NAME', 'test_db')
    monkeypatch.setenv('DB_USER', 'test_user')
    monkeypatch.setenv('DB_PASSWORD', 'test_password')


@pytest.fixture
def sample_explain_plan():
    """Sample EXPLAIN ANALYZE output in JSON format."""
    return {
        'query': "SELECT * FROM users WHERE email = 'test@example.com'",
        'explain_plan': {
            'Plan': {
                'Node Type': 'Seq Scan',
                'Relation Name': 'users',
                'Alias': 'users',
                'Startup Cost': 0.00,
                'Total Cost': 145.50,
                'Plan Rows': 1000,
                'Actual Rows': 1,
                'Actual Total Time': 2.456,
                'Filter': "(email = 'test@example.com'::text)",
                'Rows Removed by Filter': 999
            },
            'Planning Time': 0.123,
            'Execution Time': 2.580
        },
        'analyzed': True
    }


@pytest.fixture
def nested_explain_plan():
    """EXPLAIN plan with nested structure (JOIN)."""
    return {
        'query': "SELECT u.*, o.id FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'pending'",
        'explain_plan': {
            'Plan': {
                'Node Type': 'Hash Join',
                'Startup Cost': 50.00,
                'Total Cost': 350.75,
                'Plan Rows': 500,
                'Actual Rows': 450,
                'Actual Total Time': 15.234,
                'Plans': [
                    {
                        'Node Type': 'Seq Scan',
                        'Relation Name': 'users',
                        'Alias': 'u',
                        'Startup Cost': 0.00,
                        'Total Cost': 100.00,
                        'Plan Rows': 1000,
                        'Actual Rows': 1000,
                        'Actual Total Time': 5.123
                    },
                    {
                        'Node Type': 'Seq Scan',
                        'Relation Name': 'orders',
                        'Alias': 'o',
                        'Startup Cost': 0.00,
                        'Total Cost': 200.00,
                        'Plan Rows': 500,
                        'Actual Rows': 450,
                        'Actual Total Time': 8.456,
                        'Filter': "(status = 'pending'::text)",
                        'Rows Removed by Filter': 550
                    }
                ]
            },
            'Planning Time': 0.234,
            'Execution Time': 15.500
        },
        'analyzed': True
    }


class TestDatabaseConnector:
    """Test suite for DatabaseConnector class."""

    def test_init_with_env_vars(self, mock_env_vars):
        """Test initialization with environment variables."""
        with patch('psycopg2.pool.ThreadedConnectionPool'):
            connector = DatabaseConnector()
            assert connector.host == 'localhost'
            assert connector.port == 5432
            assert connector.database == 'test_db'
            assert connector.user == 'test_user'
            assert connector.password == 'test_password'

    def test_init_with_explicit_params(self):
        """Test initialization with explicit parameters."""
        with patch('psycopg2.pool.ThreadedConnectionPool'):
            connector = DatabaseConnector(
                host='custom_host',
                port=5433,
                database='custom_db',
                user='custom_user',
                password='custom_pass'
            )
            assert connector.host == 'custom_host'
            assert connector.port == 5433
            assert connector.database == 'custom_db'

    def test_init_missing_credentials(self, monkeypatch):
        """Test that initialization fails without credentials."""
        monkeypatch.delenv('DB_NAME', raising=False)
        monkeypatch.delenv('DB_USER', raising=False)
        monkeypatch.delenv('DB_PASSWORD', raising=False)

        with pytest.raises(ValueError, match="Database credentials not provided"):
            DatabaseConnector()

    def test_connection_pool_initialization(self, mock_env_vars):
        """Test that connection pool is initialized correctly."""
        with patch('psycopg2.pool.ThreadedConnectionPool') as mock_pool:
            connector = DatabaseConnector(pool_min=2, pool_max=10)
            mock_pool.assert_called_once()
            args = mock_pool.call_args
            assert args[0] == (2, 10)

    def test_get_explain_plan_structure(self, mock_env_vars, sample_explain_plan):
        """Test that get_explain_plan returns correct structure."""
        with patch('psycopg2.pool.ThreadedConnectionPool'):
            connector = DatabaseConnector()

            # Mock the database connection and cursor
            mock_cursor = Mock()
            mock_cursor.fetchone.return_value = ([sample_explain_plan['explain_plan']],)

            mock_conn = Mock()
            mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)

            connector.connection_pool = Mock()
            connector.connection_pool.getconn.return_value = mock_conn
            connector.connection_pool.putconn = Mock()

            result = connector.get_explain_plan("SELECT * FROM users WHERE email = 'test@example.com'")

            assert 'query' in result
            assert 'explain_plan' in result
            assert 'analyzed' in result
            assert result['analyzed'] is True

    def test_get_explain_plan_empty_query(self, mock_env_vars):
        """Test that empty query raises ValueError."""
        with patch('psycopg2.pool.ThreadedConnectionPool'):
            connector = DatabaseConnector()

            with pytest.raises(ValueError, match="Query cannot be empty"):
                connector.get_explain_plan("")

            with pytest.raises(ValueError, match="Query cannot be empty"):
                connector.get_explain_plan("   ")

    def test_extract_execution_metrics(self, mock_env_vars, sample_explain_plan):
        """Test extraction of execution metrics."""
        with patch('psycopg2.pool.ThreadedConnectionPool'):
            connector = DatabaseConnector()
            metrics = connector.extract_execution_metrics(sample_explain_plan)

            assert metrics['execution_time'] == 2.580
            assert metrics['planning_time'] == 0.123
            assert metrics['total_cost'] == 145.50
            assert metrics['actual_rows'] == 1
            assert metrics['node_type'] == 'Seq Scan'
            assert metrics['startup_cost'] == 0.00

    def test_detect_sequential_scans_single(self, mock_env_vars, sample_explain_plan):
        """Test detection of single sequential scan."""
        with patch('psycopg2.pool.ThreadedConnectionPool'):
            connector = DatabaseConnector()
            scans = connector.detect_sequential_scans(sample_explain_plan)

            assert len(scans) == 1
            assert scans[0]['table_name'] == 'users'
            assert scans[0]['rows_scanned'] == 1
            assert scans[0]['scan_time'] == 2.456
            assert scans[0]['total_cost'] == 145.50
            assert scans[0]['filter'] == "(email = 'test@example.com'::text)"
            assert scans[0]['rows_removed_by_filter'] == 999

    def test_detect_sequential_scans_multiple(self, mock_env_vars, nested_explain_plan):
        """Test detection of multiple sequential scans in nested plan."""
        with patch('psycopg2.pool.ThreadedConnectionPool'):
            connector = DatabaseConnector()
            scans = connector.detect_sequential_scans(nested_explain_plan)

            assert len(scans) == 2

            # Check first scan (users)
            users_scan = next(s for s in scans if s['table_name'] == 'users')
            assert users_scan['rows_scanned'] == 1000
            assert users_scan['alias'] == 'u'

            # Check second scan (orders)
            orders_scan = next(s for s in scans if s['table_name'] == 'orders')
            assert orders_scan['rows_scanned'] == 450
            assert orders_scan['alias'] == 'o'
            assert orders_scan['rows_removed_by_filter'] == 550

    def test_detect_sequential_scans_none(self, mock_env_vars):
        """Test that no sequential scans are found when using index."""
        index_scan_plan = {
            'query': "SELECT * FROM users WHERE id = 1000",
            'explain_plan': {
                'Plan': {
                    'Node Type': 'Index Scan',
                    'Relation Name': 'users',
                    'Index Name': 'users_pkey',
                    'Startup Cost': 0.00,
                    'Total Cost': 8.27,
                    'Plan Rows': 1,
                    'Actual Rows': 1,
                    'Actual Total Time': 0.045
                },
                'Planning Time': 0.089,
                'Execution Time': 0.123
            },
            'analyzed': True
        }

        with patch('psycopg2.pool.ThreadedConnectionPool'):
            connector = DatabaseConnector()
            scans = connector.detect_sequential_scans(index_scan_plan)

            assert len(scans) == 0

    def test_test_connection_success(self, mock_env_vars):
        """Test successful connection test."""
        with patch('psycopg2.pool.ThreadedConnectionPool'):
            connector = DatabaseConnector()

            mock_cursor = Mock()
            mock_cursor.fetchone.return_value = (1,)

            mock_conn = Mock()
            mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)

            connector.connection_pool = Mock()
            connector.connection_pool.getconn.return_value = mock_conn
            connector.connection_pool.putconn = Mock()

            assert connector.test_connection() is True

    def test_test_connection_failure(self, mock_env_vars):
        """Test failed connection test."""
        with patch('psycopg2.pool.ThreadedConnectionPool'):
            connector = DatabaseConnector()

            connector.connection_pool = Mock()
            connector.connection_pool.getconn.side_effect = Exception("Connection failed")

            assert connector.test_connection() is False

    def test_close_pool(self, mock_env_vars):
        """Test closing connection pool."""
        with patch('psycopg2.pool.ThreadedConnectionPool'):
            connector = DatabaseConnector()
            connector.connection_pool.closeall = Mock()

            connector.close()

            connector.connection_pool.closeall.assert_called_once()

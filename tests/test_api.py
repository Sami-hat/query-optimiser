"""
Unit tests for FastAPI endpoints
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from fastapi.testclient import TestClient

# Mock the database connector before importing the app
with patch('src.api.main.DatabaseConnector') as mock_db:
    mock_db.return_value.test_connection.return_value = True
    from src.api.main import app

client = TestClient(app)


class TestHealthEndpoint:
    """Tests for health check endpoint."""

    def test_health_check(self):
        """Health endpoint returns status."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "database_connected" in data
        assert "version" in data


class TestAnalyseEndpoint:
    """Tests for single query analysis endpoint."""

    @patch('src.api.main.db_connector')
    @patch('src.api.main.recommender')
    def test_analyse_query_success(self, mock_recommender, mock_db):
        """Test successful query analysis."""
        # Setup mocks
        mock_db.test_connection.return_value = True
        mock_db.get_explain_plan.return_value = {
            'query': 'SELECT * FROM users',
            'explain_plan': {
                'Plan': {
                    'Node Type': 'Seq Scan',
                    'Total Cost': 1000.0,
                    'Actual Rows': 100
                },
                'Execution Time': 50.0,
                'Planning Time': 1.0
            }
        }
        mock_db.extract_execution_metrics.return_value = {
            'execution_time': 50.0,
            'planning_time': 1.0,
            'total_cost': 1000.0,
            'actual_rows': 100,
            'node_type': 'Seq Scan'
        }
        mock_db.detect_sequential_scans.return_value = []
        mock_recommender.analyse_query.return_value = []

        response = client.post(
            "/analyse",
            json={"query": "SELECT * FROM users"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data['query'] == "SELECT * FROM users"
        assert 'metrics' in data
        assert 'recommendations' in data

    @patch('src.api.main.db_connector')
    def test_analyse_query_empty(self, mock_db):
        """Test empty query is rejected."""
        mock_db.test_connection.return_value = True
        response = client.post(
            "/analyse",
            json={"query": ""}
        )
        assert response.status_code == 422  # Validation error

    @patch('src.api.main.db_connector')
    def test_analyse_query_missing(self, mock_db):
        """Test missing query field is rejected."""
        mock_db.test_connection.return_value = True
        response = client.post(
            "/analyse",
            json={}
        )
        assert response.status_code == 422


class TestBatchAnalyseEndpoint:
    """Tests for batch analysis endpoint."""

    @patch('src.api.main.db_connector')
    @patch('src.api.main.BatchAnalyser')
    def test_batch_analyse_success(self, mock_analyser_class, mock_db):
        """Test successful batch analysis."""
        mock_db.test_connection.return_value = True

        # Create mock report
        mock_report = Mock()
        mock_report.timestamp = "2024-01-01T00:00:00"
        mock_report.total_queries = 2
        mock_report.analysed_queries = 2
        mock_report.failed_queries = 0
        mock_report.total_seq_scans = 1
        mock_report.seq_scans_with_recommendations = 1
        mock_report.tables_affected = ['users']
        mock_report.total_current_cost = 1000.0
        mock_report.total_estimated_cost = 500.0
        mock_report.estimated_improvement_pct = 50.0
        mock_report.top_recommendations = []
        mock_report.analysis_duration_seconds = 1.5

        mock_analyser = Mock()
        mock_analyser.analyse_queries.return_value = mock_report
        mock_analyser_class.return_value = mock_analyser

        response = client.post(
            "/batch-analyse",
            json={"queries": ["SELECT 1", "SELECT 2"]}
        )

        assert response.status_code == 200
        data = response.json()
        assert data['total_queries'] == 2

    @patch('src.api.main.db_connector')
    def test_batch_analyse_empty_queries(self, mock_db):
        """Test empty queries list is rejected."""
        mock_db.test_connection.return_value = True
        response = client.post(
            "/batch-analyse",
            json={"queries": []}
        )
        assert response.status_code == 422

    @patch('src.api.main.db_connector')
    def test_batch_analyse_max_workers_validation(self, mock_db):
        """Test max_workers validation."""
        mock_db.test_connection.return_value = True
        response = client.post(
            "/batch-analyse",
            json={"queries": ["SELECT 1"], "max_workers": 50}  # Above limit
        )
        assert response.status_code == 422


class TestTablesEndpoint:
    """Tests for tables statistics endpoint."""

    @patch('src.api.main.db_connector')
    @patch('src.api.main.BatchAnalyser')
    def test_get_tables(self, mock_analyser_class, mock_db):
        """Test getting table statistics."""
        mock_db.test_connection.return_value = True

        mock_analyser = Mock()
        mock_analyser.get_table_statistics.return_value = [
            {
                'table_name': 'users',
                'row_count': 1000,
                'dead_rows': 10,
                'total_size': '100 MB',
                'seq_scans': 50,
                'index_scans': 200,
                'write_ratio': 0.3
            }
        ]
        mock_analyser_class.return_value = mock_analyser

        response = client.get("/tables")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]['table_name'] == 'users'


class TestRecommendationsEndpoint:
    """Tests for table recommendations endpoint."""

    @patch('src.api.main.db_connector')
    @patch('src.api.main.BatchAnalyser')
    def test_get_recommendations(self, mock_analyser_class, mock_db):
        """Test getting recommendations for a table."""
        mock_db.test_connection.return_value = True

        mock_analyser = Mock()
        mock_analyser.get_existing_indexes.return_value = [
            {
                'schema': 'public',
                'table': 'users',
                'index_name': 'users_pkey',
                'definition': 'CREATE UNIQUE INDEX users_pkey ON users(id)'
            }
        ]
        mock_analyser_class.return_value = mock_analyser

        response = client.get("/recommendations/users")

        assert response.status_code == 200
        data = response.json()
        assert data['table_name'] == 'users'
        assert 'existing_indexes' in data


class TestApplyIndexesEndpoint:
    """Tests for apply indexes endpoint."""

    @patch('src.api.main.db_connector')
    def test_apply_indexes_dry_run(self, mock_db):
        """Test dry run mode."""
        mock_db.test_connection.return_value = True

        response = client.post(
            "/apply-indexes",
            json={
                "ddl_statements": ["CREATE INDEX idx_test ON users (email)"],
                "dry_run": True
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data['successful'] == 1
        assert data['results'][0]['success'] is True

    @patch('src.api.main.db_connector')
    def test_apply_indexes_invalid_ddl(self, mock_db):
        """Test invalid DDL is rejected."""
        mock_db.test_connection.return_value = True

        response = client.post(
            "/apply-indexes",
            json={
                "ddl_statements": ["DROP TABLE users"],
                "dry_run": False
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data['failed'] == 1
        assert "Only CREATE INDEX" in data['results'][0]['error']


class TestAPIAuthentication:
    """Tests for API key authentication."""

    @patch('src.api.main.get_api_keys')
    def test_no_auth_required_when_no_keys(self, mock_get_keys):
        """Test requests work when no API keys are configured."""
        mock_get_keys.return_value = []

        response = client.get("/health")
        assert response.status_code == 200

    @patch('src.api.main.get_api_keys')
    @patch('src.api.main.db_connector')
    def test_auth_required_when_keys_configured(self, mock_db, mock_get_keys):
        """Test authentication is required when keys are configured."""
        mock_get_keys.return_value = ["test-key-123"]
        mock_db.test_connection.return_value = True

        # Without key
        response = client.post(
            "/analyse",
            json={"query": "SELECT 1"}
        )
        assert response.status_code == 401

    @patch('src.api.main.get_api_keys')
    @patch('src.api.main.db_connector')
    @patch('src.api.main.recommender')
    def test_valid_api_key_works(self, mock_recommender, mock_db, mock_get_keys):
        """Test valid API key is accepted."""
        mock_get_keys.return_value = ["test-key-123"]
        mock_db.test_connection.return_value = True
        mock_db.get_explain_plan.return_value = {
            'explain_plan': {'Plan': {'Node Type': 'Result', 'Total Cost': 0}},
            'query': 'SELECT 1'
        }
        mock_db.extract_execution_metrics.return_value = {
            'execution_time': 0, 'planning_time': 0, 'total_cost': 0,
            'actual_rows': 0, 'node_type': 'Result'
        }
        mock_db.detect_sequential_scans.return_value = []
        mock_recommender.analyse_query.return_value = []

        response = client.post(
            "/analyse",
            json={"query": "SELECT 1"},
            headers={"X-API-Key": "test-key-123"}
        )
        assert response.status_code == 200


class TestOpenAPIDocumentation:
    """Tests for API documentation."""

    def test_openapi_schema(self):
        """Test OpenAPI schema is available."""
        response = client.get("/openapi.json")
        assert response.status_code == 200
        data = response.json()
        assert "openapi" in data
        assert "paths" in data

    def test_swagger_ui(self):
        """Test Swagger UI is available."""
        response = client.get("/docs")
        assert response.status_code == 200

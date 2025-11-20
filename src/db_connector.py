"""
PostgreSQL Database Connector with EXPLAIN Plan Extraction
"""
import json
import os
from typing import Dict, List, Optional, Any
from contextlib import contextmanager
import psycopg2
import psycopg2.pool
from psycopg2 import Error as PsycopgError
from dotenv import load_dotenv

load_dotenv()


class DatabaseConnector:
    """
    Handles PostgreSQL connections and EXPLAIN plan extraction.
    Uses connection pooling for concurrent queries.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        pool_min: int = 2,
        pool_max: int = 10
    ):
        """
        Initialize database connector with connection pooling.

        Args:
            host: Database host (defaults to DB_HOST env var)
            port: Database port (defaults to DB_PORT env var)
            database: Database name (defaults to DB_NAME env var)
            user: Database user (defaults to DB_USER env var)
            password: Database password (defaults to DB_PASSWORD env var)
            pool_min: Minimum pool connections
            pool_max: Maximum pool connections
        """
        self.host = host or os.getenv('DB_HOST', 'localhost')
        self.port = port or int(os.getenv('DB_PORT', '5432'))
        self.database = database or os.getenv('DB_NAME')
        self.user = user or os.getenv('DB_USER')
        self.password = password or os.getenv('DB_PASSWORD')
        self.pool_min = pool_min or int(os.getenv('DB_POOL_MIN', '2'))
        self.pool_max = pool_max or int(os.getenv('DB_POOL_MAX', '10'))

        if not all([self.database, self.user, self.password]):
            raise ValueError("Database credentials not provided. Set DB_NAME, DB_USER, and DB_PASSWORD")

        self.connection_pool = None
        self._initialize_pool()

    def _initialize_pool(self):
        """Initialize the connection pool."""
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                self.pool_min,
                self.pool_max,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
        except PsycopgError as e:
            raise ConnectionError(f"Failed to initialize connection pool: {e}")

    @contextmanager
    def get_connection(self):
        """
        Context manager for getting a connection from the pool.

        Yields:
            psycopg2 connection object
        """
        conn = None
        try:
            conn = self.connection_pool.getconn()
            yield conn
        except PsycopgError as e:
            raise ConnectionError(f"Failed to get connection from pool: {e}")
        finally:
            if conn:
                self.connection_pool.putconn(conn)

    def get_explain_plan(self, query: str, analyze: bool = True) -> Dict[str, Any]:
        """
        Execute EXPLAIN (ANALYZE) on a query and return JSON output.

        Args:
            query: SQL query to analyze
            analyze: If True, use EXPLAIN ANALYZE (actually executes query)

        Returns:
            Dict containing EXPLAIN plan in JSON format with metadata

        Raises:
            ValueError: If query is empty or invalid
            ConnectionError: If database connection fails
            RuntimeError: If EXPLAIN execution fails
        """
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")

        # Build EXPLAIN command
        explain_cmd = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)" if analyze else "EXPLAIN (FORMAT JSON)"
        full_query = f"{explain_cmd} {query}"

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(full_query)
                    result = cursor.fetchone()

                    if not result:
                        raise RuntimeError("EXPLAIN returned no results")

                    # PostgreSQL returns EXPLAIN as array with single element
                    explain_json = result[0][0] if isinstance(result[0], list) else result[0]

                    return {
                        'query': query,
                        'explain_plan': explain_json,
                        'analyzed': analyze
                    }

        except PsycopgError as e:
            raise RuntimeError(f"Failed to execute EXPLAIN: {e}")

    def extract_execution_metrics(self, explain_output: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract key metrics from EXPLAIN plan output.

        Args:
            explain_output: Output from get_explain_plan()

        Returns:
            Dict with execution metrics:
                - execution_time: Total execution time in ms
                - planning_time: Query planning time in ms
                - total_cost: Total estimated cost
                - actual_rows: Actual rows returned
                - node_type: Top-level node type
        """
        plan = explain_output['explain_plan']

        metrics = {
            'execution_time': plan.get('Execution Time', 0),
            'planning_time': plan.get('Planning Time', 0),
            'total_cost': plan['Plan'].get('Total Cost', 0),
            'actual_rows': plan['Plan'].get('Actual Rows', 0),
            'node_type': plan['Plan'].get('Node Type', 'Unknown'),
            'startup_cost': plan['Plan'].get('Startup Cost', 0),
        }

        return metrics

    def detect_sequential_scans(self, explain_output: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Recursively traverse EXPLAIN plan tree to find all sequential scans.

        Args:
            explain_output: Output from get_explain_plan()

        Returns:
            List of dicts, each containing:
                - table_name: Name of table being scanned
                - rows_scanned: Number of rows scanned
                - scan_time: Time spent on this scan (ms)
                - total_cost: Cost estimate for this scan
                - filter: Filter condition if any
        """
        sequential_scans = []

        def traverse_plan(node: Dict[str, Any]):
            """Recursively traverse plan tree."""
            node_type = node.get('Node Type', '')

            # Check if this is a sequential scan
            if node_type == 'Seq Scan':
                scan_info = {
                    'table_name': node.get('Relation Name', 'Unknown'),
                    'alias': node.get('Alias'),
                    'rows_scanned': node.get('Actual Rows', 0),
                    'rows_estimated': node.get('Plan Rows', 0),
                    'scan_time': node.get('Actual Total Time', 0),
                    'total_cost': node.get('Total Cost', 0),
                    'startup_cost': node.get('Startup Cost', 0),
                    'filter': node.get('Filter'),
                    'rows_removed_by_filter': node.get('Rows Removed by Filter', 0)
                }
                sequential_scans.append(scan_info)

            # Traverse child plans
            if 'Plans' in node:
                for child_plan in node['Plans']:
                    traverse_plan(child_plan)

        # Start traversal from root plan
        plan = explain_output['explain_plan']['Plan']
        traverse_plan(plan)

        return sequential_scans

    def test_connection(self) -> bool:
        """
        Test database connection.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result[0] == 1
        except Exception:
            return False

    def close(self):
        """Close all connections in the pool."""
        if self.connection_pool:
            self.connection_pool.closeall()
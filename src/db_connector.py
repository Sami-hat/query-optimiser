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
    Handles PostgreSQL connections and EXPLAIN plan extraction
    Uses connection pooling for concurrent queries
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
        Initialize database connector with connection pooling

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
        """Initialize the connection pool"""
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
        Context manager for getting a connection from the pool

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

    def _detect_query_type(self, query: str) -> str:
        """
        Detect the type of SQL query

        Args:
            query: SQL query string

        Returns:
            Query type: 'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DDL', or 'UNKNOWN'
        """
        query_upper = query.strip().upper()

        if query_upper.startswith('SELECT') or query_upper.startswith('WITH'):
            return 'SELECT'
        elif query_upper.startswith('INSERT'):
            return 'INSERT'
        elif query_upper.startswith('UPDATE'):
            return 'UPDATE'
        elif query_upper.startswith('DELETE'):
            return 'DELETE'
        elif any(query_upper.startswith(cmd) for cmd in ['CREATE', 'ALTER', 'DROP', 'TRUNCATE']):
            return 'DDL'
        else:
            return 'UNKNOWN'

    def get_explain_plan(self, query: str, analyze: bool = False, statement_timeout_ms: int = 30000) -> Dict[str, Any]:
        """
        Execute EXPLAIN (ANALYZE) on a query and return JSON output

        SAFETY: Defaults to analyze=False to prevent data modification and hanging
        ANALYZE will only run on SELECT queries and with a statement_timeout

        Args:
            query: SQL query to analyze
            analyze: If True, use EXPLAIN ANALYZE (actually executes query) - only safe for SELECT
            statement_timeout_ms: Timeout in milliseconds for EXPLAIN ANALYZE (default: 30000ms = 30s)

        Returns:
            Dict containing EXPLAIN plan in JSON format with metadata

        Raises:
            ValueError: If query is empty, invalid, or ANALYZE requested on DML query
            ConnectionError: If database connection fails
            RuntimeError: If EXPLAIN execution fails
        """
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")

        # Detect query type for safety
        query_type = self._detect_query_type(query)

        # Refuse ANALYZE on DML queries to prevent data modification
        if analyze and query_type in ['INSERT', 'UPDATE', 'DELETE', 'DDL']:
            raise ValueError(
                f"EXPLAIN ANALYZE refused for {query_type} query. "
                f"Use analyze=False to get plan without execution. "
                f"ANALYZE would modify data or schema."
            )

        # Build EXPLAIN command
        explain_cmd = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)" if analyze else "EXPLAIN (FORMAT JSON)"
        full_query = f"{explain_cmd} {query}"

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Set statement timeout for ANALYZE to prevent hanging
                    if analyze:
                        cursor.execute(f"SET LOCAL statement_timeout = '{statement_timeout_ms}ms'")

                    cursor.execute(full_query)
                    result = cursor.fetchone()

                    if not result:
                        raise RuntimeError("EXPLAIN returned no results")

                    # PostgreSQL returns EXPLAIN as array with single element
                    explain_json = result[0][0] if isinstance(result[0], list) else result[0]

                    # Rollback to ensure ANALYZE doesn't commit any changes
                    # (This is mostly a safety measure; we already refuse ANALYZE on DML)
                    conn.rollback()

                    return {
                        'query': query,
                        'explain_plan': explain_json,
                        'analyzed': analyze,
                        'query_type': query_type
                    }

        except PsycopgError as e:
            raise RuntimeError(f"Failed to execute EXPLAIN: {e}")

    def extract_execution_metrics(self, explain_output: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract key metrics from EXPLAIN plan output

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
        Recursively traverse EXPLAIN plan tree to find all sequential scans

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
            """Recursively traverse plan tree"""
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
        Test database connection

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

    def get_column_statistics(self, table_name: str, column_name: str) -> Dict[str, Any]:
        """
        Query pg_stats for column statistics to calculate real selectivity

        Args:
            table_name: Table name
            column_name: Column name

        Returns:
            Dict with column statistics:
                - n_distinct: Number of distinct values (-1 means unique, negative means proportion)
                - null_frac: Fraction of null values (0-1)
                - avg_width: Average width in bytes
                - n_distinct_values: Absolute count of distinct values
                - most_common_vals: Array of most common values
                - most_common_freqs: Array of frequencies for most common values
                - correlation: Statistical correlation (-1 to 1)
        """
        sql = """
            SELECT
                s.n_distinct,
                s.null_frac,
                s.avg_width,
                s.correlation,
                c.reltuples::bigint as total_rows,
                CASE
                    WHEN s.n_distinct < 0 THEN abs(s.n_distinct * c.reltuples)::bigint
                    ELSE s.n_distinct::bigint
                END as n_distinct_values
            FROM pg_stats s
            JOIN pg_class c ON c.relname = s.tablename
            JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = s.schemaname
            WHERE s.schemaname = 'public'
              AND s.tablename = %s
              AND s.attname = %s
        """

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (table_name, column_name))
                    result = cursor.fetchone()

                    if not result:
                        # Column not found in pg_stats, return defaults
                        return {
                            'n_distinct': -1,
                            'null_frac': 0.0,
                            'avg_width': 32,
                            'correlation': 0.0,
                            'total_rows': 0,
                            'n_distinct_values': 0,
                            'has_stats': False
                        }

                    return {
                        'n_distinct': result[0] or 0,
                        'null_frac': result[1] or 0.0,
                        'avg_width': result[2] or 32,
                        'correlation': result[3] or 0.0,
                        'total_rows': result[4] or 0,
                        'n_distinct_values': result[5] or 0,
                        'has_stats': True
                    }

        except Exception as e:
            # If pg_stats query fails, return defaults
            return {
                'n_distinct': -1,
                'null_frac': 0.0,
                'avg_width': 32,
                'correlation': 0.0,
                'total_rows': 0,
                'n_distinct_values': 0,
                'has_stats': False,
                'error': str(e)
            }

    def get_table_row_count(self, table_name: str) -> int:
        """
        Get estimated row count for a table from pg_class

        Args:
            table_name: Table name

        Returns:
            Estimated row count
        """
        sql = """
            SELECT c.reltuples::bigint
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname = %s
        """

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (table_name,))
                    result = cursor.fetchone()
                    return result[0] if result else 0
        except Exception:
            return 0

    def close(self):
        """Close all connections in the pool"""
        if self.connection_pool:
            self.connection_pool.closeall()
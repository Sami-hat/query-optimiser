"""
Batch Query Analyser

Analyses production queries from pg_stat_statements and generates
aggregated recommendations with parallel processing.
"""
import json
import time
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from .db_connector import DatabaseConnector
from .query_parser import QueryParser
from .recommender import IndexRecommender, IndexRecommendation


@dataclass
class QueryStats:
    """Statistics for a single query from pg_stat_statements."""
    query: str
    query_id: Optional[str] = None
    calls: int = 0
    total_time_ms: float = 0.0
    mean_time_ms: float = 0.0
    min_time_ms: float = 0.0
    max_time_ms: float = 0.0
    rows: int = 0
    shared_blks_hit: int = 0
    shared_blks_read: int = 0

    @property
    def cache_hit_ratio(self) -> float:
        """Calculate buffer cache hit ratio."""
        total = self.shared_blks_hit + self.shared_blks_read
        if total == 0:
            return 1.0
        return self.shared_blks_hit / total


@dataclass
class AnalysisResult:
    """Result of analysing a single query."""
    query: str
    query_stats: Optional[QueryStats] = None
    execution_time_ms: float = 0.0
    planning_time_ms: float = 0.0
    total_cost: float = 0.0
    seq_scans: List[Dict[str, Any]] = field(default_factory=list)
    recommendations: List[IndexRecommendation] = field(default_factory=list)
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialisation."""
        return {
            'query': self.query,
            'query_stats': asdict(self.query_stats) if self.query_stats else None,
            'execution_time_ms': self.execution_time_ms,
            'planning_time_ms': self.planning_time_ms,
            'total_cost': self.total_cost,
            'seq_scans': self.seq_scans,
            'recommendations': [
                {
                    'table': r.table_name,
                    'columns': r.columns,
                    'index_type': r.index_type,
                    'reason': r.reason,
                    'expected_improvement_pct': r.expected_improvement_pct,
                    'current_cost': r.current_cost,
                    'estimated_cost': r.estimated_cost,
                    'priority': r.priority,
                    'ddl': r.get_ddl()
                }
                for r in self.recommendations
            ],
            'error': self.error
        }


@dataclass
class BatchAnalysisReport:
    """Aggregated report from batch analysis."""
    timestamp: str
    total_queries: int = 0
    analysed_queries: int = 0
    failed_queries: int = 0
    total_seq_scans: int = 0
    seq_scans_with_recommendations: int = 0
    unique_recommendations: int = 0
    tables_affected: List[str] = field(default_factory=list)
    total_current_cost: float = 0.0
    total_estimated_cost: float = 0.0
    estimated_improvement_pct: float = 0.0
    recommendations_by_table: Dict[str, List[Dict]] = field(default_factory=dict)
    top_recommendations: List[Dict] = field(default_factory=list)
    analysis_results: List[Dict] = field(default_factory=list)
    failed_query_details: List[Dict] = field(default_factory=list)
    analysis_duration_seconds: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialisation."""
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def get_summary(self) -> str:
        """Get human-readable summary."""
        lines = [
            "=" * 60,
            "BATCH ANALYSIS REPORT",
            "=" * 60,
            f"Timestamp: {self.timestamp}",
            f"Duration: {self.analysis_duration_seconds:.2f} seconds",
            "",
            "QUERY STATISTICS",
            "-" * 40,
            f"Total queries: {self.total_queries}",
            f"Successfully analysed: {self.analysed_queries}",
            f"Failed: {self.failed_queries}",
            "",
            "SEQUENTIAL SCAN ANALYSIS",
            "-" * 40,
            f"Total sequential scans found: {self.total_seq_scans}",
            f"Scans with index recommendations: {self.seq_scans_with_recommendations}",
        ]

        if self.total_seq_scans > 0:
            elimination_pct = (self.seq_scans_with_recommendations / self.total_seq_scans) * 100
            lines.append(f"Potential scan elimination: {elimination_pct:.1f}%")

        lines.extend([
            "",
            "INDEX RECOMMENDATIONS",
            "-" * 40,
            f"Unique index recommendations: {self.unique_recommendations}",
            f"Tables affected: {len(self.tables_affected)}",
        ])

        if self.tables_affected:
            lines.append(f"  Tables: {', '.join(self.tables_affected)}")

        lines.extend([
            "",
            "ESTIMATED PERFORMANCE IMPACT",
            "-" * 40,
            f"Total current cost: {self.total_current_cost:.2f}",
            f"Estimated cost after indexing: {self.total_estimated_cost:.2f}",
            f"Estimated improvement: {self.estimated_improvement_pct:.1f}%",
        ])

        if self.top_recommendations:
            lines.extend([
                "",
                "TOP RECOMMENDATIONS",
                "-" * 40,
            ])
            for i, rec in enumerate(self.top_recommendations[:10], 1):
                lines.append(f"{i}. {rec['ddl']}")
                lines.append(f"   Reason: {rec['reason']}")
                lines.append(f"   Expected improvement: {rec['expected_improvement_pct']:.1f}%")
                lines.append("")

        lines.append("=" * 60)
        return "\n".join(lines)


class BatchAnalyser:
    """
    Analyses multiple queries from pg_stat_statements or provided list,
    with parallel processing and aggregated reporting.
    """

    def __init__(
        self,
        db_connector: DatabaseConnector,
        max_workers: int = 10,
        min_calls: int = 10,
        min_mean_time_ms: float = 100.0
    ):
        """
        Initialise batch analyser.

        Args:
            db_connector: Database connection handler
            max_workers: Maximum parallel EXPLAIN queries
            min_calls: Minimum call count to include query from pg_stat_statements
            min_mean_time_ms: Minimum mean execution time to include query
        """
        self.db_connector = db_connector
        self.recommender = IndexRecommender(db_connector)
        self.max_workers = max_workers
        self.min_calls = min_calls
        self.min_mean_time_ms = min_mean_time_ms
        self._lock = threading.Lock()

    def get_queries_from_pg_stat_statements(
        self,
        limit: int = 500,
        exclude_patterns: Optional[List[str]] = None
    ) -> List[QueryStats]:
        """
        Extract queries from pg_stat_statements extension.

        Args:
            limit: Maximum number of queries to retrieve
            exclude_patterns: SQL patterns to exclude (e.g., system queries)

        Returns:
            List of QueryStats objects
        """
        exclude_patterns = exclude_patterns or [
            'pg_%',
            'information_schema%',
            'COMMIT',
            'BEGIN',
            'ROLLBACK',
            'SET %',
            'SHOW %',
            'EXPLAIN%'
        ]

        # Build exclusion clause
        exclusions = " AND ".join([
            f"query NOT ILIKE '{pattern}'" for pattern in exclude_patterns
        ])

        sql = f"""
            SELECT
                query,
                queryid::text as query_id,
                calls,
                total_exec_time as total_time_ms,
                mean_exec_time as mean_time_ms,
                min_exec_time as min_time_ms,
                max_exec_time as max_time_ms,
                rows,
                shared_blks_hit,
                shared_blks_read
            FROM pg_stat_statements
            WHERE calls >= %s
              AND mean_exec_time >= %s
              AND {exclusions}
            ORDER BY total_exec_time DESC
            LIMIT %s
        """

        try:
            with self.db_connector.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (self.min_calls, self.min_mean_time_ms, limit))
                    rows = cur.fetchall()

                    queries = []
                    for row in rows:
                        queries.append(QueryStats(
                            query=row[0],
                            query_id=row[1],
                            calls=row[2],
                            total_time_ms=row[3],
                            mean_time_ms=row[4],
                            min_time_ms=row[5],
                            max_time_ms=row[6],
                            rows=row[7],
                            shared_blks_hit=row[8],
                            shared_blks_read=row[9]
                        ))

                    return queries
        except Exception as e:
            # pg_stat_statements might not be installed
            raise RuntimeError(
                f"Failed to query pg_stat_statements. "
                f"Ensure the extension is installed: CREATE EXTENSION pg_stat_statements; "
                f"Error: {e}"
            )

    def analyse_single_query(
        self,
        query: str,
        query_stats: Optional[QueryStats] = None
    ) -> AnalysisResult:
        """
        Analyse a single query.

        Args:
            query: SQL query string
            query_stats: Optional statistics from pg_stat_statements

        Returns:
            AnalysisResult object
        """
        result = AnalysisResult(query=query, query_stats=query_stats)

        try:
            # Skip if query contains placeholders that can't be executed
            if '$1' in query or '$2' in query:
                # Replace placeholders with dummy values for EXPLAIN
                query = self._replace_placeholders(query)

            # Get EXPLAIN plan
            explain_plan = self.db_connector.get_explain_plan(query)

            if not explain_plan:
                result.error = "Empty EXPLAIN plan returned"
                return result

            # Extract metrics
            metrics = self.db_connector.extract_execution_metrics(explain_plan)
            result.execution_time_ms = metrics.get('execution_time', 0)
            result.planning_time_ms = metrics.get('planning_time', 0)
            result.total_cost = metrics.get('total_cost', 0)

            # Detect sequential scans
            seq_scans = self.db_connector.detect_sequential_scans(explain_plan)
            result.seq_scans = seq_scans

            # Get recommendations
            recommendations = self.recommender.analyse_query(query, explain_plan)
            result.recommendations = recommendations

        except Exception as e:
            result.error = str(e)

        return result

    def _replace_placeholders(self, query: str) -> str:
        """Replace $1, $2 style placeholders with dummy values."""
        import re

        # Simple replacement - might not work for all cases
        # but good enough for EXPLAIN purposes
        placeholders = re.findall(r'\$\d+', query)
        for ph in sorted(set(placeholders), key=lambda x: -int(x[1:])):
            # Replace with a string that won't break syntax
            query = query.replace(ph, "'placeholder'")

        return query

    def analyse_queries(
        self,
        queries: List[str],
        query_stats_map: Optional[Dict[str, QueryStats]] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> BatchAnalysisReport:
        """
        Analyse multiple queries with parallel processing.

        Args:
            queries: List of SQL queries to analyse
            query_stats_map: Optional mapping of query to stats
            progress_callback: Optional callback(current, total) for progress

        Returns:
            BatchAnalysisReport with aggregated results
        """
        start_time = time.time()
        query_stats_map = query_stats_map or {}

        results: List[AnalysisResult] = []
        completed = 0

        def process_query(query: str) -> AnalysisResult:
            nonlocal completed
            stats = query_stats_map.get(query)
            result = self.analyse_single_query(query, stats)

            with self._lock:
                completed += 1
                if progress_callback:
                    progress_callback(completed, len(queries))

            return result

        # Process queries in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(process_query, q): q for q in queries}

            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    query = futures[future]
                    results.append(AnalysisResult(
                        query=query,
                        error=str(e)
                    ))

        # Aggregate results
        report = self._aggregate_results(results)
        report.analysis_duration_seconds = time.time() - start_time

        return report

    def analyse_from_pg_stat_statements(
        self,
        limit: int = 500,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> BatchAnalysisReport:
        """
        Analyse queries directly from pg_stat_statements.

        Args:
            limit: Maximum queries to analyse
            progress_callback: Optional progress callback

        Returns:
            BatchAnalysisReport
        """
        # Get queries from pg_stat_statements
        query_stats = self.get_queries_from_pg_stat_statements(limit)

        if not query_stats:
            return BatchAnalysisReport(
                timestamp=datetime.now().isoformat(),
                total_queries=0
            )

        queries = [qs.query for qs in query_stats]
        stats_map = {qs.query: qs for qs in query_stats}

        return self.analyse_queries(queries, stats_map, progress_callback)

    def _aggregate_results(self, results: List[AnalysisResult]) -> BatchAnalysisReport:
        """Aggregate individual results into a report."""
        report = BatchAnalysisReport(
            timestamp=datetime.now().isoformat(),
            total_queries=len(results)
        )

        # Collect all recommendations
        all_recommendations: Dict[str, IndexRecommendation] = {}
        recommendations_by_table: Dict[str, List[IndexRecommendation]] = {}

        for result in results:
            if result.error:
                report.failed_queries += 1
                report.failed_query_details.append({
                    'query': result.query[:200] + '...' if len(result.query) > 200 else result.query,
                    'error': result.error
                })
                continue

            report.analysed_queries += 1
            report.total_seq_scans += len(result.seq_scans)
            report.analysis_results.append(result.to_dict())

            for rec in result.recommendations:
                report.seq_scans_with_recommendations += 1
                report.total_current_cost += rec.current_cost
                report.total_estimated_cost += rec.estimated_cost

                # Deduplicate by index signature
                key = f"{rec.table_name}_{','.join(sorted(rec.columns))}"
                if key not in all_recommendations or rec.priority > all_recommendations[key].priority:
                    all_recommendations[key] = rec

                # Group by table
                if rec.table_name not in recommendations_by_table:
                    recommendations_by_table[rec.table_name] = []
                recommendations_by_table[rec.table_name].append(rec)

        # Build unique recommendations list
        unique_recs = list(all_recommendations.values())
        unique_recs.sort(key=lambda r: r.priority, reverse=True)

        report.unique_recommendations = len(unique_recs)
        report.tables_affected = list(set(r.table_name for r in unique_recs))

        # Calculate improvement percentage
        if report.total_current_cost > 0:
            savings = report.total_current_cost - report.total_estimated_cost
            report.estimated_improvement_pct = (savings / report.total_current_cost) * 100

        # Top recommendations
        report.top_recommendations = [
            {
                'table': r.table_name,
                'columns': r.columns,
                'index_type': r.index_type,
                'reason': r.reason,
                'expected_improvement_pct': r.expected_improvement_pct,
                'current_cost': r.current_cost,
                'estimated_cost': r.estimated_cost,
                'priority': r.priority,
                'ddl': r.get_ddl()
            }
            for r in unique_recs[:20]
        ]

        # Recommendations by table
        for table, recs in recommendations_by_table.items():
            # Deduplicate within table
            seen = set()
            unique_table_recs = []
            for rec in sorted(recs, key=lambda r: r.priority, reverse=True):
                key = tuple(sorted(rec.columns))
                if key not in seen:
                    seen.add(key)
                    unique_table_recs.append({
                        'columns': rec.columns,
                        'index_type': rec.index_type,
                        'reason': rec.reason,
                        'expected_improvement_pct': rec.expected_improvement_pct,
                        'ddl': rec.get_ddl()
                    })
            report.recommendations_by_table[table] = unique_table_recs

        return report

    def get_existing_indexes(self, table_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get existing indexes from the database.

        Args:
            table_name: Optional table to filter by

        Returns:
            List of index info dictionaries
        """
        sql = """
            SELECT
                schemaname,
                tablename,
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
        """

        if table_name:
            sql += " AND tablename = %s"
            params = (table_name,)
        else:
            params = None

        sql += " ORDER BY tablename, indexname"

        with self.db_connector.get_connection() as conn:
            with conn.cursor() as cur:
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)

                rows = cur.fetchall()
                return [
                    {
                        'schema': row[0],
                        'table': row[1],
                        'index_name': row[2],
                        'definition': row[3]
                    }
                    for row in rows
                ]

    def get_table_statistics(self) -> List[Dict[str, Any]]:
        """
        Get table statistics including row counts and sizes.

        Returns:
            List of table statistics
        """
        sql = """
            SELECT
                relname as table_name,
                n_live_tup as row_count,
                n_dead_tup as dead_rows,
                n_tup_ins as inserts,
                n_tup_upd as updates,
                n_tup_del as deletes,
                seq_scan as seq_scans,
                seq_tup_read as seq_rows_read,
                idx_scan as index_scans,
                idx_tup_fetch as index_rows_fetched,
                pg_size_pretty(pg_total_relation_size(relid)) as total_size
            FROM pg_stat_user_tables
            ORDER BY n_live_tup DESC
        """

        with self.db_connector.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()

                stats = []
                for row in rows:
                    total_ops = (row[3] or 0) + (row[4] or 0) + (row[5] or 0) + (row[6] or 0) + (row[8] or 0)
                    write_ops = (row[3] or 0) + (row[4] or 0) + (row[5] or 0)
                    write_ratio = write_ops / total_ops if total_ops > 0 else 0

                    stats.append({
                        'table_name': row[0],
                        'row_count': row[1],
                        'dead_rows': row[2],
                        'inserts': row[3],
                        'updates': row[4],
                        'deletes': row[5],
                        'seq_scans': row[6],
                        'seq_rows_read': row[7],
                        'index_scans': row[8],
                        'index_rows_fetched': row[9],
                        'total_size': row[10],
                        'write_ratio': write_ratio
                    })

                return stats

    def filter_recommendations_by_existing_indexes(
        self,
        recommendations: List[IndexRecommendation]
    ) -> List[IndexRecommendation]:
        """
        Filter out recommendations for columns that are already indexed.

        Args:
            recommendations: List of recommendations

        Returns:
            Filtered list
        """
        existing = self.get_existing_indexes()

        # Extract indexed columns from definitions
        indexed_columns = set()
        for idx in existing:
            # Parse index definition to extract columns
            # This is simplified - a full parser would be better
            defn = idx['definition'].lower()
            table = idx['table']

            # Extract column names between parentheses
            import re
            match = re.search(r'\(([^)]+)\)', defn)
            if match:
                cols = [c.strip() for c in match.group(1).split(',')]
                for col in cols:
                    # Remove any type casting or expressions
                    col = col.split('::')[0].strip()
                    indexed_columns.add((table, col))

        # Filter recommendations
        filtered = []
        for rec in recommendations:
            # Check if all columns are already indexed
            all_indexed = all(
                (rec.table_name, col.lower()) in indexed_columns
                for col in rec.columns
            )

            if not all_indexed:
                filtered.append(rec)

        return filtered
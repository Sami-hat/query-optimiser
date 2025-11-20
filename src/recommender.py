"""
Index Recommendation Engine

Combines EXPLAIN plan analysis and query AST parsing to recommend indexes.
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from .db_connector import DatabaseConnector
from .query_parser import QueryParser


@dataclass
class IndexRecommendation:
    """Represents a single index recommendation."""
    table_name: str
    columns: List[str]
    index_type: str = 'btree'
    reason: str = ''
    expected_improvement_pct: float = 0.0
    current_cost: float = 0.0
    estimated_cost: float = 0.0
    query_example: str = ''
    priority: int = 0  # Higher is more important

    def get_index_name(self) -> str:
        """Generate consistent index name."""
        cols_str = '_'.join(self.columns)
        return f"idx_{self.table_name}_{cols_str}"

    def get_ddl(self) -> str:
        """Generate CREATE INDEX DDL statement."""
        index_name = self.get_index_name()
        columns_str = ', '.join(self.columns)

        if self.index_type == 'gin':
            return f"CREATE INDEX {index_name} ON {self.table_name} USING GIN ({columns_str});"
        elif self.index_type == 'gist':
            return f"CREATE INDEX {index_name} ON {self.table_name} USING GIST ({columns_str});"
        else:  # btree (default)
            return f"CREATE INDEX {index_name} ON {self.table_name} ({columns_str});"


class IndexRecommender:
    """
    Analyses queries and recommends indexes based on:
    - Sequential scans detected in EXPLAIN plans
    - Columns used in WHERE, ORDER BY, and JOIN clauses
    """

    def __init__(self, db_connector: Optional[DatabaseConnector] = None):
        """
        Initialise recommender.

        Args:
            db_connector: DatabaseConnector instance (optional, for live analysis)
        """
        self.db_connector = db_connector
        self.write_overhead_per_index = 0.15  # 15% write overhead per index

    def analyse_query(
        self,
        query: str,
        explain_output: Optional[Dict[str, Any]] = None
    ) -> List[IndexRecommendation]:
        """
        Analyse a single query and generate index recommendations.

        Args:
            query: SQL query string
            explain_output: Pre-computed EXPLAIN output (if None, will execute EXPLAIN)

        Returns:
            List of IndexRecommendation objects
        """
        # Get EXPLAIN plan if not provided
        if explain_output is None:
            if self.db_connector is None:
                raise ValueError("Either provide explain_output or initialize with db_connector")
            explain_output = self.db_connector.get_explain_plan(query)

        # Parse query to extract columns
        try:
            parser = QueryParser(query)
            query_info = parser.get_all_info()
        except Exception as e:
            # If parsing fails, return empty recommendations
            return []

        # Detect sequential scans
        seq_scans = self.db_connector.detect_sequential_scans(explain_output) if self.db_connector else []

        # Generate recommendations
        recommendations = []

        for scan in seq_scans:
            table_name = scan['table_name']

            # Find columns used in WHERE clause for this table
            where_columns = list(query_info['where_columns'])

            # Recommend index on WHERE columns
            if where_columns:
                rec = self._create_recommendation(
                    table_name=table_name,
                    columns=where_columns,
                    scan_info=scan,
                    reason=f"Sequential scan on {table_name} with WHERE filter",
                    query=query
                )
                recommendations.append(rec)

            # Check for ORDER BY columns on same table
            order_columns = list(query_info['order_by_columns'])
            if order_columns and not where_columns:
                rec = self._create_recommendation(
                    table_name=table_name,
                    columns=order_columns,
                    scan_info=scan,
                    reason=f"Sequential scan on {table_name} with ORDER BY",
                    query=query
                )
                recommendations.append(rec)

        # Handle JOINs - recommend indexes on join columns
        join_columns = list(query_info['join_columns'])
        if join_columns:
            for table in query_info['tables']:
                # This is simplified - in production, we'd match columns to tables
                rec = IndexRecommendation(
                    table_name=table,
                    columns=[col for col in join_columns if col != 'id'],  # Filter out id (likely already indexed)
                    reason=f"JOIN condition on {table}",
                    query_example=query,
                    priority=2
                )
                if rec.columns:  # Only add if we have columns
                    recommendations.append(rec)

        # Prioritize and deduplicate
        recommendations = self._prioritize_recommendations(recommendations)

        return recommendations

    def _create_recommendation(
        self,
        table_name: str,
        columns: List[str],
        scan_info: Dict[str, Any],
        reason: str,
        query: str
    ) -> IndexRecommendation:
        """Create a recommendation with cost estimates."""
        current_cost = scan_info.get('total_cost', 0)
        scan_time = scan_info.get('scan_time', 0)
        rows_scanned = scan_info.get('rows_scanned', 0)

        # Estimate improvement based on selectivity
        rows_removed = scan_info.get('rows_removed_by_filter', 0)
        if rows_scanned > 0:
            selectivity = 1 - (rows_removed / max(rows_scanned, 1))
        else:
            selectivity = 0.1  # Default assumption

        # Index scan is typically much faster for selective queries
        if selectivity < 0.01:
            estimated_improvement = 0.95  # 95% improvement for very selective
        elif selectivity < 0.1:
            estimated_improvement = 0.80  # 80% for selective
        else:
            estimated_improvement = 0.50  # 50% for less selective

        estimated_cost = current_cost * (1 - estimated_improvement)

        # Priority based on current cost and improvement
        priority = int(current_cost * estimated_improvement)

        return IndexRecommendation(
            table_name=table_name,
            columns=columns,
            reason=reason,
            expected_improvement_pct=estimated_improvement * 100,
            current_cost=current_cost,
            estimated_cost=estimated_cost,
            query_example=query,
            priority=priority
        )

    def _prioritize_recommendations(
        self,
        recommendations: List[IndexRecommendation]
    ) -> List[IndexRecommendation]:
        """
        Prioritize and deduplicate recommendations.

        Args:
            recommendations: List of recommendations

        Returns:
            Sorted, deduplicated list
        """
        # Deduplicate by (table, columns)
        unique_recs = {}
        for rec in recommendations:
            key = (rec.table_name, tuple(sorted(rec.columns)))
            if key not in unique_recs or rec.priority > unique_recs[key].priority:
                unique_recs[key] = rec

        # Sort by priority (highest first)
        sorted_recs = sorted(unique_recs.values(), key=lambda x: x.priority, reverse=True)

        return sorted_recs

    def check_over_indexing(
        self,
        table_name: str,
        existing_index_count: int,
        table_write_ratio: float = 0.3
    ) -> Dict[str, Any]:
        """
        Check if adding an index would cause over-indexing.

        Args:
            table_name: Name of table
            existing_index_count: Number of existing indexes
            table_write_ratio: Ratio of writes to total operations (0-1)

        Returns:
            Dict with warning info
        """
        # Calculate total write overhead
        total_overhead = (existing_index_count + 1) * self.write_overhead_per_index

        warning = {
            'table': table_name,
            'existing_indexes': existing_index_count,
            'new_total_indexes': existing_index_count + 1,
            'write_overhead_pct': total_overhead * 100,
            'should_warn': False,
            'message': ''
        }

        # Warn if more than 5 indexes
        if existing_index_count >= 5:
            warning['should_warn'] = True
            warning['message'] = f"Table {table_name} already has {existing_index_count} indexes. " \
                                f"Adding more may degrade write performance."

        # Warn if write overhead is significant for write-heavy tables
        if table_write_ratio > 0.5 and total_overhead > 0.3:
            warning['should_warn'] = True
            warning['message'] += f" Table has high write ratio ({table_write_ratio:.0%}). " \
                                 f"Total write overhead: {total_overhead:.0%}"

        return warning

    def batch_analyse(
        self,
        queries: List[str],
        progress_callback: Optional[callable] = None
    ) -> Dict[str, Any]:
        """
        Analyse multiple queries and aggregate recommendations.

        Args:
            queries: List of SQL queries
            progress_callback: Optional callback function(current, total)

        Returns:
            Dict with aggregated results
        """
        all_recommendations = []
        failed_queries = []

        for i, query in enumerate(queries):
            if progress_callback:
                progress_callback(i + 1, len(queries))

            try:
                recs = self.analyse_query(query)
                all_recommendations.extend(recs)
            except Exception as e:
                failed_queries.append({'query': query, 'error': str(e)})

        # Aggregate by table
        table_recommendations = {}
        for rec in all_recommendations:
            if rec.table_name not in table_recommendations:
                table_recommendations[rec.table_name] = []
            table_recommendations[rec.table_name].append(rec)

        # Deduplicate and prioritize per table
        for table in table_recommendations:
            table_recommendations[table] = self._prioritize_recommendations(
                table_recommendations[table]
            )

        # Calculate statistics
        total_current_cost = sum(r.current_cost for r in all_recommendations)
        total_estimated_cost = sum(r.estimated_cost for r in all_recommendations)
        total_savings = total_current_cost - total_estimated_cost
        avg_improvement = (total_savings / total_current_cost * 100) if total_current_cost > 0 else 0

        return {
            'total_queries_analyzed': len(queries),
            'failed_queries': len(failed_queries),
            'total_recommendations': len(all_recommendations),
            'unique_recommendations': sum(len(recs) for recs in table_recommendations.values()),
            'tables_affected': list(table_recommendations.keys()),
            'recommendations_by_table': table_recommendations,
            'total_current_cost': total_current_cost,
            'total_estimated_cost': total_estimated_cost,
            'estimated_improvement_pct': avg_improvement,
            'failed_queries_details': failed_queries
        }

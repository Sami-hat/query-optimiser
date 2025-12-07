"""
Index Recommendation Engine

Combines EXPLAIN plan analysis and query AST parsing to recommend indexes
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from .db_connector import DatabaseConnector
from .query_parser import QueryParser


@dataclass
class IndexRecommendation:
    """Represents a single index recommendation"""
    table_name: str
    columns: List[str]
    index_type: str = 'btree'
    reason: str = ''
    expected_improvement_pct: float = 0.0
    current_cost: float = 0.0
    estimated_cost: float = 0.0
    query_example: str = ''
    priority: int = 0  # Higher is more important
    warning: str = ''  # Over-indexing or other warnings
    partial_index_predicate: str = ''  # WHERE clause for partial index
    include_columns: List[str] = None  # INCLUDE columns for covering indexes

    def __post_init__(self):
        """Initialize mutable default values"""
        if self.include_columns is None:
            self.include_columns = []

    def get_index_name(self) -> str:
        """Generate consistent index name"""
        cols_str = '_'.join(self.columns)
        suffix = '_partial' if self.partial_index_predicate else ''
        suffix += '_covering' if self.include_columns else ''
        return f"idx_{self.table_name}_{cols_str}{suffix}"

    def get_ddl(self) -> str:
        """Generate CREATE INDEX DDL statement"""
        index_name = self.get_index_name()
        columns_str = ', '.join(self.columns)

        # Build base index statement
        if self.index_type == 'gin':
            ddl = f"CREATE INDEX {index_name} ON {self.table_name} USING GIN ({columns_str})"
        elif self.index_type == 'gist':
            ddl = f"CREATE INDEX {index_name} ON {self.table_name} USING GIST ({columns_str})"
        else:  # btree (default)
            ddl = f"CREATE INDEX {index_name} ON {self.table_name} ({columns_str})"

            # Add INCLUDE clause for covering indexes (PostgreSQL 11+)
            if self.include_columns:
                include_str = ', '.join(self.include_columns)
                ddl += f" INCLUDE ({include_str})"

        # Add WHERE clause for partial indexes
        if self.partial_index_predicate:
            ddl += f" WHERE {self.partial_index_predicate}"

        return ddl + ";"


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

    def _order_columns_for_index(
        self,
        columns: List[str],
        predicate_types: Dict[str, str],
        order_by_columns: Optional[List[str]] = None
    ) -> List[str]:
        """
        Order columns optimally for composite index.

        Rule: Equality predicates > Range predicates > ORDER BY columns

        Args:
            columns: List of column names
            predicate_types: Dict mapping column to predicate type ('equality', 'range', 'other')
            order_by_columns: Optional list of ORDER BY columns

        Returns:
            Optimally ordered list of columns
        """
        if len(columns) <= 1:
            return columns

        # Categorize columns
        equality_cols = []
        range_cols = []
        other_cols = []

        for col in columns:
            pred_type = predicate_types.get(col, 'other')
            if pred_type == 'equality':
                equality_cols.append(col)
            elif pred_type == 'range':
                range_cols.append(col)
            else:
                other_cols.append(col)

        # Optimal order: equality first, range next, others last
        ordered = equality_cols + range_cols + other_cols

        # Add ORDER BY columns at the end if not already included
        if order_by_columns:
            for col in order_by_columns:
                if col not in ordered:
                    ordered.append(col)

        return ordered

    def analyse_query(
        self,
        query: str,
        explain_output: Optional[Dict[str, Any]] = None
    ) -> List[IndexRecommendation]:
        """
        Analyse a single query and generate index recommendations

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

        # Get column-to-table mappings and predicate types
        where_column_tables = query_info.get('where_column_tables', {})
        order_by_column_tables = query_info.get('order_by_column_tables', {})
        predicate_types = query_info.get('column_predicate_types', {})
        constant_filters = query_info.get('constant_filters', {})

        for scan in seq_scans:
            table_name = scan['table_name']

            # Find WHERE columns that belong to this table
            where_columns = [
                col for col, tbl in where_column_tables.items() if tbl == table_name
            ]

            # If no mapped columns, fall back to all WHERE columns (for unqualified references)
            if not where_columns and query_info['where_columns']:
                # Only use unqualified columns if we have a single table or this scan is on the primary table
                if len(query_info['tables']) == 1:
                    where_columns = list(query_info['where_columns'])

            # Detect constant filters for partial index support
            # Separate constant filter columns from index columns
            constant_filter_cols = []
            index_columns = []

            for col in where_columns:
                if col in constant_filters:
                    constant_filter_cols.append((col, constant_filters[col]))
                else:
                    index_columns.append(col)

            # Order index columns optimally (equality > range > other)
            if index_columns:
                index_columns = self._order_columns_for_index(index_columns, predicate_types)

            # Build partial index predicate if constant filters exist
            partial_predicate = ''
            if constant_filter_cols:
                predicates = [f"{col} = {val}" for col, val in constant_filter_cols]
                partial_predicate = ' AND '.join(predicates)

            # Recommend index on WHERE columns
            # If we have both constant filters and index columns, suggest partial index
            # If we only have constant filters, skip (index wouldn't be useful)
            if index_columns:
                rec = self._create_recommendation(
                    table_name=table_name,
                    columns=index_columns,
                    scan_info=scan,
                    reason=f"Sequential scan on {table_name} with WHERE filter" +
                           (f" (partial index on constant filter)" if partial_predicate else ""),
                    query=query,
                    partial_predicate=partial_predicate
                )
                recommendations.append(rec)
            elif where_columns and not constant_filter_cols:
                # All columns are non-constant, create regular index
                ordered_cols = self._order_columns_for_index(where_columns, predicate_types)
                rec = self._create_recommendation(
                    table_name=table_name,
                    columns=ordered_cols,
                    scan_info=scan,
                    reason=f"Sequential scan on {table_name} with WHERE filter",
                    query=query
                )
                recommendations.append(rec)

            # Check for ORDER BY columns on same table
            order_columns = [
                col for col, tbl in order_by_column_tables.items() if tbl == table_name
            ]

            # If no mapped columns, fall back to all ORDER BY columns (for single-table queries)
            if not order_columns and query_info['order_by_columns'] and not where_columns:
                if len(query_info['tables']) == 1:
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

        # Handle JOINs - recommend indexes on join columns with proper table mapping
        join_column_tables = query_info.get('join_column_tables', {})
        if join_column_tables:
            # Group columns by table
            table_join_columns = {}
            for col, table in join_column_tables.items():
                if table not in table_join_columns:
                    table_join_columns[table] = []
                # Skip 'id' as it's likely already indexed
                if col != 'id':
                    table_join_columns[table].append(col)

            # Create recommendations per table
            for table, cols in table_join_columns.items():
                if cols:
                    rec = IndexRecommendation(
                        table_name=table,
                        columns=cols,
                        reason=f"JOIN condition on {table}",
                        query_example=query,
                        priority=2
                    )
                    recommendations.append(rec)

        # Prioritize and deduplicate
        recommendations = self._prioritize_recommendations(recommendations)

        # Check for over-indexing and add warnings
        if self.db_connector:
            recommendations = self._add_over_indexing_warnings(recommendations)

        return recommendations

    def _calculate_selectivity_from_stats(
        self,
        table_name: str,
        columns: List[str],
        rows_scanned: int,
        rows_removed: int
    ) -> float:
        """
        Calculate selectivity using pg_stats data

        Args:
            table_name: Table name
            columns: Columns in the filter
            rows_scanned: Rows scanned from EXPLAIN
            rows_removed: Rows removed by filter from EXPLAIN

        Returns:
            Selectivity estimate (0-1)
        """
        if not self.db_connector:
            # Fallback to EXPLAIN-based selectivity
            if rows_scanned > 0:
                return 1 - (rows_removed / max(rows_scanned, 1))
            return 0.1

        # Get statistics for the first column (most selective usually)
        # For multi-column indexes, ideally combine statistics
        if not columns:
            return 0.1

        primary_column = columns[0]
        stats = self.db_connector.get_column_statistics(table_name, primary_column)

        if not stats.get('has_stats'):
            # No stats available, use EXPLAIN data
            if rows_scanned > 0:
                return 1 - (rows_removed / max(rows_scanned, 1))
            return 0.1

        # Calculate selectivity based on distinct values
        total_rows = stats['total_rows']
        n_distinct = stats['n_distinct_values']
        null_frac = stats['null_frac']

        if total_rows == 0 or n_distinct == 0:
            return 0.1

        # For equality predicates, selectivity ≈ 1 / n_distinct
        # Adjusted for nulls
        base_selectivity = (1.0 / n_distinct) * (1 - null_frac) if n_distinct > 0 else 0.1

        # If we have actual EXPLAIN data, combine it with pg_stats
        if rows_scanned > 0:
            explain_selectivity = 1 - (rows_removed / max(rows_scanned, 1))
            # Weighted average: trust EXPLAIN more (60%) than pg_stats (40%)
            selectivity = 0.6 * explain_selectivity + 0.4 * base_selectivity
        else:
            selectivity = base_selectivity

        return max(0.001, min(1.0, selectivity))  # Clamp to [0.001, 1.0]

    def _estimate_improvement_from_selectivity(self, selectivity: float, correlation: float = 0.0) -> float:
        """
        Estimate query improvement percentage based on selectivity and correlation

        Uses PostgreSQL cost model principles:
        - Highly selective queries (< 1%) benefit most from indexes
        - Correlation affects sequential I/O patterns
        - Low selectivity queries may not benefit from indexes

        Args:
            selectivity: Query selectivity (0-1)
            correlation: Column correlation (-1 to 1), affects I/O patterns

        Returns:
            Estimated improvement percentage (0-1)
        """
        # Base improvement from selectivity
        if selectivity < 0.001:
            # Extremely selective (< 0.1%)
            base_improvement = 0.98
        elif selectivity < 0.01:
            # Very selective (< 1%)
            base_improvement = 0.95
        elif selectivity < 0.05:
            # Selective (< 5%)
            base_improvement = 0.85
        elif selectivity < 0.1:
            # Moderately selective (< 10%)
            base_improvement = 0.70
        elif selectivity < 0.2:
            # Less selective (< 20%)
            base_improvement = 0.50
        else:
            # Not very selective (> 20%)
            # Index might not help much, could even be slower
            base_improvement = 0.20

        # Adjust for correlation
        # High correlation (close to 1 or -1) means data is physically ordered
        # which makes sequential scans more efficient
        correlation_penalty = abs(correlation) * 0.15  # Up to 15% penalty
        adjusted_improvement = base_improvement * (1 - correlation_penalty)

        return max(0.05, min(0.98, adjusted_improvement))  # Clamp to [5%, 98%]

    def _create_recommendation(
        self,
        table_name: str,
        columns: List[str],
        scan_info: Dict[str, Any],
        reason: str,
        query: str,
        partial_predicate: str = '',
        include_columns: List[str] = None
    ) -> IndexRecommendation:
        """Create a recommendation with cost estimates based on real statistics"""
        current_cost = scan_info.get('total_cost', 0)
        scan_time = scan_info.get('scan_time', 0)
        rows_scanned = scan_info.get('rows_scanned', 0)
        rows_removed = scan_info.get('rows_removed_by_filter', 0)

        # Calculate selectivity using pg_stats
        selectivity = self._calculate_selectivity_from_stats(
            table_name, columns, rows_scanned, rows_removed
        )

        # Partial indexes are more selective (smaller index)
        # Boost improvement estimate by 10-20% for partial indexes
        if partial_predicate:
            selectivity *= 0.8  # Partial index filters out rows, making it more selective

        # Get correlation for the primary column
        correlation = 0.0
        if self.db_connector and columns:
            stats = self.db_connector.get_column_statistics(table_name, columns[0])
            correlation = stats.get('correlation', 0.0)

        # Estimate improvement using selectivity and correlation
        estimated_improvement = self._estimate_improvement_from_selectivity(selectivity, correlation)

        # Covering indexes can eliminate heap lookups - boost improvement
        if include_columns:
            estimated_improvement = min(0.98, estimated_improvement * 1.15)  # 15% boost for covering

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
            priority=priority,
            partial_index_predicate=partial_predicate,
            include_columns=include_columns or []
        )

    def _prioritize_recommendations(
        self,
        recommendations: List[IndexRecommendation]
    ) -> List[IndexRecommendation]:
        """
        Prioritize and deduplicate recommendations

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

    def _get_existing_index_count(self, table_name: str) -> int:
        """
        Get the number of existing indexes on a table

        Args:
            table_name: Table name

        Returns:
            Number of existing indexes
        """
        if not self.db_connector:
            return 0

        sql = """
            SELECT COUNT(*)
            FROM pg_indexes
            WHERE schemaname = 'public' AND tablename = %s
        """

        try:
            with self.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (table_name,))
                    result = cursor.fetchone()
                    return result[0] if result else 0
        except Exception:
            return 0

    def _get_table_write_ratio(self, table_name: str) -> float:
        """
        Get the write ratio for a table (writes / total operations).

        Args:
            table_name: Table name

        Returns:
            Write ratio (0-1)
        """
        if not self.db_connector:
            return 0.3  # Default assumption

        sql = """
            SELECT
                COALESCE(n_tup_ins, 0) + COALESCE(n_tup_upd, 0) + COALESCE(n_tup_del, 0) as writes,
                COALESCE(seq_scan, 0) + COALESCE(idx_scan, 0) as reads
            FROM pg_stat_user_tables
            WHERE schemaname = 'public' AND relname = %s
        """

        try:
            with self.db_connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (table_name,))
                    result = cursor.fetchone()

                    if not result:
                        return 0.3  # Default

                    writes, reads = result
                    total_ops = writes + reads

                    if total_ops == 0:
                        return 0.3  # Default for tables with no stats

                    return writes / total_ops

        except Exception:
            return 0.3  # Default

    def _add_over_indexing_warnings(
        self,
        recommendations: List[IndexRecommendation]
    ) -> List[IndexRecommendation]:
        """
        Add over-indexing warnings to recommendations

        Args:
            recommendations: List of recommendations

        Returns:
            Recommendations with warnings added
        """
        # Group recommendations by table
        tables = set(rec.table_name for rec in recommendations)

        for table in tables:
            existing_count = self._get_existing_index_count(table)
            write_ratio = self._get_table_write_ratio(table)

            # Get recommendations for this table
            table_recs = [rec for rec in recommendations if rec.table_name == table]

            for rec in table_recs:
                # Check for over-indexing
                warning_info = self.check_over_indexing(
                    table_name=table,
                    existing_index_count=existing_count,
                    table_write_ratio=write_ratio
                )

                if warning_info['should_warn']:
                    rec.warning = warning_info['message']

                # Increment count for next recommendation on same table
                existing_count += 1

        return recommendations

    def check_over_indexing(
        self,
        table_name: str,
        existing_index_count: int,
        table_write_ratio: float = 0.3
    ) -> Dict[str, Any]:
        """
        Check if adding an index would cause over-indexing

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
        Analyse multiple queries and aggregate recommendations

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

"""
PostgreSQL Query Parser using pglast for AST analysis
"""
import pglast
from typing import Set, List, Dict, Any


class ColumnExtractor:
    """
    Extract columns from WHERE, ORDER BY, and JOIN clauses.
    Also extract table names from query.
    """

    def __init__(self):
        self.where_columns = set()
        self.order_by_columns = set()
        self.join_columns = set()
        self.tables = []

    def extract(self, ast_tree):
        """Extract information from AST tree."""
        for raw_stmt in ast_tree:
            # Unwrap RawStmt to get actual statement
            if hasattr(raw_stmt, 'stmt'):
                self._visit_node(raw_stmt.stmt, context='root')
            else:
                self._visit_node(raw_stmt, context='root')

    def _visit_node(self, node, context='root'):
        """Recursively visit AST nodes."""
        if node is None:
            return

        # Skip basic types and enums
        if isinstance(node, (str, int, float, bool)) or not hasattr(node, '__class__'):
            return

        node_type = node.__class__.__name__

        # Extract table names
        if node_type == 'RangeVar':
            if hasattr(node, 'relname') and node.relname:
                self.tables.append(node.relname)
            return  # No need to recurse into RangeVar

        # Extract columns based on context
        if node_type == 'ColumnRef':
            if hasattr(node, 'fields') and node.fields:
                last_field = node.fields[-1]
                if hasattr(last_field, 'sval'):
                    col_name = last_field.sval
                    if context == 'where':
                        self.where_columns.add(col_name)
                    elif context == 'order_by':
                        self.order_by_columns.add(col_name)
                    elif context == 'join':
                        self.join_columns.add(col_name)
            return  # No need to recurse into ColumnRef

        # Handle SELECT statements
        if node_type == 'SelectStmt':
            # Process WHERE clause with 'where' context
            if hasattr(node, 'whereClause') and node.whereClause:
                self._visit_node(node.whereClause, context='where')

            # Process ORDER BY clause with 'order_by' context
            if hasattr(node, 'sortClause') and node.sortClause:
                for sort_item in node.sortClause:
                    self._visit_node(sort_item, context='order_by')

            # Process FROM clause - keep context for recursion
            if hasattr(node, 'fromClause') and node.fromClause:
                for from_item in node.fromClause:
                    self._visit_node(from_item, context='from')
            return  # Already handled all relevant parts

        # Handle JOIN expressions
        if node_type == 'JoinExpr':
            # Process join conditions with 'join' context
            if hasattr(node, 'quals') and node.quals:
                self._visit_node(node.quals, context='join')

            # Process left and right sides
            if hasattr(node, 'larg') and node.larg:
                self._visit_node(node.larg, context='from')
            if hasattr(node, 'rarg') and node.rarg:
                self._visit_node(node.rarg, context='from')
            return  # Already handled all relevant parts

        # For all other node types, recursively visit all attributes
        # This handles A_Expr, SortBy, and other intermediate nodes
        # Common attributes to check across different node types
        common_attrs = ['lexpr', 'rexpr', 'node', 'expr', 'arg', 'args', 'val', 'sortby']

        for attr_name in common_attrs:
            if hasattr(node, attr_name):
                try:
                    attr_value = getattr(node, attr_name, None)
                    if attr_value is None:
                        continue
                    # Skip actual Python callables (methods), but not pglast nodes
                    if isinstance(attr_value, (list, tuple)):
                        for item in attr_value:
                            self._visit_node(item, context)
                    elif not isinstance(attr_value, (str, int, float, bool)):
                        self._visit_node(attr_value, context)
                except (AttributeError, TypeError):
                    continue


class QueryParser:
    """
    Parse SQL queries to extract columns and tables for index recommendations.
    """

    def __init__(self, query: str):
        """
        Initialize parser with SQL query.

        Args:
            query: SQL query string to parse

        Raises:
            ValueError: If query is empty or invalid SQL
        """
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")

        self.query = query
        try:
            self.ast = pglast.parse_sql(query)
        except Exception as e:
            raise ValueError(f"Failed to parse SQL query: {e}")

    def extract_columns(self) -> Dict[str, Set[str]]:
        """
        Extract columns from WHERE, ORDER BY, and JOIN clauses.

        Returns:
            Dict with keys:
                - where_columns: Columns used in WHERE clause
                - order_by_columns: Columns used in ORDER BY
                - join_columns: Columns used in JOIN conditions
        """
        extractor = ColumnExtractor()
        extractor.extract(self.ast)

        return {
            'where_columns': extractor.where_columns,
            'order_by_columns': extractor.order_by_columns,
            'join_columns': extractor.join_columns
        }

    def get_tables(self) -> List[str]:
        """
        Extract all table names from query.

        Returns:
            List of table names
        """
        extractor = ColumnExtractor()
        extractor.extract(self.ast)
        return extractor.tables

    def get_all_info(self) -> Dict[str, Any]:
        """
        Get all extracted information in one call.

        Returns:
            Dict containing tables and all column sets
        """
        extractor = ColumnExtractor()
        extractor.extract(self.ast)

        return {
            'tables': extractor.tables,
            'where_columns': extractor.where_columns,
            'order_by_columns': extractor.order_by_columns,
            'join_columns': extractor.join_columns
        }
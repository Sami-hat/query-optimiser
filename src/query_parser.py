"""
PostgreSQL Query Parser using pglast for AST analysis
"""
import pglast
from typing import Set, List, Dict, Any


class ColumnExtractor:
    """
    Extract columns from WHERE, ORDER BY, and JOIN clauses
    Also extract table names from query and track column-to-table mappings
    """

    def __init__(self):
        self.where_columns = set()
        self.order_by_columns = set()
        self.join_columns = set()
        self.tables = []
        self.table_aliases = {}  # Maps alias -> actual table name
        self.column_tables = {}  # Maps (column_name, context) -> table_name
        self.where_column_tables = {}  # Maps column -> table for WHERE clause
        self.order_by_column_tables = {}  # Maps column -> table for ORDER BY
        self.join_column_tables = {}  # Maps column -> table for JOIN conditions
        self.column_predicate_types = {}  # Maps column -> predicate type ('equality', 'range', 'other')
        self.constant_filters = {}  # Maps column -> constant value for partial index support

    def extract(self, ast_tree):
        """Extract information from AST tree"""
        for raw_stmt in ast_tree:
            # Unwrap RawStmt to get actual statement
            if hasattr(raw_stmt, 'stmt'):
                self._visit_node(raw_stmt.stmt, context='root')
            else:
                self._visit_node(raw_stmt, context='root')

    def _visit_node(self, node, context='root', operator=None):
        """Recursively visit AST nodes"""
        if node is None:
            return

        # Skip basic types and enums
        if isinstance(node, (str, int, float, bool)) or not hasattr(node, '__class__'):
            return

        node_type = node.__class__.__name__

        # Track operators for predicate type detection and constant filters
        if node_type == 'A_Expr' and context == 'where':
            # A_Expr has a 'name' field containing operator info
            if hasattr(node, 'name') and node.name:
                # Extract operator from the name list
                op_name = None
                for item in node.name:
                    if hasattr(item, 'sval'):
                        op_name = item.sval
                        break

                # Detect predicate type based on operator
                if op_name:
                    if op_name in ['=']:
                        operator = 'equality'

                        # Check for constant value predicates for partial indexes
                        # Pattern: column = 'constant_value'
                        column_name = None
                        constant_value = None

                        # Try to extract column from left expression
                        if hasattr(node, 'lexpr') and node.lexpr:
                            if node.lexpr.__class__.__name__ == 'ColumnRef':
                                if hasattr(node.lexpr, 'fields') and node.lexpr.fields:
                                    last_field = node.lexpr.fields[-1]
                                    if hasattr(last_field, 'sval'):
                                        column_name = last_field.sval

                        # Try to extract constant from right expression
                        if hasattr(node, 'rexpr') and node.rexpr:
                            rexpr_type = node.rexpr.__class__.__name__
                            if rexpr_type == 'A_Const':
                                # It's a constant value
                                if hasattr(node.rexpr, 'val'):
                                    const_val = node.rexpr.val
                                    if hasattr(const_val, 'sval'):
                                        constant_value = f"'{const_val.sval}'"
                                    elif hasattr(const_val, 'ival'):
                                        constant_value = str(const_val.ival)

                        # Store constant filter if both column and value found
                        if column_name and constant_value:
                            self.constant_filters[column_name] = constant_value

                    elif op_name in ['<', '>', '<=', '>=', '<>', '!=']:
                        operator = 'range'
                    else:
                        operator = 'other'

            # Visit child nodes with operator context
            if hasattr(node, 'lexpr') and node.lexpr:
                self._visit_node(node.lexpr, context, operator)
            if hasattr(node, 'rexpr') and node.rexpr:
                self._visit_node(node.rexpr, context, operator)
            return

        node_type = node.__class__.__name__

        # Extract table names and aliases
        if node_type == 'RangeVar':
            if hasattr(node, 'relname') and node.relname:
                table_name = node.relname
                self.tables.append(table_name)

                # Check for alias
                if hasattr(node, 'alias') and node.alias:
                    if hasattr(node.alias, 'aliasname'):
                        alias_name = node.alias.aliasname
                        self.table_aliases[alias_name] = table_name
                else:
                    # If no alias, table name can be used directly
                    self.table_aliases[table_name] = table_name
            return  # No need to recurse into RangeVar

        # Extract columns based on context
        if node_type == 'ColumnRef':
            if hasattr(node, 'fields') and node.fields:
                # ColumnRef can be: column_name OR table.column_name OR schema.table.column_name
                fields = node.fields
                table_name = None
                col_name = None

                # Extract column name (always the last field)
                last_field = fields[-1]
                if hasattr(last_field, 'sval'):
                    col_name = last_field.sval

                # Extract table/alias if qualified (e.g., users.email or u.email)
                if len(fields) >= 2:
                    qualifier_field = fields[-2]
                    if hasattr(qualifier_field, 'sval'):
                        qualifier = qualifier_field.sval
                        # Resolve alias to actual table name
                        table_name = self.table_aliases.get(qualifier, qualifier)

                if col_name:
                    if context == 'where':
                        self.where_columns.add(col_name)
                        if table_name:
                            self.where_column_tables[col_name] = table_name
                        # Store predicate type if available
                        if operator:
                            self.column_predicate_types[col_name] = operator
                        elif col_name not in self.column_predicate_types:
                            self.column_predicate_types[col_name] = 'other'
                    elif context == 'order_by':
                        self.order_by_columns.add(col_name)
                        if table_name:
                            self.order_by_column_tables[col_name] = table_name
                    elif context == 'join':
                        self.join_columns.add(col_name)
                        if table_name:
                            self.join_column_tables[col_name] = table_name
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
    Parse SQL queries to extract columns and tables for index recommendations
    """

    def __init__(self, query: str):
        """
        Initialize parser with SQL query

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
        Extract all table names from query

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
            Dict containing tables, column sets, and column-to-table mappings
        """
        extractor = ColumnExtractor()
        extractor.extract(self.ast)

        return {
            'tables': extractor.tables,
            'where_columns': extractor.where_columns,
            'order_by_columns': extractor.order_by_columns,
            'join_columns': extractor.join_columns,
            'table_aliases': extractor.table_aliases,
            'where_column_tables': extractor.where_column_tables,
            'order_by_column_tables': extractor.order_by_column_tables,
            'join_column_tables': extractor.join_column_tables,
            'column_predicate_types': extractor.column_predicate_types,
            'constant_filters': extractor.constant_filters
        }
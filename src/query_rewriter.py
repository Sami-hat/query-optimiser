"""
Rule-based SQL query rewrite suggestion engine.
Uses pglast AST traversal to detect SQL anti-patterns that hurt performance.
"""
import re
import pglast
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Literal, Optional, Set

LARGE_OFFSET_THRESHOLD = 1000
_IMPROVEMENT_ORDER = {'high': 0, 'medium': 1, 'low': 2}


@dataclass
class RewriteSuggestion:
    pattern_name: str
    description: str
    original_snippet: str
    suggested_rewrite: str
    reason: str
    improvement_level: Literal['high', 'medium', 'low']
    rewritten_query: Optional[str] = None


class QueryRewriter:
    """
    Analyses a SQL SELECT statement for common anti-patterns using pglast AST traversal.
    Returns RewriteSuggestion objects — no database connection required.
    """

    def analyse(
        self,
        query: str,
        explain_output: Optional[Dict] = None,
        sequential_scans: Optional[List[Dict]] = None,
    ) -> List[RewriteSuggestion]:
        """
        Entry point. Parse the query and run all pattern checkers.

        Returns:
            List of RewriteSuggestion sorted high -> medium -> low.
            Returns [] for non-SELECT statements or unparseable SQL.
        """
        if not query or not query.strip():
            return []
        try:
            ast = pglast.parse_sql(query)
        except Exception:
            return []

        results: List[RewriteSuggestion] = []
        for raw_stmt in ast:
            stmt = raw_stmt.stmt if hasattr(raw_stmt, 'stmt') else raw_stmt
            if stmt.__class__.__name__ != 'SelectStmt':
                continue
            results.extend(self._check_select_star(stmt))
            results.extend(self._check_leading_wildcard_like(stmt))
            results.extend(self._check_not_in_subquery(stmt))
            results.extend(self._check_or_on_same_column(stmt))
            results.extend(self._check_function_on_column(stmt))
            results.extend(self._check_large_offset(stmt, explain_output))
            results.extend(self._check_implicit_cast(stmt, explain_output))

        results.sort(key=lambda s: _IMPROVEMENT_ORDER.get(s.improvement_level, 99))
        return results

    # ------------------------------------------------------------------
    # AST walker
    # ------------------------------------------------------------------

    def _walk(
        self,
        node: Any,
        visitor: Callable[[Any, str], None],
        context: str = 'root',
    ) -> None:
        """
        Depth-first walk of a pglast AST subtree.
        visitor(node, context) is called for every non-primitive node.
        Context propagates 'where', 'select', 'from', 'order_by', 'join'.
        Mirrors the design of ColumnExtractor._visit_node() in query_parser.py.
        """
        if node is None:
            return
        if isinstance(node, (str, int, float, bool)):
            return
        if isinstance(node, (list, tuple)):
            for item in node:
                self._walk(item, visitor, context)
            return

        visitor(node, context)

        node_type = node.__class__.__name__

        # Context-switching for top-level statement clauses
        if node_type == 'SelectStmt':
            if hasattr(node, 'targetList') and node.targetList:
                self._walk(node.targetList, visitor, 'select')
            if hasattr(node, 'whereClause') and node.whereClause:
                self._walk(node.whereClause, visitor, 'where')
            if hasattr(node, 'fromClause') and node.fromClause:
                self._walk(node.fromClause, visitor, 'from')
            if hasattr(node, 'sortClause') and node.sortClause:
                self._walk(node.sortClause, visitor, 'order_by')
            # limitOffset handled directly in _check_large_offset
            return

        if node_type == 'JoinExpr':
            if hasattr(node, 'quals') and node.quals:
                self._walk(node.quals, visitor, 'join')
            if hasattr(node, 'larg') and node.larg:
                self._walk(node.larg, visitor, 'from')
            if hasattr(node, 'rarg') and node.rarg:
                self._walk(node.rarg, visitor, 'from')
            return

        # General recursion into child attributes
        for attr in ('lexpr', 'rexpr', 'arg', 'args', 'val', 'expr', 'quals'):
            if not hasattr(node, attr):
                continue
            try:
                child = getattr(node, attr)
                if child is None or isinstance(child, (str, int, float, bool)):
                    continue
                self._walk(child, visitor, context)
            except (AttributeError, TypeError):
                continue

    # ------------------------------------------------------------------
    # Pattern 1: SELECT *
    # ------------------------------------------------------------------

    def _check_select_star(self, stmt: Any) -> List[RewriteSuggestion]:
        target_list = getattr(stmt, 'targetList', None)
        if not target_list:
            return []

        for target in target_list:
            if target.__class__.__name__ != 'ResTarget':
                continue
            val = getattr(target, 'val', None)
            if val is None or val.__class__.__name__ != 'ColumnRef':
                continue
            fields = getattr(val, 'fields', []) or []
            for field in fields:
                if field.__class__.__name__ == 'A_Star':
                    return [RewriteSuggestion(
                        pattern_name='select_star',
                        description='SELECT * retrieves all columns unnecessarily',
                        original_snippet='SELECT *',
                        suggested_rewrite='SELECT col1, col2, ...  (only columns your code uses)',
                        reason=(
                            'SELECT * fetches every column including ones your application '
                            'never uses, increasing network transfer and memory overhead. '
                            'It also prevents PostgreSQL from using index-only scans, which '
                            'can avoid reading the heap entirely.'
                        ),
                        improvement_level='medium',
                    )]
        return []

    # ------------------------------------------------------------------
    # Pattern 2: Leading wildcard LIKE / ILIKE
    # ------------------------------------------------------------------

    def _check_leading_wildcard_like(self, stmt: Any) -> List[RewriteSuggestion]:
        found: List[RewriteSuggestion] = []

        def visitor(node: Any, context: str) -> None:
            if node.__class__.__name__ != 'A_Expr':
                return
            name_list = getattr(node, 'name', None) or []
            op = None
            for item in name_list:
                sval = getattr(item, 'sval', None)
                if sval in ('~~', '~~*'):
                    op = sval
                    break
            if op is None:
                return

            rexpr = getattr(node, 'rexpr', None)
            if rexpr is None or rexpr.__class__.__name__ != 'A_Const':
                return
            # Support both pglast 3.x (A_Const.val.sval) and 4.x (A_Const.sval)
            const_str = self._get_const_sval(rexpr)
            if const_str is None or not const_str.startswith('%'):
                return

            col_name = self._extract_colref_name(getattr(node, 'lexpr', None)) or 'column'
            op_label = 'ILIKE' if op == '~~*' else 'LIKE'
            snippet = f"{col_name} {op_label} '{const_str}'"
            found.append(RewriteSuggestion(
                pattern_name='leading_wildcard_like',
                description='Leading wildcard LIKE forces a sequential scan',
                original_snippet=snippet,
                suggested_rewrite=(
                    f"-- Option 1: GIN trigram index + LIKE\n"
                    f"CREATE INDEX ON table USING gin ({col_name} gin_trgm_ops);\n"
                    f"-- Option 2: full-text search\n"
                    f"WHERE to_tsvector({col_name}) @@ plainto_tsquery('term')"
                ),
                reason=(
                    f"'{const_str}' starts with %, so PostgreSQL cannot use a B-tree index "
                    f"on {col_name} — it must scan every row. "
                    "A pg_trgm GIN index supports arbitrary LIKE/ILIKE patterns, "
                    "or restructure the query to use a trailing-wildcard prefix search."
                ),
                improvement_level='high',
            ))

        self._walk(stmt, visitor)
        return found

    # ------------------------------------------------------------------
    # Pattern 3: NOT IN (subquery) → NOT EXISTS
    # ------------------------------------------------------------------

    def _check_not_in_subquery(self, stmt: Any) -> List[RewriteSuggestion]:
        found: List[RewriteSuggestion] = []
        seen: Set[int] = set()  # prevent duplicates from nested walks

        def visitor(node: Any, context: str) -> None:
            if node.__class__.__name__ != 'BoolExpr':
                return
            boolop = getattr(node, 'boolop', None)
            if boolop is None:
                return
            # pglast BoolExprType: AND_EXPR=0, OR_EXPR=1, NOT_EXPR=2
            # str(boolop) returns the integer string ('2'), not 'NOT_EXPR'
            try:
                is_not = int(boolop) == 2
            except (TypeError, ValueError):
                is_not = 'NOT' in str(boolop).upper()
            if not is_not:
                return
            args = getattr(node, 'args', None) or []
            for arg in args:
                if arg.__class__.__name__ == 'SubLink':
                    node_id = id(node)
                    if node_id in seen:
                        return
                    seen.add(node_id)
                    found.append(RewriteSuggestion(
                        pattern_name='not_in_subquery',
                        description='NOT IN (subquery) — prefer NOT EXISTS for NULL safety',
                        original_snippet='col NOT IN (SELECT col FROM other_table WHERE ...)',
                        suggested_rewrite='NOT EXISTS (SELECT 1 FROM other_table WHERE other_table.col = t.col)',
                        reason=(
                            'NOT IN returns zero rows if the subquery produces any NULL values, '
                            'due to SQL three-valued logic — a common source of silent bugs. '
                            'NOT EXISTS is NULL-safe, often gets a better query plan, '
                            'and makes the intent explicit.'
                        ),
                        improvement_level='high',
                    ))
                    break

        self._walk(stmt, visitor)
        return found

    # ------------------------------------------------------------------
    # Pattern 4: OR on same column → IN (...)
    # ------------------------------------------------------------------

    def _check_or_on_same_column(self, stmt: Any) -> List[RewriteSuggestion]:
        found: List[RewriteSuggestion] = []

        def visitor(node: Any, context: str) -> None:
            if context != 'where':
                return
            if node.__class__.__name__ != 'BoolExpr':
                return
            boolop = getattr(node, 'boolop', None)
            if boolop is None:
                return
            # pglast BoolExprType: AND_EXPR=0, OR_EXPR=1, NOT_EXPR=2
            # str(boolop) returns the integer string ('1'), not 'OR_EXPR'
            try:
                is_or = int(boolop) == 1
            except (TypeError, ValueError):
                is_or = 'OR' in str(boolop).upper()
            if not is_or:
                return

            column_values: Dict[str, List[str]] = {}
            args = getattr(node, 'args', None) or []
            for arg in args:
                if arg.__class__.__name__ != 'A_Expr':
                    continue
                name_list = getattr(arg, 'name', None) or []
                is_eq = any(getattr(n, 'sval', None) == '=' for n in name_list)
                if not is_eq:
                    continue
                col_name = self._extract_colref_name(getattr(arg, 'lexpr', None))
                const_val = self._extract_const_value(getattr(arg, 'rexpr', None))
                if col_name and const_val:
                    column_values.setdefault(col_name, []).append(const_val)

            for col, vals in column_values.items():
                if len(vals) >= 2:
                    original = ' OR '.join(f"{col} = {v}" for v in vals)
                    in_list = ', '.join(vals)
                    found.append(RewriteSuggestion(
                        pattern_name='or_on_same_column',
                        description=f'OR equality conditions on "{col}" can use IN (...)',
                        original_snippet=original,
                        suggested_rewrite=f"{col} IN ({in_list})",
                        reason=(
                            f'Multiple OR equality conditions on "{col}" are cleaner '
                            'as IN (...), which is easier to read, simpler to extend, '
                            'and may allow the planner to choose a more efficient bitmap index scan.'
                        ),
                        improvement_level='low',
                    ))

        self._walk(stmt, visitor)
        return found

    # ------------------------------------------------------------------
    # Pattern 5: Function call wrapping a column in WHERE
    # ------------------------------------------------------------------

    def _check_function_on_column(self, stmt: Any) -> List[RewriteSuggestion]:
        found: List[RewriteSuggestion] = []
        seen: Set[tuple] = set()

        def visitor(node: Any, context: str) -> None:
            if context != 'where':
                return
            if node.__class__.__name__ != 'FuncCall':
                return
            func_name = self._get_func_name(getattr(node, 'funcname', None))
            if not func_name:
                return
            args = getattr(node, 'args', None) or []
            for arg in args:
                if arg.__class__.__name__ != 'ColumnRef':
                    continue
                col_name = self._extract_colref_name(arg)
                if not col_name:
                    continue
                key = (func_name.upper(), col_name)
                if key in seen:
                    continue
                seen.add(key)
                snippet = f"{func_name.upper()}({col_name}) = ..."
                found.append(RewriteSuggestion(
                    pattern_name='function_on_column',
                    description=f'Function {func_name.upper()}() on column "{col_name}" blocks index use',
                    original_snippet=snippet,
                    suggested_rewrite=(
                        f"-- Create an expression index instead:\n"
                        f"CREATE INDEX ON table ({func_name.upper()}({col_name}));\n"
                        f"-- Or store the pre-computed value in a separate column"
                    ),
                    reason=(
                        f'Wrapping "{col_name}" in {func_name.upper()}() in the WHERE clause '
                        'prevents PostgreSQL from using a regular B-tree index on that column — '
                        'the function must be evaluated for every row. '
                        'Create an expression index matching the function call, '
                        'or restructure the query to compare the raw column directly.'
                    ),
                    improvement_level='high',
                ))

        self._walk(stmt, visitor)
        return found

    # ------------------------------------------------------------------
    # Pattern 6: Large OFFSET
    # ------------------------------------------------------------------

    def _check_large_offset(
        self, stmt: Any, explain_output: Optional[Dict]
    ) -> List[RewriteSuggestion]:
        limit_offset = getattr(stmt, 'limitOffset', None)
        if limit_offset is None:
            return []
        if limit_offset.__class__.__name__ != 'A_Const':
            return []

        offset_val = self._get_const_ival(limit_offset)
        if offset_val is None or offset_val < LARGE_OFFSET_THRESHOLD:
            return []

        limit_count = getattr(stmt, 'limitCount', None)
        limit_str = ''
        if limit_count and limit_count.__class__.__name__ == 'A_Const':
            lv = self._get_const_ival(limit_count)
            if lv is not None:
                limit_str = f'LIMIT {lv} '

        return [RewriteSuggestion(
            pattern_name='large_offset',
            description=f'Large OFFSET ({offset_val:,}) causes a full scan of skipped rows',
            original_snippet=f'{limit_str}OFFSET {offset_val:,}',
            suggested_rewrite=(
                'WHERE id > :last_seen_id ORDER BY id LIMIT n  -- keyset (cursor) pagination'
            ),
            reason=(
                f'OFFSET {offset_val:,} forces the database to read and discard {offset_val:,} rows '
                'before returning your page — performance degrades linearly. '
                'Keyset pagination (WHERE pk > last_seen) skips directly to the next page '
                'using the index, making deep pages as fast as the first page.'
            ),
            improvement_level='medium',
        )]

    # ------------------------------------------------------------------
    # Pattern 7: Explicit TypeCast on a column in WHERE
    # ------------------------------------------------------------------

    def _check_implicit_cast(
        self, stmt: Any, explain_output: Optional[Dict]
    ) -> List[RewriteSuggestion]:
        found: List[RewriteSuggestion] = []
        seen: Set[str] = set()

        def visitor(node: Any, context: str) -> None:
            if context != 'where':
                return
            if node.__class__.__name__ != 'TypeCast':
                return
            arg = getattr(node, 'arg', None)
            if arg is None or arg.__class__.__name__ != 'ColumnRef':
                return
            col_name = self._extract_colref_name(arg)
            if not col_name or col_name in seen:
                return
            seen.add(col_name)

            type_name_node = getattr(node, 'typeName', None)
            cast_type = self._get_type_name(type_name_node) or '...'
            snippet = f"{col_name}::{cast_type}"
            found.append(RewriteSuggestion(
                pattern_name='implicit_cast',
                description=f'Type cast on "{col_name}" in WHERE prevents index use',
                original_snippet=snippet,
                suggested_rewrite=(
                    f"Use the correct literal type — no cast needed:\n"
                    f"WHERE {col_name} = value  (match the column's declared type)"
                ),
                reason=(
                    f'Casting "{col_name}" to {cast_type} in the WHERE clause prevents '
                    'PostgreSQL from using an index on that column. '
                    'Fix the literal type on the right-hand side instead '
                    '(e.g., compare an integer column to an integer literal, not a string).'
                ),
                improvement_level='high',
            ))

        self._walk(stmt, visitor)

        # Fallback: scan EXPLAIN plan Filter strings for :: notation
        if explain_output and not found:
            self._check_explain_casts(explain_output, found, seen)

        return found

    def _check_explain_casts(
        self,
        explain_output: Dict,
        found: List[RewriteSuggestion],
        seen: Set[str],
    ) -> None:
        """Scan EXPLAIN plan Filter strings for cast notation (col::type)."""
        plan = explain_output.get('explain_plan', explain_output)
        if isinstance(plan, list) and plan:
            plan = plan[0].get('Plan', plan[0])
        elif isinstance(plan, dict):
            plan = plan.get('Plan', plan)
        self._scan_plan_for_casts(plan, found, seen)

    def _scan_plan_for_casts(
        self,
        node: Any,
        found: List[RewriteSuggestion],
        seen: Set[str],
    ) -> None:
        if not isinstance(node, dict):
            return
        filter_str = node.get('Filter', '')
        if '::' in filter_str:
            for col_name in re.findall(r'(\w+)::\w+\s*[=<>]', filter_str):
                if col_name.lower() in ('null', 'true', 'false') or col_name in seen:
                    continue
                seen.add(col_name)
                found.append(RewriteSuggestion(
                    pattern_name='implicit_cast',
                    description=f'Implicit type cast on "{col_name}" detected in query plan',
                    original_snippet=f'{col_name}::...',
                    suggested_rewrite=f'Use correct literal type: WHERE {col_name} = value',
                    reason=(
                        f'The query plan Filter shows a cast on "{col_name}", preventing index use. '
                        'Ensure the compared value matches the column\'s declared type.'
                    ),
                    improvement_level='high',
                ))
        for child in node.get('Plans', []):
            self._scan_plan_for_casts(child, found, seen)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _extract_colref_name(self, node: Any) -> Optional[str]:
        """Return the column name (last field sval) from a ColumnRef node."""
        if node is None or node.__class__.__name__ != 'ColumnRef':
            return None
        fields = getattr(node, 'fields', None) or []
        if not fields:
            return None
        last = fields[-1]
        return getattr(last, 'sval', None)

    def _extract_const_value(self, node: Any) -> Optional[str]:
        """Return a string representation of an A_Const's value."""
        if node is None or node.__class__.__name__ != 'A_Const':
            return None
        # pglast 3.x: A_Const.val.{sval,ival,fval}
        val = getattr(node, 'val', None)
        if val is not None:
            if hasattr(val, 'sval'):
                return f"'{val.sval}'"
            if hasattr(val, 'ival'):
                return str(val.ival)
            if hasattr(val, 'fval'):
                return str(val.fval)
        # pglast 4.x: A_Const.{sval,ival,fval} directly
        if hasattr(node, 'sval'):
            return f"'{node.sval}'"
        if hasattr(node, 'ival'):
            return str(node.ival)
        if hasattr(node, 'fval'):
            return str(node.fval)
        return None

    def _get_const_sval(self, node: Any) -> Optional[str]:
        """Return the string value of an A_Const, or None."""
        val = getattr(node, 'val', None)
        if val is not None and hasattr(val, 'sval'):
            return val.sval
        return getattr(node, 'sval', None)

    def _get_const_ival(self, node: Any) -> Optional[int]:
        """Return the integer value of an A_Const, or None."""
        val = getattr(node, 'val', None)
        if val is not None and hasattr(val, 'ival'):
            return val.ival
        ival = getattr(node, 'ival', None)
        if ival is not None:
            return ival
        return None

    def _get_func_name(self, funcname: Any) -> Optional[str]:
        """Extract function name string from a funcname list."""
        if not funcname:
            return None
        parts = [item.sval for item in funcname if hasattr(item, 'sval')]
        return '.'.join(parts) if parts else None

    def _get_type_name(self, type_name_node: Any) -> Optional[str]:
        """Extract type name from a TypeName node, skipping pg_catalog prefix."""
        if type_name_node is None:
            return None
        names = getattr(type_name_node, 'names', None) or []
        parts = [
            item.sval for item in names
            if hasattr(item, 'sval') and item.sval != 'pg_catalog'
        ]
        return '.'.join(parts) if parts else None

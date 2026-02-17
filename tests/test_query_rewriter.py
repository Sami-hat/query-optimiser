"""
Tests for QueryRewriter — one test per anti-pattern, plus edge cases.
No database connection required; all tests use static SQL strings.
"""
import pytest
from src.query_rewriter import QueryRewriter, RewriteSuggestion


class TestQueryRewriter:

    def setup_method(self):
        self.rw = QueryRewriter()

    def _pattern_names(self, query, **kwargs):
        return [s.pattern_name for s in self.rw.analyse(query, **kwargs)]

    # ------------------------------------------------------------------
    # Pattern 1: SELECT *
    # ------------------------------------------------------------------

    def test_select_star_detected(self):
        names = self._pattern_names("SELECT * FROM users WHERE id = 1")
        assert 'select_star' in names

    def test_select_star_table_qualified(self):
        names = self._pattern_names("SELECT u.* FROM users u WHERE u.id = 1")
        # table.* is also A_Star — should be caught
        assert 'select_star' in names

    def test_no_select_star_false_positive(self):
        names = self._pattern_names("SELECT id, email FROM users WHERE id = 1")
        assert 'select_star' not in names

    # ------------------------------------------------------------------
    # Pattern 2: Leading wildcard LIKE / ILIKE
    # ------------------------------------------------------------------

    def test_leading_wildcard_like_detected(self):
        names = self._pattern_names("SELECT id FROM users WHERE name LIKE '%smith'")
        assert 'leading_wildcard_like' in names

    def test_leading_wildcard_ilike_detected(self):
        names = self._pattern_names("SELECT id FROM users WHERE email ILIKE '%@example.com'")
        assert 'leading_wildcard_like' in names

    def test_trailing_wildcard_like_not_flagged(self):
        names = self._pattern_names("SELECT id FROM users WHERE name LIKE 'smith%'")
        assert 'leading_wildcard_like' not in names

    def test_both_wildcard_like_detected(self):
        # %value% also has a leading wildcard
        names = self._pattern_names("SELECT id FROM users WHERE name LIKE '%smith%'")
        assert 'leading_wildcard_like' in names

    # ------------------------------------------------------------------
    # Pattern 3: NOT IN (subquery)
    # ------------------------------------------------------------------

    def test_not_in_subquery_detected(self):
        query = """
            SELECT id FROM users
            WHERE id NOT IN (SELECT user_id FROM banned_users)
        """
        names = self._pattern_names(query)
        assert 'not_in_subquery' in names

    def test_in_subquery_not_flagged(self):
        # plain IN (subquery) should NOT trigger not_in_subquery
        query = "SELECT id FROM users WHERE id IN (SELECT user_id FROM vip_users)"
        names = self._pattern_names(query)
        assert 'not_in_subquery' not in names

    # ------------------------------------------------------------------
    # Pattern 4: OR on same column → IN (...)
    # ------------------------------------------------------------------

    def test_or_same_column_detected(self):
        query = "SELECT id FROM orders WHERE status = 'pending' OR status = 'processing'"
        names = self._pattern_names(query)
        assert 'or_on_same_column' in names

    def test_or_same_column_three_values(self):
        query = "SELECT id FROM orders WHERE status = 'a' OR status = 'b' OR status = 'c'"
        results = self.rw.analyse(query)
        or_results = [r for r in results if r.pattern_name == 'or_on_same_column']
        assert len(or_results) >= 1
        # Suggested rewrite should contain IN
        assert 'IN' in or_results[0].suggested_rewrite

    def test_or_different_columns_not_flagged(self):
        query = "SELECT id FROM orders WHERE status = 'pending' OR user_id = 5"
        names = self._pattern_names(query)
        assert 'or_on_same_column' not in names

    # ------------------------------------------------------------------
    # Pattern 5: Function call wrapping column in WHERE
    # ------------------------------------------------------------------

    def test_function_on_column_lower_detected(self):
        query = "SELECT id FROM users WHERE LOWER(email) = 'test@example.com'"
        names = self._pattern_names(query)
        assert 'function_on_column' in names

    def test_function_on_column_date_detected(self):
        query = "SELECT id FROM orders WHERE DATE(created_at) = '2024-01-01'"
        names = self._pattern_names(query)
        assert 'function_on_column' in names

    def test_function_in_select_not_flagged(self):
        # Function in SELECT list should not trigger
        query = "SELECT LOWER(email), id FROM users WHERE id = 1"
        names = self._pattern_names(query)
        assert 'function_on_column' not in names

    def test_function_in_order_by_not_flagged(self):
        query = "SELECT id FROM users WHERE id > 0 ORDER BY LOWER(name)"
        names = self._pattern_names(query)
        assert 'function_on_column' not in names

    # ------------------------------------------------------------------
    # Pattern 6: Large OFFSET
    # ------------------------------------------------------------------

    def test_large_offset_detected(self):
        query = "SELECT id FROM users ORDER BY id LIMIT 20 OFFSET 50000"
        names = self._pattern_names(query)
        assert 'large_offset' in names

    def test_offset_at_threshold_detected(self):
        query = "SELECT id FROM users LIMIT 10 OFFSET 1000"
        names = self._pattern_names(query)
        assert 'large_offset' in names

    def test_small_offset_not_flagged(self):
        query = "SELECT id FROM users ORDER BY id LIMIT 20 OFFSET 20"
        names = self._pattern_names(query)
        assert 'large_offset' not in names

    def test_no_offset_not_flagged(self):
        query = "SELECT id FROM users LIMIT 20"
        names = self._pattern_names(query)
        assert 'large_offset' not in names

    # ------------------------------------------------------------------
    # Pattern 7: TypeCast on column in WHERE
    # ------------------------------------------------------------------

    def test_explicit_cast_on_column_detected(self):
        query = "SELECT id FROM users WHERE CAST(user_id AS text) = '123'"
        names = self._pattern_names(query)
        assert 'implicit_cast' in names

    def test_cast_operator_on_column_detected(self):
        # PostgreSQL ::type cast syntax
        query = "SELECT id FROM users WHERE user_id::text = '123'"
        names = self._pattern_names(query)
        assert 'implicit_cast' in names

    def test_cast_in_select_not_flagged(self):
        # Cast in SELECT list, not WHERE
        query = "SELECT id::text FROM users WHERE id = 1"
        names = self._pattern_names(query)
        assert 'implicit_cast' not in names

    # ------------------------------------------------------------------
    # Result ordering
    # ------------------------------------------------------------------

    def test_high_improvements_sorted_before_low(self):
        # LOWER() in WHERE (high) + OR same col (low)
        query = """
            SELECT * FROM users
            WHERE LOWER(email) = 'x'
            AND (status = 'a' OR status = 'b')
            LIMIT 10 OFFSET 5000
        """
        results = self.rw.analyse(query)
        levels = [r.improvement_level for r in results]
        high_indices = [i for i, l in enumerate(levels) if l == 'high']
        low_indices = [i for i, l in enumerate(levels) if l == 'low']
        if high_indices and low_indices:
            assert max(high_indices) < min(low_indices)

    # ------------------------------------------------------------------
    # Edge cases / resilience
    # ------------------------------------------------------------------

    def test_empty_query_returns_empty(self):
        assert self.rw.analyse('') == []

    def test_whitespace_only_returns_empty(self):
        assert self.rw.analyse('   \n  ') == []

    def test_invalid_sql_returns_empty(self):
        assert self.rw.analyse('NOT VALID SQL @@@') == []

    def test_non_select_returns_empty(self):
        assert self.rw.analyse("UPDATE users SET name = 'x' WHERE id = 1") == []

    def test_insert_returns_empty(self):
        assert self.rw.analyse("INSERT INTO users (name) VALUES ('Alice')") == []

    def test_returns_list_of_rewrite_suggestions(self):
        results = self.rw.analyse("SELECT * FROM users WHERE LOWER(email) = 'x'")
        assert isinstance(results, list)
        for r in results:
            assert isinstance(r, RewriteSuggestion)
            assert r.pattern_name
            assert r.description
            assert r.improvement_level in ('high', 'medium', 'low')

    def test_multiple_patterns_in_one_query(self):
        query = """
            SELECT * FROM users
            WHERE LOWER(email) LIKE '%@example.com'
            AND id NOT IN (SELECT user_id FROM banned)
            LIMIT 20 OFFSET 10000
        """
        names = self._pattern_names(query)
        # Should detect: select_star, leading_wildcard_like, function_on_column,
        # not_in_subquery, large_offset
        assert 'select_star' in names
        assert 'leading_wildcard_like' in names
        assert 'not_in_subquery' in names
        assert 'large_offset' in names

"""
Tests for QueryParser
"""
import pytest
from src.query_parser import QueryParser


class TestQueryParser:
    """Test suite for QueryParser class"""

    def test_simple_where_clause(self):
        """Test parsing simple WHERE clause"""
        query = "SELECT * FROM users WHERE email = 'test@example.com'"
        parser = QueryParser(query)

        columns = parser.extract_columns()
        tables = parser.get_tables()

        assert 'email' in columns['where_columns']
        assert len(columns['order_by_columns']) == 0
        assert len(columns['join_columns']) == 0
        assert 'users' in tables

    def test_multiple_where_conditions(self):
        """Test parsing multiple WHERE conditions"""
        query = "SELECT * FROM users WHERE email LIKE 'test%' AND created_at > '2024-01-01'"
        parser = QueryParser(query)

        columns = parser.extract_columns()

        assert 'email' in columns['where_columns']
        assert 'created_at' in columns['where_columns']
        assert len(columns['where_columns']) == 2

    def test_order_by_clause(self):
        """Test parsing ORDER BY clause"""
        query = "SELECT * FROM users ORDER BY created_at DESC, name ASC"
        parser = QueryParser(query)

        columns = parser.extract_columns()

        assert 'created_at' in columns['order_by_columns']
        assert 'name' in columns['order_by_columns']
        assert len(columns['order_by_columns']) == 2

    def test_join_query(self):
        """Test parsing JOIN query"""
        query = """
            SELECT u.*, o.id
            FROM users u
            JOIN orders o ON u.id = o.user_id
            WHERE o.status = 'pending'
        """
        parser = QueryParser(query)

        columns = parser.extract_columns()
        tables = parser.get_tables()

        assert 'id' in columns['join_columns']
        assert 'user_id' in columns['join_columns']
        assert 'status' in columns['where_columns']
        assert 'users' in tables
        assert 'orders' in tables

    def test_subquery(self):
        """Test parsing query with subquery"""
        query = """
            SELECT * FROM users
            WHERE id IN (SELECT user_id FROM orders WHERE total > 100)
        """
        parser = QueryParser(query)

        columns = parser.extract_columns()
        tables = parser.get_tables()

        assert 'id' in columns['where_columns']
        assert 'user_id' in columns['where_columns']
        assert 'total' in columns['where_columns']
        assert 'users' in tables
        assert 'orders' in tables

    def test_empty_query_error(self):
        """Test that empty query raises ValueError"""
        with pytest.raises(ValueError, match="Query cannot be empty"):
            QueryParser("")

    def test_invalid_sql_error(self):
        """Test that invalid SQL raises ValueError"""
        with pytest.raises(ValueError, match="Failed to parse SQL query"):
            QueryParser("SELECT FROM WHERE")

    def test_between_operator(self):
        """Test parsing BETWEEN operator"""
        query = "SELECT * FROM users WHERE id BETWEEN 10000 AND 20000"
        parser = QueryParser(query)

        columns = parser.extract_columns()

        assert 'id' in columns['where_columns']

    def test_like_operator(self):
        """Test parsing LIKE operator"""
        query = "SELECT COUNT(*) FROM users WHERE name LIKE 'User 5%'"
        parser = QueryParser(query)

        columns = parser.extract_columns()

        assert 'name' in columns['where_columns']

    def test_get_all_info(self):
        """Test get_all_info method"""
        query = "SELECT * FROM users WHERE email = 'test' ORDER BY created_at"
        parser = QueryParser(query)

        info = parser.get_all_info()

        assert 'tables' in info
        assert 'where_columns' in info
        assert 'order_by_columns' in info
        assert 'join_columns' in info
        assert 'users' in info['tables']
        assert 'email' in info['where_columns']
        assert 'created_at' in info['order_by_columns']

    def test_multiple_tables_without_join(self):
        """Test parsing query with multiple tables in FROM clause"""
        query = "SELECT * FROM users, orders WHERE users.id = orders.user_id"
        parser = QueryParser(query)

        columns = parser.extract_columns()
        tables = parser.get_tables()

        assert 'id' in columns['where_columns']
        assert 'user_id' in columns['where_columns']
        assert 'users' in tables
        assert 'orders' in tables

    def test_aggregate_functions(self):
        """Test parsing query with aggregate functions"""
        query = "SELECT COUNT(*), MAX(created_at) FROM users WHERE age > 18"
        parser = QueryParser(query)

        columns = parser.extract_columns()

        assert 'age' in columns['where_columns']
        # created_at is in SELECT clause, not WHERE, so shouldn't be in where_columns

    def test_group_by_and_having(self):
        """Test parsing query with GROUP BY and HAVING"""
        query = """
            SELECT category, COUNT(*) as cnt
            FROM products
            WHERE price > 100
            GROUP BY category
            HAVING COUNT(*) > 5
        """
        parser = QueryParser(query)

        columns = parser.extract_columns()

        assert 'price' in columns['where_columns']
        # category is in GROUP BY, which we're not specifically tracking yet

    def test_qualified_column_names(self):
        """Test parsing qualified column names (table.column)"""
        query = "SELECT users.name FROM users WHERE users.email = 'test'"
        parser = QueryParser(query)

        columns = parser.extract_columns()

        # Should extract 'email' from 'users.email'
        assert 'email' in columns['where_columns']

    def test_complex_join_multiple_tables(self):
        """Test parsing complex JOIN with multiple tables"""
        query = """
            SELECT u.name, o.total, p.name
            FROM users u
            JOIN orders o ON u.id = o.user_id
            JOIN products p ON o.product_id = p.id
            WHERE u.active = true AND o.status = 'completed'
        """
        parser = QueryParser(query)

        columns = parser.extract_columns()
        tables = parser.get_tables()

        assert 'id' in columns['join_columns']
        assert 'user_id' in columns['join_columns']
        assert 'product_id' in columns['join_columns']
        assert 'active' in columns['where_columns']
        assert 'status' in columns['where_columns']
        assert len(tables) == 3

"""
Full demonstration of the PostgreSQL Performance Analyser
Shows before/after comparison with index creation
"""
import sys
from pathlib import Path
import time

# Add parent directory to path to allow imports from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db_connector import DatabaseConnector
from src.query_parser import QueryParser
from src.recommender import IndexRecommender


def print_header(title):
    """Print formatted header"""
    print("\n" + "="*80)
    print(f" {title}")
    print("="*80 + "\n")


def demo_single_query_optimization():
    """Demonstrate single query optimization with before/after comparison"""
    print_header("DEMO: Single Query Optimization")

    query = "SELECT * FROM users WHERE email = 'user50000@example.com'"
    print(f"Query: {query}\n")

    connector = DatabaseConnector()

    try:
        # === BEFORE: Without Index ===
        print("[BEFORE] Running query without index...")
        start = time.time()
        explain_before = connector.get_explain_plan(query)
        elapsed_before = (time.time() - start) * 1000

        metrics_before = connector.extract_execution_metrics(explain_before)
        seq_scans_before = connector.detect_sequential_scans(explain_before)

        print(f"\nResults (WITHOUT index):")
        print(f"  Execution Time: {metrics_before['execution_time']:.2f} ms")
        print(f"  Total Cost:     {metrics_before['total_cost']:.2f}")
        print(f"  Sequential Scans: {len(seq_scans_before)}")

        if seq_scans_before:
            print(f"  Rows Scanned:   {seq_scans_before[0]['rows_scanned']:,}")
            print(f"  Rows Filtered:  {seq_scans_before[0]['rows_removed_by_filter']:,}")

        # === Get Recommendation ===
        print("\n[ANALYZING] Generating index recommendation...")
        recommender = IndexRecommender(connector)
        recommendations = recommender.analyze_query(query, explain_before)

        if recommendations:
            rec = recommendations[0]
            print(f"\nRecommendation:")
            print(f"  Table:   {rec.table_name}")
            print(f"  Columns: {', '.join(rec.columns)}")
            print(f"  DDL:     {rec.get_ddl()}")
            print(f"  Expected Improvement: {rec.expected_improvement_pct:.1f}%")

            # === Create Index ===
            print(f"\n[CREATING INDEX] {rec.get_ddl()}")
            with connector.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(rec.get_ddl())
                conn.commit()
            print("  Index created successfully!")

            # === AFTER: With Index ===
            print("\n[AFTER] Running query with index...")
            start = time.time()
            explain_after = connector.get_explain_plan(query)
            elapsed_after = (time.time() - start) * 1000

            metrics_after = connector.extract_execution_metrics(explain_after)
            seq_scans_after = connector.detect_sequential_scans(explain_after)

            print(f"\nResults (WITH index):")
            print(f"  Execution Time: {metrics_after['execution_time']:.2f} ms")
            print(f"  Total Cost:     {metrics_after['total_cost']:.2f}")
            print(f"  Sequential Scans: {len(seq_scans_after)}")
            print(f"  Node Type:      {metrics_after['node_type']}")

            # === Comparison ===
            improvement_time = ((metrics_before['execution_time'] - metrics_after['execution_time']) /
                              metrics_before['execution_time'] * 100)
            improvement_cost = ((metrics_before['total_cost'] - metrics_after['total_cost']) /
                              metrics_before['total_cost'] * 100)

            print_header("IMPROVEMENT SUMMARY")
            print(f"Execution Time:")
            print(f"  Before:      {metrics_before['execution_time']:.2f} ms")
            print(f"  After:       {metrics_after['execution_time']:.2f} ms")
            print(f"  Improvement: {improvement_time:.1f}%")
            print(f"\nQuery Cost:")
            print(f"  Before:      {metrics_before['total_cost']:.2f}")
            print(f"  After:       {metrics_after['total_cost']:.2f}")
            print(f"  Improvement: {improvement_cost:.1f}%")
            print(f"\nSequential Scans:")
            print(f"  Before:      {len(seq_scans_before)}")
            print(f"  After:       {len(seq_scans_after)}")
            print(f"  Eliminated:  {len(seq_scans_before) - len(seq_scans_after)}")

        else:
            print("\nNo recommendations - query is already optimial")

    finally:
        connector.close()


def demo_multiple_queries():
    """Demonstrate batch analysis of multiple queries"""
    print_header("DEMO: Batch Query Analysis")

    queries = [
        "SELECT * FROM users WHERE email LIKE 'user1%'",
        "SELECT * FROM users WHERE age > 50",
        "SELECT * FROM users ORDER BY name LIMIT 10",
        "SELECT * FROM orders WHERE status = 'pending'",
        "SELECT * FROM orders WHERE total > 500",
    ]

    print(f"Analyzing {len(queries)} queries...\n")

    connector = DatabaseConnector()
    recommender = IndexRecommender(connector)

    try:
        all_recommendations = []

        for i, query in enumerate(queries, 1):
            print(f"[{i}/{len(queries)}] {query[:60]}...")

            try:
                explain_output = connector.get_explain_plan(query)
                recs = recommender.analyze_query(query, explain_output)
                all_recommendations.extend(recs)
                print(f"         Found {len(recs)} recommendation(s)")
            except Exception as e:
                print(f"         Error: {e}")

        # Aggregate results
        print(f"\n{'-'*80}")
        print(f"SUMMARY")
        print(f"{'-'*80}")
        print(f"Total Queries Analyzed: {len(queries)}")
        print(f"Total Recommendations:  {len(all_recommendations)}")

        # Group by table
        by_table = {}
        for rec in all_recommendations:
            if rec.table_name not in by_table:
                by_table[rec.table_name] = []
            by_table[rec.table_name].append(rec)

        print(f"\nRecommendations by Table:")
        for table, recs in by_table.items():
            print(f"\n  {table}:")
            for rec in recs:
                print(f"    - {rec.get_ddl()}")
                print(f"      Improvement: {rec.expected_improvement_pct:.1f}%, Priority: {rec.priority}")

    finally:
        connector.close()


def demo_query_parser():
    """Demonstrate query parsing capabilities"""
    print_header("DEMO: Query Parser (AST Analysis)")

    test_queries = [
        ("Simple WHERE", "SELECT * FROM users WHERE email = 'test@example.com'"),
        ("Multiple WHERE", "SELECT * FROM users WHERE email LIKE 'test%' AND age > 18"),
        ("ORDER BY", "SELECT * FROM users ORDER BY created_at DESC, name ASC"),
        ("JOIN", "SELECT u.*, o.id FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'pending'"),
    ]

    for name, query in test_queries:
        print(f"\n{name}:")
        print(f"  Query: {query}")

        parser = QueryParser(query)
        info = parser.get_all_info()

        print(f"  Tables:         {', '.join(info['tables'])}")
        print(f"  WHERE columns:  {', '.join(info['where_columns']) if info['where_columns'] else 'None'}")
        print(f"  ORDER columns:  {', '.join(info['order_by_columns']) if info['order_by_columns'] else 'None'}")
        print(f"  JOIN columns:   {', '.join(info['join_columns']) if info['join_columns'] else 'None'}")


def main():
    """Run all demonstrations"""
    print("\n" + "="*80)
    print(" PostgreSQL Performance Analyzer - COMPLETE DEMONSTRATION")
    print("="*80)

    print("""
This demo will:
1. Show query parser capabilities
2. Demonstrate single query optimization (before/after)
3. Demonstrate batch analysis of multiple queries
    """)

    input("Press Enter to start...")

    # Demo 1: Query Parser
    demo_query_parser()
    input("\n\nPress Enter to continue to optimization demo...")

    # Demo 2: Single Query Optimization
    demo_single_query_optimization()
    input("\n\nPress Enter to continue to batch analysis demo...")

    # Demo 3: Multiple Queries
    demo_multiple_queries()

    print_header("DEMO COMPLETE")
    print("""
Key Takeaways:
- The system successfully detects sequential scans
- Accurately parses SQL to identify indexable columns
- Generates valid CREATE INDEX DDL statements
- Provides cost estimates and improvement percentages
- Can analyze queries in batch mode

Next Steps:
- Run tests: pytest tests/ -v
- Try interactive mode: python analyze_cli.py
- Analyze your own queries!
    """)


if __name__ == "__main__":
    main()

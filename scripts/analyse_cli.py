"""
CLI tool for analysing queries and getting index recommendations.
"""
import sys
from pathlib import Path

# Add parent directory to path to allow imports from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db_connector import DatabaseConnector
from src.query_parser import QueryParser
from src.recommender import IndexRecommender


def analyze_single_query(query: str):
    """Analyse a single query and print recommendations."""
    print("\n" + "="*70)
    print("QUERY ANALYSIS")
    print("="*70)
    print(f"\nQuery:\n  {query}\n")

    # Connect to database
    connector = DatabaseConnector()

    try:
        # Get EXPLAIN plan
        print("[1/4] Executing EXPLAIN ANALYSE...")
        explain_output = connector.get_explain_plan(query)

        # Extract metrics
        print("[2/4] Extracting execution metrics...")
        metrics = connector.extract_execution_metrics(explain_output)

        print(f"\nExecution Metrics:")
        print(f"  Execution Time:  {metrics['execution_time']:.2f} ms")
        print(f"  Planning Time:   {metrics['planning_time']:.2f} ms")
        print(f"  Total Cost:      {metrics['total_cost']:.2f}")
        print(f"  Rows Returned:   {metrics['actual_rows']}")
        print(f"  Node Type:       {metrics['node_type']}")

        # Detect sequential scans
        print("\n[3/4] Detecting sequential scans...")
        seq_scans = connector.detect_sequential_scans(explain_output)

        if seq_scans:
            print(f"\nFound {len(seq_scans)} sequential scan(s):")
            for i, scan in enumerate(seq_scans, 1):
                print(f"\n  Scan #{i}:")
                print(f"    Table:         {scan['table_name']}")
                print(f"    Rows Scanned:  {scan['rows_scanned']:,}")
                print(f"    Scan Time:     {scan['scan_time']:.2f} ms")
                print(f"    Cost:          {scan['total_cost']:.2f}")
                if scan['filter']:
                    print(f"    Filter:        {scan['filter']}")
                if scan['rows_removed_by_filter']:
                    print(f"    Rows Filtered: {scan['rows_removed_by_filter']:,}")
        else:
            print("\n  No sequential scans detected (query is using indexes)")

        # Get index recommendations
        print("\n[4/4] Generating index recommendations...")
        recommender = IndexRecommender(connector)
        recommendations = recommender.analyse_query(query, explain_output)

        if recommendations:
            print(f"\n{'='*70}")
            print(f"INDEX RECOMMENDATIONS ({len(recommendations)} total)")
            print("="*70)

            for i, rec in enumerate(recommendations, 1):
                print(f"\nRecommendation #{i}:")
                print(f"  Table:              {rec.table_name}")
                print(f"  Columns:            {', '.join(rec.columns)}")
                print(f"  Index Type:         {rec.index_type}")
                print(f"  Reason:             {rec.reason}")
                print(f"  Current Cost:       {rec.current_cost:.2f}")
                print(f"  Estimated Cost:     {rec.estimated_cost:.2f}")
                print(f"  Expected Improvement: {rec.expected_improvement_pct:.1f}%")
                print(f"  Priority:           {rec.priority}")
                print(f"\n  DDL:")
                print(f"    {rec.get_ddl()}")
        else:
            print("\n  No index recommendations (query is already optimised)")

        print("\n" + "="*70 + "\n")

    finally:
        connector.close()


def demo_queries():
    """Run analysis on demo queries from test database."""
    queries = [
        ("Email lookup (exact match)",
         "SELECT * FROM users WHERE email = 'user50000@example.com'"),

        ("Email search (LIKE)",
         "SELECT * FROM users WHERE email LIKE 'user1%'"),

        ("ORDER BY without index",
         "SELECT * FROM users ORDER BY created_at DESC LIMIT 10"),

        ("Range query",
         "SELECT * FROM users WHERE id BETWEEN 10000 AND 20000"),

        ("Pattern match",
         "SELECT COUNT(*) FROM users WHERE name LIKE 'User 5%'"),

        ("Status filter on orders",
         "SELECT * FROM orders WHERE status = 'pending'"),

        ("JOIN query",
         """SELECT u.name, o.total
            FROM users u
            JOIN orders o ON u.id = o.user_id
            WHERE o.status = 'completed'
            LIMIT 10"""),
    ]

    print("\n" + "="*70)
    print("DEMO: Analysing Multiple Queries")
    print("="*70)

    for name, query in queries:
        print(f"\n\n{'*'*70}")
        print(f"* {name}")
        print(f"{'*'*70}")
        analyze_single_query(query)
        input("Press Enter to continue to next query...")


def interactive_mode():
    """Interactive mode for analysing custom queries."""
    print("\n" + "="*70)
    print("INTERACTIVE QUERY ANALYSER")
    print("="*70)
    print("\nEnter SQL queries to analyse (type 'quit' to exit)")
    print("Type 'demo' to run demo queries\n")

    while True:
        try:
            print("-" * 70)
            query = input("\nEnter query: ").strip()

            if query.lower() == 'quit':
                print("\nExiting...")
                break

            if query.lower() == 'demo':
                demo_queries()
                continue

            if not query:
                print("Please enter a query")
                continue

            analyze_single_query(query)

        except KeyboardInterrupt:
            print("\n\nExiting...")
            break
        except Exception as e:
            print(f"\nError: {e}")
            print("Please try again or type 'quit' to exit")


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        # Query provided as command line argument
        query = ' '.join(sys.argv[1:])
        analyze_single_query(query)
    else:
        # Interactive mode
        print("\nPostgreSQL Performance Analyzer - Query Analysis Tool\n")
        choice = input("Choose mode:\n  1. Interactive mode\n  2. Run demo queries\n\nEnter choice (1 or 2): ").strip()

        if choice == '2':
            demo_queries()
        else:
            interactive_mode()


if __name__ == "__main__":
    main()

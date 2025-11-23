#!/usr/bin/env python3
import sys
import argparse
import json
from pathlib import Path

# Add parent dir to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db_connector import DatabaseConnector
from src.batch_analyser import BatchAnalyser


def print_progress(current: int, total: int):
    """Print progress bar"""
    bar_width = 40
    progress = current / total
    filled = int(bar_width * progress)
    bar = '=' * filled + '-' * (bar_width - filled)
    print(f'\r[{bar}] {current}/{total} queries analysed', end='', flush=True)


def main():
    parser = argparse.ArgumentParser(
        description='Batch Query Analyser - Analyse multiple queries for index recommendations'
    )

    parser.add_argument(
        '--source',
        choices=['pg_stat_statements', 'file', 'stdin'],
        default='pg_stat_statements',
        help='Source of queries to analyse (default: pg_stat_statements)'
    )

    parser.add_argument(
        '--file',
        type=str,
        help='File containing queries (one per line or JSON array)'
    )

    parser.add_argument(
        '--limit',
        type=int,
        default=200,
        help='Maximum number of queries to analyse (default: 200)'
    )

    parser.add_argument(
        '--min-calls',
        type=int,
        default=10,
        help='Minimum call count for pg_stat_statements queries (default: 10)'
    )

    parser.add_argument(
        '--min-time',
        type=float,
        default=100.0,
        help='Minimum mean execution time in ms (default: 100)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=10,
        help='Number of parallel workers (default: 10)'
    )

    parser.add_argument(
        '--output',
        type=str,
        help='Output file for JSON report (optional)'
    )

    parser.add_argument(
        '--format',
        choices=['summary', 'json', 'both'],
        default='summary',
        help='Output format (default: summary)'
    )

    parser.add_argument(
        '--show-ddl',
        action='store_true',
        help='Show CREATE INDEX statements for all recommendations'
    )

    parser.add_argument(
        '--filter-existing',
        action='store_true',
        help='Filter out recommendations for already-indexed columns'
    )

    parser.add_argument(
        '--table-stats',
        action='store_true',
        help='Show table statistics including write ratios'
    )

    args = parser.parse_args()

    # Connect to database
    print("Connecting to database...")
    try:
        db = DatabaseConnector()
        if not db.test_connection():
            print("ERROR: Failed to connect to database")
            sys.exit(1)
        print("Connected successfully")
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    # Create analyser
    analyser = BatchAnalyser(
        db,
        max_workers=args.workers,
        min_calls=args.min_calls,
        min_mean_time_ms=args.min_time
    )

    # Show table statistics if requested
    if args.table_stats:
        print("\n" + "=" * 60)
        print("TABLE STATISTICS")
        print("=" * 60)

        try:
            stats = analyser.get_table_statistics()
            for s in stats:
                print(f"\n{s['table_name']}:")
                print(f"  Rows: {s['row_count']:,}")
                print(f"  Size: {s['total_size']}")
                print(f"  Sequential scans: {s['seq_scans']:,}")
                print(f"  Index scans: {s['index_scans']:,}")
                print(f"  Write ratio: {s['write_ratio']:.1%}")
        except Exception as e:
            print(f"  Error fetching stats: {e}")

        print()

    # Get queries based on source
    queries = []

    if args.source == 'pg_stat_statements':
        print(f"\nFetching top {args.limit} queries from pg_stat_statements...")
        print(f"  Filters: min_calls >= {args.min_calls}, mean_time >= {args.min_time}ms")

        try:
            query_stats = analyser.get_queries_from_pg_stat_statements(args.limit)
            queries = [qs.query for qs in query_stats]
            print(f"  Found {len(queries)} queries matching criteria")
        except RuntimeError as e:
            print(f"\nERROR: {e}")
            print("\nTo install pg_stat_statements, run:")
            print("  CREATE EXTENSION pg_stat_statements;")
            print("\nAlternatively, use --source=file to analyse queries from a file")
            sys.exit(1)

    elif args.source == 'file':
        if not args.file:
            print("ERROR: --file required when using --source=file")
            sys.exit(1)

        print(f"\nReading queries from {args.file}...")
        try:
            with open(args.file, 'r') as f:
                content = f.read().strip()

            # Try JSON first
            try:
                data = json.loads(content)
                if isinstance(data, list):
                    queries = data
                elif isinstance(data, dict) and 'queries' in data:
                    queries = data['queries']
            except json.JSONDecodeError:
                # Assume one query per line
                queries = [q.strip() for q in content.split('\n') if q.strip()]

            print(f"  Loaded {len(queries)} queries")
        except Exception as e:
            print(f"ERROR: Failed to read file: {e}")
            sys.exit(1)

    elif args.source == 'stdin':
        print("Reading queries from stdin (one per line, Ctrl+D to finish)...")
        queries = [line.strip() for line in sys.stdin if line.strip()]
        print(f"  Read {len(queries)} queries")

    if not queries:
        print("\nNo queries to analyse")
        sys.exit(0)

    # Limit queries
    if len(queries) > args.limit:
        queries = queries[:args.limit]
        print(f"  Limited to {args.limit} queries")

    # Analyse queries
    print(f"\nAnalysing {len(queries)} queries with {args.workers} parallel workers...")
    print()

    report = analyser.analyse_queries(queries, progress_callback=print_progress)

    print("\n")  # New line after progress bar

    # Filter recommendations if requested
    if args.filter_existing and report.top_recommendations:
        from src.recommender import IndexRecommendation

        recs = [
            IndexRecommendation(
                table_name=r['table'],
                columns=r['columns'],
                index_type=r['index_type'],
                reason=r['reason'],
                expected_improvement_pct=r['expected_improvement_pct'],
                current_cost=r['current_cost'],
                estimated_cost=r['estimated_cost'],
                priority=r['priority']
            )
            for r in report.top_recommendations
        ]

        filtered = analyser.filter_recommendations_by_existing_indexes(recs)

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
            for r in filtered
        ]

        report.unique_recommendations = len(filtered)
        print(f"(Filtered to {len(filtered)} recommendations not already indexed)")

    # Output results
    if args.format in ('summary', 'both'):
        print(report.get_summary())

    if args.format in ('json', 'both'):
        print("\n" + "=" * 60)
        print("JSON REPORT")
        print("=" * 60)
        print(report.to_json())

    if args.show_ddl and report.top_recommendations:
        print("\n" + "=" * 60)
        print("CREATE INDEX STATEMENTS")
        print("=" * 60)
        print("-- Copy and paste to apply recommendations:\n")
        for rec in report.top_recommendations:
            print(rec['ddl'])
        print()

    # Save to file if requested
    if args.output:
        with open(args.output, 'w') as f:
            f.write(report.to_json())
        print(f"\nReport saved to {args.output}")

    # Clean up
    db.close()

    # Exit with error code if there were failures
    if report.failed_queries > 0:
        print(f"\nWarning: {report.failed_queries} queries failed to analyse")

    return 0


if __name__ == '__main__':
    sys.exit(main())

# PostgreSQL Query Optimization System

An automated PostgreSQL performance analysis tool. Analyzes query execution plans, identifies performance bottlenecks, suggests optimal indexes based on real database statistics, and detects SQL anti-patterns with rewrite suggestions.

## Operations Pipeline

1. Query Analysis: Parses SQL queries using pglast to build an Abstract Syntax Tree
2. Execution Plan Analysis: Runs EXPLAIN to identify sequential scans and performance issues
3. Statistics Gathering: Queries pg_stats for column cardinality, null fractions, and correlation
4. Selectivity Calculation: Combines EXPLAIN data with pg_stats to estimate index effectiveness
5. Index Recommendation: Suggests optimal indexes with column ordering, partial predicates, and covering columns
6. Cost-Benefit Analysis: Estimates performance improvements and checks for over-indexing
7. Query Rewrite Detection: Scans the AST for SQL anti-patterns and suggests rewrites (no database connection required)

## Directory Structure

```
db-optimisation/
    src/
        __init__.py
        db_connector.py          # Database connection, EXPLAIN execution, statistics queries
        query_parser.py          # SQL parsing with pglast, AST traversal, column mapping
        recommender.py           # Core recommendation engine, selectivity calculation
        batch_analyser.py        # Batch processing from pg_stat_statements
        query_rewriter.py        # Rule-based SQL anti-pattern detection and rewrite suggestions
        cloudwatch_metrics.py    # AWS CloudWatch integration
        api/
            __init__.py
            main.py              # FastAPI REST endpoints
            models.py            # Pydantic response models

    infrastructure/
        cloudformation.yml       # Complete AWS stack definition

    tests/
        test_parser.py           # Parser unit tests
        test_connector.py        # Database connector tests
        test_batch_analyser.py   # Batch analyser tests
        test_query_rewriter.py   # Query rewriter unit tests (no DB required)
        test_api.py              # API endpoint tests

    scripts/
        setup_test_db.py         # Test database generator
        analyse_cli.py           # CLI analysis tool
        batch_analyse.py         # Batch analysis script
        demo.py                  # Demo script

    frontend/
        index.html               # Web interface
        css/
            styles.css           # Frontend styles
        js/
            main.js              # Main application logic
            api.js               # API client
            heatmap.js           # Heatmap visualization
            flamegraph.js        # Flamegraph visualization
            vendor/
                d3.v7.min.js     # D3.js (vendored, no CDN required)

    Dockerfile                   # Application container
    docker-compose.yml           # Local development stack
    nginx.conf                   # Nginx configuration for frontend
    Makefile                     # Docker management commands
    requirements.txt             # Python dependencies
    run_api.py                   # API server entry point
    connect.py                   # Database connection utility
    .env.example                 # Example environment configuration
    .dockerignore                # Docker ignore patterns
    .gitignore                   # Git ignore patterns
    README.md                    # This file
    LICENSE                      # License file
```

## Prerequisites

- Python 3.8 or higher
- PostgreSQL 12, 13, 14, or 15
- PostgreSQL extensions: pg_stat_statements (optional for batch analysis)
- Docker (for containerized deployment)
- AWS CLI (for AWS deployment)

## Installation

### Local Development

1. Clone the repository:
```bash
git clone <repository-url>
cd db-optimisation
```

2. Create a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure database connection:
```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=your_database
export DB_USER=your_user
export DB_PASSWORD=your_password
```

### Docker Deployment

The simplest way to run the entire stack (PostgreSQL + API + Frontend):

1. Copy environment file and edit details:
```bash
cp .env.example .env
```

2. Start all services using Make:
```bash
make up
```

Or using docker-compose directly:
```bash
docker-compose up -d
```

3. Set up test database:
```bash
make setup-test
```

4. Access the application:
   - Frontend: http://localhost
   - API: http://localhost:8000
   - API Docs: http://localhost:8000/docs
   - Database: localhost:5433

Available make commands:
```bash
make help        # Show all available commands
make build       # Build Docker images
make up          # Start all services
make down        # Stop all services
make logs        # View logs
make shell       # Access application shell
make db-shell    # Access PostgreSQL shell
make health      # Check health of all services
make clean       # Stop and remove volumes
```

## Usage

### Interactive Query Analysis

Analyze a single query and get index recommendations:

```python
from src.db_connector import DatabaseConnector
from src.query_parser import QueryParser
from src.recommender import IndexRecommender

# Connect to database
db = DatabaseConnector(
    host='localhost',
    port=5432,
    database='mydb',
    user='postgres',
    password='password'
)

# Analyze query
query = """
SELECT u.username, u.email, o.total
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.status = 'pending' AND o.created_at > '2024-01-01'
ORDER BY o.created_at DESC
LIMIT 100
"""

# Get EXPLAIN plan (analyze=False for safety, no data modification)
plan = db.get_explain_plan(query, analyze=False)

# Generate recommendations
recommender = IndexRecommender(db)
recommendations = recommender.analyse_query(query, plan)

# Display recommendations
for rec in recommendations:
    print(f"Table: {rec.table_name}")
    print(f"DDL:   {rec.get_ddl()}")
    if rec.partial_index_predicate:
        print(f"Partial: WHERE {rec.partial_index_predicate}")
    if rec.include_columns:
        print(f"Covering: INCLUDE ({', '.join(rec.include_columns)})")
    print(f"Estimated Improvement: {rec.expected_improvement_pct:.1f}%")
    print(f"Reason: {rec.reason}")
    if rec.warning:
        print(f"Warning: {rec.warning}")
    print()
```

### Batch Analysis

Analyze all queries from pg_stat_statements:

```python
from src.db_connector import DatabaseConnector
from src.batch_analyser import BatchAnalyser

db = DatabaseConnector()  # reads DB_* environment variables

# Initialize batch analyser
analyser = BatchAnalyser(
    db,
    max_workers=10,
    min_calls=10,          # Only queries executed at least 10 times
    min_mean_time_ms=100,  # Only queries averaging >= 100ms
)

# Analyze the top queries recorded by pg_stat_statements
report = analyser.analyse_from_pg_stat_statements(limit=50)

# Or analyse an explicit list of queries in parallel
# report = analyser.analyse_queries(["SELECT ...", "SELECT ..."])

print(report.get_summary())
for rec in report.top_recommendations:
    print(f"{rec['ddl']}  (+{rec['expected_improvement_pct']:.1f}%)")
```

### REST API

Start the API server (serves the web dashboard at `/` as well):

```bash
python run_api.py            # http://localhost:8000
python run_api.py --reload   # development mode
```

API endpoints:

**1. Health Check**
```bash
curl http://localhost:8000/health
```

**2. Analyze Single Query**
```bash
curl -X POST http://localhost:8000/analyse \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT * FROM users WHERE email = '\''test@example.com'\''",
    "include_explain": true,
    "analyze": false
  }'
```

Set `"analyze": true` to run `EXPLAIN ANALYZE` and get real execution times and
row counts. This actually executes the query, so it is only permitted for
SELECT statements (DML/DDL are refused) and runs under a statement timeout.

Response includes execution metrics, sequential scans, index recommendations
**and** query rewrite suggestions:
```json
{
  "analyzed": false,
  "metrics": {...},
  "sequential_scans": [...],
  "recommendations": [...],
  "query_rewrites": [
    {
      "pattern_name": "select_star",
      "description": "SELECT * retrieves all columns unnecessarily",
      "original_snippet": "SELECT *",
      "suggested_rewrite": "SELECT col1, col2, ...",
      "reason": "SELECT * prevents index-only scans and increases network transfer.",
      "improvement_level": "medium"
    }
  ]
}
```

**3. Batch Analysis**
```bash
curl -X POST http://localhost:8000/batch-analyse \
  -H "Content-Type: application/json" \
  -d '{
    "queries": ["SELECT * FROM users WHERE country = '\''FR'\''"],
    "max_workers": 10,
    "filter_existing": false
  }'
```

**4. Get Table Recommendations & Existing Indexes**
```bash
curl http://localhost:8000/recommendations/users
```

**5. Get Table Statistics**
```bash
curl http://localhost:8000/tables
```

**6. Apply Index Recommendations**
```bash
curl -X POST http://localhost:8000/apply-indexes \
  -H "Content-Type: application/json" \
  -d '{
    "ddl_statements": ["CREATE INDEX idx_users_country ON users (country);"],
    "dry_run": true
  }'
```

Indexes are created with `CREATE INDEX CONCURRENTLY` (no table lock). Only
`CREATE [UNIQUE] INDEX` statements are accepted.

## Query Rewrite Suggestions

The rewrite engine (`src/query_rewriter.py`) scans the parsed AST for seven SQL anti-patterns and returns structured suggestions alongside index recommendations. It requires no database connection and runs on every `/analyse` request.

| Pattern | Trigger | Impact |
|---------|---------|--------|
| `select_star` | `SELECT *` | medium |
| `leading_wildcard_like` | `LIKE '%value'` or `ILIKE '%...'` | high |
| `not_in_subquery` | `col NOT IN (SELECT ...)` | high |
| `function_on_column` | `LOWER(col) = ...` or `DATE(col) = ...` in WHERE | high |
| `implicit_cast` | `col::text = ...` or `CAST(col AS ...)` in WHERE | high |
| `large_offset` | `OFFSET >= 1000` | medium |
| `or_on_same_column` | `col = 'a' OR col = 'b'` | low |

Each suggestion includes the original snippet, a concrete rewrite or advice, and the reason the original pattern hurts performance. High-impact suggestions are sorted first.

Using the rewriter directly (no database needed):

```python
from src.query_rewriter import QueryRewriter

rw = QueryRewriter()
suggestions = rw.analyse("""
    SELECT * FROM users
    WHERE LOWER(email) = 'test@example.com'
    AND id NOT IN (SELECT user_id FROM banned_users)
    LIMIT 20 OFFSET 10000
""")

for s in suggestions:
    print(f"[{s.improvement_level.upper()}] {s.description}")
    print(f"  Original:  {s.original_snippet}")
    print(f"  Suggested: {s.suggested_rewrite}")
    print(f"  Reason:    {s.reason}")
    print()
```

## Testing

Run the test suite:

```bash
# Unit tests (no database required)
pytest tests/test_parser.py
pytest tests/test_query_rewriter.py

# Tests requiring a database connection
pytest tests/test_connector.py
pytest tests/test_batch_analyser.py
pytest tests/test_api.py

# All tests with coverage
pytest --cov=src tests/
```

Set up a test database with sample data:

```bash
python3 scripts/setup_test_db.py

# Or with Docker:
make setup-test
```

## AWS Deployment

Complete AWS deployment with ECS Fargate, RDS, and auto-scaling:

```bash
# Deploy infrastructure
aws cloudformation create-stack \
  --stack-name pg-optimizer \
  --template-body file://infrastructure/cloudformation.yml \
  --parameters \
      ParameterKey=Environment,ParameterValue=production \
      ParameterKey=DBPassword,ParameterValue=your-secure-password \
  --capabilities CAPABILITY_IAM

# Monitor deployment
aws cloudformation describe-stacks --stack-name pg-optimizer

# Get ALB endpoint
aws cloudformation describe-stacks \
  --stack-name pg-optimizer \
  --query 'Stacks[0].Outputs[?OutputKey==`LoadBalancerURL`].OutputValue' \
  --output text
```

## Monitoring

### CloudWatch Metrics

The system publishes the following metrics to CloudWatch:

- `QueryAnalysisCount`: Number of queries analyzed
- `QueryAnalysisLatency`: Time taken to analyze queries
- `RecommendationsGenerated`: Number of index recommendations
- `EstimatedImprovement`: Estimated performance improvement percentage
- `APIRequestCount`: API request count by endpoint
- `APIErrorCount`: API error count by endpoint

### Logs

Logs are available in:
- Local: stdout/stderr
- Docker: `docker logs <container-id>`
- AWS: CloudWatch Logs group `/ecs/pg-optimizer`

## Web Dashboard

The frontend (served at http://localhost via nginx, or http://localhost:8000
directly from the API) provides:

- Single-query analysis with optional `EXPLAIN ANALYZE` for real timings
- Execution plan visualisation (flame graph, coloured by node type)
- Index recommendations with one-click **Copy DDL** and **Apply Index**
  (applied with `CREATE INDEX CONCURRENTLY`)
- SQL anti-pattern rewrite suggestions with severity badges
- Table statistics with an index-usage heatmap
- Batch analysis with aggregated recommendations
- CSV and PDF export for single-query and batch results

## Performance

Improvements depend entirely on your schema, data distribution and workload.
Indicative results from the bundled test database (500K users, 1M orders):
- Filtered order lookup: sequential scan -> partial index, ~95% cost reduction
- Highly selective lookups typically show 90%+ estimated improvement
- Low-selectivity predicates are flagged as unlikely to benefit

## License

This project is licensed under the MIT License.

## Contributing

Contributions are welcome. Please:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Support

For issues, questions, or feature requests, please open an issue on GitHub.

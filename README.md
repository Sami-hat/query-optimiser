# PostgreSQL Query Optimizer

Analyses PostgreSQL queries and tells you what to index. It runs `EXPLAIN`, finds
sequential scans, combines the plan with `pg_stats` cardinality data to estimate how
much an index would actually help, and emits the `CREATE INDEX` DDL — including
partial and covering indexes, with correct column ordering. It also parses the query
AST to flag SQL anti-patterns (no database needed for that part).

Ships with a REST API, a web dashboard, and a CLI.

## Quick start (Docker)

```bash
cp .env.example .env    # set DB_PASSWORD
make up                 # postgres + api + frontend
make setup-test         # optional: seed a 500K-user / 1M-order test database
```

- Dashboard: http://localhost
- API + interactive docs: http://localhost:8000 · http://localhost:8000/docs
- Postgres: `localhost:5433`

`make help` lists the rest (`logs`, `db-shell`, `health`, `clean`, …).

## Quick start (local)

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
export DB_HOST=localhost DB_PORT=5432 DB_NAME=mydb DB_USER=postgres DB_PASSWORD=secret

python run_api.py                                    # API + dashboard on :8000
python scripts/analyse_cli.py "SELECT * FROM users WHERE email = 'a@b.c'"
```

Requires Python 3.8+ and PostgreSQL 12–15. `pg_stat_statements` is only needed for
batch analysis of production queries.

## Dashboard

Query analysis with an execution-plan flame graph, index recommendations with
one-click **Copy DDL** / **Apply Index**, anti-pattern rewrite suggestions, a table
index-usage heatmap, batch analysis, and CSV/PDF export.

## API

Full schema at `/docs`. All endpoints are unauthenticated unless `API_KEYS` is set,
in which case pass `X-API-Key`.

| Endpoint | Purpose |
|---|---|
| `GET /health` | API + database status |
| `POST /analyse` | Analyse one query → metrics, seq scans, index recommendations, rewrites |
| `POST /batch-analyse` | Analyse many queries in parallel → aggregated report |
| `GET /tables` | Table statistics (rows, size, scan counts, write ratio) |
| `GET /recommendations/{table}` | Existing indexes on a table |
| `POST /apply-indexes` | Execute `CREATE INDEX` DDL (`dry_run` supported) |

```bash
curl -X POST http://localhost:8000/analyse \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM orders WHERE status = '\''pending'\''", "analyze": false}'
```

`"analyze": true` runs `EXPLAIN ANALYZE` for real execution times and row counts.
It executes the query, so it is refused for anything but `SELECT` and runs under a
statement timeout. Without it, timings are planner estimates.

`/apply-indexes` rewrites DDL to `CREATE INDEX CONCURRENTLY` (no table lock) and
accepts nothing but `CREATE [UNIQUE] INDEX`.

## Query rewrite suggestions

`src/query_rewriter.py` walks the parsed AST for seven anti-patterns. It needs no
database connection and runs on every `/analyse` call. Results are sorted
high-impact first.

| Pattern | Trigger | Impact |
|---|---|---|
| `leading_wildcard_like` | `LIKE '%value'` | high |
| `not_in_subquery` | `col NOT IN (SELECT ...)` | high |
| `function_on_column` | `LOWER(col) = ...` in WHERE | high |
| `implicit_cast` | `col::text = ...` in WHERE | high |
| `select_star` | `SELECT *` | medium |
| `large_offset` | `OFFSET >= 1000` | medium |
| `or_on_same_column` | `col = 'a' OR col = 'b'` | low |

Usable standalone:

```python
from src.query_rewriter import QueryRewriter

for s in QueryRewriter().analyse("SELECT * FROM users WHERE LOWER(email) = 'a@b.c'"):
    print(f"[{s.improvement_level}] {s.description}\n  -> {s.suggested_rewrite}")
```

## Library use

```python
from src.db_connector import DatabaseConnector
from src.recommender import IndexRecommender

db = DatabaseConnector()  # reads DB_* env vars
query = "SELECT * FROM orders WHERE status = 'pending' ORDER BY created_at DESC"

for rec in IndexRecommender(db).analyse_query(query, db.get_explain_plan(query)):
    print(rec.get_ddl(), f"(+{rec.expected_improvement_pct:.0f}%)", rec.reason)
```

`BatchAnalyser(db).analyse_from_pg_stat_statements(limit=50)` does the same across
your recorded production workload and returns an aggregated report.

## Testing

```bash
pytest                  # full suite
pytest --cov=src tests/ # with coverage
```

Unit tests are mocked and need no database. A handful of integration tests use a
live connection and skip automatically when none is available.

## Layout

```
src/          db_connector · query_parser · recommender · batch_analyser
              query_rewriter · cloudwatch_metrics · api/{main,models}.py
frontend/     dashboard (vanilla JS + vendored D3)
scripts/      setup_test_db · analyse_cli · batch_analyse · demo
infrastructure/cloudformation.yml   ECS Fargate + RDS + CloudWatch stack
```

## AWS deployment

```bash
aws cloudformation create-stack \
  --stack-name pg-optimizer \
  --template-body file://infrastructure/cloudformation.yml \
  --parameters ParameterKey=DBPassword,ParameterValue=<password> \
  --capabilities CAPABILITY_IAM
```

Set `CLOUDWATCH_ENABLED=true` to publish analysis latency, recommendation counts and
API error metrics to CloudWatch. Logs go to `/ecs/pg-optimizer`.

## License

MIT.

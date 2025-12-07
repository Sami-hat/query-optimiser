import os
import time
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, Security, Request
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv

from .models import (
    AnalyseQueryRequest,
    AnalyseQueryResponse,
    BatchAnalyseRequest,
    BatchAnalyseResponse,
    TableRecommendationsResponse,
    ApplyIndexesRequest,
    ApplyIndexesResponse,
    ApplyIndexResult,
    HealthResponse,
    ErrorResponse,
    ExecutionMetrics,
    SequentialScanInfo,
    IndexRecommendationResponse,
    TableStatistics,
)
from ..db_connector import DatabaseConnector
from ..recommender import IndexRecommender
from ..batch_analyser import BatchAnalyser

load_dotenv()

# Global instances
db_connector: Optional[DatabaseConnector] = None
recommender: Optional[IndexRecommender] = None
batch_analyser: Optional[BatchAnalyser] = None

# API key security
API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

# Rate limiting storage (simple in-memory)
rate_limit_store: dict = {}
RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "100"))
RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "3600"))  # 1 hour


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    global db_connector, recommender, batch_analyser

    # Startup
    try:
        db_connector = DatabaseConnector()
        recommender = IndexRecommender(db_connector)
        batch_analyser = BatchAnalyser(db_connector)
        print("Database connection established")
    except Exception as e:
        print(f"Warning: Could not connect to database: {e}")

    yield

    # Shutdown
    if db_connector:
        db_connector.close()
        print("Database connection closed")


# Create FastAPI app
app = FastAPI(
    title="PostgreSQL Performance Analyser API",
    description="Analyse queries, detect performance issues, and get index recommendations",
    version="1.0.0",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files for frontend
frontend_path = Path(__file__).parent.parent.parent / "frontend"
if frontend_path.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_path)), name="static")


@app.get("/", include_in_schema=False)
async def serve_frontend():
    """Serve the frontend dashboard"""
    index_path = frontend_path / "index.html"
    if index_path.exists():
        return FileResponse(str(index_path))
    return {"message": "PostgreSQL Performance Analyser API", "docs": "/docs"}


def get_api_keys() -> list:
    """Get valid API keys from environment"""
    keys_str = os.getenv("API_KEYS", "")
    if not keys_str:
        return []
    return [k.strip() for k in keys_str.split(",") if k.strip()]


async def verify_api_key(api_key: str = Security(api_key_header)) -> str:
    """Verify API key if authentication is enabled"""
    valid_keys = get_api_keys()

    # If no keys configured, allow all requests
    if not valid_keys:
        return "anonymous"

    if not api_key:
        raise HTTPException(
            status_code=401,
            detail="API key required. Provide X-API-Key header."
        )

    if api_key not in valid_keys:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key"
        )

    return api_key


async def check_rate_limit(request: Request, api_key: str = Depends(verify_api_key)):
    """Check rate limit for the API key"""
    if not get_api_keys():
        return  # Rate limiting disabled if no auth

    current_time = time.time()
    key = f"ratelimit:{api_key}"

    if key not in rate_limit_store:
        rate_limit_store[key] = {"count": 0, "reset_time": current_time + RATE_LIMIT_WINDOW}

    entry = rate_limit_store[key]

    # Reset if window expired
    if current_time > entry["reset_time"]:
        entry["count"] = 0
        entry["reset_time"] = current_time + RATE_LIMIT_WINDOW

    # Check limit
    if entry["count"] >= RATE_LIMIT_REQUESTS:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded. Try again after {int(entry['reset_time'] - current_time)} seconds."
        )

    entry["count"] += 1


def require_db():
    """Dependency to ensure database is connected"""
    if not db_connector or not db_connector.test_connection():
        raise HTTPException(
            status_code=503,
            detail="Database connection not available"
        )
    return db_connector


# Health check endpoint (no auth required)
@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Check API and database health"""
    db_connected = False
    if db_connector:
        try:
            db_connected = db_connector.test_connection()
        except Exception:
            pass

    return HealthResponse(
        status="healthy" if db_connected else "degraded",
        database_connected=db_connected,
        version="1.0.0"
    )


@app.post(
    "/analyse",
    response_model=AnalyseQueryResponse,
    responses={400: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
    tags=["Analysis"],
    dependencies=[Depends(check_rate_limit)]
)
async def analyse_query(
    request: AnalyseQueryRequest,
    db: DatabaseConnector = Depends(require_db)
):
    """
    Analyse a single SQL query.

    Returns EXPLAIN plan analysis, sequential scan detection,
    and index recommendations.
    """
    try:
        # Get EXPLAIN plan
        explain_output = db.get_explain_plan(request.query)

        # Extract metrics
        metrics = db.extract_execution_metrics(explain_output)

        # Detect sequential scans
        seq_scans = db.detect_sequential_scans(explain_output)

        # Get recommendations
        recommendations = recommender.analyse_query(request.query, explain_output)

        # Build response
        return AnalyseQueryResponse(
            query=request.query,
            metrics=ExecutionMetrics(
                execution_time_ms=metrics.get('execution_time', 0),
                planning_time_ms=metrics.get('planning_time', 0),
                total_cost=metrics.get('total_cost', 0),
                actual_rows=metrics.get('actual_rows', 0),
                node_type=metrics.get('node_type', 'Unknown')
            ),
            sequential_scans=[
                SequentialScanInfo(
                    table_name=scan['table_name'],
                    rows_scanned=scan.get('rows_scanned', 0),
                    scan_time=scan.get('scan_time', 0),
                    total_cost=scan.get('total_cost', 0),
                    filter=scan.get('filter'),
                    rows_removed_by_filter=scan.get('rows_removed_by_filter', 0)
                )
                for scan in seq_scans
            ],
            recommendations=[
                IndexRecommendationResponse(
                    table=rec.table_name,
                    columns=rec.columns,
                    index_type=rec.index_type,
                    reason=rec.reason,
                    expected_improvement_pct=rec.expected_improvement_pct,
                    current_cost=rec.current_cost,
                    estimated_cost=rec.estimated_cost,
                    priority=rec.priority,
                    ddl=rec.get_ddl(),
                    warning=rec.warning,
                    partial_index_predicate=rec.partial_index_predicate,
                    include_columns=rec.include_columns
                )
                for rec in recommendations
            ],
            explain_plan=explain_output if request.include_explain else None
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@app.post(
    "/batch-analyse",
    response_model=BatchAnalyseResponse,
    responses={400: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
    tags=["Analysis"],
    dependencies=[Depends(check_rate_limit)]
)
async def batch_analyse(
    request: BatchAnalyseRequest,
    db: DatabaseConnector = Depends(require_db)
):
    """
    Analyse multiple SQL queries in batch

    Processes queries in parallel and returns aggregated recommendations
    """
    try:
        # Create analyser with specified workers
        analyser = BatchAnalyser(db, max_workers=request.max_workers)

        # Run analysis
        report = analyser.analyse_queries(request.queries)

        # Filter if requested
        if request.filter_existing and report.top_recommendations:
            from ..recommender import IndexRecommendation as IR
            recs = [
                IR(
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
            top_recs = [
                IndexRecommendationResponse(
                    table=r.table_name,
                    columns=r.columns,
                    index_type=r.index_type,
                    reason=r.reason,
                    expected_improvement_pct=r.expected_improvement_pct,
                    current_cost=r.current_cost,
                    estimated_cost=r.estimated_cost,
                    priority=r.priority,
                    ddl=r.get_ddl(),
                    warning=r.warning,
                    partial_index_predicate=r.partial_index_predicate,
                    include_columns=r.include_columns
                )
                for r in filtered
            ]
        else:
            top_recs = [
                IndexRecommendationResponse(**r)
                for r in report.top_recommendations
            ]

        return BatchAnalyseResponse(
            timestamp=report.timestamp,
            total_queries=report.total_queries,
            analysed_queries=report.analysed_queries,
            failed_queries=report.failed_queries,
            total_seq_scans=report.total_seq_scans,
            seq_scans_with_recommendations=report.seq_scans_with_recommendations,
            unique_recommendations=len(top_recs),
            tables_affected=report.tables_affected,
            total_current_cost=report.total_current_cost,
            total_estimated_cost=report.total_estimated_cost,
            estimated_improvement_pct=report.estimated_improvement_pct,
            top_recommendations=top_recs,
            analysis_duration_seconds=report.analysis_duration_seconds
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Batch analysis failed: {str(e)}")


@app.get(
    "/recommendations/{table_name}",
    response_model=TableRecommendationsResponse,
    responses={404: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
    tags=["Recommendations"],
    dependencies=[Depends(check_rate_limit)]
)
async def get_table_recommendations(
    table_name: str,
    db: DatabaseConnector = Depends(require_db)
):
    """
    Get index recommendations for a specific table

    Also returns existing indexes on the table
    """
    try:
        analyser = BatchAnalyser(db)

        # Get existing indexes
        existing = analyser.get_existing_indexes(table_name)

        if not existing:
            # Check if table exists
            stats = analyser.get_table_statistics()
            table_exists = any(s['table_name'] == table_name for s in stats)
            if not table_exists:
                raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

        # For now, return existing indexes and empty recommendations
        # (recommendations require query analysis)
        return TableRecommendationsResponse(
            table_name=table_name,
            recommendations=[],
            existing_indexes=existing
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/tables",
    response_model=list[TableStatistics],
    responses={503: {"model": ErrorResponse}},
    tags=["Statistics"],
    dependencies=[Depends(check_rate_limit)]
)
async def get_table_statistics(
    db: DatabaseConnector = Depends(require_db)
):
    """Get statistics for all tables in the database"""
    try:
        analyser = BatchAnalyser(db)
        stats = analyser.get_table_statistics()

        return [
            TableStatistics(
                table_name=s['table_name'],
                row_count=s['row_count'] or 0,
                dead_rows=s['dead_rows'] or 0,
                total_size=s['total_size'],
                seq_scans=s['seq_scans'] or 0,
                index_scans=s['index_scans'] or 0,
                write_ratio=s['write_ratio']
            )
            for s in stats
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post(
    "/apply-indexes",
    response_model=ApplyIndexesResponse,
    responses={400: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
    tags=["Indexes"],
    dependencies=[Depends(check_rate_limit)]
)
async def apply_indexes(
    request: ApplyIndexesRequest,
    db: DatabaseConnector = Depends(require_db)
):
    """
    Apply index recommendations by executing CREATE INDEX statements

    Use dry_run=true to validate without executing
    """
    results = []

    for ddl in request.ddl_statements:
        result = ApplyIndexResult(ddl=ddl, success=False)

        # Validate DDL
        if not ddl.strip().upper().startswith("CREATE INDEX"):
            result.error = "Only CREATE INDEX statements are allowed"
            results.append(result)
            continue

        if request.dry_run:
            result.success = True
            result.error = "Dry run - not executed"
            results.append(result)
            continue

        # Execute DDL
        try:
            start_time = time.time()
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(ddl)
                conn.commit()
            result.success = True
            result.execution_time_ms = (time.time() - start_time) * 1000

        except Exception as e:
            result.error = str(e)

        results.append(result)

    successful = sum(1 for r in results if r.success)
    failed = len(results) - successful

    return ApplyIndexesResponse(
        results=results,
        successful=successful,
        failed=failed
    )


# Exception handler for validation errors
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle uncaught exceptions"""
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)}
    )

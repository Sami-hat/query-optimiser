# Pydantic models for API request/response validation
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


# Request Models

class AnalyseQueryRequest(BaseModel):
    """Request model for single query analysis"""
    query: str = Field(..., min_length=1, description="SQL query to analyse")
    include_explain: bool = Field(True, description="Include full EXPLAIN plan in response")


class BatchAnalyseRequest(BaseModel):
    """Request model for batch query analysis"""
    queries: List[str] = Field(..., min_length=1, description="List of SQL queries to analyse")
    max_workers: int = Field(10, ge=1, le=20, description="Number of parallel workers")
    filter_existing: bool = Field(False, description="Filter out already-indexed columns")


class ApplyIndexesRequest(BaseModel):
    """Request model for applying index recommendations"""
    ddl_statements: List[str] = Field(..., min_length=1, description="CREATE INDEX statements to execute")
    dry_run: bool = Field(False, description="If true, validate but don't execute")


# Response Models

class SequentialScanInfo(BaseModel):
    """Sequential scan information"""
    table_name: str
    rows_scanned: int
    scan_time: float
    total_cost: float
    filter: Optional[str] = None
    rows_removed_by_filter: int = 0


class IndexRecommendationResponse(BaseModel):
    """Single index recommendation"""
    table: str
    columns: List[str]
    index_type: str = "btree"
    reason: str
    expected_improvement_pct: float
    current_cost: float
    estimated_cost: float
    priority: int
    ddl: str
    warning: str = ""  # Over-indexing or other warnings
    partial_index_predicate: str = ""  # WHERE clause for partial indexes
    include_columns: List[str] = []  # INCLUDE columns for covering indexes


class ExecutionMetrics(BaseModel):
    """Query execution metrics"""
    execution_time_ms: float
    planning_time_ms: float
    total_cost: float
    actual_rows: int
    node_type: str


class AnalyseQueryResponse(BaseModel):
    """Response model for single query analysis"""
    query: str
    metrics: ExecutionMetrics
    sequential_scans: List[SequentialScanInfo]
    recommendations: List[IndexRecommendationResponse]
    explain_plan: Optional[Dict[str, Any]] = None


class BatchAnalyseResponse(BaseModel):
    """Response model for batch analysis"""
    timestamp: str
    total_queries: int
    analysed_queries: int
    failed_queries: int
    total_seq_scans: int
    seq_scans_with_recommendations: int
    unique_recommendations: int
    tables_affected: List[str]
    total_current_cost: float
    total_estimated_cost: float
    estimated_improvement_pct: float
    top_recommendations: List[IndexRecommendationResponse]
    analysis_duration_seconds: float


class TableRecommendationsResponse(BaseModel):
    """Response model for table-specific recommendations"""
    table_name: str
    recommendations: List[IndexRecommendationResponse]
    existing_indexes: List[Dict[str, Any]]


class TableStatistics(BaseModel):
    """Table statistics"""
    table_name: str
    row_count: int
    dead_rows: int
    total_size: str
    seq_scans: int
    index_scans: int
    write_ratio: float


class ApplyIndexResult(BaseModel):
    """Result of applying a single index"""
    ddl: str
    success: bool
    error: Optional[str] = None
    execution_time_ms: Optional[float] = None


class ApplyIndexesResponse(BaseModel):
    """Response model for applying indexes"""
    results: List[ApplyIndexResult]
    successful: int
    failed: int


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    database_connected: bool
    version: str


class ErrorResponse(BaseModel):
    """Standard error response"""
    error: str
    detail: Optional[str] = None

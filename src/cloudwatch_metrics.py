"""
CloudWatch Metrics Publisher

Publishes application metrics to AWS CloudWatch for monitoring and alerting.
"""
import os
import time
from typing import Dict, List, Optional, Any
from datetime import datetime
import boto3
from botocore.exceptions import ClientError


class CloudWatchMetrics:
    """
    Publishes custom metrics to AWS CloudWatch

    Tracks:
    - Query analysis performance
    - Index recommendation metrics
    - Before/after query execution times
    - Sequential scan detection rates
    """

    def __init__(
        self,
        namespace: Optional[str] = None,
        region: Optional[str] = None,
        enabled: bool = True
    ):
        """
        Initialize CloudWatch metrics publisher

        Args:
            namespace: CloudWatch namespace (default: from CLOUDWATCH_NAMESPACE env var)
            region: AWS region (default: from AWS_REGION env var)
            enabled: Enable metrics publishing (default: True, set False for local dev)
        """
        self.namespace = namespace or os.getenv('CLOUDWATCH_NAMESPACE', 'PerformanceAnalyser')
        self.region = region or os.getenv('AWS_REGION', 'us-east-1')
        self.enabled = enabled and os.getenv('ENVIRONMENT') in ['production', 'staging']

        if self.enabled:
            try:
                self.client = boto3.client('cloudwatch', region_name=self.region)
            except Exception as e:
                print(f"Warning: Failed to initialize CloudWatch client: {e}")
                self.enabled = False
        else:
            self.client = None

    def put_metric(
        self,
        metric_name: str,
        value: float,
        unit: str = 'None',
        dimensions: Optional[Dict[str, str]] = None,
        timestamp: Optional[datetime] = None
    ) -> bool:
        """
        Publish a single metric to CloudWatch

        Args:
            metric_name: Name of the metric
            value: Metric value
            unit: CloudWatch unit (e.g., 'Milliseconds', 'Count', 'Percent')
            dimensions: Optional dimensions for filtering
            timestamp: Optional timestamp (defaults to now)

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled:
            return False

        try:
            metric_data = {
                'MetricName': metric_name,
                'Value': value,
                'Unit': unit,
                'Timestamp': timestamp or datetime.utcnow()
            }

            if dimensions:
                metric_data['Dimensions'] = [
                    {'Name': k, 'Value': v} for k, v in dimensions.items()
                ]

            self.client.put_metric_data(
                Namespace=self.namespace,
                MetricData=[metric_data]
            )
            return True

        except ClientError as e:
            print(f"Error publishing metric {metric_name}: {e}")
            return False

    def put_metrics(self, metrics: List[Dict[str, Any]]) -> bool:
        """
        Publish multiple metrics in a single call (more efficient)

        Args:
            metrics: List of metric dictionaries with keys:
                - MetricName (required)
                - Value (required)
                - Unit (optional)
                - Dimensions (optional)
                - Timestamp (optional)

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled or not metrics:
            return False

        try:
            # CloudWatch allows max 20 metrics per call
            batch_size = 20
            for i in range(0, len(metrics), batch_size):
                batch = metrics[i:i + batch_size]

                # Ensure all metrics have timestamps
                for metric in batch:
                    if 'Timestamp' not in metric:
                        metric['Timestamp'] = datetime.now(datetime.timezone.utc)

                    # Convert dimensions dict to CloudWatch format
                    if 'Dimensions' in metric and isinstance(metric['Dimensions'], dict):
                        metric['Dimensions'] = [
                            {'Name': k, 'Value': v}
                            for k, v in metric['Dimensions'].items()
                        ]

                self.client.put_metric_data(
                    Namespace=self.namespace,
                    MetricData=batch
                )

            return True

        except ClientError as e:
            print(f"Error publishing metrics batch: {e}")
            return False

    def record_query_analysis(
        self,
        execution_time_ms: float,
        planning_time_ms: float,
        total_cost: float,
        seq_scans_found: int,
        recommendations_generated: int
    ) -> bool:
        """
        Record metrics for a query analysis operation

        Args:
            execution_time_ms: Query execution time
            planning_time_ms: Query planning time
            total_cost: PostgreSQL cost estimate
            seq_scans_found: Number of sequential scans detected
            recommendations_generated: Number of index recommendations

        Returns:
            True if successful
        """
        metrics = [
            {
                'MetricName': 'QueryExecutionTime',
                'Value': execution_time_ms,
                'Unit': 'Milliseconds'
            },
            {
                'MetricName': 'QueryPlanningTime',
                'Value': planning_time_ms,
                'Unit': 'Milliseconds'
            },
            {
                'MetricName': 'QueryCost',
                'Value': total_cost,
                'Unit': 'None'
            },
            {
                'MetricName': 'SequentialScansDetected',
                'Value': seq_scans_found,
                'Unit': 'Count'
            },
            {
                'MetricName': 'RecommendationsGenerated',
                'Value': recommendations_generated,
                'Unit': 'Count'
            }
        ]

        return self.put_metrics(metrics)

    def record_batch_analysis(
        self,
        total_queries: int,
        analysed_queries: int,
        failed_queries: int,
        duration_seconds: float,
        total_recommendations: int,
        estimated_improvement_pct: float
    ) -> bool:
        """
        Record metrics for a batch analysis operation

        Args:
            total_queries: Total queries in batch
            analysed_queries: Successfully analysed queries
            failed_queries: Failed query analyses
            duration_seconds: Total batch analysis duration
            total_recommendations: Total index recommendations
            estimated_improvement_pct: Estimated performance improvement

        Returns:
            True if successful
        """
        metrics = [
            {
                'MetricName': 'BatchAnalysisQueries',
                'Value': total_queries,
                'Unit': 'Count'
            },
            {
                'MetricName': 'BatchAnalysisSuccess',
                'Value': analysed_queries,
                'Unit': 'Count'
            },
            {
                'MetricName': 'BatchAnalysisFailed',
                'Value': failed_queries,
                'Unit': 'Count'
            },
            {
                'MetricName': 'BatchAnalysisDuration',
                'Value': duration_seconds,
                'Unit': 'Seconds'
            },
            {
                'MetricName': 'BatchRecommendations',
                'Value': total_recommendations,
                'Unit': 'Count'
            },
            {
                'MetricName': 'EstimatedImprovement',
                'Value': estimated_improvement_pct,
                'Unit': 'Percent'
            },
            {
                'MetricName': 'BatchSuccessRate',
                'Value': (analysed_queries / total_queries * 100) if total_queries > 0 else 0,
                'Unit': 'Percent'
            }
        ]

        return self.put_metrics(metrics)

    def record_index_application(
        self,
        indexes_created: int,
        indexes_failed: int,
        total_creation_time_ms: float
    ) -> bool:
        """
        Record metrics for index creation operations

        Args:
            indexes_created: Number of indexes successfully created
            indexes_failed: Number of indexes that failed to create
            total_creation_time_ms: Total time for all index creations

        Returns:
            True if successful
        """
        metrics = [
            {
                'MetricName': 'IndexesCreated',
                'Value': indexes_created,
                'Unit': 'Count'
            },
            {
                'MetricName': 'IndexesFailed',
                'Value': indexes_failed,
                'Unit': 'Count'
            },
            {
                'MetricName': 'IndexCreationTime',
                'Value': total_creation_time_ms,
                'Unit': 'Milliseconds'
            }
        ]

        return self.put_metrics(metrics)

    def record_performance_improvement(
        self,
        query_id: str,
        before_time_ms: float,
        after_time_ms: float,
        improvement_pct: float
    ) -> bool:
        """
        Record before/after performance metrics for a query

        Args:
            query_id: Unique identifier for the query
            before_time_ms: Execution time before index
            after_time_ms: Execution time after index
            improvement_pct: Percentage improvement

        Returns:
            True if successful
        """
        metrics = [
            {
                'MetricName': 'QueryPerformanceBefore',
                'Value': before_time_ms,
                'Unit': 'Milliseconds',
                'Dimensions': {'QueryID': query_id}
            },
            {
                'MetricName': 'QueryPerformanceAfter',
                'Value': after_time_ms,
                'Unit': 'Milliseconds',
                'Dimensions': {'QueryID': query_id}
            },
            {
                'MetricName': 'PerformanceImprovement',
                'Value': improvement_pct,
                'Unit': 'Percent',
                'Dimensions': {'QueryID': query_id}
            }
        ]

        return self.put_metrics(metrics)

    def record_api_request(
        self,
        endpoint: str,
        status_code: int,
        response_time_ms: float
    ) -> bool:
        """
        Record API request metrics

        Args:
            endpoint: API endpoint path
            status_code: HTTP status code
            response_time_ms: Response time in milliseconds

        Returns:
            True if successful
        """
        metrics = [
            {
                'MetricName': 'APIRequests',
                'Value': 1,
                'Unit': 'Count',
                'Dimensions': {
                    'Endpoint': endpoint,
                    'StatusCode': str(status_code)
                }
            },
            {
                'MetricName': 'APIResponseTime',
                'Value': response_time_ms,
                'Unit': 'Milliseconds',
                'Dimensions': {'Endpoint': endpoint}
            }
        ]

        # Record success/error separately
        if 200 <= status_code < 300:
            metrics.append({
                'MetricName': 'APISuccess',
                'Value': 1,
                'Unit': 'Count',
                'Dimensions': {'Endpoint': endpoint}
            })
        else:
            metrics.append({
                'MetricName': 'APIError',
                'Value': 1,
                'Unit': 'Count',
                'Dimensions': {
                    'Endpoint': endpoint,
                    'StatusCode': str(status_code)
                }
            })

        return self.put_metrics(metrics)


# Global instance (lazily initialized)
_cloudwatch_metrics: Optional[CloudWatchMetrics] = None


def get_cloudwatch_metrics() -> CloudWatchMetrics:
    """
    Get global CloudWatchMetrics instance (singleton pattern)

    Returns:
        CloudWatchMetrics instance
    """
    global _cloudwatch_metrics
    if _cloudwatch_metrics is None:
        _cloudwatch_metrics = CloudWatchMetrics()
    return _cloudwatch_metrics

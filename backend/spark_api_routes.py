"""
Spark API Routes - Flask routes for Spark-based data operations
"""

from flask import Blueprint, request, jsonify
from functools import wraps
import logging
import time
import traceback
from typing import Dict, Any, Optional
from s3_spark_connector import S3SparkConnector
from query_complexity_analyzer import QueryComplexityAnalyzer
from iso_standards import ISOFormatter, ISOStandards, ISOValidator, ISOConstants

# Create blueprint for Spark routes
spark_bp = Blueprint('spark', __name__, url_prefix='/api/spark')
logger = logging.getLogger(__name__)

# Global connector instance
spark_connector: Optional[S3SparkConnector] = None

def get_spark_connector() -> S3SparkConnector:
    """Get or initialize the Spark connector"""
    global spark_connector
    if spark_connector is None:
        spark_connector = S3SparkConnector(
            app_name="DB-Spark-API",
            master_url=None,  # Will use environment variable or default to local
            s3_bucket=None    # Will use environment variable
        )
    return spark_connector

def handle_spark_errors(f):
    """Decorator to handle Spark-related errors consistently with ISO formatting"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        start_time = time.time()
        try:
            result = f(*args, **kwargs)
            processing_time = (time.time() - start_time) * 1000
            
            # Ensure result is ISO-formatted
            if isinstance(result, tuple) and len(result) == 2:
                response_data, status_code = result
                if isinstance(response_data, dict) and 'data' in response_data:
                    # Already ISO-formatted
                    if 'meta' in response_data:
                        response_data['meta']['processing_time_ms'] = round(processing_time, 2)
                    return response_data, status_code
                else:
                    # Format as ISO response
                    iso_response = ISOFormatter.format_response(response_data)
                    iso_response['meta']['processing_time_ms'] = round(processing_time, 2)
                    return iso_response, status_code
            
            return result
            
        except Exception as e:
            processing_time = (time.time() - start_time) * 1000
            logger.error(f"Spark operation failed: {str(e)}", extra={
                'operation': f.__name__,
                'processing_time': processing_time,
                'error_type': type(e).__name__
            })
            
            # Return ISO-formatted error
            error_response = ISOFormatter.format_error(
                error_code=ISOConstants.ERROR_CODES['SPARK_SESSION_ERROR'],
                message=f"Spark operation failed: {str(e)}",
                details={
                    'operation': f.__name__,
                    'processing_time_ms': round(processing_time, 2),
                    'error_type': type(e).__name__
                }
            )
            
            return error_response, 500
    
    return decorated_function

@spark_bp.route('/health', methods=['GET'])
@handle_spark_errors
def spark_health_check():
    """Check Spark cluster health and connectivity"""
    connector = get_spark_connector()
    
    try:
        spark = connector.get_spark_session()
        
        # Test basic Spark functionality
        test_df = spark.range(10)
        count = test_df.count()
        
        # Get Spark context info
        sc = spark.sparkContext
        
        health_data = {
            'status': 'healthy',
            'spark_version': spark.version,
            'master': sc.master,
            'app_name': sc.appName,
            'default_parallelism': sc.defaultParallelism,
            'test_count': count,
            's3_bucket': connector.s3_bucket,
            'connection_test_passed': count == 10
        }
        
        logger.info("Spark health check passed", extra={'operation': 'health_check'})
        
        # Return ISO-formatted response
        return ISOFormatter.format_response(health_data), 200
        
    except Exception as e:
        logger.error(f"Spark health check failed: {e}")
        raise

@spark_bp.route('/s3/list', methods=['GET'])
@handle_spark_errors
def list_s3_objects():
    """List objects in the S3 bucket"""
    prefix = request.args.get('prefix', '')
    page = request.args.get('page', 1, type=int)
    per_page = min(request.args.get('per_page', ISOConstants.DEFAULT_PAGE_SIZE, type=int), ISOConstants.MAX_PAGE_SIZE)
    
    connector = get_spark_connector()
    
    try:
        all_objects = connector.list_s3_objects(prefix=prefix)
        
        # Apply pagination
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_objects = all_objects[start_idx:end_idx]
        
        # Format objects with ISO standards
        formatted_objects = []
        for obj in paginated_objects:
            formatted_obj = {
                'key': obj['key'],
                'size_bytes': obj['size'],
                'size_formatted': ISOStandards.format_file_size(obj['size']),
                'last_modified': ISOStandards.format_iso_datetime(obj['last_modified']),
                'etag': obj['etag']
            }
            formatted_objects.append(formatted_obj)
        
        # Return ISO-formatted paginated response
        return ISOFormatter.format_pagination(
            data=formatted_objects,
            page=page,
            per_page=per_page,
            total=len(all_objects)
        ), 200
        
    except Exception as e:
        logger.error(f"Failed to list S3 objects: {e}")
        raise

@spark_bp.route('/table/info', methods=['GET'])
@handle_spark_errors
def get_table_info():
    """Get information about a table stored in S3"""
    s3_path = request.args.get('path')
    
    if not s3_path:
        error_response = ISOFormatter.format_error(
            error_code=ISOConstants.ERROR_CODES['VALIDATION_ERROR'],
            message="Missing required parameter: path"
        )
        return error_response, 400
    
    # Validate table path format
    if not ISOValidator.validate_table_name(s3_path.split('/')[-1].replace('.json', '')):
        error_response = ISOFormatter.format_error(
            error_code=ISOConstants.ERROR_CODES['VALIDATION_ERROR'],
            message="Invalid table path format"
        )
        return error_response, 400
    
    connector = get_spark_connector()
    
    try:
        info = connector.get_table_info(s3_path)
        
        # Format column information with ISO standards
        formatted_columns = []
        for col_name in info['columns']:
            formatted_columns.append({
                'name': col_name,
                'name_valid': ISOValidator.validate_column_name(col_name),
                'type': 'string'  # Default type, can be enhanced
            })
        
        # Use ISO table metadata formatter
        table_metadata = ISOFormatter.format_table_metadata(
            table_name=s3_path.split('/')[-1],
            columns=formatted_columns,
            row_count=info['row_count']
        )
        
        # Add additional metadata
        table_metadata['table']['s3_path'] = s3_path
        table_metadata['table']['sample_data'] = info.get('sample_data', [])
        
        logger.info(f"Retrieved table info for {s3_path}", extra={
            'operation': 'table_info',
            'row_count': info['row_count'],
            'column_count': info['column_count']
        })
        
        return table_metadata, 200
        
    except Exception as e:
        logger.error(f"Failed to get table info for {s3_path}: {e}")
        raise

@spark_bp.route('/query/execute', methods=['POST'])
@handle_spark_errors
def execute_sql_query():
    """Execute SQL query on S3 data using Spark"""
    data = request.get_json()
    
    if not data:
        error_response = ISOFormatter.format_error(
            error_code=ISOConstants.ERROR_CODES['VALIDATION_ERROR'],
            message="Request body must be JSON"
        )
        return error_response, 400
    
    query = data.get('query')
    s3_path = data.get('s3_path')
    limit = min(data.get('limit', ISOConstants.DEFAULT_QUERY_LIMIT), ISOConstants.MAX_QUERY_LIMIT)
    
    if not query or not s3_path:
        error_response = ISOFormatter.format_error(
            error_code=ISOConstants.ERROR_CODES['VALIDATION_ERROR'],
            message="Missing required parameters: query, s3_path"
        )
        return error_response, 400
    
    # Normalize query string
    query = ISOStandards.normalize_string(query)
    
    # Analyze query complexity
    try:
        analyzer = QueryComplexityAnalyzer()
        complexity = analyzer.analyze_query(query)
        
        if complexity['risk_level'] == 'HIGH':
            logger.warning(f"High-risk query detected", extra={
                'operation': 'query_execute',
                'risk_level': complexity['risk_level'],
                'query': query[:100]  # Log first 100 chars
            })
            
            # Optionally reject high-risk queries based on deployment mode
            # Can be configured in deployment settings
        
    except Exception as e:
        logger.warning(f"Query complexity analysis failed: {e}")
        complexity = {'risk_level': 'UNKNOWN'}
    
    connector = get_spark_connector()
    
    try:
        start_time = time.time()
        
        # Read data from S3
        df = connector.read_json_from_s3(s3_path)
        
        # Execute query
        result_df = connector.execute_sql_query(df, query, temp_view_name="main_table")
        
        # Collect results with limit
        limited_df = result_df.limit(limit)
        results = limited_df.collect()
        
        execution_time = (time.time() - start_time) * 1000
        
        # Convert results to JSON-serializable format
        result_data = []
        for row in results:
            result_data.append(row.asDict())
        
        # Use ISO query result formatter
        query_result = ISOFormatter.format_query_result(
            query=query,
            results=result_data,
            execution_time_ms=execution_time,
            row_count=len(result_data)
        )
        
        # Add additional metadata
        query_result['query'].update({
            's3_path': s3_path,
            'complexity': complexity,
            'limit_applied': limit,
            'result_limited': len(results) == limit
        })
        
        logger.info(f"Query executed successfully", extra={
            'operation': 'query_execute',
            'row_count': len(result_data),
            'query_length': len(query),
            'execution_time_ms': execution_time
        })
        
        return query_result, 200
        
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise

@spark_bp.route('/table/create', methods=['POST'])
@handle_spark_errors
def create_table_from_data():
    """Create a new table in S3 from provided data"""
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'Request body must be JSON'}), 400
    
    table_data = data.get('data')
    s3_path = data.get('s3_path')
    format_type = data.get('format', 'json')  # json or parquet
    
    if not table_data or not s3_path:
        return jsonify({'error': 'Missing required parameters: data, s3_path'}), 400
    
    connector = get_spark_connector()
    
    try:
        spark = connector.get_spark_session()
        
        # Create DataFrame from provided data
        df = spark.createDataFrame(table_data)
        
        # Write to S3
        if format_type.lower() == 'parquet':
            connector.write_parquet_to_s3(df, s3_path)
        else:
            connector.write_json_to_s3(df, s3_path)
        
        # Get info about the created table
        info = connector.get_table_info(s3_path)
        
        logger.info(f"Table created successfully", extra={
            'operation': 'table_create',
            's3_path': s3_path,
            'format': format_type,
            'row_count': info['row_count']
        })
        
        return jsonify({
            'message': 'Table created successfully',
            's3_path': s3_path,
            'format': format_type,
            'table_info': info
        }), 201
        
    except Exception as e:
        logger.error(f"Table creation failed: {e}")
        raise

@spark_bp.route('/table/sample', methods=['GET'])
@handle_spark_errors
def get_table_sample():
    """Get a sample of rows from a table"""
    s3_path = request.args.get('path')
    sample_size = request.args.get('size', 10, type=int)
    
    if not s3_path:
        return jsonify({'error': 'Missing required parameter: path'}), 400
    
    if sample_size > 1000:
        return jsonify({'error': 'Sample size cannot exceed 1000 rows'}), 400
    
    connector = get_spark_connector()
    
    try:
        df = connector.read_json_from_s3(s3_path)
        
        # Get sample data
        sample_df = df.limit(sample_size)
        sample_data = sample_df.collect()
        
        # Convert to JSON-serializable format
        results = []
        for row in sample_data:
            results.append(row.asDict())
        
        return jsonify({
            'sample_data': results,
            'sample_size': len(results),
            'requested_size': sample_size,
            's3_path': s3_path,
            'columns': df.columns
        }), 200
        
    except Exception as e:
        logger.error(f"Failed to get table sample: {e}")
        raise

@spark_bp.route('/stats/table', methods=['GET'])
@handle_spark_errors
def get_table_statistics():
    """Get statistics for a table"""
    s3_path = request.args.get('path')
    
    if not s3_path:
        return jsonify({'error': 'Missing required parameter: path'}), 400
    
    connector = get_spark_connector()
    
    try:
        df = connector.read_json_from_s3(s3_path)
        
        # Get basic statistics
        stats = {
            'row_count': df.count(),
            'column_count': len(df.columns),
            'columns': df.columns,
            'schema': df.schema.jsonValue()
        }
        
        # Get summary statistics for numeric columns
        try:
            numeric_cols = [field.name for field in df.schema.fields 
                          if field.dataType.typeName() in ['double', 'float', 'integer', 'long']]
            
            if numeric_cols:
                summary_df = df.select(numeric_cols).summary()
                summary_data = summary_df.collect()
                
                stats['numeric_summary'] = {}
                for row in summary_data:
                    summary_dict = row.asDict()
                    metric = summary_dict.pop('summary')
                    stats['numeric_summary'][metric] = summary_dict
        
        except Exception as e:
            logger.warning(f"Failed to compute numeric summary: {e}")
            stats['numeric_summary'] = None
        
        return jsonify({
            'statistics': stats,
            's3_path': s3_path
        }), 200
        
    except Exception as e:
        logger.error(f"Failed to get table statistics: {e}")
        raise

# Cleanup function to stop Spark session
def cleanup_spark():
    """Clean up Spark resources"""
    global spark_connector
    if spark_connector:
        spark_connector.stop_spark_session()
        spark_connector = None
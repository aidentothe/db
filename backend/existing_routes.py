"""
Existing Routes - Contains the original functionality from app.py
"""

from flask import Blueprint, request, jsonify, send_from_directory, send_file
from werkzeug.utils import secure_filename
from pyspark.sql.functions import col, count, avg, sum, max, min
import pandas as pd
import json
import os
import logging
import time
import traceback
from datetime import datetime, timedelta
from functools import wraps

from spark_session_manager import get_spark_manager
from query_complexity_analyzer import QueryComplexityAnalyzer
from config import get_config
from iso_standards import ISOFormatter, ISOStandards, ISOValidator, ISOConstants
from error_handlers import api_error_handler, validate_json_request, validate_query_params, require_file_upload, ErrorHandler

# Create blueprint for existing routes
existing_bp = Blueprint('existing', __name__, url_prefix='/api')
logger = logging.getLogger(__name__)

# Initialize components
config = get_config()
spark_manager = get_spark_manager(config)
complexity_analyzer = QueryComplexityAnalyzer()

# Upload configuration
UPLOAD_FOLDER = config.UPLOAD_FOLDER
ALLOWED_EXTENSIONS = config.ALLOWED_EXTENSIONS

# Create upload directory if it doesn't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def log_operation(operation_name):
    """Decorator to log operation start/end with timing"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            start_time = time.time()
            operation_id = f"{operation_name}_{int(time.time() * 1000)}"
            
            logger.info(f"{operation_name} started", extra={
                'operation': f'{operation_name}_start',
                'operation_id': operation_id,
                'remote_addr': request.remote_addr
            })
            
            try:
                result = f(*args, **kwargs)
                processing_time = (time.time() - start_time) * 1000
                
                logger.info(f"{operation_name} completed", extra={
                    'operation': f'{operation_name}_complete',
                    'operation_id': operation_id,
                    'processing_time': processing_time
                })
                
                return result
                
            except Exception as e:
                processing_time = (time.time() - start_time) * 1000
                logger.error(f"{operation_name} failed", extra={
                    'operation': f'{operation_name}_failed',
                    'operation_id': operation_id,
                    'error_type': type(e).__name__,
                    'error_message': str(e),
                    'processing_time': processing_time,
                    'traceback': traceback.format_exc()
                })
                raise
        
        return decorated_function
    return decorator

@existing_bp.route('/health', methods=['GET'])
@log_operation('health_check')
def health_check():
    """Health check endpoint"""
    try:
        spark_info = spark_manager.get_spark_info()
        
        health_data = {
            "status": "healthy",
            **spark_info
        }
        
        # Test Spark connectivity if available
        if spark_info.get("status") == "available":
            session = spark_manager.get_session()
            if session:
                spark_test = session.sparkContext.parallelize([1, 2, 3]).collect()
                health_data["spark_test_passed"] = len(spark_test) == 3
        
        # Return ISO-formatted response
        return ISOFormatter.format_response(health_data)
        
    except Exception as e:
        error_response = ISOFormatter.format_error(
            error_code=ISOConstants.ERROR_CODES['INTERNAL_SERVER_ERROR'],
            message=str(e)
        )
        return error_response, 500

@existing_bp.route('/word-count', methods=['POST'])
@api_error_handler('word_count')
@validate_json_request(['text'])
def word_count():
    """Word count using Spark"""
    spark_session = spark_manager.get_session()
    
    if not spark_session:
        return ErrorHandler.format_error_response(
            error_code='SERVICE_UNAVAILABLE',
            message='Spark session not available',
            status_code=503
        )
    
    data = request.get_json()
    text = data.get('text', '').strip()
    
    if not text:
        return ErrorHandler.handle_validation_error("Text cannot be empty", "text")
    
    logger.info("Processing word count", extra={
        'operation': 'word_count_processing',
        'text_length': len(text),
        'word_count_estimate': len(text.split())
    })
    
    # Create RDD from text
    words_rdd = spark_session.sparkContext.parallelize(text.split())
    
    # Perform word count
    word_counts = words_rdd \
        .map(lambda word: (word.lower(), 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: x[1], ascending=False) \
        .collect()
    
    return jsonify({
        "success": True,
        "result": word_counts[:20]  # Top 20 words
    })

@existing_bp.route('/analyze-csv', methods=['POST'])
@log_operation('csv_analysis')
def analyze_csv():
    """Analyze CSV data using Spark"""
    spark_session = spark_manager.get_session()
    
    if not spark_session:
        # Fallback to pandas analysis
        return _analyze_csv_pandas()
    
    try:
        data = request.json
        csv_data = data.get('data', [])
        
        if not csv_data:
            return jsonify({
                "success": False,
                "error": "No data provided"
            }), 400
        
        logger.info("Creating DataFrame for analysis", extra={
            'operation': 'dataframe_creation_start',
            'input_rows': len(csv_data)
        })
        
        # Convert to Spark DataFrame
        df = spark_session.createDataFrame(csv_data)
        
        # Basic statistics
        stats = {
            "row_count": df.count(),
            "column_count": len(df.columns),
            "columns": df.columns
        }
        
        # Get numeric columns for detailed analysis
        numeric_cols = []
        for field in df.schema.fields:
            if field.dataType.typeName() in ['integer', 'long', 'float', 'double']:
                numeric_cols.append(field.name)
        
        if numeric_cols:
            # Calculate aggregations for numeric columns
            agg_exprs = []
            for col_name in numeric_cols:
                agg_exprs.extend([
                    avg(col(col_name)).alias(f"{col_name}_avg"),
                    sum(col(col_name)).alias(f"{col_name}_sum"),
                    min(col(col_name)).alias(f"{col_name}_min"),
                    max(col(col_name)).alias(f"{col_name}_max")
                ])
            
            agg_result = df.agg(*agg_exprs).collect()[0]
            stats["aggregations"] = agg_result.asDict()
        
        # Sample data
        sample_data = df.limit(10).collect()
        stats["sample"] = [row.asDict() for row in sample_data]
        
        return jsonify({
            "success": True,
            "analysis": stats
        })
        
    except Exception as e:
        logger.error(f"Spark CSV analysis failed, falling back to pandas: {e}")
        return _analyze_csv_pandas()

def _analyze_csv_pandas():
    """Fallback CSV analysis using pandas"""
    try:
        data = request.json
        csv_data = data.get('data', [])
        
        if not csv_data:
            return jsonify({
                "success": False,
                "error": "No data provided"
            }), 400
        
        # Convert to pandas DataFrame
        df = pd.DataFrame(csv_data)
        
        # Basic statistics
        stats = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": df.columns.tolist(),
            "mode": "pandas-fallback"
        }
        
        # Numeric analysis
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        if numeric_cols:
            desc = df[numeric_cols].describe()
            stats["aggregations"] = desc.to_dict()
        
        # Sample data
        stats["sample"] = df.head(10).to_dict('records')
        
        return jsonify({
            "success": True,
            "analysis": stats
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@existing_bp.route('/execute-query', methods=['POST'])
@log_operation('query_execution')
def execute_query():
    """Execute SQL query on uploaded data"""
    spark_session = spark_manager.get_session()
    
    if not spark_session:
        return jsonify({
            "success": False,
            "error": "Spark session not available"
        }), 503
    
    try:
        data = request.json
        query = data.get('query', '')
        csv_data = data.get('data', [])
        
        if not query or not csv_data:
            return jsonify({
                "success": False,
                "error": "Query and data are required"
            }), 400
        
        # Analyze query complexity
        try:
            complexity = complexity_analyzer.analyze_query(query)
            
            if complexity['risk_level'] == 'HIGH':
                logger.warning("High-risk query detected", extra={
                    'operation': 'high_risk_query',
                    'risk_level': complexity['risk_level'],
                    'query': query[:100]
                })
        except Exception as e:
            logger.warning(f"Query complexity analysis failed: {e}")
            complexity = {'risk_level': 'UNKNOWN'}
        
        # Create DataFrame
        df = spark_session.createDataFrame(csv_data)
        df.createOrReplaceTempView("data_table")
        
        # Execute query
        result_df = spark_session.sql(query)
        results = result_df.collect()
        
        # Convert results to JSON-serializable format
        result_data = [row.asDict() for row in results]
        
        return jsonify({
            "success": True,
            "results": result_data,
            "row_count": len(result_data),
            "complexity": complexity
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@existing_bp.route('/upload', methods=['POST'])
@api_error_handler('file_upload')
@require_file_upload(list(ALLOWED_EXTENSIONS))
def upload_file():
    """Handle file upload"""
    file = request.files['file']
    
    # Secure the filename
    filename = secure_filename(file.filename)
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    
    # Save file
    file.save(filepath)
    
    # Get file info
    file_info = {
        "filename": filename,
        "size": os.path.getsize(filepath),
        "path": filepath
    }
    
    logger.info("File uploaded successfully", extra={
        'operation': 'file_upload_success',
        'filename': filename,
        'file_size': file_info['size']
    })
    
    return jsonify({
        "success": True,
        "file_info": file_info
    })

@existing_bp.route('/files', methods=['GET'])
def list_files():
    """List uploaded files"""
    try:
        files = []
        for filename in os.listdir(UPLOAD_FOLDER):
            filepath = os.path.join(UPLOAD_FOLDER, filename)
            if os.path.isfile(filepath):
                files.append({
                    "filename": filename,
                    "size": os.path.getsize(filepath),
                    "modified": datetime.fromtimestamp(os.path.getmtime(filepath)).isoformat()
                })
        
        return jsonify({
            "success": True,
            "files": files
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@existing_bp.route('/files/<filename>', methods=['DELETE'])
@log_operation('file_delete')
def delete_file(filename):
    """Delete uploaded file"""
    try:
        # Secure the filename
        filename = secure_filename(filename)
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        
        if not os.path.exists(filepath):
            return jsonify({
                "success": False,
                "error": "File not found"
            }), 404
        
        os.remove(filepath)
        
        logger.info("File deleted successfully", extra={
            'operation': 'file_delete_success',
            'filename': filename
        })
        
        return jsonify({
            "success": True,
            "message": f"File {filename} deleted successfully"
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@existing_bp.route('/files/<filename>/download', methods=['GET'])
def download_file(filename):
    """Download uploaded file"""
    try:
        # Secure the filename
        filename = secure_filename(filename)
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        
        if not os.path.exists(filepath):
            return jsonify({
                "error": "File not found"
            }), 404
        
        return send_file(filepath, as_attachment=True)
        
    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500
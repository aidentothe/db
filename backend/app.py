from flask import Flask, request, jsonify, send_from_directory, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum, max, min
import pandas as pd
import json
import os
import logging
import time
import traceback
from datetime import datetime, timedelta
from query_complexity_analyzer import QueryComplexityAnalyzer
from groq_client import GroqClient

app = Flask(__name__)
# Configure CORS to allow frontend requests
CORS(app, origins=['http://localhost:3000', 'http://localhost:3001', 'http://localhost:3002'], 
     allow_headers=['Content-Type'], 
     methods=['GET', 'POST', 'DELETE', 'OPTIONS'])

# Enhanced console formatter with colors and better readability
class EnhancedConsoleFormatter(logging.Formatter):
    """Enhanced formatter for console output with colors and structure."""
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
        'RESET': '\033[0m',     # Reset
        'BOLD': '\033[1m',      # Bold
        'TIMESTAMP': '\033[90m', # Gray
        'OPERATION': '\033[94m', # Light Blue
    }
    
    def format(self, record):
        # Get timestamp
        timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        
        # Get colors
        level_color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        
        # Format basic message
        formatted = f"{self.COLORS['TIMESTAMP']}[{timestamp}]{self.COLORS['RESET']} "
        formatted += f"{level_color}{self.COLORS['BOLD']}[{record.levelname:8}]{self.COLORS['RESET']} "
        formatted += f"{record.getMessage()}"
        
        # Add operation context if available
        if hasattr(record, 'operation'):
            formatted += f" {self.COLORS['OPERATION']}({record.operation}){self.COLORS['RESET']}"
        
        # Add key metrics inline for quick scanning
        metrics = []
        if hasattr(record, 'processing_time'):
            metrics.append(f"time={record.processing_time:.2f}ms")
        if hasattr(record, 'file_name'):
            metrics.append(f"file={record.file_name}")
        if hasattr(record, 'row_count'):
            metrics.append(f"rows={record.row_count:,}")
        if hasattr(record, 'error_type'):
            metrics.append(f"error={record.error_type}")
        
        if metrics:
            formatted += f" {self.COLORS['TIMESTAMP']}[{', '.join(metrics)}]{self.COLORS['RESET']}"
        
        # Add exception info if present
        if record.exc_info:
            formatted += "\n" + self.formatException(record.exc_info)
            
        return formatted

# Create a custom formatter for structured logging
class StructuredFormatter(logging.Formatter):
    """Enhanced structured formatter with comprehensive context tracking."""
    
    def format(self, record):
        try:
            timestamp = datetime.utcnow().isoformat()
            # Validate timestamp doesn't contain NaN
            if 'NaN' in str(timestamp) or not timestamp:
                timestamp = datetime.now().isoformat()
        except Exception:
            try:
                timestamp = datetime.now().isoformat()
            except Exception:
                # Ultimate fallback
                import time
                timestamp = time.strftime("%Y-%m-%dT%H:%M:%S")
        
        log_entry = {
            'timestamp': timestamp,
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
        }
        
        # Enhanced context tracking
        context_fields = [
            'operation', 'file_name', 'file_size', 'processing_time', 'row_count', 
            'error_type', 'error_message', 'endpoint', 'method', 'status_code',
            'user_id', 'session_id', 'correlation_id', 'component', 'severity',
            'impact', 'query', 'complexity_rating', 'memory_usage_mb'
        ]
        
        for field in context_fields:
            if hasattr(record, field):
                value = getattr(record, field)
                if value is not None:
                    log_entry[field] = value
        
        # Add exception information if present
        if record.exc_info:
            log_entry['exception'] = {
                'type': record.exc_info[0].__name__ if record.exc_info[0] else None,
                'message': str(record.exc_info[1]) if record.exc_info[1] else None,
                'traceback': self.formatException(record.exc_info)
            }
        
        # Add performance metrics
        log_entry['performance_metrics'] = {
            'memory_usage_mb': getattr(record, 'memory_usage_mb', None),
            'processing_time': getattr(record, 'processing_time', None),
            'cpu_time': getattr(record, 'cpu_time', None)
        }
        
        # Clean up null values for cleaner JSON
        log_entry = {k: v for k, v in log_entry.items() if v is not None}
        if log_entry.get('performance_metrics'):
            log_entry['performance_metrics'] = {k: v for k, v in log_entry['performance_metrics'].items() if v is not None}
            if not log_entry['performance_metrics']:
                del log_entry['performance_metrics']
        
        try:
            # Convert any NaN values to null before serialization
            def clean_nan_values(obj):
                if isinstance(obj, dict):
                    return {k: clean_nan_values(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [clean_nan_values(item) for item in obj]
                elif isinstance(obj, float) and (obj != obj):  # Check for NaN
                    return None
                elif isinstance(obj, str) and 'NaN' in obj:
                    return obj.replace('NaN', 'null')
                else:
                    return obj
            
            cleaned_entry = clean_nan_values(log_entry)
            return json.dumps(cleaned_entry, default=str, allow_nan=False, indent=2)
        except (TypeError, ValueError) as e:
            safe_entry = {
                'timestamp': timestamp,
                'level': record.levelname,
                'logger': record.name,
                'message': str(record.getMessage()),
                'serialization_error': str(e)
            }
            return json.dumps(safe_entry, default=str, allow_nan=False, indent=2)

# Configure enhanced logging with multiple handlers
# Clear any existing handlers first
logging.getLogger().handlers.clear()

# Create console handler with enhanced formatting
console_handler = logging.StreamHandler()
console_handler.setFormatter(EnhancedConsoleFormatter())
console_handler.setLevel(logging.INFO)

# Create file handler for human-readable logs
file_handler_readable = logging.FileHandler('app.log')
file_handler_readable.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
))
file_handler_readable.setLevel(logging.INFO)

# Set up file handler with structured logging
file_handler = logging.FileHandler('app_structured.log')
file_handler.setFormatter(StructuredFormatter())
file_handler.setLevel(logging.INFO)

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    handlers=[console_handler, file_handler_readable, file_handler]
)
logger = logging.getLogger(__name__)

# Configure upload settings
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv', 'txt'}
MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB max file size

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

# Create upload directory if it doesn't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Initialize Spark Session with Java compatibility fixes
import os
import sys

# Set Java options to fix compatibility issues
# Removed security manager config as it's not supported in newer Java versions

# Try to create Spark session with fallback configuration
try:
    # Set environment variables for Java compatibility
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    # Fix for newer Java versions compatibility
    os.environ['HADOOP_USER_NAME'] = 'spark'
    
    spark = SparkSession.builder \
        .appName("SparkNextJSPOC") \
        .master("local[*]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
except Exception as e:
    print(f"Failed to create Spark session with local cluster, trying local mode: {e}")
    try:
        spark = SparkSession.builder \
            .appName("SparkNextJSPOC") \
            .master("local") \
            .config("spark.driver.memory", "512m") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
    except Exception as e2:
        print(f"Failed to create Spark session: {e2}")
        print("Falling back to pandas-only mode...")
        spark = None

# Set log level to reduce output
if spark:
    spark.sparkContext.setLogLevel("ERROR")

# Initialize complexity analyzer
complexity_analyzer = QueryComplexityAnalyzer()
groq_client = GroqClient()

# Log all incoming requests
@app.before_request
def log_request_info():
    logger.info("Incoming request", extra={
        'operation': 'incoming_request',
        'method': request.method,
        'endpoint': request.endpoint or 'unknown',
        'path': request.path,
        'remote_addr': request.remote_addr,
        'user_agent': request.headers.get('User-Agent', 'unknown')
    })

@app.route('/api/health', methods=['GET'])
def health_check():
    logger.info("Health check requested", extra={
        'operation': 'health_check',
        'remote_addr': request.remote_addr
    })
    
    try:
        health_data = {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "mode": "pandas-only" if spark is None else "spark-enabled"
        }
        
        if spark:
            # Test Spark connectivity
            spark_test = spark.sparkContext.parallelize([1, 2, 3]).collect()
            health_data.update({
                "spark_version": spark.version,
                "app_name": spark.sparkContext.appName,
                "spark_master": spark.sparkContext.master
            })
        
        logger.info("Health check completed successfully", extra={
            'operation': 'health_check_success',
            'spark_available': spark is not None
        })
        
        return jsonify(health_data)
        
    except Exception as e:
        logger.error("Health check failed", extra={
            'operation': 'health_check_failed',
            'error_type': type(e).__name__,
            'error_message': str(e),
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }), 500

@app.route('/api/word-count', methods=['POST'])
def word_count():
    start_time = time.time()
    wordcount_id = f"wordcount_{int(time.time() * 1000)}"
    
    logger.info("Word count processing started", extra={
        'operation': 'word_count_start',
        'wordcount_id': wordcount_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        data = request.json
        text = data.get('text', '')
        
        logger.info("Word count parameters", extra={
            'operation': 'word_count_parameters',
            'wordcount_id': wordcount_id,
            'text_length': len(text),
            'word_count_estimate': len(text.split())
        })
        
        # Create RDD from text
        rdd_start = time.time()
        words_rdd = spark.sparkContext.parallelize(text.split())
        rdd_time = time.time() - rdd_start
        
        logger.info("RDD creation completed", extra={
            'operation': 'rdd_creation_complete',
            'wordcount_id': wordcount_id,
            'processing_time': rdd_time
        })
        
        # Perform word count
        processing_start = time.time()
        word_counts = words_rdd \
            .map(lambda word: (word.lower(), 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .sortBy(lambda x: x[1], ascending=False) \
            .collect()
        processing_time = time.time() - processing_start
        
        total_time = time.time() - start_time
        logger.info("Word count processing completed", extra={
            'operation': 'word_count_complete',
            'wordcount_id': wordcount_id,
            'unique_words': len(word_counts),
            'top_words_returned': min(20, len(word_counts)),
            'processing_time': processing_time,
            'total_time': total_time
        })
        
        return jsonify({
            "success": True,
            "result": word_counts[:20]  # Top 20 words
        })
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("Word count processing failed", extra={
            'operation': 'word_count_failed',
            'wordcount_id': wordcount_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/analyze-csv', methods=['POST'])
def analyze_csv():
    start_time = time.time()
    analysis_id = f"analysis_{int(time.time() * 1000)}"
    
    logger.info("CSV analysis started", extra={
        'operation': 'csv_analysis_start',
        'analysis_id': analysis_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        data = request.json
        csv_data = data.get('data', [])
        
        logger.info("Creating DataFrame for analysis", extra={
            'operation': 'dataframe_creation_start',
            'analysis_id': analysis_id,
            'input_rows': len(csv_data)
        })
        
        # Convert to DataFrame (Spark or Pandas)
        df_start = time.time()
        if spark:
            df = spark.createDataFrame(csv_data)
            row_count = df.count()
            column_count = len(df.columns)
        else:
            # Use pandas if Spark is not available
            df = pd.DataFrame(csv_data)
            row_count = len(df)
            column_count = len(df.columns)
        df_time = time.time() - df_start
        
        logger.info("DataFrame created successfully", extra={
            'operation': 'dataframe_creation_complete',
            'analysis_id': analysis_id,
            'row_count': row_count,
            'column_count': column_count,
            'processing_time': df_time
        })
        
        # Basic statistics
        stats = {}
        numeric_cols = [field.name for field in df.schema.fields 
                       if field.dataType.typeName() in ['integer', 'double', 'float', 'long']]
        
        logger.info("Computing statistics", extra={
            'operation': 'statistics_computation_start',
            'analysis_id': analysis_id,
            'numeric_columns': len(numeric_cols),
            'numeric_column_names': numeric_cols
        })
        
        stats_start = time.time()
        for col_name in numeric_cols:
            col_stats = df.select(
                avg(col(col_name)).alias('avg'),
                min(col(col_name)).alias('min'),
                max(col(col_name)).alias('max'),
                sum(col(col_name)).alias('sum'),
                count(col(col_name)).alias('count')
            ).collect()[0]
            
            stats[col_name] = {
                'average': float(col_stats['avg']) if col_stats['avg'] else 0,
                'min': float(col_stats['min']) if col_stats['min'] else 0,
                'max': float(col_stats['max']) if col_stats['max'] else 0,
                'sum': float(col_stats['sum']) if col_stats['sum'] else 0,
                'count': int(col_stats['count'])
            }
        
        stats_time = time.time() - stats_start
        
        logger.info("Statistics computation completed", extra={
            'operation': 'statistics_computation_complete',
            'analysis_id': analysis_id,
            'columns_analyzed': len(numeric_cols),
            'processing_time': stats_time
        })
        
        # Get schema info
        schema_info = [{"name": field.name, "type": field.dataType.typeName()} 
                      for field in df.schema.fields]
        
        total_time = time.time() - start_time
        logger.info("CSV analysis completed successfully", extra={
            'operation': 'csv_analysis_complete',
            'analysis_id': analysis_id,
            'row_count': row_count,
            'column_count': column_count,
            'numeric_columns': len(numeric_cols),
            'processing_time': total_time
        })
        
        return jsonify({
            "success": True,
            "stats": stats,
            "schema": schema_info,
            "row_count": row_count
        })
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("CSV analysis failed", extra={
            'operation': 'csv_analysis_failed',
            'analysis_id': analysis_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/sql-query', methods=['POST'])
def sql_query():
    start_time = time.time()
    query_id = f"query_{int(time.time() * 1000)}"
    
    try:
        data = request.json
        csv_data = data.get('data', [])
        query = data.get('query', '')
        analyze_complexity = data.get('analyze_complexity', True)
        
        logger.info("SQL query execution started", extra={
            'operation': 'sql_query_start',
            'query_id': query_id,
            'data_rows': len(csv_data),
            'query_length': len(query),
            'analyze_complexity': analyze_complexity,
            'remote_addr': request.remote_addr
        })
        
        # Create temporary view
        df_start = time.time()
        df = spark.createDataFrame(csv_data)
        df.createOrReplaceTempView("data_table")
        df_time = time.time() - df_start
        
        row_count = df.count()
        column_count = len(df.columns)
        
        logger.info("Spark DataFrame created", extra={
            'operation': 'dataframe_creation_complete',
            'query_id': query_id,
            'row_count': row_count,
            'column_count': column_count,
            'processing_time': df_time
        })
        
        # Analyze query complexity if requested
        complexity_analysis = None
        if analyze_complexity:
            complexity_start = time.time()
            # Get data statistics for complexity analysis
            data_stats = {
                'row_count': row_count,
                'column_count': column_count,
                'schema': [{"name": field.name, "type": field.dataType.typeName()} 
                          for field in df.schema.fields]
            }
            complexity_analysis = complexity_analyzer.analyze_query(query, data_stats)
            complexity_time = time.time() - complexity_start
            
            logger.info("Query complexity analysis completed", extra={
                'operation': 'query_complexity_analysis_complete',
                'query_id': query_id,
                'complexity_score': complexity_analysis.get('estimated_complexity', 'unknown'),
                'processing_time': complexity_time
            })
        
        # Execute SQL query
        logger.info("Executing SQL query", extra={
            'operation': 'sql_execution_start',
            'query_id': query_id,
            'query': query[:200] + '...' if len(query) > 200 else query  # Truncate long queries for logging
        })
        
        execution_start = time.time()
        result_df = spark.sql(query)
        
        # Convert to list of dictionaries
        results = [row.asDict() for row in result_df.collect()[:100]]  # Limit to 100 rows
        execution_time = time.time() - execution_start
        
        logger.info("SQL query executed successfully", extra={
            'operation': 'sql_execution_complete',
            'query_id': query_id,
            'result_rows': len(results),
            'execution_time': execution_time
        })
        
        response = {
            "success": True,
            "results": results,
            "row_count": len(results)
        }
        
        if complexity_analysis:
            response["complexity_analysis"] = complexity_analysis
        
        total_time = time.time() - start_time
        logger.info("SQL query processing completed", extra={
            'operation': 'sql_query_complete',
            'query_id': query_id,
            'total_processing_time': total_time,
            'result_rows': len(results)
        })
        
        return jsonify(response)
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("SQL query execution failed", extra={
            'operation': 'sql_query_failed',
            'query_id': query_id,
            'query': query[:200] + '...' if len(query) > 200 else query,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/machine-learning/kmeans', methods=['POST'])
def kmeans_clustering():
    start_time = time.time()
    ml_id = f"kmeans_{int(time.time() * 1000)}"
    
    logger.info("K-means clustering started", extra={
        'operation': 'kmeans_clustering_start',
        'ml_id': ml_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        from pyspark.ml.clustering import KMeans
        from pyspark.ml.feature import VectorAssembler
        
        data = request.json
        csv_data = data.get('data', [])
        features = data.get('features', [])
        k = data.get('k', 3)
        
        logger.info("K-means parameters", extra={
            'operation': 'kmeans_parameters',
            'ml_id': ml_id,
            'data_rows': len(csv_data),
            'features': features,
            'k_clusters': k
        })
        
        # Create DataFrame
        df_start = time.time()
        df = spark.createDataFrame(csv_data)
        df_time = time.time() - df_start
        
        logger.info("DataFrame created for ML", extra={
            'operation': 'ml_dataframe_creation_complete',
            'ml_id': ml_id,
            'row_count': len(csv_data),
            'features_count': len(features),
            'processing_time': df_time
        })
        
        # Prepare features
        feature_start = time.time()
        assembler = VectorAssembler(inputCols=features, outputCol="features")
        dataset = assembler.transform(df)
        feature_time = time.time() - feature_start
        
        logger.info("Feature assembly completed", extra={
            'operation': 'feature_assembly_complete',
            'ml_id': ml_id,
            'processing_time': feature_time
        })
        
        # Train KMeans model
        training_start = time.time()
        kmeans = KMeans(k=k, seed=1)
        model = kmeans.fit(dataset)
        training_time = time.time() - training_start
        
        logger.info("K-means model training completed", extra={
            'operation': 'kmeans_training_complete',
            'ml_id': ml_id,
            'k_clusters': k,
            'training_cost': model.summary.trainingCost,
            'processing_time': training_time
        })
        
        # Make predictions
        prediction_start = time.time()
        predictions = model.transform(dataset)
        prediction_time = time.time() - prediction_start
        
        # Get cluster centers
        centers = model.clusterCenters()
        centers_list = [center.tolist() for center in centers]
        
        # Convert predictions to list
        results = predictions.select("prediction", *features).collect()
        predictions_list = [{"cluster": row["prediction"], 
                           **{f: row[f] for f in features}} 
                          for row in results]
        
        logger.info("K-means predictions completed", extra={
            'operation': 'kmeans_predictions_complete',
            'ml_id': ml_id,
            'prediction_count': len(predictions_list),
            'processing_time': prediction_time
        })
        
        total_time = time.time() - start_time
        logger.info("K-means clustering completed successfully", extra={
            'operation': 'kmeans_clustering_complete',
            'ml_id': ml_id,
            'k_clusters': k,
            'data_points': len(predictions_list),
            'training_cost': model.summary.trainingCost,
            'processing_time': total_time
        })
        
        return jsonify({
            "success": True,
            "predictions": predictions_list[:100],  # Limit to 100 rows
            "centers": centers_list,
            "cost": model.summary.trainingCost
        })
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("K-means clustering failed", extra={
            'operation': 'kmeans_clustering_failed',
            'ml_id': ml_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/analyze-query-complexity', methods=['POST'])
def analyze_query_complexity():
    """Analyze SQL query complexity without executing it"""
    start_time = time.time()
    analysis_id = f"query_complexity_{int(time.time() * 1000)}"
    
    logger.info("Query complexity analysis started", extra={
        'operation': 'query_complexity_analysis_start',
        'analysis_id': analysis_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        data = request.json
        query = data.get('query', '')
        data_stats = data.get('data_stats', None)
        
        if not query:
            logger.warning("Query complexity analysis failed: No query provided", extra={
                'operation': 'query_complexity_analysis_failed',
                'analysis_id': analysis_id,
                'error_type': 'no_query_provided'
            })
            return jsonify({
                "success": False,
                "error": "Query is required"
            }), 400
        
        logger.info("Analyzing query complexity", extra={
            'operation': 'query_complexity_analysis_process',
            'analysis_id': analysis_id,
            'query_length': len(query),
            'has_data_stats': data_stats is not None
        })
        
        # Analyze query complexity
        analysis_start = time.time()
        complexity_analysis = complexity_analyzer.analyze_query(query, data_stats)
        analysis_time = time.time() - analysis_start
        
        total_time = time.time() - start_time
        logger.info("Query complexity analysis completed", extra={
            'operation': 'query_complexity_analysis_complete',
            'analysis_id': analysis_id,
            'complexity_score': complexity_analysis.get('estimated_complexity', 'unknown'),
            'analysis_time': analysis_time,
            'total_time': total_time
        })
        
        return jsonify({
            "success": True,
            "analysis": complexity_analysis
        })
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("Query complexity analysis failed", extra={
            'operation': 'query_complexity_analysis_failed',
            'analysis_id': analysis_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/explain-complexity', methods=['POST'])
def explain_complexity_topic():
    """Provide detailed explanations for complexity analysis topics"""
    start_time = time.time()
    explanation_id = f"explanation_{int(time.time() * 1000)}"
    
    logger.info("Complexity explanation started", extra={
        'operation': 'complexity_explanation_start',
        'explanation_id': explanation_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        data = request.json
        topic = data.get('topic', '')
        context = data.get('context', None)  # Optional analysis context
        
        if not topic:
            logger.warning("Complexity explanation failed: No topic provided", extra={
                'operation': 'complexity_explanation_failed',
                'explanation_id': explanation_id,
                'error_type': 'no_topic_provided'
            })
            return jsonify({
                "success": False,
                "error": "Topic is required"
            }), 400
        
        logger.info("Generating complexity explanation", extra={
            'operation': 'complexity_explanation_process',
            'explanation_id': explanation_id,
            'topic': topic,
            'has_context': context is not None
        })
        
        # Generate explanation
        explanation_start = time.time()
        explanation = complexity_analyzer.get_explanation(topic, context)
        explanation_time = time.time() - explanation_start
        
        total_time = time.time() - start_time
        logger.info("Complexity explanation completed", extra={
            'operation': 'complexity_explanation_complete',
            'explanation_id': explanation_id,
            'topic': topic,
            'explanation_time': explanation_time,
            'total_time': total_time
        })
        
        return jsonify({
            "success": True,
            "topic": topic,
            "explanation": explanation
        })
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("Complexity explanation failed", extra={
            'operation': 'complexity_explanation_failed',
            'explanation_id': explanation_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/ask-complexity-question', methods=['POST'])
def ask_complexity_question():
    """Answer user questions about complexity analysis using Groq AI"""
    start_time = time.time()
    question_id = f"question_{int(time.time() * 1000)}"
    
    logger.info("Complexity question started", extra={
        'operation': 'complexity_question_start',
        'question_id': question_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        data = request.json
        question = data.get('question', '')
        analysis_context = data.get('analysis_context', None)
        
        if not question:
            logger.warning("Complexity question failed: No question provided", extra={
                'operation': 'complexity_question_failed',
                'question_id': question_id,
                'error_type': 'no_question_provided'
            })
            return jsonify({
                "success": False,
                "error": "Question is required"
            }), 400
        
        logger.info("Processing complexity question", extra={
            'operation': 'complexity_question_process',
            'question_id': question_id,
            'question_length': len(question),
            'has_context': analysis_context is not None
        })
        
        # Get answer from Groq
        answer_start = time.time()
        answer = groq_client.ask_complexity_question(question, analysis_context)
        answer_time = time.time() - answer_start
        
        total_time = time.time() - start_time
        logger.info("Complexity question completed", extra={
            'operation': 'complexity_question_complete',
            'question_id': question_id,
            'answer_length': len(answer),
            'answer_time': answer_time,
            'total_time': total_time
        })
        
        return jsonify({
            "success": True,
            "question": question,
            "answer": answer,
            "response_time": answer_time
        })
        
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("Complexity question failed", extra={
            'operation': 'complexity_question_failed',
            'question_id': question_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/optimize-query', methods=['POST'])
def optimize_query():
    """Generate detailed optimization suggestions for a query using Groq AI"""
    start_time = time.time()
    optimization_id = f"optimization_{int(time.time() * 1000)}"
    
    logger.info("Query optimization started", extra={
        'operation': 'query_optimization_start',
        'optimization_id': optimization_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        data = request.json
        query = data.get('query', '')
        analysis = data.get('analysis', {})
        
        if not query:
            logger.warning("Query optimization failed: No query provided", extra={
                'operation': 'query_optimization_failed',
                'optimization_id': optimization_id,
                'error_type': 'no_query_provided'
            })
            return jsonify({
                "success": False,
                "error": "Query is required"
            }), 400
        
        logger.info("Generating optimization suggestions", extra={
            'operation': 'query_optimization_process',
            'optimization_id': optimization_id,
            'query_length': len(query),
            'has_analysis': bool(analysis)
        })
        
        # Generate optimization suggestions using Groq
        optimization_start = time.time()
        suggestions = groq_client.generate_optimization_suggestions(query, analysis)
        optimization_time = time.time() - optimization_start
        
        total_time = time.time() - start_time
        logger.info("Query optimization completed", extra={
            'operation': 'query_optimization_complete',
            'optimization_id': optimization_id,
            'suggestions_length': len(suggestions),
            'optimization_time': optimization_time,
            'total_time': total_time
        })
        
        return jsonify({
            "success": True,
            "query": query,
            "optimization_suggestions": suggestions,
            "response_time": optimization_time
        })
        
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("Query optimization failed", extra={
            'operation': 'query_optimization_failed',
            'optimization_id': optimization_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/explain-score', methods=['POST'])
def explain_complexity_score():
    """Explain why a query received its complexity score using Groq AI"""
    start_time = time.time()
    explanation_id = f"score_explanation_{int(time.time() * 1000)}"
    
    logger.info("Score explanation started", extra={
        'operation': 'score_explanation_start',
        'explanation_id': explanation_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        data = request.json
        analysis = data.get('analysis', {})
        
        if not analysis:
            logger.warning("Score explanation failed: No analysis provided", extra={
                'operation': 'score_explanation_failed',
                'explanation_id': explanation_id,
                'error_type': 'no_analysis_provided'
            })
            return jsonify({
                "success": False,
                "error": "Analysis results are required"
            }), 400
        
        logger.info("Generating score explanation", extra={
            'operation': 'score_explanation_process',
            'explanation_id': explanation_id,
            'complexity_rating': analysis.get('complexity_rating', 'unknown')
        })
        
        # Generate explanation using Groq
        explanation_start = time.time()
        explanation = groq_client.explain_complexity_score(analysis)
        explanation_time = time.time() - explanation_start
        
        total_time = time.time() - start_time
        logger.info("Score explanation completed", extra={
            'operation': 'score_explanation_complete',
            'explanation_id': explanation_id,
            'explanation_length': len(explanation),
            'explanation_time': explanation_time,
            'total_time': total_time
        })
        
        return jsonify({
            "success": True,
            "analysis": analysis,
            "explanation": explanation,
            "response_time": explanation_time
        })
        
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("Score explanation failed", extra={
            'operation': 'score_explanation_failed',
            'explanation_id': explanation_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/analyze-csv-complexity', methods=['POST'])
def analyze_csv_complexity():
    """Analyze complexity of CSV data"""
    start_time = time.time()
    analysis_id = f"csv_complexity_{int(time.time() * 1000)}"
    
    logger.info("CSV complexity analysis started", extra={
        'operation': 'csv_complexity_analysis_start',
        'analysis_id': analysis_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        data = request.json
        csv_data = data.get('data', [])
        
        if not csv_data:
            logger.warning("CSV complexity analysis failed: No data provided", extra={
                'operation': 'csv_complexity_analysis_failed',
                'analysis_id': analysis_id,
                'error_type': 'no_data_provided'
            })
            return jsonify({
                "success": False,
                "error": "CSV data is required"
            }), 400
        
        logger.info("Processing CSV data for complexity analysis", extra={
            'operation': 'csv_complexity_analysis_process',
            'analysis_id': analysis_id,
            'data_rows': len(csv_data),
            'data_columns': len(csv_data[0].keys()) if csv_data else 0
        })
        
        # Convert to pandas DataFrame for analysis
        df_start = time.time()
        df = pd.DataFrame(csv_data)
        df_time = time.time() - df_start
        
        logger.info("DataFrame created for complexity analysis", extra={
            'operation': 'csv_complexity_dataframe_complete',
            'analysis_id': analysis_id,
            'processing_time': df_time
        })
        
        # Analyze dataset complexity
        analysis_start = time.time()
        complexity_stats = complexity_analyzer.analyze_csv_complexity(df)
        analysis_time = time.time() - analysis_start
        
        total_time = time.time() - start_time
        logger.info("CSV complexity analysis completed", extra={
            'operation': 'csv_complexity_analysis_complete',
            'analysis_id': analysis_id,
            'complexity_score': complexity_stats.get('estimated_complexity', 'unknown'),
            'analysis_time': analysis_time,
            'total_time': total_time
        })
        
        return jsonify({
            "success": True,
            "complexity_stats": complexity_stats
        })
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("CSV complexity analysis failed", extra={
            'operation': 'csv_complexity_analysis_failed',
            'analysis_id': analysis_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/sample-datasets', methods=['GET'])
def get_sample_datasets():
    """Get list of available sample datasets"""
    start_time = time.time()
    request_id = f"sample_list_{int(time.time() * 1000)}"
    
    logger.info("Sample datasets listing started", extra={
        'operation': 'sample_datasets_list_start',
        'request_id': request_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        import os
        sample_dir = "../sample_datasets"
        
        if not os.path.exists(sample_dir):
            logger.warning("Sample datasets directory not found", extra={
                'operation': 'sample_datasets_list_failed',
                'request_id': request_id,
                'sample_dir': sample_dir,
                'error_type': 'directory_not_found'
            })
            return jsonify({
                "success": False,
                "error": "Sample datasets directory not found"
            }), 404
        
        datasets = []
        processed_files = 0
        failed_files = 0
        
        for filename in os.listdir(sample_dir):
            if filename.endswith('.csv'):
                file_path = os.path.join(sample_dir, filename)
                try:
                    logger.debug("Processing sample dataset", extra={
                        'operation': 'sample_dataset_process',
                        'request_id': request_id,
                        'file_name': filename
                    })
                    
                    # Get basic file info
                    df = pd.read_csv(file_path, nrows=5)  # Read just first 5 rows for preview
                    file_size = os.path.getsize(file_path)
                    
                    # Get full dataset for complexity analysis
                    full_df = pd.read_csv(file_path)
                    complexity_stats = complexity_analyzer.analyze_csv_complexity(full_df)
                    
                    # Clean preview data to handle NaN values and ensure JSON serializable
                    preview_df = df.fillna('')  # Replace NaN with empty strings
                    preview_data = []
                    try:
                        preview_data = preview_df.to_dict('records')
                        # Clean any remaining NaN values that might slip through
                        for record in preview_data:
                            for key, value in record.items():
                                if isinstance(value, float) and (value != value):  # Check for NaN
                                    record[key] = ''
                                elif str(value) == 'nan':
                                    record[key] = ''
                    except Exception as e:
                        logger.warning(f"Error processing sample dataset preview: {e}")
                        preview_data = []

                    datasets.append({
                        'filename': filename,
                        'name': filename.replace('.csv', '').replace('_', ' ').title(),
                        'rows': len(full_df),
                        'columns': len(full_df.columns),
                        'size_bytes': file_size,
                        'complexity': complexity_stats['estimated_complexity'],
                        'preview': preview_data,
                        'column_types': complexity_stats['data_types']
                    })
                    
                    processed_files += 1
                    
                except Exception as e:
                    failed_files += 1
                    logger.warning("Failed to process sample dataset", extra={
                        'operation': 'sample_dataset_process_failed',
                        'request_id': request_id,
                        'file_name': filename,
                        'error_type': type(e).__name__,
                        'error_message': str(e)
                    })
                    continue
        
        total_time = time.time() - start_time
        logger.info("Sample datasets listing completed", extra={
            'operation': 'sample_datasets_list_complete',
            'request_id': request_id,
            'datasets_found': len(datasets),
            'processed_files': processed_files,
            'failed_files': failed_files,
            'processing_time': total_time
        })
        
        return jsonify({
            "success": True,
            "datasets": sorted(datasets, key=lambda x: x['complexity'])
        })
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("Sample datasets listing failed", extra={
            'operation': 'sample_datasets_list_failed',
            'request_id': request_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/load-sample-dataset/<filename>', methods=['GET'])
def load_sample_dataset(filename):
    """Load a specific sample dataset"""
    start_time = time.time()
    load_id = f"sample_load_{int(time.time() * 1000)}"
    
    logger.info("Sample dataset load started", extra={
        'operation': 'sample_dataset_load_start',
        'load_id': load_id,
        'file_name': filename,
        'remote_addr': request.remote_addr
    })
    
    try:
        import os
        sample_dir = "../sample_datasets"
        file_path = os.path.join(sample_dir, filename)
        
        if not os.path.exists(file_path):
            logger.warning("Sample dataset not found", extra={
                'operation': 'sample_dataset_load_failed',
                'load_id': load_id,
                'file_name': filename,
                'file_path': file_path,
                'error_type': 'file_not_found'
            })
            return jsonify({
                "success": False,
                "error": "Dataset not found"
            }), 404
        
        file_size = os.path.getsize(file_path)
        logger.info("Loading sample dataset", extra={
            'operation': 'sample_dataset_load_start',
            'load_id': load_id,
            'file_name': filename,
            'file_size': file_size
        })
        
        # Load dataset
        load_start = time.time()
        df = pd.read_csv(file_path)
        load_time = time.time() - load_start
        
        row_count = len(df)
        column_count = len(df.columns)
        
        logger.info("Sample dataset loaded", extra={
            'operation': 'sample_dataset_load_complete',
            'load_id': load_id,
            'file_name': filename,
            'row_count': row_count,
            'column_count': column_count,
            'processing_time': load_time
        })
        
        # Limit size for frontend (max 1000 rows for display)
        if row_count > 1000:
            sample_df = df.sample(n=1000, random_state=42)
            full_stats = complexity_analyzer.analyze_csv_complexity(df)
            is_sample = True
            logger.info("Large sample dataset reduced", extra={
                'operation': 'sample_dataset_sampling',
                'load_id': load_id,
                'file_name': filename,
                'original_rows': row_count,
                'sampled_rows': 1000
            })
        else:
            sample_df = df
            full_stats = complexity_analyzer.analyze_csv_complexity(df)
            is_sample = False
        
        total_time = time.time() - start_time
        logger.info("Sample dataset load completed successfully", extra={
            'operation': 'sample_dataset_load_complete',
            'load_id': load_id,
            'file_name': filename,
            'row_count': row_count,
            'displayed_rows': len(sample_df),
            'is_sample': is_sample,
            'processing_time': total_time
        })
        
        # Clean data to handle NaN values and ensure JSON serializable
        cleaned_df = sample_df.fillna('')  # Replace NaN with empty strings
        try:
            data_records = cleaned_df.to_dict('records')
            # Clean any remaining NaN values that might slip through
            for record in data_records:
                for key, value in record.items():
                    if isinstance(value, float) and (value != value):  # Check for NaN
                        record[key] = ''
                    elif str(value) == 'nan':
                        record[key] = ''
        except Exception as e:
            logger.error(f"Error processing sample dataset data: {e}")
            data_records = []

        return jsonify({
            "success": True,
            "data": data_records,
            "full_stats": full_stats,
            "is_sample": is_sample,
            "original_row_count": row_count
        })
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("Sample dataset load failed", extra={
            'operation': 'sample_dataset_load_failed',
            'load_id': load_id,
            'file_name': filename,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/api/upload-csv', methods=['POST'])
def upload_csv():
    """Upload and process CSV file"""
    start_time = time.time()
    upload_id = f"upload_{int(time.time() * 1000)}"
    
    logger.info("File upload started", extra={
        'operation': 'file_upload_start',
        'upload_id': upload_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        # Check if file is present
        if 'file' not in request.files:
            logger.warning("Upload failed: No file provided", extra={
                'operation': 'file_upload_validation_failed',
                'upload_id': upload_id,
                'error_type': 'no_file_provided'
            })
            return jsonify({
                "success": False,
                "error": "No file provided"
            }), 400
        
        file = request.files['file']
        
        # Check if file is selected
        if file.filename == '':
            logger.warning("Upload failed: No file selected", extra={
                'operation': 'file_upload_validation_failed',
                'upload_id': upload_id,
                'error_type': 'no_file_selected'
            })
            return jsonify({
                "success": False,
                "error": "No file selected"
            }), 400
        
        # Log file details
        logger.info("File validation started", extra={
            'operation': 'file_validation',
            'upload_id': upload_id,
            'original_filename': file.filename,
            'content_length': request.content_length
        })
        
        # Check if file is allowed
        if not allowed_file(file.filename):
            logger.warning("Upload failed: Invalid file type", extra={
                'operation': 'file_upload_validation_failed',
                'upload_id': upload_id,
                'file_name': file.filename,
                'error_type': 'invalid_file_type'
            })
            return jsonify({
                "success": False,
                "error": "File type not allowed. Only CSV files are supported."
            }), 400
        
        # Secure filename and save
        filename = secure_filename(file.filename)
        try:
            # Use datetime directly to avoid pandas NaN issues
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        except Exception:
            # Ultimate fallback with basic timestamp
            import time
            timestamp = str(int(time.time()))
        unique_filename = f"{timestamp}_{filename}"
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
        
        logger.info("Saving file to disk", extra={
            'operation': 'file_save_start',
            'upload_id': upload_id,
            'file_name': filename,
            'unique_filename': unique_filename,
            'file_path': file_path
        })
        
        file.save(file_path)
        file_size = os.path.getsize(file_path)
        
        logger.info("File saved successfully", extra={
            'operation': 'file_save_complete',
            'upload_id': upload_id,
            'file_name': filename,
            'file_size': file_size
        })
        
        # Process the CSV file
        try:
            logger.info("Starting CSV processing", extra={
                'operation': 'csv_processing_start',
                'upload_id': upload_id,
                'file_name': filename,
                'file_size': file_size
            })
            
            processing_start = time.time()
            try:
                df = pd.read_csv(file_path)
            except pd.errors.EmptyDataError:
                raise ValueError("The CSV file is empty or contains no data")
            except pd.errors.ParserError as e:
                raise ValueError(f"CSV parsing error: {str(e)}")
            except UnicodeDecodeError as e:
                raise ValueError(f"File encoding error: {str(e)}. Please ensure the file is saved with UTF-8 encoding")
            except Exception as e:
                raise ValueError(f"Error reading CSV file: {str(e)}")
            
            row_count = len(df)
            column_count = len(df.columns)
            
            # Validate that we have actual data
            if row_count == 0:
                raise ValueError("The CSV file contains no data rows")
            if column_count == 0:
                raise ValueError("The CSV file contains no columns")
            
            logger.info("CSV file loaded successfully", extra={
                'operation': 'csv_load_complete',
                'upload_id': upload_id,
                'file_name': filename,
                'row_count': row_count,
                'column_count': column_count,
                'processing_time': time.time() - processing_start
            })
            
            # Analyze complexity
            complexity_start = time.time()
            complexity_stats = complexity_analyzer.analyze_csv_complexity(df)
            complexity_time = time.time() - complexity_start
            
            logger.info("Complexity analysis completed", extra={
                'operation': 'complexity_analysis_complete',
                'upload_id': upload_id,
                'file_name': filename,
                'complexity_score': complexity_stats.get('estimated_complexity', 'unknown'),
                'processing_time': complexity_time
            })
            
            # Create sample for preview (first 100 rows)
            preview_data = df.head(100).fillna('').to_dict('records')
            
            # Store file info
            file_info = {
                'original_filename': file.filename,
                'stored_filename': unique_filename,
                'upload_timestamp': timestamp,
                'file_size': file_size,
                'rows': row_count,
                'columns': column_count,
                'complexity_stats': complexity_stats
            }
            
            total_time = time.time() - start_time
            logger.info("File upload completed successfully", extra={
                'operation': 'file_upload_complete',
                'upload_id': upload_id,
                'file_name': filename,
                'file_size': file_size,
                'row_count': row_count,
                'column_count': column_count,
                'processing_time': total_time
            })
            
            return jsonify({
                "success": True,
                "file_info": file_info,
                "preview": preview_data,
                "message": f"File uploaded successfully. {row_count} rows, {column_count} columns."
            })
            
        except Exception as e:
            # Remove file if processing failed
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info("Cleaned up failed upload file", extra={
                    'operation': 'file_cleanup',
                    'upload_id': upload_id,
                    'file_path': file_path
                })
            
            logger.error("CSV processing failed", extra={
                'operation': 'csv_processing_failed',
                'upload_id': upload_id,
                'file_name': filename,
                'error_type': type(e).__name__,
                'error_message': str(e),
                'traceback': traceback.format_exc()
            })
            
            return jsonify({
                "success": False,
                "error": f"Error processing CSV file: {str(e)}"
            }), 400
            
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("File upload failed", extra={
            'operation': 'file_upload_failed',
            'upload_id': upload_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/uploaded-files', methods=['GET'])
def get_uploaded_files():
    """Get list of uploaded files"""
    start_time = time.time()
    request_id = f"list_files_{int(time.time() * 1000)}"
    
    logger.info("File listing started", extra={
        'operation': 'file_list_start',
        'request_id': request_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        upload_dir = app.config['UPLOAD_FOLDER']
        files = []
        processed_files = 0
        failed_files = 0
        
        for filename in os.listdir(upload_dir):
            if filename.endswith('.csv'):
                file_path = os.path.join(upload_dir, filename)
                
                try:
                    logger.debug("Processing file for listing", extra={
                        'operation': 'file_list_process',
                        'request_id': request_id,
                        'file_name': filename
                    })
                    
                    # Get basic file info
                    file_size = os.path.getsize(file_path)
                    df = pd.read_csv(file_path, nrows=5)  # Just for column info
                    full_df = pd.read_csv(file_path)
                    
                    # Analyze complexity
                    complexity_stats = complexity_analyzer.analyze_csv_complexity(full_df)
                    
                    # Extract timestamp from filename
                    parts = filename.split('_', 2)
                    timestamp = f"{parts[0]}_{parts[1]}" if len(parts) >= 2 else "unknown"
                    original_name = '_'.join(parts[2:]) if len(parts) > 2 else filename
                    
                    # Clean preview data to handle NaN values and ensure JSON serializable
                    preview_df = df.fillna('')  # Replace NaN with empty strings
                    preview_data = []
                    try:
                        preview_data = preview_df.to_dict('records')
                        # Clean any remaining NaN values that might slip through
                        for record in preview_data:
                            for key, value in record.items():
                                if isinstance(value, float) and (value != value):  # Check for NaN
                                    record[key] = ''
                                elif str(value) == 'nan':
                                    record[key] = ''
                    except Exception as e:
                        logger.warning(f"Error processing preview data: {e}")
                        preview_data = []
                    
                    files.append({
                        'filename': filename,
                        'original_name': original_name,
                        'upload_time': timestamp,
                        'size_bytes': file_size,
                        'rows': len(full_df),
                        'columns': len(full_df.columns),
                        'complexity': complexity_stats['estimated_complexity'],
                        'preview': preview_data
                    })
                    
                    processed_files += 1
                    
                except Exception as e:
                    failed_files += 1
                    logger.warning("Failed to process file in listing", extra={
                        'operation': 'file_list_process_failed',
                        'request_id': request_id,
                        'file_name': filename,
                        'error_type': type(e).__name__,
                        'error_message': str(e)
                    })
                    continue
        
        # Sort by upload time (newest first)
        files.sort(key=lambda x: x['upload_time'], reverse=True)
        
        total_time = time.time() - start_time
        logger.info("File listing completed", extra={
            'operation': 'file_list_complete',
            'request_id': request_id,
            'files_found': len(files),
            'processed_files': processed_files,
            'failed_files': failed_files,
            'processing_time': total_time
        })
        
        return jsonify({
            "success": True,
            "files": files
        })
        
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("File listing failed", extra={
            'operation': 'file_list_failed',
            'request_id': request_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/load-uploaded-file/<filename>', methods=['GET'])
def load_uploaded_file(filename):
    """Load data from an uploaded file"""
    start_time = time.time()
    load_id = f"load_{int(time.time() * 1000)}"
    
    logger.info("File load started", extra={
        'operation': 'file_load_start',
        'load_id': load_id,
        'file_name': filename,
        'remote_addr': request.remote_addr,
        'max_rows_requested': request.args.get('max_rows', 1000)
    })
    
    try:
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
        if not os.path.exists(file_path):
            logger.warning("File load failed: File not found", extra={
                'operation': 'file_load_failed',
                'load_id': load_id,
                'file_name': filename,
                'file_path': file_path,
                'error_type': 'file_not_found'
            })
            return jsonify({
                "success": False,
                "error": "File not found"
            }), 404
        
        file_size = os.path.getsize(file_path)
        logger.info("Loading CSV file", extra={
            'operation': 'csv_load_start',
            'load_id': load_id,
            'file_name': filename,
            'file_size': file_size
        })
        
        # Load the CSV file
        load_start = time.time()
        df = pd.read_csv(file_path)
        load_time = time.time() - load_start
        
        row_count = len(df)
        column_count = len(df.columns)
        
        logger.info("CSV file loaded", extra={
            'operation': 'csv_load_complete',
            'load_id': load_id,
            'file_name': filename,
            'row_count': row_count,
            'column_count': column_count,
            'processing_time': load_time
        })
        
        # For large files, return a sample
        max_rows = int(request.args.get('max_rows', 1000))
        if row_count > max_rows:
            sample_df = df.sample(n=max_rows, random_state=42)
            is_sample = True
            logger.info("Large dataset sampled", extra={
                'operation': 'dataset_sampling',
                'load_id': load_id,
                'file_name': filename,
                'original_rows': row_count,
                'sampled_rows': max_rows
            })
        else:
            sample_df = df
            is_sample = False
        
        # Get complexity stats
        complexity_start = time.time()
        complexity_stats = complexity_analyzer.analyze_csv_complexity(df)
        complexity_time = time.time() - complexity_start
        
        logger.info("Complexity analysis completed", extra={
            'operation': 'complexity_analysis_complete',
            'load_id': load_id,
            'file_name': filename,
            'complexity_score': complexity_stats.get('estimated_complexity', 'unknown'),
            'processing_time': complexity_time
        })
        
        total_time = time.time() - start_time
        logger.info("File load completed successfully", extra={
            'operation': 'file_load_complete',
            'load_id': load_id,
            'file_name': filename,
            'row_count': row_count,
            'displayed_rows': len(sample_df),
            'is_sample': is_sample,
            'processing_time': total_time
        })
        
        # Clean data to handle NaN values and ensure JSON serializable
        cleaned_df = sample_df.fillna('')  # Replace NaN with empty strings
        try:
            data_records = cleaned_df.to_dict('records')
            # Clean any remaining NaN values that might slip through
            for record in data_records:
                for key, value in record.items():
                    if isinstance(value, float) and (value != value):  # Check for NaN
                        record[key] = ''
                    elif str(value) == 'nan':
                        record[key] = ''
        except Exception as e:
            logger.error(f"Error processing uploaded file data: {e}")
            data_records = []

        return jsonify({
            "success": True,
            "data": data_records,
            "complexity_stats": complexity_stats,
            "is_sample": is_sample,
            "total_rows": row_count,
            "displayed_rows": len(sample_df)
        })
        
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("File load failed", extra={
            'operation': 'file_load_failed',
            'load_id': load_id,
            'file_name': filename,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/delete-uploaded-file/<filename>', methods=['DELETE'])
def delete_uploaded_file(filename):
    """Delete an uploaded file"""
    delete_id = f"delete_{int(time.time() * 1000)}"
    
    logger.info("File deletion started", extra={
        'operation': 'file_delete_start',
        'delete_id': delete_id,
        'file_name': filename,
        'remote_addr': request.remote_addr
    })
    
    try:
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
        if not os.path.exists(file_path):
            logger.warning("File deletion failed: File not found", extra={
                'operation': 'file_delete_failed',
                'delete_id': delete_id,
                'file_name': filename,
                'file_path': file_path,
                'error_type': 'file_not_found'
            })
            return jsonify({
                "success": False,
                "error": "File not found"
            }), 404
        
        # Get file info before deletion
        file_size = os.path.getsize(file_path)
        
        logger.info("Deleting file", extra={
            'operation': 'file_delete_execute',
            'delete_id': delete_id,
            'file_name': filename,
            'file_path': file_path,
            'file_size': file_size
        })
        
        os.remove(file_path)
        
        logger.info("File deleted successfully", extra={
            'operation': 'file_delete_complete',
            'delete_id': delete_id,
            'file_name': filename,
            'file_size': file_size
        })
        
        return jsonify({
            "success": True,
            "message": "File deleted successfully"
        })
        
    except Exception as e:
        logger.error("File deletion failed", extra={
            'operation': 'file_delete_failed',
            'delete_id': delete_id,
            'file_name': filename,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/generate-large-dataset', methods=['POST'])
def generate_large_dataset():
    """Generate large synthetic dataset to demonstrate Spark's capabilities"""
    start_time = time.time()
    generation_id = f"generate_{int(time.time() * 1000)}"
    
    logger.info("Large dataset generation started", extra={
        'operation': 'large_dataset_generation_start',
        'generation_id': generation_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        data = request.json
        num_rows = data.get('num_rows', 100000)
        num_partitions = data.get('num_partitions', 4)
        include_nulls = data.get('include_nulls', True)
        
        # Limit to reasonable size for demo
        num_rows = min(num_rows, 1000000)  # Max 1M rows
        
        logger.info("Dataset generation parameters", extra={
            'operation': 'dataset_generation_parameters',
            'generation_id': generation_id,
            'num_rows': num_rows,
            'num_partitions': num_partitions,
            'include_nulls': include_nulls
        })
        
        # Generate large dataset using Spark
        generation_start = time.time()
        
        # Create RDD for parallel generation
        def generate_row(seed):
            import random
            import math
            random.seed(seed)
            
            base_val = random.randint(1, 1000)
            return {
                'id': seed,
                'value': base_val,
                'category': f"cat_{base_val % 10}",
                'score': round(random.uniform(0, 100), 2),
                'timestamp': f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
                'amount': round(random.uniform(10, 10000), 2),
                'is_active': random.choice([True, False]),
                'description': f"Item {seed} in category {base_val % 10}",
                'latitude': round(random.uniform(-90, 90), 6),
                'longitude': round(random.uniform(-180, 180), 6),
                'rating': random.randint(1, 5),
                'sales': random.randint(0, 1000),
                'profit_margin': round(random.uniform(0.1, 0.5), 3),
                'region': random.choice(['North', 'South', 'East', 'West', 'Central']),
                'null_field': None if include_nulls and random.random() < 0.1 else f"value_{seed}"
            }
        
        # Generate data in parallel using Spark
        rdd = spark.sparkContext.parallelize(range(num_rows), num_partitions)
        data_rdd = rdd.map(generate_row)
        
        # Convert to DataFrame
        df = spark.createDataFrame(data_rdd)
        
        generation_time = time.time() - generation_start
        
        logger.info("Large dataset generated", extra={
            'operation': 'dataset_generation_complete',
            'generation_id': generation_id,
            'rows_generated': num_rows,
            'partitions': num_partitions,
            'generation_time': generation_time
        })
        
        # Get basic statistics
        stats_start = time.time()
        row_count = df.count()
        schema_info = [{"name": field.name, "type": field.dataType.typeName()} 
                      for field in df.schema.fields]
        
        # Sample data for preview
        sample_data = df.sample(0.001, seed=42).limit(100).collect()
        preview_data = [row.asDict() for row in sample_data]
        
        stats_time = time.time() - stats_start
        
        # Store dataset in memory for subsequent operations
        df.cache()
        df.createOrReplaceTempView("large_dataset")
        
        total_time = time.time() - start_time
        logger.info("Large dataset generation completed", extra={
            'operation': 'large_dataset_generation_complete',
            'generation_id': generation_id,
            'row_count': row_count,
            'schema_fields': len(schema_info),
            'total_time': total_time
        })
        
        return jsonify({
            "success": True,
            "dataset_info": {
                "row_count": row_count,
                "schema": schema_info,
                "partitions": num_partitions,
                "generation_time": generation_time,
                "cached": True
            },
            "preview": preview_data[:20],
            "message": f"Generated {row_count:,} rows in {generation_time:.2f} seconds"
        })
        
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("Large dataset generation failed", extra={
            'operation': 'large_dataset_generation_failed',
            'generation_id': generation_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/performance-comparison', methods=['POST'])
def performance_comparison():
    """Compare Spark vs Pandas performance on various operations"""
    start_time = time.time()
    comparison_id = f"comparison_{int(time.time() * 1000)}"
    
    logger.info("Performance comparison started", extra={
        'operation': 'performance_comparison_start',
        'comparison_id': comparison_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        data = request.json
        operation_type = data.get('operation', 'aggregation')
        data_size = data.get('data_size', 100000)
        
        logger.info("Performance comparison parameters", extra={
            'operation': 'performance_comparison_parameters',
            'comparison_id': comparison_id,
            'operation_type': operation_type,
            'data_size': data_size
        })
        
        results = {}
        
        # Generate test data
        def generate_test_row(seed):
            import random
            random.seed(seed)
            return {
                'id': seed,
                'category': f"cat_{seed % 100}",
                'value': random.uniform(0, 1000),
                'flag': random.choice([True, False])
            }
        
        # Spark implementation
        spark_start = time.time()
        spark_rdd = spark.sparkContext.parallelize(range(data_size), 8)
        spark_data_rdd = spark_rdd.map(generate_test_row)
        spark_df = spark.createDataFrame(spark_data_rdd)
        spark_df.cache()  # Cache for fair comparison
        
        if operation_type == 'aggregation':
            spark_result = spark_df.groupBy('category').agg(
                avg('value').alias('avg_value'),
                count('*').alias('count'),
                sum('value').alias('sum_value')
            ).collect()
        elif operation_type == 'filtering':
            spark_result = spark_df.filter(col('value') > 500).count()
        elif operation_type == 'join':
            # Create second dataset for join
            spark_df2 = spark.createDataFrame(
                spark.sparkContext.parallelize(range(0, data_size, 2))
                .map(lambda x: {'id': x, 'extra': f'extra_{x}'})
            )
            spark_result = spark_df.join(spark_df2, 'id').count()
        else:
            spark_result = spark_df.count()
        
        spark_time = time.time() - spark_start
        
        # Pandas implementation
        pandas_start = time.time()
        import pandas as pd
        pandas_data = [generate_test_row(i) for i in range(data_size)]
        pandas_df = pd.DataFrame(pandas_data)
        
        if operation_type == 'aggregation':
            pandas_result = pandas_df.groupby('category').agg({
                'value': ['mean', 'count', 'sum']
            }).values.tolist()
        elif operation_type == 'filtering':
            pandas_result = len(pandas_df[pandas_df['value'] > 500])
        elif operation_type == 'join':
            pandas_df2 = pd.DataFrame([
                {'id': i, 'extra': f'extra_{i}'} for i in range(0, data_size, 2)
            ])
            pandas_result = len(pandas_df.merge(pandas_df2, on='id'))
        else:
            pandas_result = len(pandas_df)
        
        pandas_time = time.time() - pandas_start
        
        # Calculate performance metrics
        speedup = pandas_time / spark_time if spark_time > 0 else 0
        
        results = {
            'spark': {
                'execution_time': spark_time,
                'result_size': len(spark_result) if hasattr(spark_result, '__len__') else 1
            },
            'pandas': {
                'execution_time': pandas_time,
                'result_size': len(pandas_result) if hasattr(pandas_result, '__len__') else 1
            },
            'comparison': {
                'speedup': speedup,
                'winner': 'Spark' if spark_time < pandas_time else 'Pandas',
                'data_size': data_size,
                'operation': operation_type
            }
        }
        
        total_time = time.time() - start_time
        logger.info("Performance comparison completed", extra={
            'operation': 'performance_comparison_complete',
            'comparison_id': comparison_id,
            'spark_time': spark_time,
            'pandas_time': pandas_time,
            'speedup': speedup,
            'winner': results['comparison']['winner'],
            'total_time': total_time
        })
        
        return jsonify({
            "success": True,
            "results": results,
            "analysis": {
                "recommendation": f"For {operation_type} on {data_size:,} rows, {results['comparison']['winner']} performed better",
                "notes": [
                    "Spark overhead more noticeable on smaller datasets",
                    "Spark advantages increase with data size and complexity",
                    "Memory caching benefits repeated operations",
                    "Pandas better for single-machine operations under ~1M rows"
                ]
            }
        })
        
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("Performance comparison failed", extra={
            'operation': 'performance_comparison_failed',
            'comparison_id': comparison_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/complex-etl', methods=['POST'])
def complex_etl():
    """Demonstrate complex ETL pipeline with multiple transformations"""
    start_time = time.time()
    etl_id = f"etl_{int(time.time() * 1000)}"
    
    logger.info("Complex ETL pipeline started", extra={
        'operation': 'complex_etl_start',
        'etl_id': etl_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        from pyspark.sql.functions import when, isnan, isnull, col, regexp_replace, split, explode
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
        
        data = request.json
        source_type = data.get('source_type', 'sales_data')
        apply_transformations = data.get('transformations', True)
        
        logger.info("ETL pipeline parameters", extra={
            'operation': 'etl_parameters',
            'etl_id': etl_id,
            'source_type': source_type,
            'apply_transformations': apply_transformations
        })
        
        # Generate complex source data
        def generate_messy_sales_data(seed):
            import random
            import datetime
            random.seed(seed)
            
            # Simulate messy real-world data
            categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports']
            regions = ['North America', 'Europe', 'Asia Pacific', 'South America', 'Africa']
            
            return {
                'transaction_id': f"TXN-{seed:06d}",
                'product_name': f"Product {random.randint(1, 1000)}",
                'category': random.choice(categories) if random.random() > 0.05 else None,  # 5% missing
                'price': round(random.uniform(10, 1000), 2) if random.random() > 0.02 else None,  # 2% missing
                'quantity': random.randint(1, 10),
                'customer_email': f"customer{seed}@{random.choice(['gmail.com', 'yahoo.com', 'company.com'])}",
                'region': random.choice(regions),
                'sale_date': f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
                'discount_pct': random.uniform(0, 0.3) if random.random() > 0.7 else 0,  # 30% have discounts
                'payment_method': random.choice(['Credit Card', 'PayPal', 'Bank Transfer', 'Cash']),
                'customer_segment': random.choice(['Premium', 'Standard', 'Basic']),
                'notes': f"Sale notes for transaction {seed}" if random.random() > 0.8 else "",  # 20% have notes
                'raw_data': f"messy_field_with_pipes|value_{seed}|more_data",  # Needs parsing
                'corrupted_field': 'valid_data' if random.random() > 0.1 else f"ERROR_{seed}"  # 10% corrupted
            }
        
        # Create source DataFrame
        source_start = time.time()
        num_records = 50000
        source_rdd = spark.sparkContext.parallelize(range(num_records), 8)
        source_data_rdd = source_rdd.map(generate_messy_sales_data)
        raw_df = spark.createDataFrame(source_data_rdd)
        source_time = time.time() - source_start
        
        logger.info("Source data generated", extra={
            'operation': 'source_data_complete',
            'etl_id': etl_id,
            'records_generated': num_records,
            'generation_time': source_time
        })
        
        if apply_transformations:
            # ETL Transformations
            transform_start = time.time()
            
            # 1. Data Cleaning
            cleaned_df = raw_df \
                .filter(col('price').isNotNull()) \
                .filter(col('category').isNotNull()) \
                .filter(~col('corrupted_field').startswith('ERROR_'))
            
            # 2. Data Enrichment
            enriched_df = cleaned_df \
                .withColumn('total_amount', col('price') * col('quantity')) \
                .withColumn('discounted_amount', 
                           col('total_amount') * (1 - col('discount_pct'))) \
                .withColumn('email_domain', 
                           regexp_replace(col('customer_email'), '.*@', '')) \
                .withColumn('sale_month', 
                           split(col('sale_date'), '-')[1].cast('int')) \
                .withColumn('is_high_value', 
                           when(col('total_amount') > 500, True).otherwise(False))
            
            # 3. Parse complex fields
            parsed_df = enriched_df \
                .withColumn('parsed_data', split(col('raw_data'), '\\|')) \
                .withColumn('parsed_value', col('parsed_data')[1])
            
            # 4. Aggregations by category and region
            category_summary = parsed_df.groupBy('category', 'region') \
                .agg({
                    'total_amount': 'sum',
                    'discounted_amount': 'sum', 
                    'quantity': 'sum',
                    'transaction_id': 'count'
                }) \
                .withColumnRenamed('sum(total_amount)', 'total_revenue') \
                .withColumnRenamed('sum(discounted_amount)', 'net_revenue') \
                .withColumnRenamed('sum(quantity)', 'total_quantity') \
                .withColumnRenamed('count(transaction_id)', 'transaction_count')
            
            # 5. Customer segment analysis
            segment_analysis = parsed_df.groupBy('customer_segment', 'payment_method') \
                .agg({
                    'total_amount': 'avg',
                    'discount_pct': 'avg',
                    'transaction_id': 'count'
                }) \
                .withColumnRenamed('avg(total_amount)', 'avg_transaction_value') \
                .withColumnRenamed('avg(discount_pct)', 'avg_discount') \
                .withColumnRenamed('count(transaction_id)', 'segment_transactions')
            
            transform_time = time.time() - transform_start
            
            # Collect results
            collect_start = time.time()
            final_df = parsed_df
            category_results = category_summary.collect()
            segment_results = segment_analysis.collect()
            
            # Quality metrics
            original_count = raw_df.count()
            cleaned_count = final_df.count()
            data_quality = (cleaned_count / original_count) * 100
            
            collect_time = time.time() - collect_start
            
            logger.info("ETL transformations completed", extra={
                'operation': 'etl_transformations_complete',
                'etl_id': etl_id,
                'original_records': original_count,
                'cleaned_records': cleaned_count,
                'data_quality_pct': data_quality,
                'transform_time': transform_time,
                'collect_time': collect_time
            })
            
            # Sample of transformed data
            sample_data = final_df.sample(0.002).limit(20).collect()
            preview_data = [row.asDict() for row in sample_data]
            
            results = {
                'pipeline_stats': {
                    'original_records': original_count,
                    'cleaned_records': cleaned_count,
                    'data_quality_percentage': round(data_quality, 2),
                    'records_filtered': original_count - cleaned_count
                },
                'category_summary': [row.asDict() for row in category_results],
                'segment_analysis': [row.asDict() for row in segment_results],
                'sample_data': preview_data,
                'performance': {
                    'source_generation_time': source_time,
                    'transformation_time': transform_time,
                    'collection_time': collect_time
                }
            }
        else:
            # Just return raw data stats
            original_count = raw_df.count()
            sample_data = raw_df.sample(0.002).limit(20).collect()
            preview_data = [row.asDict() for row in sample_data]
            
            results = {
                'pipeline_stats': {
                    'original_records': original_count,
                    'transformations_applied': False
                },
                'sample_data': preview_data,
                'performance': {
                    'source_generation_time': source_time
                }
            }
        
        total_time = time.time() - start_time
        logger.info("Complex ETL pipeline completed", extra={
            'operation': 'complex_etl_complete',
            'etl_id': etl_id,
            'total_time': total_time,
            'transformations_applied': apply_transformations
        })
        
        return jsonify({
            "success": True,
            "results": results,
            "message": f"ETL pipeline processed {results['pipeline_stats']['original_records']:,} records"
        })
        
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("Complex ETL pipeline failed", extra={
            'operation': 'complex_etl_failed',
            'etl_id': etl_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/streaming-demo', methods=['POST'])
def streaming_demo():
    """Demonstrate streaming data processing simulation"""
    start_time = time.time()
    stream_id = f"stream_{int(time.time() * 1000)}"
    
    logger.info("Streaming demo started", extra={
        'operation': 'streaming_demo_start',
        'stream_id': stream_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        from pyspark.sql.functions import window, col, count, avg, sum as spark_sum
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
        import random
        from datetime import datetime, timedelta
        
        data = request.json
        duration_seconds = data.get('duration', 30)  # Simulate streaming for X seconds
        batch_size = data.get('batch_size', 1000)
        
        logger.info("Streaming demo parameters", extra={
            'operation': 'streaming_demo_parameters',
            'stream_id': stream_id,
            'duration_seconds': duration_seconds,
            'batch_size': batch_size
        })
        
        # Simulate streaming data batches
        streaming_results = []
        batch_count = 0
        
        for batch_num in range(min(duration_seconds // 2, 15)):  # Max 15 batches for demo
            batch_start = time.time()
            
            # Generate streaming batch data
            def generate_streaming_record(seed):
                base_time = datetime.now() - timedelta(seconds=random.randint(0, 300))
                return {
                    'event_id': f"evt_{seed}_{batch_num}",
                    'user_id': f"user_{random.randint(1, 1000)}",
                    'event_type': random.choice(['click', 'view', 'purchase', 'signup', 'logout']),
                    'value': round(random.uniform(1, 100), 2),
                    'timestamp': base_time,
                    'session_id': f"session_{random.randint(1, 100)}",
                    'device': random.choice(['mobile', 'desktop', 'tablet']),
                    'location': random.choice(['US', 'UK', 'DE', 'FR', 'JP'])
                }
            
            # Create batch DataFrame
            batch_rdd = spark.sparkContext.parallelize(
                [generate_streaming_record(i) for i in range(batch_size)], 4
            )
            batch_df = spark.createDataFrame(batch_rdd)
            
            # Process streaming batch (simulating real-time analytics)
            batch_analytics = batch_df.groupBy('event_type', 'device').agg(
                count('*').alias('event_count'),
                avg('value').alias('avg_value'),
                spark_sum('value').alias('total_value')
            ).collect()
            
            # User activity analysis
            user_activity = batch_df.groupBy('user_id').agg(
                count('*').alias('activity_count'),
                spark_sum('value').alias('user_total_value')
            ).filter(col('activity_count') > 2).collect()
            
            batch_time = time.time() - batch_start
            batch_count += 1
            
            streaming_results.append({
                'batch_number': batch_num + 1,
                'batch_size': batch_size,
                'processing_time': batch_time,
                'analytics': [row.asDict() for row in batch_analytics],
                'high_activity_users': len(user_activity),
                'timestamp': datetime.now().isoformat()
            })
            
            logger.info("Streaming batch processed", extra={
                'operation': 'streaming_batch_complete',
                'stream_id': stream_id,
                'batch_number': batch_num + 1,
                'batch_size': batch_size,
                'processing_time': batch_time,
                'analytics_records': len(batch_analytics)
            })
            
            # Simulate real-time delay
            time.sleep(1)
        
        # Aggregate streaming results
        total_events = sum(result['batch_size'] for result in streaming_results)
        avg_processing_time = sum(result['processing_time'] for result in streaming_results) / len(streaming_results)
        
        total_time = time.time() - start_time
        logger.info("Streaming demo completed", extra={
            'operation': 'streaming_demo_complete',
            'stream_id': stream_id,
            'total_batches': batch_count,
            'total_events': total_events,
            'avg_processing_time': avg_processing_time,
            'total_time': total_time
        })
        
        return jsonify({
            "success": True,
            "streaming_results": {
                "total_batches": batch_count,
                "total_events_processed": total_events,
                "avg_batch_processing_time": round(avg_processing_time, 3),
                "batches": streaming_results
            },
            "insights": {
                "throughput_events_per_second": round(total_events / total_time, 2),
                "scalability_note": "Spark Streaming can handle thousands of events per second",
                "use_cases": [
                    "Real-time fraud detection",
                    "Live dashboard updates", 
                    "IoT sensor data processing",
                    "Social media sentiment analysis"
                ]
            },
            "message": f"Processed {total_events:,} events across {batch_count} batches"
        })
        
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("Streaming demo failed", extra={
            'operation': 'streaming_demo_failed',
            'stream_id': stream_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/advanced-ml-pipeline', methods=['POST'])
def advanced_ml_pipeline():
    """Demonstrate advanced ML pipeline with feature engineering and model evaluation"""
    start_time = time.time()
    pipeline_id = f"ml_pipeline_{int(time.time() * 1000)}"
    
    logger.info("Advanced ML pipeline started", extra={
        'operation': 'advanced_ml_pipeline_start',
        'pipeline_id': pipeline_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        from pyspark.ml import Pipeline
        from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
        from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
        from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
        from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
        from pyspark.sql.functions import when, col, rand
        
        data = request.json
        model_type = data.get('model_type', 'random_forest')
        dataset_size = data.get('dataset_size', 10000)
        use_cross_validation = data.get('cross_validation', True)
        
        logger.info("ML pipeline parameters", extra={
            'operation': 'ml_pipeline_parameters',
            'pipeline_id': pipeline_id,
            'model_type': model_type,
            'dataset_size': dataset_size,
            'use_cross_validation': use_cross_validation
        })
        
        # Generate realistic customer churn dataset
        def generate_customer_data(seed):
            import random
            random.seed(seed)
            
            age = random.randint(18, 80)
            income = random.gauss(50000, 15000)
            tenure = random.randint(1, 120)  # months
            monthly_charges = random.gauss(70, 20)
            total_charges = monthly_charges * tenure + random.gauss(0, 100)
            
            # Create some realistic patterns for churn
            churn_probability = 0.1  # Base probability
            if monthly_charges > 80: churn_probability += 0.2
            if tenure < 12: churn_probability += 0.3
            if age < 25: churn_probability += 0.1
            if income < 30000: churn_probability += 0.15
            
            churn = 1 if random.random() < churn_probability else 0
            
            return {
                'customer_id': f"cust_{seed}",
                'age': age,
                'income': max(income, 15000),  # Minimum income
                'tenure_months': tenure,
                'monthly_charges': max(monthly_charges, 20),  # Minimum charge
                'total_charges': max(total_charges, monthly_charges),
                'contract_type': random.choice(['Month-to-month', 'One year', 'Two year']),
                'payment_method': random.choice(['Electronic check', 'Mailed check', 'Bank transfer', 'Credit card']),
                'internet_service': random.choice(['DSL', 'Fiber optic', 'No']),
                'online_security': random.choice(['Yes', 'No', 'No internet service']),
                'tech_support': random.choice(['Yes', 'No', 'No internet service']),
                'device_protection': random.choice(['Yes', 'No', 'No internet service']),
                'paperless_billing': random.choice(['Yes', 'No']),
                'senior_citizen': 1 if age >= 65 else 0,
                'partner': random.choice(['Yes', 'No']),
                'dependents': random.choice(['Yes', 'No']),
                'churn': churn
            }
        
        # Create dataset
        dataset_start = time.time()
        customer_rdd = spark.sparkContext.parallelize(range(dataset_size), 8)
        customer_data_rdd = customer_rdd.map(generate_customer_data)
        df = spark.createDataFrame(customer_data_rdd)
        dataset_time = time.time() - dataset_start
        
        logger.info("Dataset generated", extra={
            'operation': 'dataset_generation_complete',
            'pipeline_id': pipeline_id,
            'dataset_size': dataset_size,
            'generation_time': dataset_time
        })
        
        # Feature Engineering Pipeline
        feature_start = time.time()
        
        # String indexing for categorical variables
        categorical_columns = ['contract_type', 'payment_method', 'internet_service', 
                              'online_security', 'tech_support', 'device_protection', 
                              'paperless_billing', 'partner', 'dependents']
        
        indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_index") 
                   for col in categorical_columns]
        
        # One-hot encoding
        encoders = [OneHotEncoder(inputCols=[f"{col}_index"], outputCols=[f"{col}_encoded"]) 
                   for col in categorical_columns]
        
        # Assemble features
        feature_columns = (['age', 'income', 'tenure_months', 'monthly_charges', 
                          'total_charges', 'senior_citizen'] + 
                         [f"{col}_encoded" for col in categorical_columns])
        
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features_raw")
        scaler = StandardScaler(inputCol="features_raw", outputCol="features")
        
        # Create ML Pipeline stages
        pipeline_stages = indexers + encoders + [assembler, scaler]
        
        # Add model to pipeline
        if model_type == 'random_forest':
            classifier = RandomForestClassifier(featuresCol="features", labelCol="churn", 
                                              numTrees=20, seed=42)
        else:
            classifier = LogisticRegression(featuresCol="features", labelCol="churn")
        
        pipeline_stages.append(classifier)
        
        # Create pipeline
        pipeline = Pipeline(stages=pipeline_stages)
        
        feature_time = time.time() - feature_start
        
        logger.info("Feature engineering pipeline created", extra={
            'operation': 'feature_engineering_complete',
            'pipeline_id': pipeline_id,
            'categorical_features': len(categorical_columns),
            'total_features': len(feature_columns),
            'pipeline_stages': len(pipeline_stages),
            'feature_time': feature_time
        })
        
        # Split data
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        
        # Training
        training_start = time.time()
        
        if use_cross_validation:
            # Cross-validation with hyperparameter tuning
            if model_type == 'random_forest':
                paramGrid = ParamGridBuilder() \
                    .addGrid(classifier.numTrees, [10, 20, 30]) \
                    .addGrid(classifier.maxDepth, [5, 10, 15]) \
                    .build()
            else:
                paramGrid = ParamGridBuilder() \
                    .addGrid(classifier.regParam, [0.01, 0.1, 1.0]) \
                    .addGrid(classifier.elasticNetParam, [0.0, 0.5, 1.0]) \
                    .build()
            
            evaluator = BinaryClassificationEvaluator(labelCol="churn")
            crossval = CrossValidator(estimator=pipeline,
                                    estimatorParamMaps=paramGrid,
                                    evaluator=evaluator,
                                    numFolds=3,
                                    seed=42)
            
            model = crossval.fit(train_df)
            best_model = model.bestModel
        else:
            model = pipeline.fit(train_df)
            best_model = model
        
        training_time = time.time() - training_start
        
        logger.info("Model training completed", extra={
            'operation': 'model_training_complete',
            'pipeline_id': pipeline_id,
            'model_type': model_type,
            'use_cross_validation': use_cross_validation,
            'training_time': training_time
        })
        
        # Evaluation
        eval_start = time.time()
        
        # Make predictions
        train_predictions = best_model.transform(train_df)
        test_predictions = best_model.transform(test_df)
        
        # Evaluate model
        binary_evaluator = BinaryClassificationEvaluator(labelCol="churn")
        multiclass_evaluator = MulticlassClassificationEvaluator(labelCol="churn")
        
        # Training metrics
        train_auc = binary_evaluator.evaluate(train_predictions)
        train_accuracy = multiclass_evaluator.evaluate(train_predictions, {multiclass_evaluator.metricName: "accuracy"})
        train_precision = multiclass_evaluator.evaluate(train_predictions, {multiclass_evaluator.metricName: "weightedPrecision"})
        train_recall = multiclass_evaluator.evaluate(train_predictions, {multiclass_evaluator.metricName: "weightedRecall"})
        
        # Test metrics
        test_auc = binary_evaluator.evaluate(test_predictions)
        test_accuracy = multiclass_evaluator.evaluate(test_predictions, {multiclass_evaluator.metricName: "accuracy"})
        test_precision = multiclass_evaluator.evaluate(test_predictions, {multiclass_evaluator.metricName: "weightedPrecision"})
        test_recall = multiclass_evaluator.evaluate(test_predictions, {multiclass_evaluator.metricName: "weightedRecall"})
        
        eval_time = time.time() - eval_start
        
        # Feature importance (if Random Forest)
        feature_importance = None
        if model_type == 'random_forest':
            rf_model = best_model.stages[-1]
            importances = rf_model.featureImportances.toArray()
            feature_importance = list(zip(feature_columns, importances))
            feature_importance.sort(key=lambda x: x[1], reverse=True)
        
        # Sample predictions
        sample_predictions = test_predictions.select("customer_id", "churn", "prediction", "probability").limit(10).collect()
        
        total_time = time.time() - start_time
        logger.info("Advanced ML pipeline completed", extra={
            'operation': 'advanced_ml_pipeline_complete',
            'pipeline_id': pipeline_id,
            'model_type': model_type,
            'test_accuracy': test_accuracy,
            'test_auc': test_auc,
            'total_time': total_time
        })
        
        results = {
            'model_info': {
                'type': model_type,
                'features_used': len(feature_columns),
                'training_samples': train_df.count(),
                'test_samples': test_df.count(),
                'cross_validation': use_cross_validation
            },
            'performance_metrics': {
                'training': {
                    'accuracy': round(train_accuracy, 4),
                    'auc': round(train_auc, 4),
                    'precision': round(train_precision, 4),
                    'recall': round(train_recall, 4)
                },
                'testing': {
                    'accuracy': round(test_accuracy, 4),
                    'auc': round(test_auc, 4),
                    'precision': round(test_precision, 4),
                    'recall': round(test_recall, 4)
                }
            },
            'feature_importance': feature_importance[:10] if feature_importance else None,
            'sample_predictions': [
                {
                    'customer_id': row['customer_id'],
                    'actual_churn': int(row['churn']),
                    'predicted_churn': int(row['prediction']),
                    'churn_probability': float(row['probability'][1])
                } for row in sample_predictions
            ],
            'timing': {
                'dataset_generation': dataset_time,
                'feature_engineering': feature_time,
                'model_training': training_time,
                'evaluation': eval_time,
                'total_time': total_time
            }
        }
        
        return jsonify({
            "success": True,
            "results": results,
            "insights": {
                "model_quality": "Good" if test_auc > 0.8 else "Fair" if test_auc > 0.7 else "Needs Improvement",
                "overfitting_check": "Good" if abs(train_accuracy - test_accuracy) < 0.1 else "Possible Overfitting",
                "spark_advantages": [
                    "Automated feature engineering pipeline",
                    "Built-in cross-validation and hyperparameter tuning",
                    "Scalable to massive datasets",
                    "Integrated model evaluation metrics"
                ]
            },
            "message": f"ML pipeline completed with {test_accuracy:.1%} accuracy on {test_df.count():,} test samples"
        })
        
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("Advanced ML pipeline failed", extra={
            'operation': 'advanced_ml_pipeline_failed',
            'pipeline_id': pipeline_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/catalyst-explain', methods=['POST'])
def catalyst_explain():
    """Show detailed Catalyst optimizer execution plans and rule applications"""
    start_time = time.time()
    explain_id = f"explain_{int(time.time() * 1000)}"
    
    logger.info("Catalyst explanation started", extra={
        'operation': 'catalyst_explain_start',
        'explain_id': explain_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        # Check if Spark is available - if not, provide mock analysis
        if spark is None:
            logger.warning("Spark not available, providing mock Catalyst analysis", extra={
                'operation': 'mock_catalyst_analysis',
                'explain_id': explain_id
            })
            
            # Generate mock analysis for demonstration
            mock_analysis = generate_mock_catalyst_analysis(query, len(dataset_data) if dataset_data else 0)
            
            total_time = time.time() - start_time
            logger.info("Mock Catalyst explanation completed", extra={
                'operation': 'mock_catalyst_explain_complete',
                'explain_id': explain_id,
                'processing_time': total_time
            })
            
            return jsonify(mock_analysis)
        
        data = request.json
        query = data.get('query', '')
        dataset_data = data.get('data', [])
        
        if not query.strip():
            return jsonify({
                "success": False,
                "error": "Query is required"
            }), 400
        
        logger.info("Creating DataFrame for Catalyst analysis", extra={
            'operation': 'catalyst_dataframe_creation',
            'explain_id': explain_id,
            'query_length': len(query),
            'data_rows': len(dataset_data)
        })
        
        # Create DataFrame from data
        if dataset_data:
            df = spark.createDataFrame(dataset_data)
            df.createOrReplaceTempView("data_table")
        
        # Get detailed execution plan
        sql_df = spark.sql(query)
        
        # Get different plan representations
        plan_analysis = {}
        
        # 1. Logical Plan (parsed)
        try:
            logical_plan = sql_df._jdf.logicalPlan().toString()
            plan_analysis['logical_plan'] = parse_logical_plan(logical_plan)
        except Exception as e:
            plan_analysis['logical_plan'] = f"Error getting logical plan: {str(e)}"
        
        # 2. Optimized Logical Plan
        try:
            optimized_plan = sql_df._jdf.queryExecution().optimizedPlan().toString()
            plan_analysis['optimized_plan'] = parse_optimized_plan(optimized_plan)
        except Exception as e:
            plan_analysis['optimized_plan'] = f"Error getting optimized plan: {str(e)}"
        
        # 3. Physical Plan
        try:
            physical_plan = sql_df._jdf.queryExecution().executedPlan().toString()
            plan_analysis['physical_plan'] = parse_physical_plan(physical_plan)
        except Exception as e:
            plan_analysis['physical_plan'] = f"Error getting physical plan: {str(e)}"
        
        # 4. Get explain output
        import io
        import sys
        
        # Capture explain output
        old_stdout = sys.stdout
        sys.stdout = captured_output = io.StringIO()
        try:
            sql_df.explain('extended')
            explain_output = captured_output.getvalue()
        finally:
            sys.stdout = old_stdout
        
        # 5. Analyze optimization opportunities
        optimization_analysis = analyze_query_optimizations(query, dataset_data)
        
        # 6. Simulate Catalyst rules applied
        catalyst_rules = [
            {
                'rule_name': 'ConstantFolding',
                'description': 'Replaces expressions that can be statically evaluated with their results',
                'applied': 'WHERE 1=1' in query.upper() or 'WHERE 2>1' in query.upper(),
                'impact': 'Eliminates unnecessary condition evaluation'
            },
            {
                'rule_name': 'PredicatePushdown',
                'description': 'Pushes filter conditions down to data sources',
                'applied': 'WHERE' in query.upper(),
                'impact': 'Reduces data read from source by early filtering'
            },
            {
                'rule_name': 'ColumnPruning',
                'description': 'Eliminates unused columns from query execution',
                'applied': 'SELECT *' not in query.upper(),
                'impact': 'Reduces memory usage and I/O'
            },
            {
                'rule_name': 'ProjectionCollapsing',
                'description': 'Combines adjacent projection operations',
                'applied': query.upper().count('SELECT') == 1,
                'impact': 'Reduces intermediate data materialization'
            },
            {
                'rule_name': 'BooleanSimplification',
                'description': 'Simplifies boolean expressions',
                'applied': 'AND' in query.upper() or 'OR' in query.upper(),
                'impact': 'Faster boolean expression evaluation'
            }
        ]
        
        # 7. Performance estimation
        performance_estimation = estimate_query_performance(query, len(dataset_data) if dataset_data else 0)
        
        total_time = time.time() - start_time
        logger.info("Catalyst explanation completed", extra={
            'operation': 'catalyst_explain_complete',
            'explain_id': explain_id,
            'processing_time': total_time
        })
        
        return jsonify({
            "success": True,
            "plan_analysis": plan_analysis,
            "catalyst_rules": catalyst_rules,
            "optimization_analysis": optimization_analysis,
            "performance_estimation": performance_estimation,
            "explain_output": explain_output,
            "query_complexity": len(query.split()) // 3 + 1  # Simple complexity metric
        })
        
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("Catalyst explanation failed", extra={
            'operation': 'catalyst_explain_failed',
            'explain_id': explain_id,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

def parse_logical_plan(plan_str):
    """Parse logical plan string into structured format"""
    lines = plan_str.split('\n')
    parsed_nodes = []
    
    for line in lines:
        if line.strip():
            # Extract operation type and details
            if '+- ' in line or '   ' in line:
                indent_level = (len(line) - len(line.lstrip())) // 3
                operation = line.strip().split('(')[0] if '(' in line else line.strip()
                parsed_nodes.append({
                    'operation': operation,
                    'indent': indent_level,
                    'details': line.strip(),
                    'type': 'logical'
                })
    
    return parsed_nodes

def parse_optimized_plan(plan_str):
    """Parse optimized logical plan"""
    lines = plan_str.split('\n')
    optimizations_found = []
    
    for line in lines:
        if line.strip():
            if any(opt in line.lower() for opt in ['filter', 'project', 'aggregate', 'join']):
                optimizations_found.append({
                    'operation': line.strip(),
                    'optimization_type': 'pushdown' if 'filter' in line.lower() else 'projection',
                    'description': f"Optimized: {line.strip()}"
                })
    
    return optimizations_found

def parse_physical_plan(plan_str):
    """Parse physical execution plan"""
    lines = plan_str.split('\n')
    physical_ops = []
    
    for line in lines:
        if line.strip() and ('Exchange' in line or 'Scan' in line or 'HashAggregate' in line):
            physical_ops.append({
                'operation': line.strip(),
                'cost_estimate': estimate_operation_cost(line),
                'parallelism': extract_parallelism_info(line)
            })
    
    return physical_ops

def analyze_query_optimizations(query, data):
    """Analyze potential optimizations for the query"""
    optimizations = []
    
    # Check for common optimization opportunities
    if 'SELECT *' in query.upper():
        optimizations.append({
            'type': 'column_pruning',
            'description': 'Consider selecting only needed columns instead of SELECT *',
            'impact': 'high',
            'suggestion': 'Replace SELECT * with specific column names'
        })
    
    if 'ORDER BY' in query.upper() and 'LIMIT' not in query.upper():
        optimizations.append({
            'type': 'sort_optimization',
            'description': 'Full sort without limit may be expensive',
            'impact': 'medium',
            'suggestion': 'Consider adding LIMIT if you don\'t need all results'
        })
    
    if query.upper().count('JOIN') > 1:
        optimizations.append({
            'type': 'join_optimization',
            'description': 'Multiple joins detected - consider join order optimization',
            'impact': 'high',
            'suggestion': 'Ensure smaller tables are on the right side of joins'
        })
    
    return optimizations

def estimate_operation_cost(operation_line):
    """Estimate cost of physical operation"""
    if 'Exchange' in operation_line:
        return {'type': 'network', 'cost': 'high', 'reason': 'Data shuffle required'}
    elif 'Scan' in operation_line:
        return {'type': 'io', 'cost': 'medium', 'reason': 'Data source scan'}
    elif 'Aggregate' in operation_line:
        return {'type': 'compute', 'cost': 'medium', 'reason': 'Aggregation computation'}
    else:
        return {'type': 'compute', 'cost': 'low', 'reason': 'Simple operation'}

def extract_parallelism_info(operation_line):
    """Extract parallelism information from operation"""
    if 'Exchange' in operation_line:
        return {'parallel': True, 'reason': 'Data redistribution across partitions'}
    elif 'Scan' in operation_line:
        return {'parallel': True, 'reason': 'Parallel data source reading'}
    else:
        return {'parallel': False, 'reason': 'Sequential operation'}

def estimate_query_performance(query, data_size):
    """Estimate query performance characteristics"""
    complexity_score = 1
    
    # Increase complexity based on operations
    if 'JOIN' in query.upper():
        complexity_score += 3
    if 'GROUP BY' in query.upper():
        complexity_score += 2
    if 'ORDER BY' in query.upper():
        complexity_score += 2
    if 'WINDOW' in query.upper():
        complexity_score += 4
    
    # Estimate based on data size
    base_time = 0.1  # Base execution time in seconds
    size_factor = 1 if data_size <= 10000 else data_size / 10000  # Scaling factor
    
    estimated_time = base_time * complexity_score * size_factor
    
    return {
        'complexity_score': complexity_score,
        'estimated_execution_time': round(estimated_time, 2),
        'bottleneck_prediction': predict_bottleneck(query),
        'scaling_characteristics': analyze_scaling(complexity_score)
    }

def predict_bottleneck(query):
    """Predict likely performance bottlenecks"""
    if 'JOIN' in query.upper() and 'GROUP BY' in query.upper():
        return {'type': 'shuffle', 'description': 'Join followed by aggregation likely to cause data shuffle'}
    elif 'ORDER BY' in query.upper():
        return {'type': 'sort', 'description': 'Global sort may require data shuffle'}
    elif 'DISTINCT' in query.upper():
        return {'type': 'deduplication', 'description': 'Distinct operation requires data comparison'}
    else:
        return {'type': 'io', 'description': 'Data reading likely to be the main cost'}

def analyze_scaling(complexity_score):
    """Analyze how the query scales with data size"""
    if complexity_score <= 2:
        return {'scaling': 'linear', 'description': 'Performance scales linearly with data size'}
    elif complexity_score <= 5:
        return {'scaling': 'n_log_n', 'description': 'Performance scales as O(n log n) due to sorting/grouping'}
    else:
        return {'scaling': 'quadratic', 'description': 'Complex operations may scale quadratically'}

def generate_mock_catalyst_analysis(query, data_size):
    """Generate mock Catalyst analysis when Spark is not available"""
    
    # Analyze query structure
    query_upper = query.upper()
    has_where = 'WHERE' in query_upper
    has_group_by = 'GROUP BY' in query_upper
    has_order_by = 'ORDER BY' in query_upper
    has_join = 'JOIN' in query_upper
    has_subquery = '(' in query and 'SELECT' in query_upper
    has_aggregate = any(func in query_upper for func in ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN'])
    
    # Mock logical plan
    logical_plan = []
    if has_order_by:
        logical_plan.append({
            'operation': 'Sort',
            'details': f"Sort [total_revenue DESC NULLS LAST]",
            'type': 'logical'
        })
    if has_group_by:
        logical_plan.append({
            'operation': 'Aggregate',
            'details': f"Aggregate [category], [count() as transaction_count, avg(price * quantity) as avg_revenue, sum(price * quantity) as total_revenue]",
            'type': 'logical'
        })
    if has_where:
        logical_plan.append({
            'operation': 'Filter',
            'details': f"Filter (price > 100)",
            'type': 'logical'
        })
    logical_plan.append({
        'operation': 'Relation',
        'details': f"Relation [data_table] ({data_size} rows)",
        'type': 'logical'
    })
    
    # Mock optimized plan
    optimized_plan = []
    if has_where:
        optimized_plan.append({
            'operation': 'Filter pushed down',
            'optimization_type': 'pushdown',
            'description': 'Predicate pushdown applied: filter moved closer to data source'
        })
    if has_aggregate:
        optimized_plan.append({
            'operation': 'Aggregate optimization',
            'optimization_type': 'aggregation',
            'description': 'Partial aggregation enabled for better performance'
        })
    
    # Mock physical plan
    physical_plan = []
    if has_order_by:
        physical_plan.append({
            'operation': 'TakeOrderedAndProject',
            'cost_estimate': {'cost': 'high', 'type': 'sort'},
            'parallelism': {'parallel': False}
        })
    if has_group_by:
        physical_plan.append({
            'operation': 'HashAggregate',
            'cost_estimate': {'cost': 'medium', 'type': 'hash'},
            'parallelism': {'parallel': True}
        })
    if has_where:
        physical_plan.append({
            'operation': 'Filter',
            'cost_estimate': {'cost': 'low', 'type': 'filter'},
            'parallelism': {'parallel': True}
        })
    physical_plan.append({
        'operation': 'FileScan',
        'cost_estimate': {'cost': 'medium', 'type': 'io'},
        'parallelism': {'parallel': True}
    })
    
    # Mock Catalyst rules
    catalyst_rules = [
        {
            'rule_name': 'ConstantFolding',
            'description': 'Replaces expressions that can be statically evaluated with their results',
            'applied': 'WHERE 1=1' in query_upper or any(op in query for op in ['100', '5']),
            'impact': 'Eliminates unnecessary condition evaluation'
        },
        {
            'rule_name': 'PredicatePushdown',
            'description': 'Pushes filter conditions down to data sources',
            'applied': has_where,
            'impact': 'Reduces data read from source by early filtering'
        },
        {
            'rule_name': 'ColumnPruning',
            'description': 'Eliminates unused columns from query execution',
            'applied': 'SELECT *' not in query_upper,
            'impact': 'Reduces memory usage and I/O'
        },
        {
            'rule_name': 'AggregateOptimization',
            'description': 'Optimizes aggregation operations',
            'applied': has_aggregate,
            'impact': 'Uses hash-based aggregation for better performance'
        },
        {
            'rule_name': 'BooleanSimplification',
            'description': 'Simplifies boolean expressions',
            'applied': 'AND' in query_upper or 'OR' in query_upper or has_where,
            'impact': 'Faster boolean expression evaluation'
        }
    ]
    
    # Mock optimization analysis
    optimization_analysis = []
    if not has_where and data_size > 1000:
        optimization_analysis.append({
            'type': 'filter_missing',
            'description': 'No WHERE clause found on large dataset',
            'impact': 'high',
            'suggestion': 'Add filtering conditions to reduce data processing'
        })
    if 'SELECT *' in query_upper:
        optimization_analysis.append({
            'type': 'column_selection',
            'description': 'SELECT * reads all columns',
            'impact': 'medium',
            'suggestion': 'Select only required columns to reduce I/O'
        })
    if has_order_by and not has_where:
        optimization_analysis.append({
            'type': 'sort_optimization',
            'description': 'Global sort without filtering',
            'impact': 'high',
            'suggestion': 'Consider filtering data before sorting to reduce sort cost'
        })
    
    # Mock performance estimation
    performance_estimation = estimate_query_performance(query, data_size)
    
    # Mock explain output
    explain_output = f"""== Parsed Logical Plan ==
{chr(10).join([f"+- {op['operation']}: {op['details']}" for op in logical_plan])}

== Analyzed Logical Plan ==
category: string, transaction_count: bigint, avg_revenue: double, total_revenue: double
{chr(10).join([f"+- {op['operation']}: {op['details']}" for op in logical_plan])}

== Optimized Logical Plan ==
{chr(10).join([f"+- {op['operation']}" for op in optimized_plan])}

== Physical Plan ==
{chr(10).join([f"*({i+1}) {op['operation']}" for i, op in enumerate(physical_plan)])}

[Note: This is a mock analysis generated when Spark is not available]"""
    
    return {
        "success": True,
        "plan_analysis": {
            "logical_plan": logical_plan,
            "optimized_plan": optimized_plan,
            "physical_plan": physical_plan
        },
        "catalyst_rules": catalyst_rules,
        "optimization_analysis": optimization_analysis,
        "performance_estimation": performance_estimation,
        "explain_output": explain_output,
        "query_complexity": 10 if len(query.split()) // 3 + 1 > 10 else len(query.split()) // 3 + 1
    }

@app.route('/api/delta-operations', methods=['POST'])
def delta_operations():
    """Execute Delta Lake operations with ACID transactions and time travel"""
    start_time = time.time()
    operation_id = f"delta_op_{int(time.time() * 1000)}"
    
    logger.info("Delta Lake operation started", extra={
        'operation': 'delta_operation_start',
        'operation_id': operation_id,
        'remote_addr': request.remote_addr
    })
    
    try:
        data = request.get_json()
        operation = data.get('operation')
        sql = data.get('sql')
        time_travel = data.get('time_travel')
        
        logger.info("Delta operation parameters", extra={
            'operation': 'delta_operation_params',
            'operation_id': operation_id,
            'delta_operation': operation,
            'sql_length': len(sql) if sql else 0,
            'time_travel_enabled': bool(time_travel)
        })
        
        # Configure Spark for Delta Lake
        spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # Enable optimizations
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # Execute operation
        execution_start = time.time()
        
        # Apply time travel if specified
        if time_travel and time_travel.get('enabled'):
            if time_travel.get('version') is not None:
                sql = f"SELECT * FROM delta_transactions VERSION AS OF {time_travel['version']}"
            elif time_travel.get('timestamp'):
                sql = f"SELECT * FROM delta_transactions TIMESTAMP AS OF '{time_travel['timestamp']}'"
        
        # Execute the SQL
        result_df = spark.sql(sql)
        
        # For queries that return data, collect results
        operation_result = None
        if sql.strip().upper().startswith('SELECT'):
            results = [row.asDict() for row in result_df.collect()[:50]]
            operation_result = {
                'query_results': results,
                'row_count': len(results)
            }
        else:
            # For DDL/DML operations, just execute
            operation_result = {
                'success': True,
                'message': f'{operation} completed successfully'
            }
        
        execution_time = time.time() - execution_start
        
        # Get table information
        table_info = get_delta_table_info()
        
        # Get version history
        version_history = get_delta_version_history()
        
        # Create transaction result
        transaction_result = {
            'success': True,
            'version': len(version_history),
            'timestamp': datetime.now().isoformat(),
            'rows_added': 0,
            'rows_updated': 0,
            'rows_deleted': 0,
            'transaction_id': operation_id
        }
        
        # Estimate transaction impact based on operation
        if operation == 'insert_data':
            transaction_result['rows_added'] = 5
        elif operation == 'upsert_merge':
            transaction_result['rows_added'] = 1
            transaction_result['rows_updated'] = 1
        elif operation == 'delete_records':
            transaction_result['rows_deleted'] = 1
        
        logger.info("Delta Lake operation completed", extra={
            'operation': 'delta_operation_complete',
            'operation_id': operation_id,
            'delta_operation': operation,
            'execution_time': execution_time,
            'version_count': len(version_history)
        })
        
        response = {
            "success": True,
            "operation_result": operation_result,
            "table_info": table_info,
            "version_history": version_history,
            "transaction_result": transaction_result,
            "execution_time": execution_time
        }
        
        return jsonify(response)
        
    except Exception as e:
        total_time = time.time() - start_time
        logger.error("Delta Lake operation failed", extra={
            'operation': 'delta_operation_failed',
            'operation_id': operation_id,
            'delta_operation': operation,
            'error_type': type(e).__name__,
            'error_message': str(e),
            'processing_time': total_time,
            'traceback': traceback.format_exc()
        })
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

def get_delta_table_info():
    """Get Delta table metadata and information"""
    try:
        # Mock Delta table info - in real implementation would use Delta APIs
        return {
            'path': '/tmp/delta-table/delta_transactions',
            'partitionColumns': ['category'],
            'format': 'delta',
            'id': 'delta_transactions_001',
            'name': 'delta_transactions',
            'description': 'Sample transaction table with Delta Lake features',
            'provider': 'delta',
            'properties': {
                'delta.autoOptimize.optimizeWrite': 'true',
                'delta.autoOptimize.autoCompact': 'true'
            }
        }
    except Exception:
        return None

def get_delta_version_history():
    """Get Delta table version history"""
    try:
        # Mock version history - in real implementation would query Delta log
        base_time = datetime.now()
        versions = []
        
        operations = [
            ('CREATE TABLE', 'Table creation with partitioning'),
            ('INSERT', 'Initial data load'),
            ('MERGE', 'UPSERT operation'),
            ('DELETE', 'Data cleanup'),
            ('OPTIMIZE', 'File compaction'),
            ('VACUUM', 'Old file cleanup')
        ]
        
        for i, (op, desc) in enumerate(operations):
            version_time = base_time - timedelta(minutes=(len(operations) - i) * 5)
            versions.append({
                'version': i,
                'timestamp': version_time.isoformat(),
                'operation': op,
                'operation_parameters': {'description': desc},
                'user_metadata': {},
                'is_blind_append': op == 'INSERT',
                'ready_commit_timestamp': int(version_time.timestamp() * 1000)
            })
        
        return versions
    except Exception:
        return []

# Global error handlers
@app.errorhandler(404)
def handle_404(error):
    return jsonify({
        'success': False,
        'error': 'API endpoint not found',
        'message': 'The requested API endpoint does not exist. Please check the URL and try again.',
        'details': {
            'requested_url': request.url,
            'method': request.method,
            'available_endpoints': [
                '/api/health',
                '/api/word-count',
                '/api/analyze-csv',
                '/api/sql-query',
                '/api/analyze-query-complexity',
                '/api/sample-datasets',
                '/api/upload-csv'
            ]
        }
    }), 404

@app.errorhandler(405)
def handle_405(error):
    return jsonify({
        'success': False,
        'error': 'Method not allowed',
        'message': f'HTTP method {request.method} not allowed for this endpoint.',
        'details': {
            'requested_method': request.method,
            'requested_url': request.url
        }
    }), 405

@app.errorhandler(500)
def handle_500(error):
    error_id = str(int(time.time() * 1000))
    logger.error(f"Internal server error: {error}", extra={
        'operation': 'global_error_handler',
        'error_type': type(error).__name__,
        'error_id': error_id,
        'url': request.url,
        'method': request.method
    })
    return jsonify({
        'success': False,
        'error': 'Internal server error',
        'message': 'An internal server error occurred. Please try again later.',
        'details': {
            'error_id': error_id,
            'timestamp': datetime.now().isoformat()
        }
    }), 500

# Catch-all route for undefined API endpoints
@app.route('/api/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'])
def catch_all_api(path):
    return jsonify({
        'success': False,
        'error': 'API endpoint not found',
        'message': f"API endpoint '/api/{path}' not found.",
        'details': {
            'requested_path': f'/api/{path}',
            'method': request.method,
            'suggestion': 'Check API documentation for available endpoints'
        }
    }), 404

# API-only routes - frontend served by Next.js dev server

if __name__ == '__main__':
    logger.info("Starting Flask API server", extra={
        'operation': 'app_startup',
        'port': 5002,
        'mode': 'development'
    })
    app.run(debug=True, port=5002, host='0.0.0.0')
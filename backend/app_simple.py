from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename
import pandas as pd
import json
import os
import sqlite3
from query_complexity_analyzer import QueryComplexityAnalyzer

def clean_dataframe_for_json(df):
    """Clean dataframe to handle NaN values and ensure JSON serializable"""
    cleaned_df = df.fillna('')  # Replace NaN with empty strings
    try:
        data_records = cleaned_df.to_dict('records')
        # Clean any remaining NaN values that might slip through
        for record in data_records:
            for key, value in record.items():
                if isinstance(value, float) and (value != value):  # Check for NaN
                    record[key] = ''
                elif str(value) == 'nan':
                    record[key] = ''
        return data_records
    except Exception as e:
        print(f"Error processing dataframe for JSON: {e}")
        return []

app = Flask(__name__)
CORS(app)

# Configure upload settings
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv', 'txt'}
MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB max file size

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

# Create upload directory if it doesn't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Initialize complexity analyzer
complexity_analyzer = QueryComplexityAnalyzer()

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy",
        "backend": "pandas-sqlite",
        "complexity_analyzer": "active"
    })

@app.route('/api/sql-query', methods=['POST'])
def sql_query():
    try:
        data = request.json
        csv_data = data.get('data', [])
        query = data.get('query', '')
        analyze_complexity = data.get('analyze_complexity', True)
        
        if not csv_data:
            return jsonify({
                "success": False,
                "error": "No data provided"
            }), 400
        
        if not query:
            return jsonify({
                "success": False,
                "error": "No query provided"
            }), 400
        
        # Convert to pandas DataFrame
        df = pd.DataFrame(csv_data)
        
        # Analyze query complexity if requested
        complexity_analysis = None
        if analyze_complexity:
            data_stats = {
                'row_count': len(df),
                'column_count': len(df.columns),
                'schema': [{"name": col, "type": str(df[col].dtype)} for col in df.columns]
            }
            complexity_analysis = complexity_analyzer.analyze_query(query, data_stats)
        
        # Execute SQL query using SQLite in-memory database
        conn = sqlite3.connect(':memory:')
        df.to_sql('data_table', conn, index=False, if_exists='replace')
        
        try:
            result_df = pd.read_sql_query(query, conn)
            results = result_df.to_dict('records')[:100]  # Limit to 100 rows
            
            response = {
                "success": True,
                "results": results,
                "row_count": len(results)
            }
            
            if complexity_analysis:
                response["complexity_analysis"] = complexity_analysis
            
            return jsonify(response)
            
        except Exception as sql_error:
            return jsonify({
                "success": False,
                "error": f"SQL execution error: {str(sql_error)}"
            }), 400
        finally:
            conn.close()
            
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/analyze-query-complexity', methods=['POST'])
def analyze_query_complexity():
    """Analyze SQL query complexity without executing it"""
    try:
        data = request.json
        query = data.get('query', '')
        data_stats = data.get('data_stats', None)
        
        if not query:
            return jsonify({
                "success": False,
                "error": "Query is required"
            }), 400
        
        # Analyze query complexity
        complexity_analysis = complexity_analyzer.analyze_query(query, data_stats)
        
        return jsonify({
            "success": True,
            "analysis": complexity_analysis
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/analyze-csv-complexity', methods=['POST'])
def analyze_csv_complexity():
    """Analyze complexity of CSV data"""
    try:
        data = request.json
        csv_data = data.get('data', [])
        
        if not csv_data:
            return jsonify({
                "success": False,
                "error": "CSV data is required"
            }), 400
        
        # Convert to pandas DataFrame for analysis
        df = pd.DataFrame(csv_data)
        
        # Analyze dataset complexity
        complexity_stats = complexity_analyzer.analyze_csv_complexity(df)
        
        return jsonify({
            "success": True,
            "complexity_stats": complexity_stats
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/sample-datasets', methods=['GET'])
def get_sample_datasets():
    """Get list of available sample datasets"""
    try:
        sample_dir = "../sample_datasets"
        
        if not os.path.exists(sample_dir):
            return jsonify({
                "success": False,
                "error": "Sample datasets directory not found"
            }), 404
        
        datasets = []
        for filename in os.listdir(sample_dir):
            if filename.endswith('.csv'):
                file_path = os.path.join(sample_dir, filename)
                try:
                    # Get basic file info
                    try:
                        df = pd.read_csv(file_path, nrows=5)  # Read just first 5 rows for preview
                    except pd.errors.ParserError:
                        try:
                            df = pd.read_csv(file_path, nrows=5, sep=';')
                        except pd.errors.ParserError:
                            df = pd.read_csv(file_path, nrows=5, on_bad_lines='skip')
                    file_size = os.path.getsize(file_path)
                    
                    # Get full dataset for complexity analysis
                    try:
                        full_df = pd.read_csv(file_path)
                    except pd.errors.ParserError:
                        try:
                            full_df = pd.read_csv(file_path, sep=';')
                        except pd.errors.ParserError:
                            full_df = pd.read_csv(file_path, on_bad_lines='skip')
                    complexity_stats = complexity_analyzer.analyze_csv_complexity(full_df)
                    
                    datasets.append({
                        'filename': filename,
                        'name': filename.replace('.csv', '').replace('_', ' ').title(),
                        'rows': len(full_df),
                        'columns': len(full_df.columns),
                        'size_bytes': file_size,
                        'complexity': complexity_stats['estimated_complexity'],
                        'preview': clean_dataframe_for_json(df),
                        'column_types': complexity_stats['data_types']
                    })
                except Exception as e:
                    print(f"Error processing {filename}: {e}")
                    continue
        
        return jsonify({
            "success": True,
            "datasets": sorted(datasets, key=lambda x: x['complexity'])
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/load-sample-dataset/<filename>', methods=['GET'])
def load_sample_dataset(filename):
    """Load a specific sample dataset"""
    try:
        sample_dir = "../sample_datasets"
        file_path = os.path.join(sample_dir, filename)
        
        if not os.path.exists(file_path):
            return jsonify({
                "success": False,
                "error": "Dataset not found"
            }), 404
        
        # Load dataset
        try:
            df = pd.read_csv(file_path)
        except pd.errors.ParserError:
            try:
                df = pd.read_csv(file_path, sep=';')
            except pd.errors.ParserError:
                df = pd.read_csv(file_path, on_bad_lines='skip')
        
        # Limit size for frontend (max 1000 rows for display)
        if len(df) > 1000:
            sample_df = df.sample(n=1000, random_state=42)
            full_stats = complexity_analyzer.analyze_csv_complexity(df)
        else:
            sample_df = df
            full_stats = complexity_analyzer.analyze_csv_complexity(df)
        
        return jsonify({
            "success": True,
            "data": clean_dataframe_for_json(sample_df),
            "full_stats": full_stats,
            "is_sample": len(df) > 1000,
            "original_row_count": len(df)
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/upload-csv', methods=['POST'])
def upload_csv():
    """Upload and process CSV file"""
    try:
        # Check if file is present
        if 'file' not in request.files:
            return jsonify({
                "success": False,
                "error": "No file provided"
            }), 400
        
        file = request.files['file']
        
        # Check if file is selected
        if file.filename == '':
            return jsonify({
                "success": False,
                "error": "No file selected"
            }), 400
        
        # Check if file is allowed
        if not allowed_file(file.filename):
            return jsonify({
                "success": False,
                "error": "File type not allowed. Only CSV files are supported."
            }), 400
        
        # Secure filename and save
        filename = secure_filename(file.filename)
        try:
            # Use datetime directly to avoid pandas NaN issues
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        except Exception:
            # Ultimate fallback with basic timestamp
            import time
            timestamp = str(int(time.time()))
        unique_filename = f"{timestamp}_{filename}"
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
        
        file.save(file_path)
        
        # Process the CSV file
        try:
            # Try reading with different parameters to handle various CSV formats
            try:
                df = pd.read_csv(file_path)
            except pd.errors.ParserError:
                # Try with different separator
                try:
                    df = pd.read_csv(file_path, sep=';')
                except pd.errors.ParserError:
                    # Try with different quoting
                    try:
                        df = pd.read_csv(file_path, quoting=1)  # QUOTE_ALL
                    except pd.errors.ParserError:
                        # Try with error handling for malformed lines
                        df = pd.read_csv(file_path, on_bad_lines='skip')
            
            # Analyze complexity
            complexity_stats = complexity_analyzer.analyze_csv_complexity(df)
            
            # Create sample for preview (first 100 rows)
            preview_data = clean_dataframe_for_json(df.head(100))
            
            # Store file info
            file_info = {
                'original_filename': file.filename,
                'stored_filename': unique_filename,
                'upload_timestamp': timestamp,
                'file_size': os.path.getsize(file_path),
                'rows': len(df),
                'columns': len(df.columns),
                'complexity_stats': complexity_stats
            }
            
            return jsonify({
                "success": True,
                "file_info": file_info,
                "preview": preview_data,
                "message": f"File uploaded successfully. {len(df)} rows, {len(df.columns)} columns."
            })
            
        except Exception as e:
            # Remove file if processing failed
            if os.path.exists(file_path):
                os.remove(file_path)
            return jsonify({
                "success": False,
                "error": f"Error processing CSV file: {str(e)}"
            }), 400
            
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/uploaded-files', methods=['GET'])
def get_uploaded_files():
    """Get list of uploaded files"""
    try:
        upload_dir = app.config['UPLOAD_FOLDER']
        files = []
        
        for filename in os.listdir(upload_dir):
            if filename.endswith('.csv'):
                file_path = os.path.join(upload_dir, filename)
                
                try:
                    # Get basic file info
                    file_size = os.path.getsize(file_path)
                    try:
                        df = pd.read_csv(file_path, nrows=5)  # Just for column info
                    except pd.errors.ParserError:
                        try:
                            df = pd.read_csv(file_path, nrows=5, sep=';')
                        except pd.errors.ParserError:
                            df = pd.read_csv(file_path, nrows=5, on_bad_lines='skip')
                    try:
                        full_df = pd.read_csv(file_path)
                    except pd.errors.ParserError:
                        try:
                            full_df = pd.read_csv(file_path, sep=';')
                        except pd.errors.ParserError:
                            full_df = pd.read_csv(file_path, on_bad_lines='skip')
                    
                    # Analyze complexity
                    complexity_stats = complexity_analyzer.analyze_csv_complexity(full_df)
                    
                    # Extract timestamp from filename
                    parts = filename.split('_', 2)
                    timestamp = f"{parts[0]}_{parts[1]}" if len(parts) >= 2 else "unknown"
                    original_name = '_'.join(parts[2:]) if len(parts) > 2 else filename
                    
                    files.append({
                        'filename': filename,
                        'original_name': original_name,
                        'upload_time': timestamp,
                        'size_bytes': file_size,
                        'rows': len(full_df),
                        'columns': len(full_df.columns),
                        'complexity': complexity_stats['estimated_complexity'],
                        'preview': clean_dataframe_for_json(df)
                    })
                    
                except Exception as e:
                    print(f"Error processing {filename}: {e}")
                    continue
        
        # Sort by upload time (newest first)
        files.sort(key=lambda x: x['upload_time'], reverse=True)
        
        return jsonify({
            "success": True,
            "files": files
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/load-uploaded-file/<filename>', methods=['GET'])
def load_uploaded_file(filename):
    """Load data from an uploaded file"""
    try:
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
        if not os.path.exists(file_path):
            return jsonify({
                "success": False,
                "error": "File not found"
            }), 404
        
        # Load the CSV file
        try:
            df = pd.read_csv(file_path)
        except pd.errors.ParserError:
            try:
                df = pd.read_csv(file_path, sep=';')
            except pd.errors.ParserError:
                df = pd.read_csv(file_path, on_bad_lines='skip')
        
        # For large files, return a sample
        max_rows = int(request.args.get('max_rows', 1000))
        if len(df) > max_rows:
            sample_df = df.sample(n=max_rows, random_state=42)
            is_sample = True
        else:
            sample_df = df
            is_sample = False
        
        # Get complexity stats
        complexity_stats = complexity_analyzer.analyze_csv_complexity(df)
        
        return jsonify({
            "success": True,
            "data": clean_dataframe_for_json(sample_df),
            "complexity_stats": complexity_stats,
            "is_sample": is_sample,
            "total_rows": len(df),
            "displayed_rows": len(sample_df)
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/delete-uploaded-file/<filename>', methods=['DELETE'])
def delete_uploaded_file(filename):
    """Delete an uploaded file"""
    try:
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
        if not os.path.exists(file_path):
            return jsonify({
                "success": False,
                "error": "File not found"
            }), 404
        
        os.remove(file_path)
        
        return jsonify({
            "success": True,
            "message": "File deleted successfully"
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

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
                '/api/sql-query',
                '/api/analyze-query-complexity',
                '/api/analyze-csv-complexity',
                '/api/sample-datasets',
                '/api/upload-csv',
                '/api/uploaded-files'
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

if __name__ == '__main__':
    app.run(debug=True, port=5002)
"""
Comprehensive error handling utilities for Flask applications
"""

from flask import request, jsonify
from functools import wraps
import logging
import time
import traceback
from typing import Dict, Any, Optional, Callable
from datetime import datetime

logger = logging.getLogger(__name__)

class ErrorHandler:
    """Centralized error handling for API endpoints"""
    
    @staticmethod
    def format_error_response(error_code: str, message: str, details: Optional[Dict[str, Any]] = None, status_code: int = 500) -> tuple:
        """Format a standardized error response"""
        error_response = {
            'success': False,
            'error': error_code,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'request_info': {
                'url': request.url if request else None,
                'method': request.method if request else None,
                'remote_addr': request.remote_addr if request else None
            }
        }
        
        if details:
            error_response['details'] = details
            
        return jsonify(error_response), status_code
    
    @staticmethod
    def handle_validation_error(message: str, field: Optional[str] = None) -> tuple:
        """Handle validation errors"""
        details = {'field': field} if field else None
        return ErrorHandler.format_error_response(
            error_code='VALIDATION_ERROR',
            message=message,
            details=details,
            status_code=400
        )
    
    @staticmethod
    def handle_not_found_error(resource: str = "Resource") -> tuple:
        """Handle resource not found errors"""
        return ErrorHandler.format_error_response(
            error_code='NOT_FOUND',
            message=f"{resource} not found",
            status_code=404
        )
    
    @staticmethod
    def handle_internal_error(error: Exception, operation: str = "unknown") -> tuple:
        """Handle internal server errors"""
        error_id = str(int(time.time() * 1000))
        
        logger.error(f"Internal error in {operation}: {error}", extra={
            'operation': operation,
            'error_type': type(error).__name__,
            'error_id': error_id,
            'traceback': traceback.format_exc()
        })
        
        return ErrorHandler.format_error_response(
            error_code='INTERNAL_SERVER_ERROR',
            message='An internal server error occurred. Please try again later.',
            details={'error_id': error_id},
            status_code=500
        )

def api_error_handler(operation_name: str = None):
    """
    Decorator for comprehensive API error handling
    
    Usage:
    @api_error_handler('user_creation')
    def create_user():
        # Your endpoint logic here
        pass
    """
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def decorated_function(*args, **kwargs):
            start_time = time.time()
            op_name = operation_name or f.__name__
            
            try:
                # Log operation start
                logger.info(f"Starting {op_name}", extra={
                    'operation': f'{op_name}_start',
                    'remote_addr': request.remote_addr if request else None,
                    'method': request.method if request else None,
                    'url': request.url if request else None
                })
                
                # Execute the function
                result = f(*args, **kwargs)
                
                # Log successful completion
                processing_time = (time.time() - start_time) * 1000
                logger.info(f"Completed {op_name}", extra={
                    'operation': f'{op_name}_complete',
                    'processing_time_ms': round(processing_time, 2)
                })
                
                return result
                
            except ValueError as e:
                # Handle validation/value errors
                processing_time = (time.time() - start_time) * 1000
                logger.warning(f"Validation error in {op_name}: {e}", extra={
                    'operation': f'{op_name}_validation_error',
                    'processing_time_ms': round(processing_time, 2)
                })
                return ErrorHandler.handle_validation_error(str(e))
                
            except FileNotFoundError as e:
                # Handle file not found errors
                processing_time = (time.time() - start_time) * 1000
                logger.warning(f"File not found in {op_name}: {e}", extra={
                    'operation': f'{op_name}_file_not_found',
                    'processing_time_ms': round(processing_time, 2)
                })
                return ErrorHandler.handle_not_found_error("File")
                
            except PermissionError as e:
                # Handle permission errors
                processing_time = (time.time() - start_time) * 1000
                logger.error(f"Permission error in {op_name}: {e}", extra={
                    'operation': f'{op_name}_permission_error',
                    'processing_time_ms': round(processing_time, 2)
                })
                return ErrorHandler.format_error_response(
                    error_code='PERMISSION_DENIED',
                    message='Permission denied for this operation',
                    status_code=403
                )
                
            except Exception as e:
                # Handle all other exceptions
                processing_time = (time.time() - start_time) * 1000
                logger.error(f"Unexpected error in {op_name}: {e}", extra={
                    'operation': f'{op_name}_error',
                    'error_type': type(e).__name__,
                    'processing_time_ms': round(processing_time, 2),
                    'traceback': traceback.format_exc()
                })
                return ErrorHandler.handle_internal_error(e, op_name)
        
        return decorated_function
    return decorator

def validate_json_request(required_fields: list = None, optional_fields: list = None):
    """
    Decorator to validate JSON request data
    
    Usage:
    @validate_json_request(['name', 'email'], ['age', 'phone'])
    def create_user():
        data = request.get_json()
        # data is guaranteed to have required fields
    """
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Check if request has JSON content type
            if not request.is_json:
                return ErrorHandler.handle_validation_error(
                    "Request must have Content-Type: application/json"
                )
            
            # Get JSON data
            try:
                data = request.get_json()
            except Exception as e:
                return ErrorHandler.handle_validation_error(
                    f"Invalid JSON format: {str(e)}"
                )
            
            if data is None:
                return ErrorHandler.handle_validation_error(
                    "Request body must contain valid JSON"
                )
            
            # Validate required fields
            if required_fields:
                missing_fields = []
                for field in required_fields:
                    if field not in data or data[field] is None:
                        missing_fields.append(field)
                
                if missing_fields:
                    return ErrorHandler.handle_validation_error(
                        f"Missing required fields: {', '.join(missing_fields)}",
                        field=missing_fields[0]
                    )
            
            # Validate field types if specified
            all_fields = (required_fields or []) + (optional_fields or [])
            for field in all_fields:
                if field in data and data[field] is not None:
                    # Add any specific type validation here
                    if isinstance(data[field], str) and len(data[field].strip()) == 0:
                        return ErrorHandler.handle_validation_error(
                            f"Field '{field}' cannot be empty",
                            field=field
                        )
            
            return f(*args, **kwargs)
        
        return decorated_function
    return decorator

def validate_query_params(required_params: list = None, optional_params: list = None):
    """
    Decorator to validate query parameters
    
    Usage:
    @validate_query_params(['id'], ['limit', 'offset'])
    def get_user():
        # Query params are validated
    """
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Validate required parameters
            if required_params:
                missing_params = []
                for param in required_params:
                    if param not in request.args or not request.args.get(param):
                        missing_params.append(param)
                
                if missing_params:
                    return ErrorHandler.handle_validation_error(
                        f"Missing required query parameters: {', '.join(missing_params)}"
                    )
            
            return f(*args, **kwargs)
        
        return decorated_function
    return decorator

def require_file_upload(allowed_extensions: list = None):
    """
    Decorator to validate file uploads
    
    Usage:
    @require_file_upload(['csv', 'json', 'txt'])
    def upload_file():
        file = request.files['file']
        # file is guaranteed to exist and have valid extension
    """
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'file' not in request.files:
                return ErrorHandler.handle_validation_error(
                    "No file provided in request"
                )
            
            file = request.files['file']
            
            if file.filename == '':
                return ErrorHandler.handle_validation_error(
                    "No file selected"
                )
            
            if allowed_extensions:
                file_ext = file.filename.rsplit('.', 1)[1].lower() if '.' in file.filename else ''
                if file_ext not in allowed_extensions:
                    return ErrorHandler.handle_validation_error(
                        f"File type not allowed. Allowed types: {', '.join(allowed_extensions)}"
                    )
            
            return f(*args, **kwargs)
        
        return decorated_function
    return decorator
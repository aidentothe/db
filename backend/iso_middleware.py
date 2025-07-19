"""
ISO Middleware - Ensures all requests and responses follow ISO standards
"""

from flask import request, jsonify, g
from functools import wraps
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional

from iso_standards import ISOFormatter, ISOStandards, ISOValidator, ISOConstants

logger = logging.getLogger(__name__)

class ISOMiddleware:
    """Middleware to enforce ISO standards across all API endpoints"""
    
    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)
    
    def init_app(self, app):
        """Initialize the middleware with Flask app"""
        app.before_request(self.before_request)
        app.after_request(self.after_request)
        app.errorhandler(400)(self.handle_bad_request)
        app.errorhandler(404)(self.handle_not_found)
        app.errorhandler(500)(self.handle_internal_server_error)
    
    def before_request(self):
        """Process request before routing"""
        g.start_time = time.time()
        g.request_id = f"req_{int(time.time() * 1000)}"
        
        # Log request in ISO format
        logger.info("Request started", extra={
            'operation': 'request_start',
            'request_id': g.request_id,
            'method': request.method,
            'path': request.path,
            'endpoint': request.endpoint,
            'remote_addr': request.remote_addr,
            'user_agent': request.headers.get('User-Agent', 'unknown'),
            'timestamp': ISOStandards.format_iso_datetime()
        })
        
        # Validate request format for JSON endpoints
        if request.content_type == 'application/json' and request.data:
            try:
                request.get_json()
            except Exception as e:
                logger.error("Invalid JSON in request", extra={
                    'operation': 'request_validation_failed',
                    'request_id': g.request_id,
                    'error': str(e)
                })
                return ISOFormatter.format_error(
                    error_code=ISOConstants.ERROR_CODES['VALIDATION_ERROR'],
                    message="Invalid JSON format"
                ), 400
    
    def after_request(self, response):
        """Process response after routing"""
        processing_time = (time.time() - g.start_time) * 1000
        
        # Log response in ISO format
        logger.info("Request completed", extra={
            'operation': 'request_complete',
            'request_id': g.request_id,
            'status_code': response.status_code,
            'processing_time_ms': processing_time,
            'response_size': len(response.data) if response.data else 0
        })
        
        # Add standard headers
        response.headers['X-Request-ID'] = g.request_id
        response.headers['X-Processing-Time'] = f"{processing_time:.2f}ms"
        response.headers['X-Timestamp'] = ISOStandards.format_iso_datetime()
        response.headers['X-API-Version'] = ISOConstants.API_VERSION
        
        # Ensure response is JSON with proper content type
        if response.content_type.startswith('application/json'):
            response.headers['Content-Type'] = 'application/json; charset=utf-8'
        
        return response
    
    def handle_bad_request(self, error):
        """Handle 400 Bad Request errors"""
        logger.warning("Bad request", extra={
            'operation': 'bad_request',
            'request_id': getattr(g, 'request_id', 'unknown'),
            'error': str(error)
        })
        
        return ISOFormatter.format_error(
            error_code=ISOConstants.ERROR_CODES['VALIDATION_ERROR'],
            message="Bad request"
        ), 400
    
    def handle_not_found(self, error):
        """Handle 404 Not Found errors"""
        logger.warning("Resource not found", extra={
            'operation': 'resource_not_found',
            'request_id': getattr(g, 'request_id', 'unknown'),
            'path': request.path
        })
        
        return ISOFormatter.format_error(
            error_code=ISOConstants.ERROR_CODES['RESOURCE_NOT_FOUND'],
            message=f"Resource not found: {request.path}"
        ), 404
    
    def handle_internal_server_error(self, error):
        """Handle 500 Internal Server Error"""
        logger.error("Internal server error", extra={
            'operation': 'internal_server_error',
            'request_id': getattr(g, 'request_id', 'unknown'),
            'error': str(error)
        })
        
        return ISOFormatter.format_error(
            error_code=ISOConstants.ERROR_CODES['INTERNAL_SERVER_ERROR'],
            message="Internal server error"
        ), 500

def iso_response(f):
    """Decorator to ensure response follows ISO format"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            result = f(*args, **kwargs)
            
            # If already a tuple with status code, return as-is
            if isinstance(result, tuple):
                return result
            
            # If it's a dict with 'meta' key, it's already ISO-formatted
            if isinstance(result, dict) and 'meta' in result:
                return result
            
            # Otherwise, format as ISO response
            return ISOFormatter.format_response(result)
            
        except Exception as e:
            logger.error(f"Error in {f.__name__}: {e}", extra={
                'operation': f.__name__,
                'error_type': type(e).__name__
            })
            
            error_response = ISOFormatter.format_error(
                error_code=ISOConstants.ERROR_CODES['INTERNAL_SERVER_ERROR'],
                message=str(e)
            )
            return error_response, 500
    
    return decorated_function

def validate_request_data(required_fields: list, optional_fields: list = None):
    """Decorator to validate request data against ISO standards"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not request.is_json:
                error_response = ISOFormatter.format_error(
                    error_code=ISOConstants.ERROR_CODES['VALIDATION_ERROR'],
                    message="Request must be JSON"
                )
                return error_response, 400
            
            data = request.get_json()
            if not data:
                error_response = ISOFormatter.format_error(
                    error_code=ISOConstants.ERROR_CODES['VALIDATION_ERROR'],
                    message="Request body cannot be empty"
                )
                return error_response, 400
            
            # Check required fields
            missing_fields = []
            for field in required_fields:
                if field not in data:
                    missing_fields.append(field)
            
            if missing_fields:
                error_response = ISOFormatter.format_error(
                    error_code=ISOConstants.ERROR_CODES['VALIDATION_ERROR'],
                    message=f"Missing required fields: {', '.join(missing_fields)}"
                )
                return error_response, 400
            
            # Validate field formats
            validation_errors = []
            
            # Check for valid naming conventions in field names
            all_fields = required_fields + (optional_fields or [])
            for field in all_fields:
                if field in data:
                    # Validate field name format
                    if not ISOStandards.validate_naming_convention(field, 'snake_case'):
                        validation_errors.append(f"Field '{field}' does not follow snake_case convention")
                    
                    # Validate string fields are normalized
                    if isinstance(data[field], str):
                        normalized = ISOStandards.normalize_string(data[field])
                        if normalized != data[field]:
                            logger.warning(f"Field '{field}' contains non-normalized text")
            
            if validation_errors:
                error_response = ISOFormatter.format_error(
                    error_code=ISOConstants.ERROR_CODES['VALIDATION_ERROR'],
                    message="Validation errors",
                    details={'errors': validation_errors}
                )
                return error_response, 400
            
            return f(*args, **kwargs)
        
        return decorated_function
    return decorator

def validate_pagination():
    """Decorator to validate and normalize pagination parameters"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Get pagination parameters
            page = request.args.get('page', 1, type=int)
            per_page = request.args.get('per_page', ISOConstants.DEFAULT_PAGE_SIZE, type=int)
            
            # Validate pagination parameters
            if page < 1:
                error_response = ISOFormatter.format_error(
                    error_code=ISOConstants.ERROR_CODES['VALIDATION_ERROR'],
                    message="Page number must be >= 1"
                )
                return error_response, 400
            
            if per_page < 1 or per_page > ISOConstants.MAX_PAGE_SIZE:
                error_response = ISOFormatter.format_error(
                    error_code=ISOConstants.ERROR_CODES['VALIDATION_ERROR'],
                    message=f"Per page must be between 1 and {ISOConstants.MAX_PAGE_SIZE}"
                )
                return error_response, 400
            
            # Add validated parameters to g for use in route
            g.page = page
            g.per_page = per_page
            
            return f(*args, **kwargs)
        
        return decorated_function
    return decorator

def validate_table_name():
    """Decorator to validate table names according to ISO standards"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Check for table name in various locations
            table_name = None
            
            # Check URL parameters
            if 'table_name' in request.args:
                table_name = request.args.get('table_name')
            elif 'path' in request.args:
                # Extract table name from path
                path = request.args.get('path')
                table_name = path.split('/')[-1].replace('.json', '').replace('.parquet', '')
            
            # Check JSON body
            if request.is_json:
                data = request.get_json()
                if data and 'table_name' in data:
                    table_name = data['table_name']
            
            if table_name and not ISOValidator.validate_table_name(table_name):
                error_response = ISOFormatter.format_error(
                    error_code=ISOConstants.ERROR_CODES['VALIDATION_ERROR'],
                    message=f"Invalid table name: {table_name}. Must follow snake_case convention."
                )
                return error_response, 400
            
            return f(*args, **kwargs)
        
        return decorated_function
    return decorator

def rate_limit(requests_per_minute: int = 60):
    """Decorator to implement rate limiting"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Simple rate limiting implementation
            # In production, use Redis or similar for distributed rate limiting
            
            client_ip = request.remote_addr
            current_time = time.time()
            
            # For demo purposes, just log the rate limit check
            logger.debug(f"Rate limit check for {client_ip}: {requests_per_minute} requests/minute")
            
            # In a real implementation, you would check against a rate limit store
            # For now, we'll just proceed
            
            return f(*args, **kwargs)
        
        return decorated_function
    return decorator
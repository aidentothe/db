"""
Main Flask Application - Entry point for the DB Spark API
"""

from flask import Flask, request
from flask_cors import CORS
import logging
import os
import atexit
from datetime import datetime

# Import configuration and logging
from config import get_config
from logging_config import setup_logging
from spark_session_manager import cleanup_spark

# Import route blueprints
from spark_api_routes import spark_bp
from existing_routes import existing_bp
from deployment_routes import deployment_bp

# Import ISO middleware
from iso_middleware import ISOMiddleware

def create_app(config_name=None):
    """
    Application factory pattern for creating Flask app
    """
    app = Flask(__name__)
    
    # Load configuration
    config = get_config(config_name)
    app.config.from_object(config)
    
    # Configure CORS
    CORS(app, origins=config.CORS_ORIGINS,
         allow_headers=['Content-Type'], 
         methods=['GET', 'POST', 'DELETE', 'OPTIONS'])
    
    # Configure logging
    logger = setup_logging(config)
    
    # Initialize ISO middleware
    iso_middleware = ISOMiddleware(app)
    
    # Register blueprints
    app.register_blueprint(spark_bp)
    app.register_blueprint(existing_bp)
    app.register_blueprint(deployment_bp)
    
    # Register cleanup function
    atexit.register(cleanup_spark)
    
    # Global error handlers
    @app.errorhandler(404)
    def handle_404(error):
        from iso_standards import ISOFormatter, ISOConstants
        error_response = ISOFormatter.format_error(
            error_code=ISOConstants.ERROR_CODES.get('NOT_FOUND', 'API_NOT_FOUND'),
            message="API endpoint not found. Please check the URL and try again.",
            details={
                'requested_url': request.url,
                'method': request.method,
                'available_endpoints': {
                    'health': '/health',
                    'api_health': '/api/health', 
                    'spark_health': '/api/spark/health',
                    'deployment_config': '/api/deployment/config'
                }
            }
        )
        return error_response, 404
    
    @app.errorhandler(405)
    def handle_405(error):
        from iso_standards import ISOFormatter, ISOConstants
        error_response = ISOFormatter.format_error(
            error_code=ISOConstants.ERROR_CODES.get('METHOD_NOT_ALLOWED', 'METHOD_NOT_ALLOWED'),
            message="HTTP method not allowed for this endpoint.",
            details={
                'requested_method': request.method,
                'requested_url': request.url
            }
        )
        return error_response, 405
    
    @app.errorhandler(500)
    def handle_500(error):
        from iso_standards import ISOFormatter, ISOConstants
        logger.error(f"Internal server error: {error}", extra={
            'operation': 'global_error_handler',
            'error_type': type(error).__name__,
            'url': request.url,
            'method': request.method
        })
        error_response = ISOFormatter.format_error(
            error_code=ISOConstants.ERROR_CODES.get('INTERNAL_SERVER_ERROR', 'INTERNAL_SERVER_ERROR'),
            message="An internal server error occurred. Please try again later.",
            details={'error_id': str(int(time.time() * 1000))}
        )
        return error_response, 500
    
    # Catch-all route for API endpoints
    @app.route('/api/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'])
    def catch_all_api(path):
        from iso_standards import ISOFormatter, ISOConstants
        error_response = ISOFormatter.format_error(
            error_code=ISOConstants.ERROR_CODES.get('NOT_FOUND', 'API_NOT_FOUND'),
            message=f"API endpoint '/api/{path}' not found.",
            details={
                'requested_path': f'/api/{path}',
                'method': request.method,
                'suggestion': 'Check API documentation for available endpoints'
            }
        )
        return error_response, 404
    
    # Main health check endpoint (root level)
    @app.route('/health')
    def health_check():
        from iso_standards import ISOFormatter
        
        health_data = {
            'status': 'healthy',
            'environment': config.ENVIRONMENT,
            'version': '1.0.0'
        }
        
        return ISOFormatter.format_response(health_data)
    
    logger.info("Application created successfully", extra={
        'operation': 'app_creation',
        'environment': config.ENVIRONMENT,
        'debug_mode': config.FLASK_DEBUG
    })
    
    return app

if __name__ == '__main__':
    app = create_app()
    
    # Get configuration
    config = get_config()
    
    logger = logging.getLogger(__name__)
    logger.info(f"Starting application on {config.HOST}:{config.PORT}, debug={config.FLASK_DEBUG}")
    
    app.run(
        host=config.HOST,
        port=config.PORT,
        debug=config.FLASK_DEBUG
    )
"""
Deployment Routes - API endpoints for managing deployment configuration
"""

from flask import Blueprint, request, jsonify
import logging
import os
from deployment_config import get_deployment_config, set_deployment_mode, DeploymentMode
from spark_session_manager import get_spark_manager, cleanup_spark
from config import get_config
from iso_standards import ISOFormatter, ISOStandards, ISOValidator, ISOConstants
from error_handlers import api_error_handler, validate_json_request, ErrorHandler

# Create blueprint for deployment routes
deployment_bp = Blueprint('deployment', __name__, url_prefix='/api/deployment')
logger = logging.getLogger(__name__)

@deployment_bp.route('/config', methods=['GET'])
def get_deployment_configuration():
    """Get current deployment configuration"""
    try:
        deployment_config = get_deployment_config()
        
        config_info = {
            "current_mode": deployment_config.mode.value,
            "available_modes": [mode.value for mode in DeploymentMode],
            "spark_config": deployment_config.get_spark_config(),
            "data_source_config": deployment_config.get_data_source_config(),
            "performance_limits": deployment_config.get_performance_limits(),
            "monitoring_config": deployment_config.get_monitoring_config(),
            "s3_enabled": deployment_config.is_s3_enabled(),
            "local_mode": deployment_config.is_local_mode(),
            "cors_origins": deployment_config.get_cors_origins(),
            "rate_limits": deployment_config.get_api_rate_limits(),
            "caching_config": deployment_config.get_caching_config()
        }
        
        # Return ISO-formatted response
        return ISOFormatter.format_response(config_info)
        
    except Exception as e:
        logger.error(f"Failed to get deployment configuration: {e}")
        error_response = ISOFormatter.format_error(
            error_code=ISOConstants.ERROR_CODES['INTERNAL_SERVER_ERROR'],
            message=str(e)
        )
        return error_response, 500

@deployment_bp.route('/mode', methods=['POST'])
@api_error_handler('set_deployment_mode')
@validate_json_request(['mode'])
def set_deployment_mode_endpoint():
    """Set deployment mode (requires restart)"""
    data = request.get_json()
    new_mode = data.get('mode', '').strip()
    
    if not new_mode:
        return ErrorHandler.handle_validation_error("Mode parameter cannot be empty", "mode")
    
    # Validate mode
    try:
        DeploymentMode(new_mode)
    except ValueError:
        return ErrorHandler.handle_validation_error(
            f"Invalid mode. Available modes: {[mode.value for mode in DeploymentMode]}",
            "mode"
        )
    
    # Set the new mode
    old_mode = os.getenv('DEPLOYMENT_MODE', 'local_small')
    set_deployment_mode(new_mode)
    
    logger.info(f"Deployment mode changed from {old_mode} to {new_mode}", extra={
        'operation': 'deployment_mode_change',
        'old_mode': old_mode,
        'new_mode': new_mode
    })
    
    return jsonify({
        "success": True,
        "message": f"Deployment mode set to {new_mode}",
        "old_mode": old_mode,
        "new_mode": new_mode,
        "restart_required": True
    })

@deployment_bp.route('/restart-spark', methods=['POST'])
def restart_spark_session():
    """Restart Spark session with current configuration"""
    try:
        # Clean up current session
        cleanup_spark()
        
        # Get new session with current config
        config = get_config()
        spark_manager = get_spark_manager(config)
        
        # Test the new session
        session = spark_manager.get_session()
        if session:
            # Test basic functionality
            test_rdd = session.sparkContext.parallelize([1, 2, 3])
            result = test_rdd.collect()
            
            spark_info = spark_manager.get_spark_info()
            
            logger.info("Spark session restarted successfully", extra={
                'operation': 'spark_restart_success',
                'deployment_mode': spark_info.get('deployment_mode')
            })
            
            return jsonify({
                "success": True,
                "message": "Spark session restarted successfully",
                "spark_info": spark_info,
                "test_passed": len(result) == 3
            })
        else:
            return jsonify({
                "success": False,
                "error": "Failed to create new Spark session"
            }), 500
            
    except Exception as e:
        logger.error(f"Failed to restart Spark session: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@deployment_bp.route('/mode/toggle', methods=['POST'])
def toggle_deployment_mode():
    """Toggle between local_small and aws_large modes"""
    try:
        deployment_config = get_deployment_config()
        current_mode = deployment_config.mode
        
        # Toggle logic
        if current_mode == DeploymentMode.LOCAL_SMALL:
            new_mode = DeploymentMode.AWS_LARGE.value
        else:
            new_mode = DeploymentMode.LOCAL_SMALL.value
        
        # Set the new mode
        set_deployment_mode(new_mode)
        
        logger.info(f"Deployment mode toggled from {current_mode.value} to {new_mode}", extra={
            'operation': 'deployment_mode_toggle',
            'old_mode': current_mode.value,
            'new_mode': new_mode
        })
        
        return jsonify({
            "success": True,
            "message": f"Deployment mode toggled to {new_mode}",
            "old_mode": current_mode.value,
            "new_mode": new_mode,
            "restart_required": True
        })
        
    except Exception as e:
        logger.error(f"Failed to toggle deployment mode: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@deployment_bp.route('/performance-limits', methods=['GET'])
def get_performance_limits():
    """Get current performance limits based on deployment mode"""
    try:
        deployment_config = get_deployment_config()
        limits = deployment_config.get_performance_limits()
        
        return jsonify({
            "success": True,
            "deployment_mode": deployment_config.mode.value,
            "performance_limits": limits
        })
        
    except Exception as e:
        logger.error(f"Failed to get performance limits: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@deployment_bp.route('/data-source-info', methods=['GET'])
def get_data_source_info():
    """Get data source configuration and status"""
    try:
        deployment_config = get_deployment_config()
        data_config = deployment_config.get_data_source_config()
        
        # Add status information
        if data_config['type'] == 's3':
            # Check S3 connectivity if in AWS mode
            s3_status = "unknown"
            s3_bucket = data_config.get('bucket')
            
            if s3_bucket:
                try:
                    import boto3
                    from botocore.exceptions import ClientError
                    
                    s3_client = boto3.client('s3')
                    s3_client.head_bucket(Bucket=s3_bucket)
                    s3_status = "accessible"
                except ClientError:
                    s3_status = "inaccessible"
                except Exception:
                    s3_status = "error"
            else:
                s3_status = "not_configured"
            
            data_config['s3_status'] = s3_status
        else:
            # Check local directory
            import os
            base_path = data_config['base_path']
            os.makedirs(base_path, exist_ok=True)
            data_config['local_status'] = "accessible" if os.path.exists(base_path) else "inaccessible"
        
        return jsonify({
            "success": True,
            "deployment_mode": deployment_config.mode.value,
            "data_source": data_config
        })
        
    except Exception as e:
        logger.error(f"Failed to get data source info: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@deployment_bp.route('/environment-info', methods=['GET'])
def get_environment_info():
    """Get comprehensive environment information"""
    try:
        deployment_config = get_deployment_config()
        config = get_config()
        
        # Gather environment information
        env_info = {
            "deployment_mode": deployment_config.mode.value,
            "flask_env": config.FLASK_ENV,
            "environment": config.ENVIRONMENT,
            "debug_mode": config.FLASK_DEBUG,
            "host": config.HOST,
            "port": config.PORT,
            "log_level": config.LOG_LEVEL,
            "s3_configured": bool(config.S3_BUCKET),
            "aws_region": config.AWS_REGION,
            "environment_variables": {
                "DEPLOYMENT_MODE": os.getenv('DEPLOYMENT_MODE'),
                "FLASK_ENV": os.getenv('FLASK_ENV'),
                "ENVIRONMENT": os.getenv('ENVIRONMENT'),
                "S3_BUCKET": bool(os.getenv('S3_BUCKET')),  # Don't expose actual bucket name
                "SPARK_MASTER_URL": os.getenv('SPARK_MASTER_URL'),
                "AWS_REGION": os.getenv('AWS_REGION')
            }
        }
        
        return jsonify({
            "success": True,
            "environment_info": env_info
        })
        
    except Exception as e:
        logger.error(f"Failed to get environment info: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500
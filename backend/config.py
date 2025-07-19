"""
Configuration settings for the DB Spark application
"""

import os
from typing import Dict, Any

class Config:
    """Base configuration class"""
    
    # Flask Configuration
    FLASK_ENV = os.getenv('FLASK_ENV', 'development')
    FLASK_DEBUG = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
    
    # Server Configuration
    HOST = os.getenv('HOST', '0.0.0.0')
    PORT = int(os.getenv('PORT', 5000))
    
    # Deployment Mode
    DEPLOYMENT_MODE = os.getenv('DEPLOYMENT_MODE', 'local_small')
    
    # Upload Configuration
    UPLOAD_FOLDER = 'uploads'
    ALLOWED_EXTENSIONS = {'csv', 'txt', 'json'}
    MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB
    
    # Logging Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
    LOG_FILE = 'app.log'
    STRUCTURED_LOG_FILE = 'app_structured.log'
    
    # AWS Configuration
    AWS_REGION = os.getenv('AWS_REGION', 'us-west-2')
    S3_BUCKET = os.getenv('S3_BUCKET')
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    # Environment
    ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
    
    @property
    def CORS_ORIGINS(self):
        """Get CORS origins based on deployment configuration"""
        from deployment_config import get_deployment_config
        deployment_config = get_deployment_config()
        return deployment_config.get_cors_origins()
    
    @classmethod
    def get_spark_config(cls) -> Dict[str, Any]:
        """Get Spark configuration dictionary based on deployment mode"""
        from deployment_config import get_deployment_config
        deployment_config = get_deployment_config()
        return deployment_config.get_spark_config()

class DevelopmentConfig(Config):
    """Development configuration"""
    FLASK_DEBUG = True

class ProductionConfig(Config):
    """Production configuration"""
    FLASK_DEBUG = False
    LOG_LEVEL = 'WARNING'

class TestingConfig(Config):
    """Testing configuration"""
    TESTING = True
    FLASK_DEBUG = True

# Configuration mapping
config_map = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}

def get_config(config_name: str = None) -> Config:
    """Get configuration class based on environment"""
    if config_name is None:
        config_name = os.getenv('FLASK_ENV', 'development')
    
    return config_map.get(config_name, config_map['default'])
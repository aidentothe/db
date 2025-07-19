"""
Spark Session Manager - Handles Spark session creation and configuration
"""

import os
import sys
import logging
from typing import Optional
from pyspark.sql import SparkSession
from config import Config
from deployment_config import get_deployment_config

logger = logging.getLogger(__name__)

class SparkSessionManager:
    """Manages Spark session lifecycle and configuration"""
    
    def __init__(self, config: Config):
        self.config = config
        self.deployment_config = get_deployment_config()
        self._spark_session: Optional[SparkSession] = None
        self._setup_environment()
        
        # Print configuration summary on startup
        self.deployment_config.print_configuration_summary()
    
    def _setup_environment(self):
        """Set up environment variables for Spark"""
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        os.environ['HADOOP_USER_NAME'] = 'spark'
    
    def get_session(self) -> Optional[SparkSession]:
        """Get or create Spark session with deployment-aware configuration"""
        if self._spark_session is not None:
            return self._spark_session
        
        try:
            self._spark_session = self._create_spark_session()
            if self._spark_session:
                # Set log level based on deployment mode
                if self.deployment_config.is_local_mode():
                    self._spark_session.sparkContext.setLogLevel("ERROR")
                else:
                    self._spark_session.sparkContext.setLogLevel("WARN")
                
                spark_config = self.deployment_config.get_spark_config()
                logger.info("Spark session created successfully", extra={
                    'operation': 'spark_session_created',
                    'deployment_mode': self.deployment_config.mode.value,
                    'master_url': spark_config.get('spark.master'),
                    'app_name': spark_config.get('spark.app.name')
                })
            return self._spark_session
            
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}", extra={
                'operation': 'spark_session_failed',
                'deployment_mode': self.deployment_config.mode.value,
                'error_type': type(e).__name__
            })
            return None
    
    def _create_spark_session(self) -> Optional[SparkSession]:
        """Create Spark session based on deployment configuration"""
        
        spark_config = self.deployment_config.get_spark_config()
        master_url = spark_config.get('spark.master')
        
        try:
            return self._build_spark_session(spark_config)
        except Exception as e:
            logger.warning(f"Failed to create Spark session with {master_url}: {e}")
            
            # Fallback strategy based on deployment mode
            if self.deployment_config.is_local_mode():
                return self._build_fallback_local_session()
            else:
                # For AWS mode, try local fallback as last resort
                logger.info("AWS mode failed, falling back to local mode")
                return self._build_fallback_local_session()
    
    def _build_spark_session(self, spark_config: dict) -> SparkSession:
        """Build Spark session with deployment-specific configuration"""
        builder = SparkSession.builder
        
        # Apply all configuration from deployment config
        for key, value in spark_config.items():
            if key == 'spark.master':
                builder = builder.master(value)
            elif key == 'spark.app.name':
                builder = builder.appName(value)
            else:
                builder = builder.config(key, value)
        
        return builder.getOrCreate()
    
    def _build_fallback_local_session(self) -> Optional[SparkSession]:
        """Build minimal local Spark session as fallback"""
        try:
            logger.info("Creating fallback local Spark session")
            return SparkSession.builder \
                .appName("DB-Fallback-Local") \
                .master("local") \
                .config("spark.driver.memory", "512m") \
                .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
                .config("spark.ui.enabled", "false") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .config("spark.sql.adaptive.enabled", "false") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
        except Exception as e:
            logger.error(f"Failed to create fallback Spark session: {e}")
            return None
    
    def stop_session(self):
        """Stop the Spark session and clean up resources"""
        if self._spark_session:
            try:
                self._spark_session.stop()
                logger.info("Spark session stopped", extra={'operation': 'spark_session_stopped'})
            except Exception as e:
                logger.error(f"Error stopping Spark session: {e}")
            finally:
                self._spark_session = None
    
    def is_spark_available(self) -> bool:
        """Check if Spark is available and working"""
        session = self.get_session()
        if session is None:
            return False
        
        try:
            # Test basic Spark functionality
            test_rdd = session.sparkContext.parallelize([1, 2, 3])
            result = test_rdd.collect()
            return len(result) == 3
        except Exception as e:
            logger.warning(f"Spark availability test failed: {e}")
            return False
    
    def get_spark_info(self) -> dict:
        """Get information about the current Spark session"""
        session = self.get_session()
        deployment_mode = self.deployment_config.mode.value
        
        base_info = {
            "deployment_mode": deployment_mode,
            "s3_enabled": self.deployment_config.is_s3_enabled(),
            "local_mode": self.deployment_config.is_local_mode()
        }
        
        if session is None:
            return {
                **base_info,
                "status": "unavailable", 
                "mode": "pandas-only"
            }
        
        try:
            sc = session.sparkContext
            return {
                **base_info,
                "status": "available",
                "spark_version": session.version,
                "app_name": sc.appName,
                "master": sc.master,
                "default_parallelism": sc.defaultParallelism,
                "mode": "spark-enabled"
            }
        except Exception as e:
            logger.error(f"Error getting Spark info: {e}")
            return {
                **base_info,
                "status": "error", 
                "error": str(e)
            }

# Global session manager instance
_session_manager: Optional[SparkSessionManager] = None

def get_spark_manager(config: Config = None) -> SparkSessionManager:
    """Get or create the global Spark session manager"""
    global _session_manager
    
    if _session_manager is None:
        if config is None:
            from config import get_config
            config = get_config()
        _session_manager = SparkSessionManager(config)
    
    return _session_manager

def cleanup_spark():
    """Clean up the global Spark session"""
    global _session_manager
    if _session_manager:
        _session_manager.stop_session()
        _session_manager = None
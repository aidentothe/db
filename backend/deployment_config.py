"""
Deployment Configuration - Toggle between local and AWS modes
"""

import os
from enum import Enum
from typing import Dict, Any, Optional
from config import Config

class DeploymentMode(Enum):
    """Deployment mode enumeration"""
    LOCAL_SMALL = "local_small"
    AWS_LARGE = "aws_large"

class DeploymentConfig:
    """Manages deployment-specific configurations"""
    
    def __init__(self):
        self.mode = self._get_deployment_mode()
    
    def _get_deployment_mode(self) -> DeploymentMode:
        """Determine deployment mode from environment"""
        mode_str = os.getenv('DEPLOYMENT_MODE', 'local_small').lower()
        
        try:
            return DeploymentMode(mode_str)
        except ValueError:
            print(f"Invalid deployment mode '{mode_str}', defaulting to local_small")
            return DeploymentMode.LOCAL_SMALL
    
    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration based on deployment mode"""
        if self.mode == DeploymentMode.LOCAL_SMALL:
            return self._get_local_small_config()
        elif self.mode == DeploymentMode.AWS_LARGE:
            return self._get_aws_large_config()
        else:
            return self._get_local_small_config()
    
    def _get_local_small_config(self) -> Dict[str, Any]:
        """Configuration for small local deployment"""
        return {
            "spark.master": "local[*]",
            "spark.app.name": "DB-Local-Small",
            "spark.driver.memory": "512m",
            "spark.executor.memory": "512m",
            "spark.sql.warehouse.dir": "/tmp/spark-warehouse",
            "spark.driver.bindAddress": "127.0.0.1",
            "spark.driver.host": "127.0.0.1",
            "spark.ui.enabled": "false",
            "spark.sql.execution.arrow.pyspark.enabled": "false",
            "spark.sql.adaptive.enabled": "false",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.shuffle.partitions": "2",  # Small number for local
            "spark.default.parallelism": "2",
            "spark.driver.maxResultSize": "100m",
            # Local file system
            "spark.hadoop.fs.defaultFS": "file:///",
        }
    
    def _get_aws_large_config(self) -> Dict[str, Any]:
        """Configuration for large AWS deployment"""
        return {
            "spark.master": os.getenv('SPARK_MASTER_URL', 'spark://localhost:7077'),
            "spark.app.name": "DB-AWS-Large",
            "spark.driver.memory": "4g",
            "spark.executor.memory": "8g",
            "spark.executor.cores": "4",
            "spark.executor.instances": "4",
            "spark.sql.warehouse.dir": "/opt/spark/warehouse",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.shuffle.partitions": "200",
            "spark.default.parallelism": "16",
            "spark.driver.maxResultSize": "2g",
            # S3 Configuration
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider",
            "spark.hadoop.fs.s3a.multipart.size": "104857600",  # 100MB
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.hadoop.fs.s3a.block.size": "134217728",  # 128MB
            "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB
            "spark.sql.files.openCostInBytes": "4194304",  # 4MB
            # Performance optimizations
            "spark.network.timeout": "800s",
            "spark.executor.heartbeatInterval": "60s",
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.minExecutors": "1",
            "spark.dynamicAllocation.maxExecutors": "10",
            "spark.dynamicAllocation.initialExecutors": "2",
        }
    
    def get_data_source_config(self) -> Dict[str, Any]:
        """Get data source configuration based on mode"""
        if self.mode == DeploymentMode.LOCAL_SMALL:
            return {
                "type": "local",
                "base_path": "./data",
                "max_file_size": "10MB",
                "supported_formats": ["json", "csv", "parquet"],
                "default_format": "json"
            }
        elif self.mode == DeploymentMode.AWS_LARGE:
            return {
                "type": "s3",
                "bucket": os.getenv('S3_BUCKET'),
                "region": os.getenv('AWS_REGION', 'us-west-2'),
                "max_file_size": "1GB",
                "supported_formats": ["json", "csv", "parquet", "delta"],
                "default_format": "parquet"
            }
    
    def get_performance_limits(self) -> Dict[str, Any]:
        """Get performance limits based on deployment mode"""
        if self.mode == DeploymentMode.LOCAL_SMALL:
            return {
                "max_rows_per_query": 10000,
                "max_query_timeout": 30,  # seconds
                "max_concurrent_queries": 2,
                "max_memory_per_query": "256m",
                "query_complexity_limit": "MEDIUM"
            }
        elif self.mode == DeploymentMode.AWS_LARGE:
            return {
                "max_rows_per_query": 1000000,
                "max_query_timeout": 300,  # seconds
                "max_concurrent_queries": 10,
                "max_memory_per_query": "2g",
                "query_complexity_limit": "HIGH"
            }
    
    def get_monitoring_config(self) -> Dict[str, Any]:
        """Get monitoring configuration"""
        if self.mode == DeploymentMode.LOCAL_SMALL:
            return {
                "metrics_enabled": False,
                "spark_ui_enabled": False,
                "history_server_enabled": False,
                "detailed_logging": False
            }
        elif self.mode == DeploymentMode.AWS_LARGE:
            return {
                "metrics_enabled": True,
                "spark_ui_enabled": True,
                "spark_ui_port": 4040,
                "history_server_enabled": True,
                "history_server_port": 18080,
                "detailed_logging": True,
                "cloudwatch_enabled": True
            }
    
    def is_s3_enabled(self) -> bool:
        """Check if S3 is enabled for current mode"""
        return self.mode == DeploymentMode.AWS_LARGE
    
    def is_local_mode(self) -> bool:
        """Check if running in local mode"""
        return self.mode == DeploymentMode.LOCAL_SMALL
    
    def get_cors_origins(self) -> list:
        """Get CORS origins based on deployment mode"""
        base_origins = [
            'http://localhost:3000',
            'http://localhost:3001', 
            'http://localhost:3002'
        ]
        
        if self.mode == DeploymentMode.AWS_LARGE:
            base_origins.extend([
                'https://*.vercel.app',
                'https://*.amazonaws.com'
            ])
        
        return base_origins
    
    def get_api_rate_limits(self) -> Dict[str, Any]:
        """Get API rate limiting configuration"""
        if self.mode == DeploymentMode.LOCAL_SMALL:
            return {
                "enabled": False,
                "requests_per_minute": 60,
                "burst_limit": 10
            }
        elif self.mode == DeploymentMode.AWS_LARGE:
            return {
                "enabled": True,
                "requests_per_minute": 300,
                "burst_limit": 50
            }
    
    def get_caching_config(self) -> Dict[str, Any]:
        """Get caching configuration"""
        if self.mode == DeploymentMode.LOCAL_SMALL:
            return {
                "enabled": False,
                "cache_size": "50MB",
                "ttl_seconds": 300
            }
        elif self.mode == DeploymentMode.AWS_LARGE:
            return {
                "enabled": True,
                "cache_size": "1GB",
                "ttl_seconds": 3600,
                "redis_enabled": True
            }
    
    def print_configuration_summary(self):
        """Print a summary of the current configuration"""
        print(f"\n{'='*50}")
        print(f"DEPLOYMENT CONFIGURATION SUMMARY")
        print(f"{'='*50}")
        print(f"Mode: {self.mode.value.upper()}")
        print(f"S3 Enabled: {self.is_s3_enabled()}")
        print(f"Local Mode: {self.is_local_mode()}")
        
        perf_limits = self.get_performance_limits()
        print(f"\nPerformance Limits:")
        print(f"  Max Rows: {perf_limits['max_rows_per_query']:,}")
        print(f"  Query Timeout: {perf_limits['max_query_timeout']}s")
        print(f"  Concurrent Queries: {perf_limits['max_concurrent_queries']}")
        
        spark_config = self.get_spark_config()
        print(f"\nSpark Configuration:")
        print(f"  Master: {spark_config.get('spark.master', 'N/A')}")
        print(f"  Driver Memory: {spark_config.get('spark.driver.memory', 'N/A')}")
        print(f"  Executor Memory: {spark_config.get('spark.executor.memory', 'N/A')}")
        
        data_config = self.get_data_source_config()
        print(f"\nData Source:")
        print(f"  Type: {data_config['type'].upper()}")
        if data_config['type'] == 's3':
            print(f"  Bucket: {data_config.get('bucket', 'Not configured')}")
        else:
            print(f"  Path: {data_config['base_path']}")
        
        print(f"{'='*50}\n")

# Global deployment configuration instance
_deployment_config: Optional[DeploymentConfig] = None

def get_deployment_config() -> DeploymentConfig:
    """Get the global deployment configuration instance"""
    global _deployment_config
    if _deployment_config is None:
        _deployment_config = DeploymentConfig()
    return _deployment_config

def set_deployment_mode(mode: str):
    """Set deployment mode programmatically"""
    global _deployment_config
    os.environ['DEPLOYMENT_MODE'] = mode
    _deployment_config = None  # Force recreation with new mode
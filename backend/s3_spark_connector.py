"""
S3 Spark Connector - Handles data access from S3 using Apache Spark
"""

import os
import logging
from typing import Optional, Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import boto3
from botocore.exceptions import ClientError

class S3SparkConnector:
    """
    Manages Spark session and S3 data access for JSON table processing
    """
    
    def __init__(self, 
                 app_name: str = "S3SparkConnector",
                 master_url: Optional[str] = None,
                 s3_bucket: Optional[str] = None):
        """
        Initialize the S3 Spark connector
        
        Args:
            app_name: Name for the Spark application
            master_url: Spark master URL (e.g., "spark://localhost:7077")
            s3_bucket: S3 bucket name for data access
        """
        self.app_name = app_name
        self.master_url = master_url or os.getenv('SPARK_MASTER_URL', 'local[*]')
        self.s3_bucket = s3_bucket or os.getenv('S3_BUCKET')
        self.spark_session = None
        self.logger = logging.getLogger(__name__)
        
        # Initialize S3 client
        self.s3_client = boto3.client('s3')
        
    def get_spark_session(self) -> SparkSession:
        """
        Get or create a Spark session configured for S3 access
        
        Returns:
            SparkSession configured for S3 operations
        """
        if self.spark_session is None:
            self.logger.info(f"Creating Spark session: {self.app_name}")
            
            builder = SparkSession.builder \
                .appName(self.app_name) \
                .master(self.master_url)
            
            # Configure S3 access
            spark_config = {
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.adaptive.skewJoin.enabled": "true",
                "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB
                "spark.sql.files.openCostInBytes": "4194304",      # 4MB
            }
            
            # Apply configuration
            for key, value in spark_config.items():
                builder = builder.config(key, value)
            
            self.spark_session = builder.getOrCreate()
            self.spark_session.sparkContext.setLogLevel("WARN")
            
            self.logger.info(f"Spark session created successfully. Master: {self.master_url}")
            
        return self.spark_session
    
    def list_s3_objects(self, prefix: str = "") -> List[Dict[str, Any]]:
        """
        List objects in the S3 bucket with optional prefix filter
        
        Args:
            prefix: S3 object key prefix to filter by
            
        Returns:
            List of S3 object metadata dictionaries
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=prefix
            )
            
            objects = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    objects.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'etag': obj['ETag']
                    })
            
            self.logger.info(f"Found {len(objects)} objects in s3://{self.s3_bucket}/{prefix}")
            return objects
            
        except ClientError as e:
            self.logger.error(f"Error listing S3 objects: {e}")
            raise
    
    def read_json_from_s3(self, 
                         s3_path: str, 
                         schema: Optional[StructType] = None,
                         multiline: bool = True) -> DataFrame:
        """
        Read JSON data from S3 into a Spark DataFrame
        
        Args:
            s3_path: S3 path (e.g., "data/table1.json" or "s3a://bucket/data/table1.json")
            schema: Optional predefined schema for the JSON data
            multiline: Whether to read multiline JSON objects
            
        Returns:
            Spark DataFrame containing the JSON data
        """
        spark = self.get_spark_session()
        
        # Construct full S3 path if not provided
        if not s3_path.startswith('s3a://'):
            full_path = f"s3a://{self.s3_bucket}/{s3_path}"
        else:
            full_path = s3_path
        
        try:
            self.logger.info(f"Reading JSON data from: {full_path}")
            
            reader = spark.read.option("multiline", str(multiline).lower())
            
            if schema:
                reader = reader.schema(schema)
            
            df = reader.json(full_path)
            
            row_count = df.count()
            self.logger.info(f"Successfully read {row_count:,} rows from {full_path}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading JSON from S3: {e}")
            raise
    
    def write_json_to_s3(self, 
                        df: DataFrame, 
                        s3_path: str,
                        mode: str = "overwrite",
                        coalesce_partitions: int = 1) -> None:
        """
        Write DataFrame to S3 as JSON
        
        Args:
            df: Spark DataFrame to write
            s3_path: S3 destination path
            mode: Write mode ("overwrite", "append", "ignore", "error")
            coalesce_partitions: Number of partitions to coalesce to
        """
        # Construct full S3 path if not provided
        if not s3_path.startswith('s3a://'):
            full_path = f"s3a://{self.s3_bucket}/{s3_path}"
        else:
            full_path = s3_path
        
        try:
            self.logger.info(f"Writing DataFrame to: {full_path}")
            
            writer = df.coalesce(coalesce_partitions).write.mode(mode)
            writer.json(full_path)
            
            self.logger.info(f"Successfully wrote DataFrame to {full_path}")
            
        except Exception as e:
            self.logger.error(f"Error writing JSON to S3: {e}")
            raise
    
    def read_parquet_from_s3(self, s3_path: str) -> DataFrame:
        """
        Read Parquet data from S3 into a Spark DataFrame
        
        Args:
            s3_path: S3 path to the Parquet file(s)
            
        Returns:
            Spark DataFrame containing the Parquet data
        """
        spark = self.get_spark_session()
        
        if not s3_path.startswith('s3a://'):
            full_path = f"s3a://{self.s3_bucket}/{s3_path}"
        else:
            full_path = s3_path
        
        try:
            self.logger.info(f"Reading Parquet data from: {full_path}")
            df = spark.read.parquet(full_path)
            
            row_count = df.count()
            self.logger.info(f"Successfully read {row_count:,} rows from {full_path}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading Parquet from S3: {e}")
            raise
    
    def write_parquet_to_s3(self, 
                           df: DataFrame, 
                           s3_path: str,
                           mode: str = "overwrite",
                           partition_cols: Optional[List[str]] = None) -> None:
        """
        Write DataFrame to S3 as Parquet
        
        Args:
            df: Spark DataFrame to write
            s3_path: S3 destination path
            mode: Write mode ("overwrite", "append", "ignore", "error")
            partition_cols: Optional list of columns to partition by
        """
        if not s3_path.startswith('s3a://'):
            full_path = f"s3a://{self.s3_bucket}/{s3_path}"
        else:
            full_path = s3_path
        
        try:
            self.logger.info(f"Writing DataFrame to Parquet: {full_path}")
            
            writer = df.write.mode(mode)
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer.parquet(full_path)
            
            self.logger.info(f"Successfully wrote DataFrame to {full_path}")
            
        except Exception as e:
            self.logger.error(f"Error writing Parquet to S3: {e}")
            raise
    
    def execute_sql_query(self, df: DataFrame, query: str, temp_view_name: str = "temp_table") -> DataFrame:
        """
        Execute SQL query on a DataFrame
        
        Args:
            df: Source DataFrame
            query: SQL query to execute
            temp_view_name: Name for the temporary view
            
        Returns:
            Result DataFrame
        """
        spark = self.get_spark_session()
        
        try:
            # Create temporary view
            df.createOrReplaceTempView(temp_view_name)
            
            # Execute query
            result_df = spark.sql(query)
            
            self.logger.info(f"Successfully executed SQL query on {temp_view_name}")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error executing SQL query: {e}")
            raise
    
    def get_table_info(self, s3_path: str) -> Dict[str, Any]:
        """
        Get metadata information about a table stored in S3
        
        Args:
            s3_path: S3 path to the table
            
        Returns:
            Dictionary containing table metadata
        """
        try:
            df = self.read_json_from_s3(s3_path)
            
            info = {
                'row_count': df.count(),
                'column_count': len(df.columns),
                'columns': df.columns,
                'schema': df.schema.jsonValue(),
                'sample_data': df.limit(5).collect()
            }
            
            return info
            
        except Exception as e:
            self.logger.error(f"Error getting table info: {e}")
            raise
    
    def stop_spark_session(self) -> None:
        """
        Stop the Spark session and clean up resources
        """
        if self.spark_session:
            self.logger.info("Stopping Spark session")
            self.spark_session.stop()
            self.spark_session = None
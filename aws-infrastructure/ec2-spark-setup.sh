#!/bin/bash

# EC2 Spark Cluster Setup Script
# This script sets up Apache Spark on an EC2 instance

set -e

# Configuration
SPARK_VERSION="3.5.0"
HADOOP_VERSION="3"
JAVA_VERSION="11"
PYTHON_VERSION="3.11"

echo "=== Starting Spark Cluster Setup on EC2 ==="

# Update system
sudo apt-get update -y
sudo apt-get upgrade -y

# Install Java 11
echo "Installing Java ${JAVA_VERSION}..."
sudo apt-get install -y openjdk-${JAVA_VERSION}-jdk

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64
echo "export JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64" >> ~/.bashrc

# Install Python and pip
echo "Installing Python ${PYTHON_VERSION}..."
sudo apt-get install -y python${PYTHON_VERSION} python${PYTHON_VERSION}-pip python${PYTHON_VERSION}-venv

# Create symbolic links
sudo ln -sf /usr/bin/python${PYTHON_VERSION} /usr/bin/python3
sudo ln -sf /usr/bin/python${PYTHON_VERSION} /usr/bin/python

# Download and install Spark
echo "Downloading Spark ${SPARK_VERSION}..."
cd /opt
sudo wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
sudo tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
sudo mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark
sudo chown -R ubuntu:ubuntu /opt/spark

# Set Spark environment variables
echo "Configuring Spark environment..."
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# Add to bashrc
cat >> ~/.bashrc << EOF
export SPARK_HOME=/opt/spark
export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
EOF

# Install Python dependencies
echo "Installing Python dependencies..."
pip3 install --upgrade pip
pip3 install pyspark==${SPARK_VERSION} boto3 s3fs pandas numpy

# Configure Spark for S3 access
echo "Configuring Spark for S3 access..."
cd /opt/spark/jars

# Download AWS SDK and Hadoop AWS JARs
sudo wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
sudo wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# Create Spark configuration
sudo mkdir -p /opt/spark/conf
sudo tee /opt/spark/conf/spark-defaults.conf << EOF
# S3 Configuration
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true

# Performance tuning
spark.executor.memory=4g
spark.driver.memory=2g
spark.executor.cores=2
spark.default.parallelism=8
spark.sql.shuffle.partitions=200
EOF

# Create systemd service for Spark master
sudo tee /etc/systemd/system/spark-master.service << EOF
[Unit]
Description=Apache Spark Master
After=network.target

[Service]
Type=forking
User=ubuntu
Group=ubuntu
ExecStart=/opt/spark/sbin/start-master.sh
ExecStop=/opt/spark/sbin/stop-master.sh
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

# Create systemd service for Spark worker
sudo tee /etc/systemd/system/spark-worker.service << EOF
[Unit]
Description=Apache Spark Worker
After=network.target spark-master.service

[Service]
Type=forking
User=ubuntu
Group=ubuntu
ExecStart=/opt/spark/sbin/start-worker.sh spark://localhost:7077
ExecStop=/opt/spark/sbin/stop-worker.sh
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

# Enable and start services
sudo systemctl daemon-reload
sudo systemctl enable spark-master
sudo systemctl enable spark-worker
sudo systemctl start spark-master
sudo systemctl start spark-worker

# Install additional monitoring tools
echo "Installing monitoring tools..."
pip3 install psutil py4j

# Create log directory
sudo mkdir -p /var/log/spark
sudo chown ubuntu:ubuntu /var/log/spark

# Set up log rotation
sudo tee /etc/logrotate.d/spark << EOF
/var/log/spark/*.log {
    daily
    missingok
    rotate 7
    compress
    notifempty
    create 644 ubuntu ubuntu
    postrotate
        systemctl reload spark-master spark-worker
    endscript
}
EOF

echo "=== Spark Cluster Setup Complete ==="
echo "Spark Master UI: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4):8080"
echo "Spark History Server: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4):18080"
echo ""
echo "To test the installation:"
echo "spark-submit --class org.apache.spark.examples.SparkPi \$SPARK_HOME/examples/jars/spark-examples_2.12-${SPARK_VERSION}.jar 10"
#!/bin/bash

# User data script for EC2 instance setup
set -e

# Log output
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

# Update system
apt-get update -y
apt-get upgrade -y

# Install basic dependencies
apt-get install -y wget curl unzip awscli

# Set S3 bucket environment variable
echo "export S3_BUCKET=${s3_bucket}" >> /home/ubuntu/.bashrc

# Download and run Spark setup script
cd /home/ubuntu
wget https://raw.githubusercontent.com/your-repo/db/main/aws-infrastructure/ec2-spark-setup.sh
chmod +x ec2-spark-setup.sh
./ec2-spark-setup.sh

# Clone your application repository (replace with your actual repo)
# git clone https://github.com/your-username/your-repo.git /home/ubuntu/app
# chown -R ubuntu:ubuntu /home/ubuntu/app

echo "EC2 instance setup completed successfully"
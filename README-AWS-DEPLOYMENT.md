# AWS Spark Deployment Guide

This guide covers deploying your DB application with Apache Spark on AWS EC2 while maintaining your Vercel frontend.

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Vercel        │    │   AWS EC2       │    │   AWS S3        │
│   (Frontend)    │◄──►│   (Spark +      │◄──►│   (JSON Data)   │
│                 │    │    Backend API) │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Prerequisites

1. **AWS Account** with appropriate permissions
2. **AWS CLI** configured with credentials
3. **Terraform** installed (>= 1.0)
4. **SSH key pair** for EC2 access

## Quick Start

### 1. Configure AWS Credentials

```bash
aws configure
# Enter your Access Key ID, Secret Access Key, and region
```

### 2. Deploy Infrastructure

```bash
cd aws-infrastructure
./deploy.sh deploy
```

This will:
- Create VPC, subnets, and security groups
- Launch EC2 instance with Spark cluster
- Create S3 bucket for data storage
- Set up IAM roles and policies
- Deploy your backend application

### 3. Update Frontend Configuration

Update your Vercel environment variables:

```env
NEXT_PUBLIC_API_URL=http://YOUR_EC2_IP:5000
```

## Manual Setup (Alternative)

### 1. Infrastructure Setup

```bash
cd aws-infrastructure/terraform
terraform init
terraform plan
terraform apply
```

### 2. Application Deployment

```bash
# Get EC2 IP from Terraform output
EC2_IP=$(terraform output -raw spark_master_public_ip)

# Upload application
scp -r ../../backend/ ubuntu@$EC2_IP:~/app/

# SSH into instance
ssh ubuntu@$EC2_IP

# Install dependencies and start services
cd ~/app
pip3 install -r requirements.txt
python3 app_main.py
```

## Configuration

### Environment Variables

Set these in your EC2 instance:

```env
SPARK_MASTER_URL=spark://localhost:7077
S3_BUCKET=your-bucket-name
AWS_REGION=us-west-2
FLASK_ENV=production
```

### Spark Configuration

Default Spark settings in `/opt/spark/conf/spark-defaults.conf`:

```properties
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.sql.adaptive.enabled=true
```

## API Endpoints

Your Spark-enabled API will be available at:

- **Health Check**: `GET /health`
- **Spark Health**: `GET /api/spark/health`
- **List S3 Objects**: `GET /api/spark/s3/list`
- **Table Info**: `GET /api/spark/table/info?path=data/table.json`
- **Execute Query**: `POST /api/spark/query/execute`
- **Table Sample**: `GET /api/spark/table/sample?path=data/table.json`

### Example Query Request

```json
POST /api/spark/query/execute
{
    "query": "SELECT * FROM main_table WHERE age > 25 LIMIT 100",
    "s3_path": "data/users.json",
    "limit": 100
}
```

## Data Management

### Uploading Data to S3

```bash
# Upload JSON files to your S3 bucket
aws s3 cp local-data.json s3://your-bucket/data/table1.json
aws s3 cp another-file.json s3://your-bucket/data/table2.json
```

### Supported Formats

- **JSON**: Multi-line JSON objects
- **Parquet**: Columnar format for better performance
- **CSV**: With automatic schema inference

## Monitoring

### Spark Web UI
Access at: `http://YOUR_EC2_IP:8080`

### Application Logs
```bash
# View application logs
sudo journalctl -u spark-api -f

# View Spark logs
tail -f /var/log/spark/*.log
```

## Security Considerations

1. **IAM Roles**: EC2 instances use IAM roles instead of hardcoded credentials
2. **Security Groups**: Only necessary ports are opened
3. **VPC**: Resources are isolated in a private network
4. **HTTPS**: Consider adding ALB with SSL certificate for production

## Cost Optimization

- **Instance Types**: 
  - Master: t3.large (~$0.08/hour)
  - Workers: t3.medium (~$0.04/hour each)
- **S3 Storage**: Pay for what you store (~$0.023/GB/month)
- **Data Transfer**: Minimal costs for API calls

## Scaling

### Horizontal Scaling
Add worker nodes:

```bash
# In terraform/main.tf, increase worker_count
variable "worker_count" {
  default = 4  # Increase from 2
}

terraform apply
```

### Vertical Scaling
Use larger instance types:

```bash
# In terraform/variables.tf
variable "master_instance_type" {
  default = "t3.xlarge"  # Upgrade from t3.large
}
```

## Troubleshooting

### Common Issues

1. **SSH Connection Failed**
   ```bash
   # Wait longer for instance to initialize
   sleep 60
   ssh ubuntu@$EC2_IP
   ```

2. **Spark Not Starting**
   ```bash
   sudo systemctl status spark-master
   sudo journalctl -u spark-master
   ```

3. **S3 Access Denied**
   - Check IAM role permissions
   - Verify bucket policy

4. **API Not Responding**
   ```bash
   sudo systemctl status spark-api
   sudo journalctl -u spark-api
   ```

### Performance Tuning

```properties
# Increase memory allocation
spark.executor.memory=8g
spark.driver.memory=4g

# Optimize for S3
spark.hadoop.fs.s3a.multipart.size=104857600
spark.hadoop.fs.s3a.fast.upload=true
```

## Cleanup

To destroy all resources:

```bash
./deploy.sh destroy
```

Or manually:

```bash
cd terraform
terraform destroy
```

## Integration with Frontend

Update your Next.js/React frontend to use the new API:

```javascript
// Use environment variable for API URL
const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:5000';

// Example API call
const queryData = async (query, tablePath) => {
  const response = await fetch(`${API_URL}/api/spark/query/execute`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      query,
      s3_path: tablePath,
      limit: 1000
    })
  });
  return response.json();
};
```

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review AWS CloudWatch logs
3. Examine Spark Web UI for cluster health
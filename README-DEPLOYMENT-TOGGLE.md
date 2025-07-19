# Deployment Mode Toggle

The DB Spark application now supports easy toggling between **local small** and **large AWS** deployment modes.

## Quick Start

### Option 1: Environment Variable
```bash
# For local development
export DEPLOYMENT_MODE=local_small

# For AWS production
export DEPLOYMENT_MODE=aws_large
```

### Option 2: Script Toggle
```bash
# Interactive toggle
./scripts/toggle-deployment.sh

# Direct mode setting
./scripts/toggle-deployment.sh local_small
./scripts/toggle-deployment.sh aws_large

# Auto toggle (switches to other mode)
./scripts/toggle-deployment.sh auto
```

### Option 3: API Toggle
```bash
# Toggle via API
curl -X POST http://localhost:5000/api/deployment/mode/toggle

# Set specific mode
curl -X POST http://localhost:5000/api/deployment/mode \
  -H "Content-Type: application/json" \
  -d '{"mode": "aws_large"}'

# Get current configuration
curl http://localhost:5000/api/deployment/config
```

## Deployment Modes

### 🏠 Local Small (`local_small`)
**Perfect for development and testing**

- **Spark**: Local cluster with minimal resources
- **Memory**: 512MB driver, 512MB executor  
- **Data Storage**: Local file system (`./data/`)
- **Parallelism**: 2 cores
- **S3**: Disabled
- **UI**: Disabled for performance
- **Max Query Rows**: 10,000
- **Query Timeout**: 30 seconds

```bash
DEPLOYMENT_MODE=local_small
```

### 🚀 AWS Large (`aws_large`)
**Optimized for production workloads**

- **Spark**: AWS EC2 cluster
- **Memory**: 4GB driver, 8GB executor
- **Data Storage**: AWS S3
- **Parallelism**: 16 cores (auto-scaling 1-10 executors)
- **S3**: Enabled with optimizations
- **UI**: Enabled (port 4040, 18080)
- **Max Query Rows**: 1,000,000
- **Query Timeout**: 300 seconds

```bash
DEPLOYMENT_MODE=aws_large
S3_BUCKET=your-bucket-name
SPARK_MASTER_URL=spark://your-ec2-ip:7077
```

## Configuration Comparison

| Feature | Local Small | AWS Large |
|---------|-------------|-----------|
| **Spark Master** | `local[*]` | `spark://ec2-ip:7077` |
| **Driver Memory** | 512MB | 4GB |
| **Executor Memory** | 512MB | 8GB |
| **Executor Cores** | 2 | 4 |
| **Max Executors** | 1 | 10 |
| **Data Source** | Local files | S3 |
| **Spark UI** | Disabled | Enabled |
| **History Server** | Disabled | Enabled |
| **Adaptive Query** | Disabled | Enabled |
| **Dynamic Allocation** | Disabled | Enabled |
| **Max Rows/Query** | 10,000 | 1,000,000 |
| **Query Timeout** | 30s | 300s |
| **Concurrent Queries** | 2 | 10 |

## API Endpoints

### Get Current Configuration
```bash
GET /api/deployment/config
```

**Response:**
```json
{
  "success": true,
  "configuration": {
    "current_mode": "local_small",
    "available_modes": ["local_small", "aws_large"],
    "spark_config": { "spark.master": "local[*]", ... },
    "performance_limits": { "max_rows_per_query": 10000, ... },
    "s3_enabled": false,
    "local_mode": true
  }
}
```

### Set Deployment Mode
```bash
POST /api/deployment/mode
Content-Type: application/json

{
  "mode": "aws_large"
}
```

### Toggle Mode
```bash
POST /api/deployment/mode/toggle
```

### Restart Spark Session
```bash
POST /api/deployment/restart-spark
```

### Get Performance Limits
```bash
GET /api/deployment/performance-limits
```

### Get Data Source Info
```bash
GET /api/deployment/data-source-info
```

## Environment Variables

### Required for Both Modes
```env
DEPLOYMENT_MODE=local_small  # or aws_large
FLASK_ENV=development
PORT=5000
LOG_LEVEL=INFO
```

### Additional for AWS Large Mode
```env
DEPLOYMENT_MODE=aws_large
SPARK_MASTER_URL=spark://your-ec2-ip:7077
S3_BUCKET=your-spark-data-bucket
AWS_REGION=us-west-2
```

### Optional AWS Credentials
```env
# For local development (use IAM roles in production)
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
```

## Data Access Patterns

### Local Small Mode
```python
# Reads from ./data/ directory
df = spark.read.json("./data/table.json")

# API endpoints expect local paths
POST /api/spark/table/info?path=table.json
```

### AWS Large Mode
```python
# Reads from S3
df = spark.read.json("s3a://bucket/data/table.json")

# API endpoints expect S3 paths
POST /api/spark/table/info?path=data/table.json
```

## Frontend Integration

### Check Current Mode
```javascript
const getDeploymentInfo = async () => {
  const response = await fetch('/api/deployment/config');
  const { configuration } = await response.json();
  
  console.log('Current mode:', configuration.current_mode);
  console.log('S3 enabled:', configuration.s3_enabled);
  
  return configuration;
};
```

### Dynamic API Calls
```javascript
const queryData = async (query, tablePath) => {
  const config = await getDeploymentInfo();
  
  // Adjust API call based on mode
  const endpoint = config.s3_enabled 
    ? '/api/spark/query/execute'  // S3-enabled endpoints
    : '/api/execute-query';       // Local endpoints
    
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, s3_path: tablePath })
  });
  
  return response.json();
};
```

### Toggle Mode from Frontend
```javascript
const toggleDeploymentMode = async () => {
  const response = await fetch('/api/deployment/mode/toggle', {
    method: 'POST'
  });
  
  const result = await response.json();
  
  if (result.restart_required) {
    alert('Deployment mode changed. Please restart the backend.');
  }
  
  return result;
};
```

## Performance Considerations

### Local Small Mode
- ✅ Fast startup
- ✅ Low memory usage
- ✅ No external dependencies
- ❌ Limited data size
- ❌ Single machine processing

### AWS Large Mode
- ✅ Handles large datasets
- ✅ Distributed processing
- ✅ Production-ready
- ✅ Auto-scaling
- ❌ Requires AWS setup
- ❌ Higher costs

## Troubleshooting

### Mode Switch Issues
```bash
# Check current mode
curl http://localhost:5000/api/deployment/config

# Restart Spark session
curl -X POST http://localhost:5000/api/deployment/restart-spark

# Check environment info
curl http://localhost:5000/api/deployment/environment-info
```

### AWS Mode Requirements
1. **S3 Bucket**: Must exist and be accessible
2. **EC2 Instance**: Spark cluster must be running
3. **IAM Roles**: Proper permissions for S3 access
4. **Network**: Security groups allow Spark communication

### Common Errors
```bash
# S3 access denied
DEPLOYMENT_MODE=aws_large
# Ensure IAM role has S3 permissions

# Spark master unreachable
SPARK_MASTER_URL=spark://wrong-ip:7077
# Verify EC2 instance IP and Spark is running

# Mode not recognized
DEPLOYMENT_MODE=invalid_mode
# Use only 'local_small' or 'aws_large'
```

## Development Workflow

### 1. Start with Local Development
```bash
./scripts/toggle-deployment.sh local_small
python backend/app_main.py
```

### 2. Test with Small Data
Upload small JSON files and test queries locally.

### 3. Switch to AWS for Production
```bash
# Deploy AWS infrastructure first
cd aws-infrastructure && ./deploy.sh deploy

# Switch to AWS mode
./scripts/toggle-deployment.sh aws_large

# Restart application
python backend/app_main.py
```

### 4. Upload Production Data
```bash
# Upload to S3
aws s3 cp local-data.json s3://your-bucket/data/

# Test with production data
curl -X POST http://localhost:5000/api/spark/query/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM main_table LIMIT 100", "s3_path": "data/production-data.json"}'
```

The toggle system provides seamless switching between development and production environments while maintaining the full power of Apache Spark in both modes.
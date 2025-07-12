# Comprehensive Logging Implementation

## Overview
This implementation adds detailed, structured logging throughout the Spark NextJS POC application to track file processing, uploads, failures, and performance metrics.

## Fixed Issues
✅ **CORS Configuration**: Updated to properly allow requests from frontend ports (3000, 3002)  
✅ **Port Consistency**: Fixed frontend to use correct backend port (5000)  
✅ **Request Logging**: Added pre-request logging to track all incoming requests  

## Logging Features

### Backend Logging (Python/Flask)
- **Structured JSON logging** for production analysis
- **Human-readable console logging** for development
- **Unique operation IDs** to track requests across the pipeline
- **Performance timing** for all operations
- **Comprehensive error tracking** with stack traces

### Frontend Logging (TypeScript/React)
- **API request/response tracking** with timing
- **User interaction logging** (file uploads, tab switches, etc.)
- **Error boundary integration**
- **Development vs production logging modes**

## Log Files Generated
- `app.log` - Human-readable logs
- `app_structured.log` - JSON structured logs for analysis
- Browser console - Frontend logs with context

## Starting the Application

### 1. Backend (Terminal 1)
```bash
cd backend/backend
pip install flask flask-cors pyspark pandas
python app.py
```
The backend will start on **http://localhost:5000**

### 2. Frontend (Terminal 2)  
```bash
cd frontend
npm install
npm run dev
```
The frontend will start on **http://localhost:3002**

## Testing the Logging

### Option 1: Manual Testing
1. Start both backend and frontend
2. Open browser to http://localhost:3002
3. Interact with the application (upload files, load datasets, run queries)
4. Check the generated log files for detailed logging output

### Option 2: Automated Testing
```bash
# Install requests if needed
pip install requests pandas

# Run the logging test
python3 test_logging_simple.py
```

## What Gets Logged

### File Operations
- **Upload start/completion/failure** with file size tracking
- **File validation** and security checks  
- **CSV processing** and complexity analysis
- **File listing, loading, and deletion**

### Data Analysis  
- **SQL query execution** with complexity analysis
- **Spark DataFrame operations** and performance metrics
- **Machine learning operations** (K-means clustering)
- **Statistical analysis** and aggregations

### Error Handling
- **Exception tracking** with full stack traces
- **Operation-specific error categorization**
- **Automatic cleanup logging** on failures

### User Interactions (Frontend)
- **File drag-and-drop** operations
- **Tab navigation** and component interactions
- **API request failures** and retries
- **Performance metrics** for user actions

## Log Structure Example

```json
{
  "timestamp": "2025-01-10T12:34:56.789Z",
  "level": "INFO", 
  "operation": "file_upload_complete",
  "upload_id": "upload_1641825296789",
  "file_name": "data.csv",
  "file_size": 1048576,
  "row_count": 5000,
  "column_count": 8,
  "processing_time": 2.45,
  "remote_addr": "127.0.0.1"
}
```

## Troubleshooting

### CORS Issues
If you see CORS errors, ensure:
- Backend is running on port 5000
- Frontend is accessing the correct backend URL
- CORS is properly configured for your frontend port

### Port Issues  
- Backend: http://localhost:5000
- Frontend: http://localhost:3002
- Update the URLs in DatasetSelector.tsx if using different ports

### Log File Permissions
Ensure the backend directory is writable for log file creation:
```bash
chmod 755 backend/backend/
```

## Monitoring and Analysis

The structured logs can be:
- **Parsed with tools** like jq, grep, or log analysis platforms
- **Imported into databases** for analytics
- **Monitored in real-time** for performance and errors
- **Used for debugging** complex operation sequences

Example log analysis:
```bash
# Find all failed file uploads
grep '"operation":"file_upload_failed"' app_structured.log

# Analyze average processing times
grep '"processing_time"' app_structured.log | jq .processing_time

# Track specific upload by ID  
grep '"upload_id":"upload_1641825296789"' app_structured.log
```
# 🚀 Single Port Integration - Spark NextJS POC

## Overview
The application has been integrated to run on a **single port (localhost:5000)** with comprehensive logging. Flask serves both the API endpoints and the static frontend files.

## ✅ What's Been Fixed
- **Single Port Setup**: Everything runs on localhost:5000
- **CORS Issues Resolved**: No more cross-origin problems
- **Relative URLs**: Frontend uses relative paths to API
- **Static File Serving**: Flask serves the built frontend
- **Comprehensive Logging**: Detailed logging for all operations

## 🏗️ Architecture

```
localhost:5000
├── /                          → Frontend (index.html)
├── /_next/static/*            → Frontend assets (JS, CSS)
├── /api/health               → Backend API endpoints
├── /api/upload-csv           → File upload
├── /api/sql-query            → SQL execution
└── /api/*                    → All other API routes
```

## 🚀 Quick Start

### Option 1: Automatic Start (Recommended)
```bash
# From project root
python3 start.py
```

### Option 2: Manual Start
```bash
# 1. Build frontend (if not already built)
cd frontend
npm install
npm run build
cd ..

# 2. Start backend (serves frontend + API)
cd backend/backend
python3 app.py
```

## 📂 Project Structure

```
spark-nextjs-poc/
├── start.py                  # 🚀 Single-port startup script
├── backend/backend/
│   └── app.py               # 🔧 Enhanced Flask app (API + static serving)
├── frontend/
│   ├── out/                 # 📦 Built frontend (served by Flask)
│   ├── components/          # ⚛️ React components (with logging)
│   └── utils/logger.ts      # 📝 Frontend logging utility
└── SINGLE_PORT_README.md    # 📖 This documentation
```

## 🔧 How It Works

### Backend Changes
1. **Flask Configuration**: 
   ```python
   app = Flask(__name__, static_folder='../../frontend/out', static_url_path='')
   ```

2. **Static File Routes**:
   ```python
   @app.route('/')
   def serve_frontend():
       return send_file('../../frontend/out/index.html')
   
   @app.route('/<path:path>')
   def serve_static_files(path):
       return send_from_directory('../../frontend/out', path)
   ```

3. **API Routes**: All existing `/api/*` routes unchanged

### Frontend Changes
1. **Static Export**: Next.js configured for static generation
   ```typescript
   const nextConfig = {
     output: 'export',
     trailingSlash: true,
     distDir: 'out'
   };
   ```

2. **Relative URLs**: All API calls use relative paths
   ```typescript
   // Before: 'http://localhost:5000/api/upload-csv'
   // After:  '/api/upload-csv'
   ```

## 📝 Logging Features

### Backend Logging
- **Structured JSON logs**: `app_structured.log`
- **Human-readable logs**: `app.log`
- **Operation tracking**: Unique IDs for request tracing
- **Performance metrics**: Timing for all operations

### Frontend Logging
- **API request tracking**: Success/failure with timing
- **User interactions**: File uploads, tab switches, clicks
- **Error handling**: Comprehensive error logging
- **Development/production modes**: Different logging levels

## 🧪 Testing the Setup

### 1. Verify Everything Works
```bash
# Start the application
python3 start.py

# Open browser to http://localhost:5000
# You should see the frontend interface
```

### 2. Test API Endpoints
```bash
# Health check
curl http://localhost:5000/api/health

# Upload a file (test with a small CSV)
curl -X POST -F "file=@test.csv" http://localhost:5000/api/upload-csv
```

### 3. Check Logs
```bash
# View human-readable logs
tail -f backend/backend/app.log

# View structured JSON logs
tail -f backend/backend/app_structured.log
```

## 🛠️ Development vs Production

### Development Mode
- Run `python3 start.py` for quick development
- Frontend auto-rebuilds when you change source files
- Detailed console logging enabled

### Production Mode
- Built frontend served as static files
- Structured JSON logging for analysis
- Optimized asset serving

## 🔧 Customization

### Change Port
Edit `backend/backend/app.py`:
```python
app.run(debug=True, port=5000, host='0.0.0.0')  # Change port here
```

### Frontend Development
For active frontend development:
```bash
# Terminal 1: Backend only
cd backend/backend && python3 app.py

# Terminal 2: Frontend dev server
cd frontend && npm run dev  # Runs on localhost:3002
```

### Add New API Routes
Add to `backend/backend/app.py`:
```python
@app.route('/api/my-new-endpoint', methods=['POST'])
def my_new_endpoint():
    # Your API logic here
    return jsonify({"success": True})
```

## 📊 Monitoring and Logs

### Log Structure
```json
{
  "timestamp": "2025-01-10T12:34:56.789Z",
  "level": "INFO",
  "operation": "file_upload_complete",
  "upload_id": "upload_1641825296789",
  "file_name": "data.csv",
  "processing_time": 2.45,
  "remote_addr": "127.0.0.1"
}
```

### Key Operations Logged
- ✅ File uploads (start, progress, completion, failure)
- ✅ Dataset loading and processing
- ✅ SQL query execution and performance
- ✅ API requests and responses
- ✅ User interactions and navigation
- ✅ Error handling and debugging info

## 🚨 Troubleshooting

### Frontend Not Loading
```bash
# Check if frontend is built
ls frontend/out/index.html

# If missing, rebuild
cd frontend && npm run build
```

### API Calls Failing
```bash
# Check Flask is serving API routes
curl http://localhost:5000/api/health

# Check logs for errors
tail backend/backend/app.log
```

### Build Errors
```bash
# Clean and rebuild frontend
cd frontend
rm -rf out .next
npm install
npm run build
```

### Import Errors
```bash
# Install Python dependencies
pip install flask flask-cors pyspark pandas

# Install Node.js dependencies
cd frontend && npm install
```

## 🎉 Benefits

✅ **Single URL**: Everything on `localhost:5000`  
✅ **No CORS Issues**: Same origin for frontend and API  
✅ **Easy Deployment**: One server, one port  
✅ **Comprehensive Logging**: Track everything  
✅ **Development Friendly**: Quick startup and testing  
✅ **Production Ready**: Optimized static file serving  

## 📞 Support

Check logs first:
```bash
# Backend logs
tail -f backend/backend/app.log

# Check if server is running
curl http://localhost:5000/api/health
```

The application now provides a seamless, single-port experience with comprehensive logging for debugging and monitoring!
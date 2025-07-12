# 🚀 Spark NextJS POC with Comprehensive Logging

## One-Command Setup & Start

### First Time Setup
```bash
./setup.sh
```

### Start Everything
```bash
cd frontend
npm run dev
```

🌐 **Open http://localhost:3000** - Done! 

## What You Get

✅ **Comprehensive Logging** - Track every file upload, SQL query, user interaction  
✅ **File Upload** - Drag & drop CSV files with detailed progress tracking  
✅ **SQL Analysis** - Interactive query editor with complexity analysis  
✅ **Data Visualization** - Charts and tables for results  
✅ **Performance Monitoring** - Timing and metrics for all operations  
✅ **Error Tracking** - Detailed error logging with stack traces  

## How It Works

- **Frontend**: Next.js on localhost:3000 (with hot reload)
- **Backend**: Flask API on localhost:5000 (auto-started)
- **Logging**: Comprehensive tracking on both frontend and backend
- **Single Command**: `npm run dev` starts everything

## Features

### File Processing with Logging
- Upload progress tracking
- File validation and security checks
- Processing time metrics
- Error handling with detailed logs

### SQL Query Analysis
- Query complexity analysis
- Performance estimation
- Optimization suggestions
- Execution time tracking

### User Interaction Tracking
- Button clicks and navigation
- Drag & drop operations
- Tab switching and component interactions
- API request/response monitoring

### Error Monitoring
- Frontend error boundary logging
- Backend exception tracking with stack traces
- API failure analysis
- Performance bottleneck identification

## Log Files Generated

- `backend/backend/app.log` - Human-readable logs
- `backend/backend/app_structured.log` - JSON structured logs
- Browser console - Frontend interaction logs

## Troubleshooting

If something doesn't work:

1. **Run setup again**: `./setup.sh`
2. **Check logs**: Open browser console or check `backend/backend/app.log`
3. **Restart**: `Ctrl+C` then `npm run dev` again

---

**That's it!** One setup command, one start command, comprehensive logging everywhere. 🎉
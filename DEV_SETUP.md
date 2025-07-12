# 🚀 Development Setup - Standard npm Workflow

## Quick Start

### 1. Install Dependencies

**Backend:**
```bash
pip install -r requirements.txt
```

**Frontend:**
```bash
cd frontend
npm install
```

### 2. Start Development Servers

**Terminal 1 - Backend API (Flask):**
```bash
cd backend/backend
python3 app.py
```
✅ Backend API running on **http://localhost:5000**

**Terminal 2 - Frontend (Next.js):**
```bash
cd frontend
npm run dev
```
✅ Frontend running on **http://localhost:3000**

### 3. Open Your Browser
🌐 **http://localhost:3000** - Full application with hot reload

## How It Works

- **Frontend**: Next.js dev server on port 3000 with hot reload
- **Backend**: Flask API server on port 5000
- **API Proxy**: Next.js automatically proxies `/api/*` requests to Flask
- **No CORS Issues**: Next.js handles the proxy seamlessly
- **Full Logging**: Comprehensive logging on both frontend and backend

## Features

✅ **Hot Reload**: Frontend updates instantly when you edit files  
✅ **API Proxy**: Seamless communication between frontend and backend  
✅ **Comprehensive Logging**: Track all operations and errors  
✅ **File Upload**: Drag-and-drop CSV file uploads  
✅ **SQL Queries**: Interactive SQL query editor with complexity analysis  
✅ **Data Visualization**: Charts and tables for query results  

## File Structure

```
frontend/                     # Next.js React app
├── components/              # React components with logging
│   ├── DatasetSelector.tsx  # File upload and dataset management
│   ├── SQLQueryEditor.tsx   # SQL query interface
│   ├── ResultsViewer.tsx    # Data visualization
│   └── ComplexityDashboard.tsx # Performance analysis
├── utils/
│   └── logger.ts           # Frontend logging utility
└── next.config.ts          # API proxy configuration

backend/backend/             # Flask API server
├── app.py                  # Main Flask app with comprehensive logging
├── query_complexity_analyzer.py # SQL complexity analysis
└── uploads/                # Uploaded CSV files
```

## Development Tips

### Frontend Development
```bash
cd frontend
npm run dev          # Start with hot reload
npm run build        # Build for production
npm run lint         # Check code quality
```

### Backend Development
```bash
cd backend/backend
python3 app.py       # Start Flask API server
```

### View Logs
```bash
# Backend logs
tail -f backend/backend/app.log                # Human readable
tail -f backend/backend/app_structured.log     # JSON structured

# Frontend logs
# Check browser console for detailed logging
```

### Add New Features

**New API Endpoint:**
```python
# In backend/backend/app.py
@app.route('/api/my-new-feature', methods=['POST'])
def my_new_feature():
    logger.info("New feature called", extra={'operation': 'new_feature'})
    return jsonify({"success": True})
```

**New Frontend Component:**
```typescript
// In frontend/components/MyComponent.tsx
import { logger } from '../utils/logger';

export default function MyComponent() {
    const handleClick = () => {
        logger.info('Button clicked', {
            operation: 'button_click',
            component: 'MyComponent'
        });
    };
    // ... component code
}
```

## Testing

### Test API Endpoints
```bash
# Health check
curl http://localhost:5000/api/health

# Upload test file
curl -X POST -F "file=@test.csv" http://localhost:5000/api/upload-csv
```

### Test Frontend
1. Open http://localhost:3000
2. Upload a CSV file
3. Run SQL queries
4. Check browser console for logs

## Troubleshooting

### Port Already in Use
```bash
# Kill process on port 3000
lsof -ti:3000 | xargs kill -9

# Kill process on port 5000  
lsof -ti:5000 | xargs kill -9
```

### Module Not Found
```bash
# Backend dependencies
pip install -r requirements.txt

# Frontend dependencies
cd frontend && npm install
```

### API Not Working
1. Check Flask server is running on port 5000
2. Check Next.js proxy configuration in `next.config.ts`
3. Check browser network tab for failed requests
4. Check backend logs for errors

---

That's it! Standard `npm install` and `npm run dev` workflow with comprehensive logging. 🎉
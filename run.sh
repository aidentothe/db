#!/bin/bash

echo "🚀 Spark NextJS POC - Single Port Setup"
echo "========================================"

# Check if we're in the right directory
if [ ! -f "backend/app.py" ]; then
    echo "❌ Please run this script from the project root directory"
    echo "   Expected: backend/app.py"
    exit 1
fi

# Check if frontend is built
if [ ! -d "frontend/out" ]; then
    echo "📦 Building frontend..."
    cd frontend
    npm install
    npm run build
    cd ..
    echo "✅ Frontend built successfully!"
else
    echo "✅ Frontend build found"
fi

echo ""
echo "🚀 Starting Spark NextJS POC on http://localhost:5000"
echo "   - Frontend: Static files served by Flask"
echo "   - Backend: API endpoints on /api/*" 
echo "   - Logging: Comprehensive logging enabled"
echo "   - Single Port: Everything on localhost:5000"
echo ""
echo "📁 Starting from: $(pwd)/backend"
echo "🌐 Open http://localhost:5000 in your browser"
echo ""

# Start the Flask application
cd backend
python3 app.py
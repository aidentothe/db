#!/usr/bin/env python3
"""
Single-port startup script for Spark NextJS POC
Serves both frontend and backend API on localhost:5000
"""

import os
import sys
import subprocess

def check_frontend_build():
    """Check if frontend is built"""
    frontend_out = "frontend/out"
    if not os.path.exists(frontend_out):
        print("❌ Frontend not built!")
        print("Building frontend...")
        try:
            os.chdir("frontend")
            subprocess.run(["npm", "run", "build"], check=True)
            os.chdir("..")
            print("✅ Frontend built successfully!")
        except subprocess.CalledProcessError:
            print("❌ Frontend build failed. Please install dependencies:")
            print("   cd frontend && npm install && npm run build")
            return False
        except FileNotFoundError:
            print("❌ npm not found. Please install Node.js and npm.")
            return False
    else:
        print("✅ Frontend build found")
    return True

def start_application():
    """Start the integrated application"""
    print("🚀 Starting Spark NextJS POC on http://localhost:5000")
    print("   - Frontend: Static files served by Flask")
    print("   - Backend: API endpoints on /api/*")
    print("   - Logging: Comprehensive logging enabled")
    print("   - Single Port: Everything on localhost:5000")
    print()
    
    try:
        # Change to backend directory and run the app
        backend_path = os.path.join(os.getcwd(), "backend", "backend")
        app_path = os.path.join(backend_path, "app.py")
        
        if not os.path.exists(app_path):
            print(f"❌ Flask app not found at {app_path}")
            return False
            
        print(f"📁 Starting from: {backend_path}")
        os.chdir(backend_path)
        
        # Execute the Flask app
        subprocess.run([sys.executable, "app.py"], check=True)
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running Flask app: {e}")
        return False
    except Exception as e:
        print(f"❌ Error starting application: {e}")
        return False

def main():
    print("Spark NextJS POC - Single Port Setup")
    print("=" * 40)
    
    # Check if we're in the right directory
    if not os.path.exists("backend/backend/app.py"):
        print("❌ Please run this script from the project root directory")
        print("   Expected structure: backend/backend/app.py")
        sys.exit(1)
    
    # Check dependencies
    print("📦 Checking dependencies...")
    try:
        import flask
        import pyspark
        print("✅ Python dependencies found")
    except ImportError as e:
        print(f"❌ Missing Python dependencies: {e}")
        print("Please install: pip install flask flask-cors pyspark pandas")
        sys.exit(1)
    
    # Check and build frontend
    if not check_frontend_build():
        sys.exit(1)
    
    # Start the application
    start_application()

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Simple test script to validate logging implementation
"""

import requests
import json
import time

BASE_URL = "http://localhost:5000"

def test_endpoints():
    """Test key endpoints to verify logging is working"""
    tests = []
    
    # Test health check
    try:
        response = requests.get(f"{BASE_URL}/api/health", timeout=10)
        tests.append(("Health Check", response.status_code == 200))
    except Exception as e:
        tests.append(("Health Check", False))
    
    # Test file listing
    try:
        response = requests.get(f"{BASE_URL}/api/uploaded-files", timeout=10)
        tests.append(("File Listing", response.status_code == 200))
    except Exception as e:
        tests.append(("File Listing", False))
    
    # Test sample datasets
    try:
        response = requests.get(f"{BASE_URL}/api/sample-datasets", timeout=10)
        tests.append(("Sample Datasets", response.status_code == 200))
    except Exception as e:
        tests.append(("Sample Datasets", False))
    
    # Test SQL query
    try:
        test_data = [
            {'id': 1, 'name': 'Alice', 'age': 25},
            {'id': 2, 'name': 'Bob', 'age': 30}
        ]
        response = requests.post(f"{BASE_URL}/api/sql-query", 
                                json={
                                    'data': test_data, 
                                    'query': 'SELECT * FROM data_table'
                                }, timeout=10)
        tests.append(("SQL Query", response.status_code == 200))
    except Exception as e:
        tests.append(("SQL Query", False))
    
    # Print results
    print("Logging Test Results:")
    print("=" * 30)
    for test_name, passed in tests:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    passed = sum(1 for _, result in tests if result)
    total = len(tests)
    print(f"\nSummary: {passed}/{total} tests passed")
    
    return passed == total

if __name__ == "__main__":
    print("Testing logging implementation...")
    success = test_endpoints()
    print("\nCheck app.log and app_structured.log files for detailed logging output.")
    exit(0 if success else 1)
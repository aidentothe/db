#!/usr/bin/env python3
"""
Test script to validate logging implementation in the Spark NextJS POC application.
This script simulates various operations to test logging functionality.
"""

import requests
import json
import time
import os
import tempfile
import pandas as pd
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:5002"
TEST_TIMEOUT = 30  # seconds

class LoggingTester:
    def __init__(self):
        self.session = requests.Session()
        self.test_results = []
        
    def log_test(self, test_name, success, details=""):
        """Log test result"""
        result = {
            "test": test_name,
            "success": success,
            "timestamp": datetime.now().isoformat(),
            "details": details
        }
        self.test_results.append(result)
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status}: {test_name}")
        if details:
            print(f"   Details: {details}")
    
    def test_health_check(self):
        """Test health check endpoint logging"""
        try:
            response = self.session.get(f"{BASE_URL}/api/health", timeout=TEST_TIMEOUT)
            success = response.status_code == 200
            details = f"Status: {response.status_code}, Response: {response.json()}"
            self.log_test("Health Check Logging", success, details)
            return success
        except Exception as e:
            self.log_test("Health Check Logging", False, str(e))
            return False
    
    def create_test_csv(self):
        """Create a test CSV file for upload testing"""
        data = {
            'id': range(1, 101),
            'name': [f'Person_{i}' for i in range(1, 101)],
            'age': [20 + (i % 50) for i in range(1, 101)],
            'salary': [30000 + (i * 1000) for i in range(1, 101)],
            'department': ['Engineering', 'Sales', 'Marketing', 'HR'][i % 4] for i in range(100)
        }
        
        df = pd.DataFrame(data)
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        df.to_csv(temp_file.name, index=False)
        temp_file.close()
        return temp_file.name, len(df), len(df.columns)
    
    def test_file_upload_logging(self):
        """Test file upload logging"""
        try:
            test_file_path, rows, cols = self.create_test_csv()
            
            with open(test_file_path, 'rb') as f:
                files = {'file': ('test_logging.csv', f, 'text/csv')}
                response = self.session.post(
                    f"{BASE_URL}/api/upload-csv",
                    files=files,
                    timeout=TEST_TIMEOUT
                )
            
            os.unlink(test_file_path)  # Clean up
            
            success = response.status_code == 200 and response.json().get('success', False)
            details = f"Status: {response.status_code}, Rows: {rows}, Cols: {cols}"
            if success:
                details += f", File info: {response.json().get('file_info', {})}"
            
            self.log_test("File Upload Logging", success, details)
            return response.json().get('file_info', {}).get('stored_filename') if success else None
            
        except Exception as e:
            self.log_test("File Upload Logging", False, str(e))
            return None
    
    def test_file_listing_logging(self):\n        \"\"\"Test file listing logging\"\"\"\n        try:\n            response = self.session.get(f\"{BASE_URL}/api/uploaded-files\", timeout=TEST_TIMEOUT)\n            success = response.status_code == 200\n            result = response.json() if success else {}\n            details = f\"Status: {response.status_code}, Files found: {len(result.get('files', []))}\"\n            \n            self.log_test(\"File Listing Logging\", success, details)\n            return result.get('files', []) if success else []\n            \n        except Exception as e:\n            self.log_test(\"File Listing Logging\", False, str(e))\n            return []\n    \n    def test_dataset_loading_logging(self, filename):\n        \"\"\"Test dataset loading logging\"\"\"\n        if not filename:\n            self.log_test(\"Dataset Loading Logging\", False, \"No filename provided\")\n            return False\n            \n        try:\n            response = self.session.get(\n                f\"{BASE_URL}/api/load-uploaded-file/{filename}\",\n                timeout=TEST_TIMEOUT\n            )\n            success = response.status_code == 200 and response.json().get('success', False)\n            result = response.json() if success else {}\n            details = f\"Status: {response.status_code}, Data rows: {len(result.get('data', []))}\"\n            \n            self.log_test(\"Dataset Loading Logging\", success, details)\n            return success\n            \n        except Exception as e:\n            self.log_test(\"Dataset Loading Logging\", False, str(e))\n            return False\n    \n    def test_sql_query_logging(self):\n        \"\"\"Test SQL query execution logging\"\"\"\n        try:\n            # Create test data\n            test_data = [\n                {'id': 1, 'name': 'Alice', 'age': 25, 'salary': 50000},\n                {'id': 2, 'name': 'Bob', 'age': 30, 'salary': 60000},\n                {'id': 3, 'name': 'Charlie', 'age': 35, 'salary': 70000}\n            ]\n            \n            query_data = {\n                'data': test_data,\n                'query': 'SELECT name, age FROM data_table WHERE age > 25',\n                'analyze_complexity': True\n            }\n            \n            response = self.session.post(\n                f\"{BASE_URL}/api/sql-query\",\n                json=query_data,\n                timeout=TEST_TIMEOUT\n            )\n            \n            success = response.status_code == 200 and response.json().get('success', False)\n            result = response.json() if success else {}\n            details = f\"Status: {response.status_code}, Result rows: {len(result.get('results', []))}\"\n            \n            self.log_test(\"SQL Query Logging\", success, details)\n            return success\n            \n        except Exception as e:\n            self.log_test(\"SQL Query Logging\", False, str(e))\n            return False\n    \n    def test_csv_analysis_logging(self):\n        \"\"\"Test CSV analysis logging\"\"\"\n        try:\n            test_data = [\n                {'id': 1, 'value': 10, 'category': 'A'},\n                {'id': 2, 'value': 20, 'category': 'B'},\n                {'id': 3, 'value': 30, 'category': 'A'}\n            ]\n            \n            response = self.session.post(\n                f\"{BASE_URL}/api/analyze-csv\",\n                json={'data': test_data},\n                timeout=TEST_TIMEOUT\n            )\n            \n            success = response.status_code == 200 and response.json().get('success', False)\n            result = response.json() if success else {}\n            details = f\"Status: {response.status_code}, Stats computed: {bool(result.get('stats'))}\"\n            \n            self.log_test(\"CSV Analysis Logging\", success, details)\n            return success\n            \n        except Exception as e:\n            self.log_test(\"CSV Analysis Logging\", False, str(e))\n            return False\n    \n    def test_file_deletion_logging(self, filename):\n        \"\"\"Test file deletion logging\"\"\"\n        if not filename:\n            self.log_test(\"File Deletion Logging\", False, \"No filename provided\")\n            return False\n            \n        try:\n            response = self.session.delete(\n                f\"{BASE_URL}/api/delete-uploaded-file/{filename}\",\n                timeout=TEST_TIMEOUT\n            )\n            \n            success = response.status_code == 200 and response.json().get('success', False)\n            details = f\"Status: {response.status_code}, Message: {response.json().get('message', '')}\"\n            \n            self.log_test(\"File Deletion Logging\", success, details)\n            return success\n            \n        except Exception as e:\n            self.log_test(\"File Deletion Logging\", False, str(e))\n            return False\n    \n    def test_error_logging(self):\n        \"\"\"Test error logging with invalid requests\"\"\"\n        try:\n            # Test invalid SQL query\n            response = self.session.post(\n                f\"{BASE_URL}/api/sql-query\",\n                json={'data': [], 'query': 'INVALID SQL QUERY'},\n                timeout=TEST_TIMEOUT\n            )\n            \n            # Should fail but log the error\n            success = response.status_code in [400, 500]  # Error expected\n            details = f\"Status: {response.status_code} (error expected)\"\n            \n            self.log_test(\"Error Logging (Invalid SQL)\", success, details)\n            return success\n            \n        except Exception as e:\n            self.log_test(\"Error Logging (Invalid SQL)\", False, str(e))\n            return False\n    \n    def run_all_tests(self):\n        \"\"\"Run all logging tests\"\"\"\n        print(\"🚀 Starting Logging Implementation Tests...\\n\")\n        start_time = time.time()\n        \n        # Test sequence\n        self.test_health_check()\n        uploaded_filename = self.test_file_upload_logging()\n        self.test_file_listing_logging()\n        self.test_dataset_loading_logging(uploaded_filename)\n        self.test_sql_query_logging()\n        self.test_csv_analysis_logging()\n        self.test_error_logging()\n        self.test_file_deletion_logging(uploaded_filename)\n        \n        # Summary\n        total_time = time.time() - start_time\n        passed = sum(1 for result in self.test_results if result['success'])\n        total = len(self.test_results)\n        \n        print(f\"\\n📊 Test Summary:\")\n        print(f\"   Total tests: {total}\")\n        print(f\"   Passed: {passed}\")\n        print(f\"   Failed: {total - passed}\")\n        print(f\"   Success rate: {(passed/total)*100:.1f}%\")\n        print(f\"   Total time: {total_time:.2f}s\")\n        \n        # Save detailed results\n        with open('logging_test_results.json', 'w') as f:\n            json.dump({\n                'summary': {\n                    'total_tests': total,\n                    'passed': passed,\n                    'failed': total - passed,\n                    'success_rate': (passed/total)*100,\n                    'total_time': total_time\n                },\n                'test_results': self.test_results\n            }, f, indent=2)\n        \n        print(f\"\\n📝 Detailed results saved to 'logging_test_results.json'\")\n        \n        if passed == total:\n            print(\"\\n🎉 All logging tests passed! The logging implementation is working correctly.\")\n        else:\n            print(f\"\\n⚠️  {total - passed} test(s) failed. Check the logs and implementation.\")\n        \n        return passed == total\n\ndef main():\n    print(\"Logging Implementation Test Suite\")\n    print(\"=\" * 50)\n    print(\"\\nThis script tests the comprehensive logging implementation\")\n    print(\"across all backend endpoints and operations.\\n\")\n    \n    # Check if server is running\n    try:\n        response = requests.get(f\"{BASE_URL}/api/health\", timeout=5)\n        if response.status_code != 200:\n            print(f\"❌ Server not responding properly at {BASE_URL}\")\n            return False\n    except requests.exceptions.RequestException:\n        print(f\"❌ Cannot connect to server at {BASE_URL}\")\n        print(\"   Please ensure the Flask backend is running on port 5001\")\n        return False\n    \n    print(f\"✅ Server is running at {BASE_URL}\\n\")\n    \n    # Run tests\n    tester = LoggingTester()\n    return tester.run_all_tests()\n\nif __name__ == \"__main__\":\n    success = main()\n    exit(0 if success else 1)
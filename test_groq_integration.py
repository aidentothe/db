#!/usr/bin/env python3
"""
Test script for Groq integration with complexity analysis
This script validates that our Groq implementation works correctly
"""

import sys
import os
import json

# Add backend directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

try:
    from groq_client import GroqClient
    print("✓ Successfully imported GroqClient")
except ImportError as e:
    print(f"✗ Failed to import GroqClient: {e}")
    print("Make sure groq library is installed: pip install groq")
    sys.exit(1)

def test_basic_connection():
    """Test basic connection to Groq API"""
    print("\n=== Testing Basic Connection ===")
    
    try:
        client = GroqClient()
        print("✓ GroqClient initialized successfully")
        print(f"✓ Using model: {client.model}")
        return client
    except Exception as e:
        print(f"✗ Failed to initialize GroqClient: {e}")
        return None

def test_complexity_question(client):
    """Test asking a complexity question"""
    print("\n=== Testing Complexity Question ===")
    
    try:
        question = "What makes a SQL query have a high compute score?"
        
        # Mock analysis context
        analysis_context = {
            "complexity_rating": 8,
            "compute_score": 9,
            "memory_score": 6,
            "components": {
                "joins": ["INNER JOIN", "LEFT JOIN"],
                "subqueries": 2,
                "aggregate_functions": ["COUNT", "SUM"],
                "window_functions": ["ROW_NUMBER"]
            }
        }
        
        print(f"Question: {question}")
        print("Analysis context provided: ✓")
        
        answer = client.ask_complexity_question(question, analysis_context)
        
        print(f"✓ Received answer ({len(answer)} characters)")
        print(f"Answer preview: {answer[:200]}...")
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to get complexity question answer: {e}")
        return False

def test_optimization_suggestions(client):
    """Test generating optimization suggestions"""
    print("\n=== Testing Optimization Suggestions ===")
    
    try:
        query = """
        SELECT u.name, COUNT(*) as order_count, SUM(o.total) as total_spent
        FROM users u
        INNER JOIN orders o ON u.id = o.user_id
        LEFT JOIN order_items oi ON o.id = oi.order_id
        WHERE o.created_at > '2023-01-01'
        GROUP BY u.id, u.name
        ORDER BY total_spent DESC
        """
        
        analysis = {
            "complexity_rating": 6,
            "compute_score": 7,
            "memory_score": 5,
            "components": {
                "tables": ["users", "orders", "order_items"],
                "joins": ["INNER JOIN", "LEFT JOIN"],
                "aggregate_functions": ["COUNT", "SUM"]
            }
        }
        
        print("Query provided: ✓")
        print("Analysis provided: ✓")
        
        suggestions = client.generate_optimization_suggestions(query, analysis)
        
        print(f"✓ Received optimization suggestions ({len(suggestions)} characters)")
        print(f"Suggestions preview: {suggestions[:200]}...")
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to get optimization suggestions: {e}")
        return False

def test_score_explanation(client):
    """Test explaining complexity scores"""
    print("\n=== Testing Score Explanation ===")
    
    try:
        analysis = {
            "complexity_rating": 7,
            "compute_score": 8,
            "memory_score": 6,
            "components": {
                "tables": ["users", "orders", "products"],
                "joins": ["INNER JOIN", "LEFT JOIN", "INNER JOIN"],
                "subqueries": 1,
                "aggregate_functions": ["COUNT", "SUM", "AVG"],
                "window_functions": ["ROW_NUMBER"],
                "distinct": True
            },
            "performance_estimate": {
                "estimated_execution_time_seconds": 15.2,
                "estimated_result_rows": 25000,
                "memory_usage_category": "High",
                "performance_category": "Medium"
            }
        }
        
        print("Analysis provided: ✓")
        
        explanation = client.explain_complexity_score(analysis)
        
        print(f"✓ Received score explanation ({len(explanation)} characters)")
        print(f"Explanation preview: {explanation[:200]}...")
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to get score explanation: {e}")
        return False

def main():
    """Run all tests"""
    print("🔬 Testing Groq Integration for Complexity Analysis")
    print("=" * 60)
    
    # Test basic connection
    client = test_basic_connection()
    if not client:
        print("\n❌ Basic connection failed. Cannot proceed with other tests.")
        return False
    
    # Run all tests
    tests = [
        ("Complexity Question", lambda: test_complexity_question(client)),
        ("Optimization Suggestions", lambda: test_optimization_suggestions(client)),
        ("Score Explanation", lambda: test_score_explanation(client))
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"✗ {test_name} test crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("🧪 TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status:<8} {test_name}")
    
    print("-" * 60)
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Groq integration is working correctly.")
        return True
    else:
        print("⚠️  Some tests failed. Check the errors above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
# ISO Formatting Standards

The DB Spark application now enforces **ISO standards** across all components for consistent, professional-grade formatting.

## 🌍 ISO Standards Implemented

### **ISO 8601 - Date and Time**
```json
{
  "timestamp": "2024-01-15T14:30:45.123Z",
  "created_at": "2024-01-15T14:30:45.123Z",
  "last_modified": "2024-01-15T14:30:45.123Z"
}
```

### **ISO 3166 - Country Codes**
```json
{
  "country_code": "US",
  "region": "United States"
}
```

### **ISO 4217 - Currency Codes**
```json
{
  "currency": "USD",
  "amount": "1234.56 USD"
}
```

### **ISO 639 - Language Codes**
```json
{
  "language": "en",
  "locale": "en-US"
}
```

## 📐 Response Format Standards

### **Standard API Response Structure**
```json
{
  "meta": {
    "timestamp": "2024-01-15T14:30:45.123Z",
    "status": "success",
    "version": "1.0.0",
    "format": "application/json",
    "processing_time_ms": 125.45
  },
  "data": {
    // Your response data here
  }
}
```

### **Error Response Structure**
```json
{
  "meta": {
    "timestamp": "2024-01-15T14:30:45.123Z",
    "status": "error",
    "version": "1.0.0",
    "format": "application/json"
  },
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid input parameters",
    "details": {
      "field": "table_name",
      "reason": "Must follow snake_case convention"
    }
  }
}
```

### **Paginated Response Structure**
```json
{
  "meta": {
    "timestamp": "2024-01-15T14:30:45.123Z",
    "status": "success",
    "version": "1.0.0",
    "format": "application/json"
  },
  "data": [
    // Array of items
  ],
  "pagination": {
    "page": 1,
    "per_page": 100,
    "total_items": 1500,
    "total_pages": 15,
    "has_next": true,
    "has_previous": false
  }
}
```

## 🔤 Naming Conventions

### **Snake Case for API Fields**
```json
{
  "user_name": "john_doe",
  "first_name": "John",
  "last_name": "Doe",
  "created_at": "2024-01-15T14:30:45.123Z",
  "is_active": true
}
```

### **Table and Column Names**
```sql
-- Valid table names
CREATE TABLE user_profiles;
CREATE TABLE order_items;
CREATE TABLE product_categories;

-- Valid column names
SELECT user_id, first_name, created_at FROM users;
```

## 📊 Data Type Standards

### **File Size Formatting (ISO/IEC 80000)**
```json
{
  "file_size_bytes": 1073741824,
  "file_size_formatted": "1.0 GiB",
  "sizes": {
    "small": "1.5 KiB",
    "medium": "2.3 MiB", 
    "large": "4.7 GiB"
  }
}
```

### **Query Results**
```json
{
  "meta": {
    "timestamp": "2024-01-15T14:30:45.123Z",
    "status": "success",
    "version": "1.0.0",
    "format": "application/json"
  },
  "query": {
    "sql": "SELECT * FROM users WHERE age > 25",
    "execution_time_ms": 245.67,
    "row_count": 1500,
    "limited": false
  },
  "data": [
    {
      "user_id": 1,
      "user_name": "john_doe",
      "created_at": "2024-01-15T14:30:45.123Z"
    }
  ]
}
```

### **Table Metadata**
```json
{
  "meta": {
    "timestamp": "2024-01-15T14:30:45.123Z",
    "status": "success",
    "version": "1.0.0",
    "format": "application/json"
  },
  "table": {
    "name": "user_profiles",
    "created_at": "2024-01-15T14:30:45.123Z",
    "row_count": 50000,
    "column_count": 12,
    "columns": [
      {
        "name": "user_id",
        "type": "integer",
        "name_valid": true
      },
      {
        "name": "first_name",
        "type": "string",
        "name_valid": true
      }
    ]
  }
}
```

## 🔒 Error Codes (ISO-Style)

### **Standardized Error Codes**
```javascript
const ERROR_CODES = {
  'VALIDATION_ERROR': 'Input validation failed',
  'AUTHENTICATION_ERROR': 'Authentication required',
  'AUTHORIZATION_ERROR': 'Insufficient permissions',
  'RESOURCE_NOT_FOUND': 'Requested resource not found',
  'INTERNAL_SERVER_ERROR': 'Internal server error',
  'SERVICE_UNAVAILABLE': 'Service temporarily unavailable',
  'QUERY_TIMEOUT': 'Query execution timeout',
  'QUERY_SYNTAX_ERROR': 'SQL syntax error',
  'DATA_FORMAT_ERROR': 'Data format error',
  'SPARK_SESSION_ERROR': 'Spark session error',
  'S3_ACCESS_ERROR': 'S3 access error'
};
```

### **Error Response Examples**
```json
{
  "meta": {
    "timestamp": "2024-01-15T14:30:45.123Z",
    "status": "error",
    "version": "1.0.0",
    "format": "application/json"
  },
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid table name format",
    "details": {
      "table_name": "Invalid-Name",
      "expected_format": "snake_case",
      "example": "user_profiles"
    }
  }
}
```

## 🛠️ API Endpoints

### **Health Check**
```bash
GET /health
```
**Response:**
```json
{
  "meta": {
    "timestamp": "2024-01-15T14:30:45.123Z",
    "status": "success",
    "version": "1.0.0",
    "format": "application/json"
  },
  "data": {
    "status": "healthy",
    "environment": "production",
    "deployment_mode": "aws_large",
    "spark_version": "3.5.0"
  }
}
```

### **S3 Object List (Paginated)**
```bash
GET /api/spark/s3/list?page=1&per_page=50&prefix=data/
```
**Response:**
```json
{
  "meta": {
    "timestamp": "2024-01-15T14:30:45.123Z",
    "status": "success",
    "version": "1.0.0",
    "format": "application/json"
  },
  "data": [
    {
      "key": "data/users.json",
      "size_bytes": 1048576,
      "size_formatted": "1.0 MiB",
      "last_modified": "2024-01-15T14:30:45.123Z",
      "etag": "d41d8cd98f00b204e9800998ecf8427e"
    }
  ],
  "pagination": {
    "page": 1,
    "per_page": 50,
    "total_items": 150,
    "total_pages": 3,
    "has_next": true,
    "has_previous": false
  }
}
```

### **Query Execution**
```bash
POST /api/spark/query/execute
Content-Type: application/json

{
  "query": "SELECT user_id, user_name FROM users WHERE age > 25",
  "s3_path": "data/users.json",
  "limit": 1000
}
```
**Response:**
```json
{
  "meta": {
    "timestamp": "2024-01-15T14:30:45.123Z",
    "status": "success",
    "version": "1.0.0",
    "format": "application/json"
  },
  "query": {
    "sql": "SELECT user_id, user_name FROM users WHERE age > 25",
    "execution_time_ms": 245.67,
    "row_count": 850,
    "limited": false,
    "s3_path": "data/users.json",
    "complexity": {
      "risk_level": "LOW"
    }
  },
  "data": [
    {
      "user_id": 1,
      "user_name": "john_doe"
    }
  ]
}
```

## 📏 Validation Rules

### **Field Name Validation**
```javascript
// Valid field names (snake_case)
✅ user_name
✅ first_name
✅ created_at
✅ is_active
✅ order_id

// Invalid field names
❌ userName (camelCase)
❌ FirstName (PascalCase)
❌ user-name (kebab-case)
❌ USER_NAME (CONSTANT_CASE - unless it's a constant)
```

### **Table Name Validation**
```javascript
// Valid table names
✅ users
✅ user_profiles
✅ order_items
✅ product_categories

// Invalid table names
❌ Users (PascalCase)
❌ user-profiles (kebab-case)
❌ userProfiles (camelCase)
❌ 1_users (starts with number)
```

### **Request Validation**
```javascript
// All requests are validated for:
✅ Content-Type: application/json
✅ Valid JSON structure
✅ Required fields present
✅ Field names follow snake_case
✅ String values are normalized
✅ Pagination parameters are valid
```

## 🔧 Middleware Features

### **Automatic Headers**
Every response includes:
```http
X-Request-ID: req_1705334445123
X-Processing-Time: 125.45ms
X-Timestamp: 2024-01-15T14:30:45.123Z
X-API-Version: 1.0.0
Content-Type: application/json; charset=utf-8
```

### **Request Logging**
```json
{
  "timestamp": "2024-01-15T14:30:45.123Z",
  "level": "INFO",
  "operation": "request_start",
  "request_id": "req_1705334445123",
  "method": "POST",
  "path": "/api/spark/query/execute",
  "remote_addr": "192.168.1.100"
}
```

### **Response Logging**
```json
{
  "timestamp": "2024-01-15T14:30:45.123Z",
  "level": "INFO", 
  "operation": "request_complete",
  "request_id": "req_1705334445123",
  "status_code": 200,
  "processing_time_ms": 125.45,
  "response_size": 2048
}
```

## 🌐 Frontend Integration

### **API Client Example**
```javascript
class ISOAPIClient {
  constructor(baseURL) {
    this.baseURL = baseURL;
  }

  async request(endpoint, options = {}) {
    const response = await fetch(`${this.baseURL}${endpoint}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...options.headers
      }
    });

    const data = await response.json();

    // All responses follow ISO structure
    if (data.meta.status === 'error') {
      throw new Error(`[${data.error.code}] ${data.error.message}`);
    }

    return {
      data: data.data,
      meta: data.meta,
      pagination: data.pagination // if present
    };
  }

  async getHealth() {
    const response = await this.request('/health');
    return response.data;
  }

  async executeQuery(query, s3Path, limit = 1000) {
    const response = await this.request('/api/spark/query/execute', {
      method: 'POST',
      body: JSON.stringify({
        query,
        s3_path: s3Path,
        limit
      })
    });

    return {
      results: response.data,
      query: response.meta.query,
      executionTime: response.meta.query.execution_time_ms
    };
  }

  async listS3Objects(prefix = '', page = 1, perPage = 100) {
    const response = await this.request(
      `/api/spark/s3/list?prefix=${prefix}&page=${page}&per_page=${perPage}`
    );

    return {
      objects: response.data,
      pagination: response.pagination
    };
  }
}

// Usage
const api = new ISOAPIClient('http://localhost:5000');

try {
  const health = await api.getHealth();
  console.log('System status:', health.status);
  
  const results = await api.executeQuery(
    'SELECT * FROM users LIMIT 10',
    'data/users.json'
  );
  console.log('Query results:', results.results);
  console.log('Execution time:', results.executionTime, 'ms');
  
} catch (error) {
  console.error('API Error:', error.message);
}
```

## 📈 Benefits of ISO Formatting

1. **Consistency** - All responses follow the same structure
2. **Predictability** - Clients know exactly what to expect
3. **Internationalization** - ISO standards work globally
4. **Debugging** - Clear error codes and detailed logging
5. **Pagination** - Standardized pagination across all endpoints
6. **Performance** - Processing time tracking built-in
7. **Validation** - Automatic input validation and normalization
8. **Compliance** - Follows international standards

The ISO formatting system ensures your API is professional, consistent, and ready for enterprise use while maintaining the full power of Apache Spark processing.
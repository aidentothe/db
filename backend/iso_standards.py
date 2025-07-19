"""
ISO Standards Configuration - Ensures consistent formatting and standards across the application
"""

import re
from datetime import datetime
from typing import Dict, Any, Optional, List
from enum import Enum

class ISOStandards:
    """ISO standardization utilities and constants"""
    
    # ISO 8601 datetime format
    ISO_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
    ISO_DATE_FORMAT = "%Y-%m-%d"
    ISO_TIME_FORMAT = "%H:%M:%S"
    
    # ISO 3166 country codes (common ones)
    ISO_COUNTRY_CODES = {
        'US': 'United States',
        'CA': 'Canada',
        'GB': 'United Kingdom',
        'DE': 'Germany',
        'FR': 'France',
        'JP': 'Japan',
        'AU': 'Australia'
    }
    
    # ISO 4217 currency codes
    ISO_CURRENCY_CODES = {
        'USD': 'US Dollar',
        'EUR': 'Euro',
        'GBP': 'British Pound',
        'JPY': 'Japanese Yen',
        'CAD': 'Canadian Dollar'
    }
    
    # ISO 639 language codes
    ISO_LANGUAGE_CODES = {
        'en': 'English',
        'es': 'Spanish',
        'fr': 'French',
        'de': 'German',
        'ja': 'Japanese',
        'zh': 'Chinese'
    }
    
    # ISO naming conventions
    NAMING_PATTERNS = {
        'snake_case': r'^[a-z][a-z0-9_]*$',
        'kebab_case': r'^[a-z][a-z0-9-]*$',
        'camel_case': r'^[a-z][a-zA-Z0-9]*$',
        'pascal_case': r'^[A-Z][a-zA-Z0-9]*$',
        'constant_case': r'^[A-Z][A-Z0-9_]*$'
    }
    
    @staticmethod
    def format_iso_datetime(dt: datetime = None) -> str:
        """Format datetime to ISO 8601 standard"""
        if dt is None:
            dt = datetime.utcnow()
        return dt.strftime(ISOStandards.ISO_DATETIME_FORMAT)
    
    @staticmethod
    def parse_iso_datetime(iso_string: str) -> datetime:
        """Parse ISO 8601 datetime string"""
        # Handle various ISO formats
        formats = [
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S"
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(iso_string, fmt)
            except ValueError:
                continue
        
        raise ValueError(f"Unable to parse datetime: {iso_string}")
    
    @staticmethod
    def validate_naming_convention(name: str, convention: str) -> bool:
        """Validate naming convention against ISO standards"""
        pattern = ISOStandards.NAMING_PATTERNS.get(convention)
        if not pattern:
            return False
        return bool(re.match(pattern, name))
    
    @staticmethod
    def normalize_string(text: str) -> str:
        """Normalize string to ISO standards"""
        # Remove non-printable characters
        normalized = re.sub(r'[^\x20-\x7E]', '', text)
        # Normalize whitespace
        normalized = re.sub(r'\s+', ' ', normalized)
        return normalized.strip()
    
    @staticmethod
    def format_currency(amount: float, currency_code: str) -> str:
        """Format currency according to ISO 4217"""
        if currency_code not in ISOStandards.ISO_CURRENCY_CODES:
            raise ValueError(f"Invalid currency code: {currency_code}")
        
        # Basic formatting (can be enhanced with locale-specific formatting)
        return f"{amount:.2f} {currency_code}"
    
    @staticmethod
    def format_file_size(size_bytes: int) -> str:
        """Format file size in ISO/IEC 80000 standard (binary prefixes)"""
        if size_bytes == 0:
            return "0 B"
        
        # ISO/IEC 80000 binary prefixes
        prefixes = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
        base = 1024
        
        for i, prefix in enumerate(prefixes):
            if size_bytes < base ** (i + 1):
                if i == 0:
                    return f"{size_bytes} {prefix}"
                else:
                    value = size_bytes / (base ** i)
                    return f"{value:.1f} {prefix}"
        
        # For very large files
        value = size_bytes / (base ** (len(prefixes) - 1))
        return f"{value:.1f} {prefixes[-1]}"

class ISODataTypes(Enum):
    """ISO standardized data types"""
    
    # ISO 8601 temporal types
    DATETIME = "datetime"
    DATE = "date"
    TIME = "time"
    DURATION = "duration"
    
    # ISO numeric types
    INTEGER = "integer"
    DECIMAL = "decimal"
    FLOAT = "float"
    CURRENCY = "currency"
    
    # ISO text types
    STRING = "string"
    TEXT = "text"
    UUID = "uuid"
    URI = "uri"
    
    # ISO binary types
    BINARY = "binary"
    BASE64 = "base64"
    
    # ISO geographic types
    LATITUDE = "latitude"
    LONGITUDE = "longitude"
    COORDINATE = "coordinate"
    
    # ISO identification types
    COUNTRY_CODE = "country_code"
    LANGUAGE_CODE = "language_code"
    CURRENCY_CODE = "currency_code"

class ISOFormatter:
    """ISO-compliant formatting utilities"""
    
    @staticmethod
    def format_response(data: Any, status: str = "success", timestamp: datetime = None) -> Dict[str, Any]:
        """Format API response in ISO standard structure"""
        response = {
            "meta": {
                "timestamp": ISOStandards.format_iso_datetime(timestamp),
                "status": status,
                "version": "1.0.0",
                "format": "application/json"
            },
            "data": data
        }
        
        if status == "error":
            response["meta"]["error_code"] = getattr(data, 'code', 'UNKNOWN_ERROR')
            response["meta"]["error_message"] = str(data)
        
        return response
    
    @staticmethod
    def format_error(error_code: str, message: str, details: Dict[str, Any] = None) -> Dict[str, Any]:
        """Format error response in ISO standard structure"""
        error_response = {
            "meta": {
                "timestamp": ISOStandards.format_iso_datetime(),
                "status": "error",
                "version": "1.0.0",
                "format": "application/json"
            },
            "error": {
                "code": error_code,
                "message": message,
                "details": details or {}
            }
        }
        
        return error_response
    
    @staticmethod
    def format_pagination(data: List[Any], page: int, per_page: int, total: int) -> Dict[str, Any]:
        """Format paginated response in ISO standard structure"""
        total_pages = (total + per_page - 1) // per_page
        
        return {
            "meta": {
                "timestamp": ISOStandards.format_iso_datetime(),
                "status": "success",
                "version": "1.0.0",
                "format": "application/json"
            },
            "data": data,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total_items": total,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            }
        }
    
    @staticmethod
    def format_table_metadata(table_name: str, columns: List[Dict[str, Any]], 
                             row_count: int, created_at: datetime = None) -> Dict[str, Any]:
        """Format table metadata in ISO standard structure"""
        return {
            "meta": {
                "timestamp": ISOStandards.format_iso_datetime(),
                "status": "success",
                "version": "1.0.0",
                "format": "application/json"
            },
            "table": {
                "name": table_name,
                "created_at": ISOStandards.format_iso_datetime(created_at) if created_at else None,
                "row_count": row_count,
                "column_count": len(columns),
                "columns": columns
            }
        }
    
    @staticmethod
    def format_query_result(query: str, results: List[Dict[str, Any]], 
                           execution_time_ms: float, row_count: int) -> Dict[str, Any]:
        """Format query result in ISO standard structure"""
        return {
            "meta": {
                "timestamp": ISOStandards.format_iso_datetime(),
                "status": "success",
                "version": "1.0.0",
                "format": "application/json"
            },
            "query": {
                "sql": query,
                "execution_time_ms": round(execution_time_ms, 2),
                "row_count": row_count,
                "limited": len(results) < row_count if row_count > 0 else False
            },
            "data": results
        }

class ISOValidator:
    """ISO compliance validation utilities"""
    
    @staticmethod
    def validate_datetime(dt_string: str) -> bool:
        """Validate ISO 8601 datetime format"""
        try:
            ISOStandards.parse_iso_datetime(dt_string)
            return True
        except ValueError:
            return False
    
    @staticmethod
    def validate_country_code(code: str) -> bool:
        """Validate ISO 3166 country code"""
        return code.upper() in ISOStandards.ISO_COUNTRY_CODES
    
    @staticmethod
    def validate_currency_code(code: str) -> bool:
        """Validate ISO 4217 currency code"""
        return code.upper() in ISOStandards.ISO_CURRENCY_CODES
    
    @staticmethod
    def validate_language_code(code: str) -> bool:
        """Validate ISO 639 language code"""
        return code.lower() in ISOStandards.ISO_LANGUAGE_CODES
    
    @staticmethod
    def validate_api_response(response: Dict[str, Any]) -> bool:
        """Validate API response format compliance"""
        required_fields = ["meta", "data"]
        meta_fields = ["timestamp", "status", "version", "format"]
        
        # Check top-level structure
        if not all(field in response for field in required_fields):
            return False
        
        # Check meta structure
        if not all(field in response["meta"] for field in meta_fields):
            return False
        
        # Validate timestamp format
        if not ISOValidator.validate_datetime(response["meta"]["timestamp"]):
            return False
        
        # Validate status
        if response["meta"]["status"] not in ["success", "error"]:
            return False
        
        return True
    
    @staticmethod
    def validate_table_name(name: str) -> bool:
        """Validate table name according to ISO standards"""
        # Should be snake_case
        if not ISOStandards.validate_naming_convention(name, 'snake_case'):
            return False
        
        # Should not be too long
        if len(name) > 63:  # Common database limit
            return False
        
        # Should not start with number
        if name[0].isdigit():
            return False
        
        return True
    
    @staticmethod
    def validate_column_name(name: str) -> bool:
        """Validate column name according to ISO standards"""
        return ISOValidator.validate_table_name(name)  # Same rules

class ISOConstants:
    """ISO standardized constants"""
    
    # HTTP status codes (ISO-like standard)
    HTTP_STATUS_CODES = {
        200: "OK",
        201: "Created",
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        500: "Internal Server Error",
        503: "Service Unavailable"
    }
    
    # MIME types
    MIME_TYPES = {
        'json': 'application/json',
        'csv': 'text/csv',
        'xml': 'application/xml',
        'parquet': 'application/octet-stream',
        'text': 'text/plain'
    }
    
    # Error codes
    ERROR_CODES = {
        'VALIDATION_ERROR': 'VALIDATION_ERROR',
        'AUTHENTICATION_ERROR': 'AUTHENTICATION_ERROR',
        'AUTHORIZATION_ERROR': 'AUTHORIZATION_ERROR',
        'RESOURCE_NOT_FOUND': 'RESOURCE_NOT_FOUND',
        'INTERNAL_SERVER_ERROR': 'INTERNAL_SERVER_ERROR',
        'SERVICE_UNAVAILABLE': 'SERVICE_UNAVAILABLE',
        'QUERY_TIMEOUT': 'QUERY_TIMEOUT',
        'QUERY_SYNTAX_ERROR': 'QUERY_SYNTAX_ERROR',
        'DATA_FORMAT_ERROR': 'DATA_FORMAT_ERROR',
        'SPARK_SESSION_ERROR': 'SPARK_SESSION_ERROR',
        'S3_ACCESS_ERROR': 'S3_ACCESS_ERROR'
    }
    
    # API versioning
    API_VERSION = "1.0.0"
    API_FORMAT = "application/json"
    
    # Pagination defaults
    DEFAULT_PAGE_SIZE = 100
    MAX_PAGE_SIZE = 1000
    
    # Query limits
    DEFAULT_QUERY_LIMIT = 1000
    MAX_QUERY_LIMIT = 10000
    
    # Timeouts (in seconds)
    DEFAULT_QUERY_TIMEOUT = 30
    MAX_QUERY_TIMEOUT = 300
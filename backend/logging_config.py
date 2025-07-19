"""
Logging configuration and formatters
"""

import logging
import json
import time
from datetime import datetime
from typing import Dict, Any

class EnhancedConsoleFormatter(logging.Formatter):
    """Enhanced formatter for console output with colors and structure."""
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
        'RESET': '\033[0m',     # Reset
        'BOLD': '\033[1m',      # Bold
        'TIMESTAMP': '\033[90m', # Gray
        'OPERATION': '\033[94m', # Light Blue
    }
    
    def format(self, record):
        # Get timestamp
        timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        
        # Get colors
        level_color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        
        # Format basic message
        formatted = f"{self.COLORS['TIMESTAMP']}[{timestamp}]{self.COLORS['RESET']} "
        formatted += f"{level_color}{self.COLORS['BOLD']}[{record.levelname:8}]{self.COLORS['RESET']} "
        formatted += f"{record.getMessage()}"
        
        # Add operation context if available
        if hasattr(record, 'operation'):
            formatted += f" {self.COLORS['OPERATION']}({record.operation}){self.COLORS['RESET']}"
        
        # Add key metrics inline for quick scanning
        metrics = []
        if hasattr(record, 'processing_time'):
            metrics.append(f"time={record.processing_time:.2f}ms")
        if hasattr(record, 'file_name'):
            metrics.append(f"file={record.file_name}")
        if hasattr(record, 'row_count'):
            metrics.append(f"rows={record.row_count:,}")
        if hasattr(record, 'error_type'):
            metrics.append(f"error={record.error_type}")
        
        if metrics:
            formatted += f" {self.COLORS['TIMESTAMP']}[{', '.join(metrics)}]{self.COLORS['RESET']}"
        
        # Add exception info if present
        if record.exc_info:
            formatted += "\n" + self.formatException(record.exc_info)
            
        return formatted

class StructuredFormatter(logging.Formatter):
    """Enhanced structured formatter with comprehensive context tracking."""
    
    def format(self, record):
        try:
            timestamp = datetime.utcnow().isoformat()
            # Validate timestamp doesn't contain NaN
            if 'NaN' in str(timestamp) or not timestamp:
                timestamp = datetime.now().isoformat()
        except Exception:
            try:
                timestamp = datetime.now().isoformat()
            except Exception:
                # Ultimate fallback
                timestamp = time.strftime("%Y-%m-%dT%H:%M:%S")
        
        log_entry = {
            'timestamp': timestamp,
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
        }
        
        # Enhanced context tracking
        context_fields = [
            'operation', 'file_name', 'file_size', 'processing_time', 'row_count', 
            'error_type', 'error_message', 'endpoint', 'method', 'status_code',
            'user_id', 'session_id', 'correlation_id', 'component', 'severity',
            'impact', 'query', 'complexity_rating', 'memory_usage_mb'
        ]
        
        for field in context_fields:
            if hasattr(record, field):
                value = getattr(record, field)
                if value is not None:
                    log_entry[field] = value
        
        # Add exception information if present
        if record.exc_info:
            log_entry['exception'] = {
                'type': record.exc_info[0].__name__ if record.exc_info[0] else None,
                'message': str(record.exc_info[1]) if record.exc_info[1] else None,
                'traceback': self.formatException(record.exc_info)
            }
        
        # Add performance metrics
        log_entry['performance_metrics'] = {
            'memory_usage_mb': getattr(record, 'memory_usage_mb', None),
            'processing_time': getattr(record, 'processing_time', None),
            'cpu_time': getattr(record, 'cpu_time', None)
        }
        
        # Clean up null values for cleaner JSON
        log_entry = {k: v for k, v in log_entry.items() if v is not None}
        if log_entry.get('performance_metrics'):
            log_entry['performance_metrics'] = {k: v for k, v in log_entry['performance_metrics'].items() if v is not None}
            if not log_entry['performance_metrics']:
                del log_entry['performance_metrics']
        
        try:
            # Convert any NaN values to null before serialization
            def clean_nan_values(obj):
                if isinstance(obj, dict):
                    return {k: clean_nan_values(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [clean_nan_values(item) for item in obj]
                elif isinstance(obj, float) and (obj != obj):  # Check for NaN
                    return None
                elif isinstance(obj, str) and 'NaN' in obj:
                    return obj.replace('NaN', 'null')
                else:
                    return obj
            
            cleaned_entry = clean_nan_values(log_entry)
            return json.dumps(cleaned_entry, default=str, allow_nan=False, indent=2)
        except (TypeError, ValueError) as e:
            safe_entry = {
                'timestamp': timestamp,
                'level': record.levelname,
                'logger': record.name,
                'message': str(record.getMessage()),
                'serialization_error': str(e)
            }
            return json.dumps(safe_entry, default=str, allow_nan=False, indent=2)

def setup_logging(config):
    """Set up application logging with multiple handlers"""
    
    # Clear any existing handlers first
    logging.getLogger().handlers.clear()
    
    # Create console handler with enhanced formatting
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(EnhancedConsoleFormatter())
    console_handler.setLevel(getattr(logging, config.LOG_LEVEL))
    
    # Create file handler for human-readable logs
    file_handler_readable = logging.FileHandler(config.LOG_FILE)
    file_handler_readable.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    file_handler_readable.setLevel(logging.INFO)
    
    # Set up file handler with structured logging
    file_handler = logging.FileHandler(config.STRUCTURED_LOG_FILE)
    file_handler.setFormatter(StructuredFormatter())
    file_handler.setLevel(logging.INFO)
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        handlers=[console_handler, file_handler_readable, file_handler]
    )
    
    # Configure specific loggers
    logging.getLogger('pyspark').setLevel(logging.WARNING)
    logging.getLogger('py4j').setLevel(logging.WARNING)
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured at level: {config.LOG_LEVEL}")
    
    return logger
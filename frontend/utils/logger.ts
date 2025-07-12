/**
 * Frontend logging utility for structured logging and error tracking
 */

export interface LogContext {
  operation?: string;
  component?: string;
  file_name?: string;
  file_size?: number;
  processing_time?: number;
  error_type?: string;
  error_message?: string;
  error_stack?: string;
  user_action?: string;
  request_id?: string;
  session_id?: string;
  user_id?: string;
  correlation_id?: string;
  performance_metrics?: {
    memory_usage?: number;
    cpu_time?: number;
    network_latency?: number;
  };
  metadata?: {
    browser?: string;
    screen_resolution?: string;
    viewport_size?: string;
    connection_type?: string;
  };
  [key: string]: any;
}

export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

class Logger {
  private isDevelopment = process.env.NODE_ENV === 'development';
  private sessionId = this.generateSessionId();

  private generateSessionId(): string {
    return `session_${Date.now()}_${Math.random().toString(36).substring(2, 15)}`;
  }

  private generateCorrelationId(): string {
    return `corr_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
  }

  private getBrowserMetadata() {
    if (typeof window === 'undefined') return {};
    
    return {
      browser: navigator.userAgent,
      screen_resolution: `${screen.width}x${screen.height}`,
      viewport_size: `${window.innerWidth}x${window.innerHeight}`,
      connection_type: (navigator as any).connection?.effectiveType || 'unknown',
      language: navigator.language,
      platform: navigator.platform,
      cookie_enabled: navigator.cookieEnabled,
    };
  }

  private getPerformanceMetrics() {
    if (typeof performance === 'undefined') return {};
    
    try {
      const memory = (performance as any).memory;
      return {
        memory_usage: memory ? Math.round(memory.usedJSHeapSize / 1024 / 1024) : undefined,
        timing: performance.timing ? {
          page_load_time: performance.timing.loadEventEnd - performance.timing.navigationStart,
          dom_ready_time: performance.timing.domContentLoadedEventEnd - performance.timing.navigationStart,
        } : undefined,
      };
    } catch (error) {
      return {};
    }
  }

  private formatLogEntry(level: LogLevel, message: string, context?: LogContext) {
    const baseEntry = {
      timestamp: new Date().toISOString(),
      level: level.toUpperCase(),
      message,
      session_id: this.sessionId,
      correlation_id: context?.correlation_id || this.generateCorrelationId(),
      url: typeof window !== 'undefined' ? window.location.href : '',
      metadata: {
        ...this.getBrowserMetadata(),
        ...context?.metadata,
      },
      performance_metrics: {
        ...this.getPerformanceMetrics(),
        ...context?.performance_metrics,
      },
      ...context
    };

    // Clean up undefined values for cleaner logs
    return JSON.parse(JSON.stringify(baseEntry, (key, value) => 
      value === undefined ? null : value
    ));
  }

  private getLogColors() {
    return {
      debug: '\x1b[36m',    // Cyan
      info: '\x1b[32m',     // Green
      warn: '\x1b[33m',     // Yellow
      error: '\x1b[31m',    // Red
      reset: '\x1b[0m',     // Reset
      bold: '\x1b[1m',      // Bold
      timestamp: '\x1b[90m', // Gray
    };
  }

  private formatConsoleMessage(level: LogLevel, message: string, context?: LogContext): string {
    const colors = this.getLogColors();
    const timestamp = new Date().toLocaleTimeString();
    const levelColor = colors[level] || colors.reset;
    
    let formattedMessage = `${colors.timestamp}[${timestamp}]${colors.reset} `;
    formattedMessage += `${levelColor}${colors.bold}[${level.toUpperCase()}]${colors.reset} `;
    formattedMessage += `${message}`;
    
    if (context?.operation) {
      formattedMessage += ` ${colors.timestamp}(${context.operation})${colors.reset}`;
    }
    
    if (context?.component) {
      formattedMessage += ` ${colors.timestamp}[${context.component}]${colors.reset}`;
    }

    return formattedMessage;
  }

  private logToConsole(level: LogLevel, message: string, context?: LogContext) {
    const logEntry = this.formatLogEntry(level, message, context);
    
    if (this.isDevelopment) {
      // Enhanced formatting in development
      const consoleMethod = level === 'error' ? 'error' : level === 'warn' ? 'warn' : 'log';
      const formattedMessage = this.formatConsoleMessage(level, message, context);
      
      if (context && Object.keys(context).length > 0) {
        // Group related logs for better readability
        console.group(formattedMessage);
        
        // Show important context first
        if (context.error_message || context.error_type) {
          console.error(`Error: ${context.error_type || 'Unknown'} - ${context.error_message || 'No message'}`);
        }
        
        if (context.error_stack) {
          console.error('Stack Trace:', context.error_stack);
        }
        
        if (context.processing_time) {
          console.log(`Processing Time: ${context.processing_time}ms`);
        }
        
        if (context.performance_metrics) {
          console.log('Performance:', context.performance_metrics);
        }
        
        // Show full context in a collapsible group
        console.groupCollapsed('Full Context');
        console.log(context);
        console.groupEnd();
        
        console.groupEnd();
      } else {
        console[consoleMethod](formattedMessage);
      }
    } else {
      // Structured logging in production
      console.log(JSON.stringify(logEntry, null, 2));
    }
  }

  private async logToServer(level: LogLevel, message: string, context?: LogContext) {
    try {
      // Only send error and warn logs to server in production
      if (level === 'error' || level === 'warn') {
        const logEntry = this.formatLogEntry(level, message, context);
        
        // You can implement server-side logging endpoint here
        // await fetch('/api/logs', {
        //   method: 'POST',
        //   headers: { 'Content-Type': 'application/json' },
        //   body: JSON.stringify(logEntry)
        // });
      }
    } catch (error) {
      // Silent fail - don't crash the app if logging fails
      console.error('Failed to send log to server:', error);
    }
  }

  debug(message: string, context?: LogContext) {
    this.logToConsole('debug', message, context);
  }

  info(message: string, context?: LogContext) {
    this.logToConsole('info', message, context);
    this.logToServer('info', message, context);
  }

  warn(message: string, context?: LogContext) {
    this.logToConsole('warn', message, context);
    this.logToServer('warn', message, context);
  }

  error(message: string, context?: LogContext) {
    this.logToConsole('error', message, context);
    this.logToServer('error', message, context);
  }

  // Utility methods for common operations
  logFileUploadStart(fileName: string, fileSize: number) {
    this.info('File upload started', {
      operation: 'file_upload_start',
      file_name: fileName,
      file_size: fileSize,
      user_action: 'file_upload'
    });
  }

  logFileUploadSuccess(fileName: string, fileSize: number, processingTime: number) {
    this.info('File upload completed successfully', {
      operation: 'file_upload_success',
      file_name: fileName,
      file_size: fileSize,
      processing_time: processingTime,
      user_action: 'file_upload'
    });
  }

  logFileUploadError(fileName: string, error: any, processingTime?: number) {
    this.error('File upload failed', {
      operation: 'file_upload_failed',
      file_name: fileName,
      error_type: error?.name || typeof error,
      error_message: error?.message || String(error || 'Unknown error'),
      error_stack: error?.stack || new Error().stack,
      processing_time: processingTime,
      user_action: 'file_upload',
      component: 'FileUpload',
      severity: 'high',
      impact: 'user_workflow_blocked'
    });
  }

  logDatasetLoadStart(datasetName: string, isUploaded: boolean) {
    this.info('Dataset loading started', {
      operation: 'dataset_load_start',
      dataset_name: datasetName,
      is_uploaded: isUploaded,
      user_action: 'dataset_load'
    });
  }

  logDatasetLoadSuccess(datasetName: string, rowCount: number, processingTime: number) {
    this.info('Dataset loaded successfully', {
      operation: 'dataset_load_success',
      dataset_name: datasetName,
      row_count: rowCount,
      processing_time: processingTime,
      user_action: 'dataset_load'
    });
  }

  logDatasetLoadError(datasetName: string, error: any, processingTime?: number) {
    this.error('Dataset loading failed', {
      operation: 'dataset_load_failed',
      dataset_name: datasetName,
      error_type: error?.name || typeof error,
      error_message: error?.message || String(error || 'Unknown error'),
      error_stack: error?.stack || new Error().stack,
      processing_time: processingTime,
      user_action: 'dataset_load',
      component: 'DatasetLoader',
      severity: 'high',
      impact: 'user_workflow_blocked'
    });
  }

  logApiRequest(endpoint: string, method: string = 'GET') {
    this.debug('API request started', {
      operation: 'api_request_start',
      endpoint,
      method,
      user_action: 'api_request'
    });
  }

  logApiSuccess(endpoint: string, method: string, processingTime: number) {
    this.info('API request completed', {
      operation: 'api_request_success',
      endpoint,
      method,
      processing_time: processingTime,
      user_action: 'api_request'
    });
  }

  logApiError(endpoint: string, method: string, error: any, processingTime?: number, statusCode?: number) {
    this.error('API request failed', {
      operation: 'api_request_failed',
      endpoint,
      method,
      status_code: statusCode,
      error_type: error?.name || typeof error,
      error_message: error?.message || String(error || 'Unknown error'),
      error_stack: error?.stack,
      processing_time: processingTime,
      user_action: 'api_request',
      component: 'ApiClient',
      severity: statusCode >= 500 ? 'critical' : 'high',
      impact: 'api_communication_failed'
    });
  }

  // Enhanced error logging utilities
  logUnhandledError(error: Error, context?: Partial<LogContext>) {
    this.error('Unhandled error occurred', {
      operation: 'unhandled_error',
      error_type: error.name,
      error_message: error.message,
      error_stack: error.stack,
      component: 'Global',
      severity: 'critical',
      impact: 'application_stability',
      ...context
    });
  }

  logPerformanceIssue(operation: string, actualTime: number, expectedTime: number, context?: Partial<LogContext>) {
    const severity = actualTime > expectedTime * 3 ? 'critical' : actualTime > expectedTime * 2 ? 'high' : 'medium';
    
    this.warn('Performance degradation detected', {
      operation: 'performance_issue',
      affected_operation: operation,
      actual_time_ms: actualTime,
      expected_time_ms: expectedTime,
      performance_ratio: actualTime / expectedTime,
      severity,
      impact: 'user_experience_degraded',
      ...context
    });
  }

  logUserAction(action: string, component: string, metadata?: any) {
    this.info('User interaction logged', {
      operation: 'user_action',
      user_action: action,
      component,
      action_metadata: metadata,
      severity: 'low',
      impact: 'user_analytics'
    });
  }

  logSecurityEvent(eventType: string, details: any, severity: 'low' | 'medium' | 'high' | 'critical' = 'high') {
    this.warn('Security event detected', {
      operation: 'security_event',
      security_event_type: eventType,
      security_details: details,
      component: 'Security',
      severity,
      impact: 'security_concern'
    });
  }

  // Performance monitoring
  startTimer(): () => number {
    const startTime = performance.now();
    return () => performance.now() - startTime;
  }

  // Enhanced timer with automatic performance logging
  startOperationTimer(operation: string, expectedTimeMs?: number): () => void {
    const startTime = performance.now();
    const correlationId = this.generateCorrelationId();
    
    this.debug('Operation started', {
      operation: `${operation}_start`,
      correlation_id: correlationId,
      expected_time_ms: expectedTimeMs
    });

    return () => {
      const actualTime = performance.now() - startTime;
      
      this.info('Operation completed', {
        operation: `${operation}_complete`,
        correlation_id: correlationId,
        processing_time: Math.round(actualTime),
        expected_time_ms: expectedTimeMs
      });

      // Automatically log performance issues
      if (expectedTimeMs && actualTime > expectedTimeMs * 1.5) {
        this.logPerformanceIssue(operation, actualTime, expectedTimeMs, {
          correlation_id: correlationId
        });
      }
    };
  }
}

export const logger = new Logger();
export default logger;
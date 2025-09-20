"""
MatrixOne Logger Module

Provides logging functionality for MatrixOne Python SDK with support for:
1. Default logger configuration
2. Custom logger integration
3. Structured logging
4. Performance logging
5. Error tracking
"""

import logging
import sys
from typing import Optional, Dict, Any
from datetime import datetime


class MatrixOneLogger:
    """
    MatrixOne Logger class that provides structured logging for the SDK
    
    Features:
    - Default logger configuration
    - Custom logger integration
    - Performance logging
    - Error tracking
    - Structured log messages
    """
    
    def __init__(self, 
                 logger: Optional[logging.Logger] = None,
                 level: int = logging.INFO,
                 format_string: Optional[str] = None,
                 enable_performance_logging: bool = False,
                 enable_sql_logging: bool = False):
        """
        Initialize MatrixOne logger
        
        Args:
            logger: Custom logger instance. If None, creates a default logger
            level: Logging level (default: INFO)
            format_string: Custom format string for log messages
            enable_performance_logging: Enable performance logging
            enable_sql_logging: Enable SQL query logging
        """
        self.enable_performance_logging = enable_performance_logging
        self.enable_sql_logging = enable_sql_logging
        
        if logger is not None:
            # Use provided logger
            self.logger = logger
            self._is_custom = True
        else:
            # Create default logger
            self.logger = self._create_default_logger(level, format_string)
            self._is_custom = False
    
    def _create_default_logger(self, level: int, format_string: Optional[str]) -> logging.Logger:
        """Create default logger with standard configuration"""
        logger = logging.getLogger('matrixone')
        
        # Avoid adding multiple handlers
        if logger.handlers:
            return logger
        
        # Set level
        logger.setLevel(level)
        
        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        
        # Set format
        if format_string is None:
            format_string = '%(asctime)s|%(name)s|%(levelname)s|[%(filename)s:%(lineno)d]: %(message)s'
        
        formatter = logging.Formatter(format_string)
        console_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(console_handler)
        
        # Prevent propagation to root logger
        logger.propagate = False
        
        return logger
    
    def debug(self, message: str, **kwargs):
        """Log debug message"""
        # Use findCaller to get the actual caller's file and line
        import sys
        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name, logging.DEBUG, filename, lineno, 
            self._format_message(message, **kwargs), (), None
        )
        self.logger.handle(record)
    
    def info(self, message: str, **kwargs):
        """Log info message"""
        # Use findCaller to get the actual caller's file and line
        import sys
        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name, logging.INFO, filename, lineno, 
            self._format_message(message, **kwargs), (), None
        )
        self.logger.handle(record)
    
    def warning(self, message: str, **kwargs):
        """Log warning message"""
        # Use findCaller to get the actual caller's file and line
        import sys
        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name, logging.WARNING, filename, lineno, 
            self._format_message(message, **kwargs), (), None
        )
        self.logger.handle(record)
    
    def error(self, message: str, **kwargs):
        """Log error message"""
        # Use findCaller to get the actual caller's file and line
        import sys
        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name, logging.ERROR, filename, lineno, 
            self._format_message(message, **kwargs), (), None
        )
        self.logger.handle(record)
    
    def critical(self, message: str, **kwargs):
        """Log critical message"""
        # Use findCaller to get the actual caller's file and line
        import sys
        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name, logging.CRITICAL, filename, lineno, 
            self._format_message(message, **kwargs), (), None
        )
        self.logger.handle(record)
    
    def _format_message(self, message: str, **kwargs) -> str:
        """Format message with additional context"""
        if not kwargs:
            return message
        
        # Add context information
        context_parts = []
        for key, value in kwargs.items():
            context_parts.append(f"{key}={value}")
        
        context_str = " | ".join(context_parts)
        return f"{message} | {context_str}"
    
    def log_connection(self, host: str, port: int, user: str, database: str, success: bool = True):
        """Log connection events"""
        status = "✓ Connected" if success else "✗ Connection failed"
        # Use findCaller to get the actual caller's file and line
        import sys
        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name, logging.INFO, filename, lineno, 
            self._format_message(f"{status} to MatrixOne", 
                               host=host, port=port, user=user, database=database), (), None
        )
        self.logger.handle(record)
    
    def log_disconnection(self, success: bool = True):
        """Log disconnection events"""
        status = "✓ Disconnected" if success else "✗ Disconnection failed"
        # Use findCaller to get the actual caller's file and line
        import sys
        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name, logging.INFO, filename, lineno, 
            f"{status} from MatrixOne", (), None
        )
        self.logger.handle(record)
    
    def log_query(self, query: str, execution_time: Optional[float] = None, 
                  affected_rows: Optional[int] = None, success: bool = True):
        """Log SQL query execution"""
        if not self.enable_sql_logging:
            return
        
        status = "✓ Query executed" if success else "✗ Query failed"
        message = f"{status}"
        
        kwargs = {}
        if execution_time is not None:
            kwargs['execution_time'] = f"{execution_time:.3f}s"
        if affected_rows is not None:
            kwargs['affected_rows'] = affected_rows
        
        # Truncate long queries for readability
        display_query = query[:100] + "..." if len(query) > 100 else query
        kwargs['query'] = display_query
        
        # Use findCaller to get the actual caller's file and line
        import sys
        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name, logging.INFO, filename, lineno, 
            self._format_message(message, **kwargs), (), None
        )
        self.logger.handle(record)
    
    def log_performance(self, operation: str, duration: float, **kwargs):
        """Log performance metrics"""
        if not self.enable_performance_logging:
            return
        
        # Use findCaller to get the actual caller's file and line
        import sys
        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        
        kwargs['duration'] = f"{duration:.3f}s"
        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name, logging.INFO, filename, lineno, 
            self._format_message(f"Performance: {operation}", **kwargs), (), None
        )
        self.logger.handle(record)
    
    def log_error(self, error: Exception, context: Optional[str] = None):
        """Log errors with context"""
        error_type = type(error).__name__
        error_message = str(error)
        
        kwargs = {'error_type': error_type, 'error_message': error_message}
        if context:
            kwargs['context'] = context
        
        # Use findCaller to get the actual caller's file and line
        import sys
        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name, logging.ERROR, filename, lineno, 
            self._format_message(f"Error occurred: {error_type}", **kwargs), (), None
        )
        self.logger.handle(record)
    
    def log_transaction(self, action: str, success: bool = True, **kwargs):
        """Log transaction events"""
        status = "✓ Transaction" if success else "✗ Transaction failed"
        
        # Use findCaller to get the actual caller's file and line
        import sys
        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        
        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name, logging.INFO, filename, lineno, 
            self._format_message(f"{status}: {action}", **kwargs), (), None
        )
        self.logger.handle(record)
    
    def log_account_operation(self, operation: str, account_name: Optional[str] = None, 
                             user_name: Optional[str] = None, success: bool = True):
        """Log account management operations"""
        status = "✓ Account operation" if success else "✗ Account operation failed"
        kwargs = {'operation': operation}
        if account_name:
            kwargs['account'] = account_name
        if user_name:
            kwargs['user'] = user_name
        
        # Use findCaller to get the actual caller's file and line
        import sys
        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        
        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name, logging.INFO, filename, lineno, 
            self._format_message(f"{status}: {operation}", **kwargs), (), None
        )
        self.logger.handle(record)
    
    def log_snapshot_operation(self, operation: str, snapshot_name: Optional[str] = None, 
                              level: Optional[str] = None, success: bool = True):
        """Log snapshot operations"""
        status = "✓ Snapshot operation" if success else "✗ Snapshot operation failed"
        kwargs = {'operation': operation}
        if snapshot_name:
            kwargs['snapshot'] = snapshot_name
        if level:
            kwargs['level'] = level
        
        # Use findCaller to get the actual caller's file and line
        import sys
        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        
        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name, logging.INFO, filename, lineno, 
            self._format_message(f"{status}: {operation}", **kwargs), (), None
        )
        self.logger.handle(record)
    
    def log_pubsub_operation(self, operation: str, publication_name: Optional[str] = None,
                            subscription_name: Optional[str] = None, success: bool = True):
        """Log PubSub operations"""
        status = "✓ PubSub operation" if success else "✗ PubSub operation failed"
        kwargs = {'operation': operation}
        if publication_name:
            kwargs['publication'] = publication_name
        if subscription_name:
            kwargs['subscription'] = subscription_name
        
        # Use findCaller to get the actual caller's file and line
        import sys
        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        
        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name, logging.INFO, filename, lineno, 
            self._format_message(f"{status}: {operation}", **kwargs), (), None
        )
        self.logger.handle(record)
    
    def set_level(self, level: int):
        """Set logging level"""
        self.logger.setLevel(level)
        for handler in self.logger.handlers:
            handler.setLevel(level)
    
    def add_handler(self, handler: logging.Handler):
        """Add custom handler to logger"""
        self.logger.addHandler(handler)
    
    def remove_handler(self, handler: logging.Handler):
        """Remove handler from logger"""
        self.logger.removeHandler(handler)
    
    def is_custom(self) -> bool:
        """Check if using custom logger"""
        return self._is_custom


def create_default_logger(level: int = logging.INFO, 
                         format_string: Optional[str] = None,
                         enable_performance_logging: bool = False,
                         enable_sql_logging: bool = False) -> MatrixOneLogger:
    """
    Create a default MatrixOne logger
    
    Args:
        level: Logging level
        format_string: Custom format string
        enable_performance_logging: Enable performance logging
        enable_sql_logging: Enable SQL logging
    
    Returns:
        MatrixOneLogger instance
    """
    return MatrixOneLogger(
        logger=None,
        level=level,
        format_string=format_string,
        enable_performance_logging=enable_performance_logging,
        enable_sql_logging=enable_sql_logging
    )


def create_custom_logger(logger: logging.Logger,
                        enable_performance_logging: bool = False,
                        enable_sql_logging: bool = False) -> MatrixOneLogger:
    """
    Create MatrixOne logger from custom logger
    
    Args:
        logger: Custom logger instance
        enable_performance_logging: Enable performance logging
        enable_sql_logging: Enable SQL logging
    
    Returns:
        MatrixOneLogger instance
    """
    return MatrixOneLogger(
        logger=logger,
        enable_performance_logging=enable_performance_logging,
        enable_sql_logging=enable_sql_logging
    )

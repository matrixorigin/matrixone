# Copyright 2021 - 2022 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
from typing import Optional


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

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        level: int = logging.INFO,
        format_string: Optional[str] = None,
        sql_log_mode: str = "auto",
        slow_query_threshold: float = 1.0,
        max_sql_display_length: int = 500,
    ):
        """
        Initialize MatrixOne logger

        Args::

            logger: Custom logger instance. If None, creates a default logger
            level: Logging level (default: INFO)
            format_string: Custom format string for log messages
            sql_log_mode: SQL logging mode ('off', 'auto', 'simple', 'full')
                - 'off': No SQL logging
                - 'auto': Smart logging - short SQL shown fully, long SQL summarized (default)
                - 'simple': Show operation summary only (e.g., "INSERT INTO table (5 rows)")
                - 'full': Show complete SQL regardless of length
            slow_query_threshold: Threshold in seconds for slow query warnings (default: 1.0)
            max_sql_display_length: Maximum SQL length in auto mode before summarizing (default: 500)
        """
        # Validate sql_log_mode
        valid_modes = ['off', 'auto', 'simple', 'full']
        if sql_log_mode not in valid_modes:
            raise ValueError(f"Invalid sql_log_mode '{sql_log_mode}'. Must be one of {valid_modes}")

        self.sql_log_mode = sql_log_mode
        self.slow_query_threshold = slow_query_threshold
        self.max_sql_display_length = max_sql_display_length

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
        logger = logging.getLogger("matrixone")

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
            format_string = "%(asctime)s|%(name)s|%(levelname)s|[%(filename)s:%(lineno)d]: %(message)s"

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
            self.logger.name,
            logging.DEBUG,
            filename,
            lineno,
            self._format_message(message, **kwargs),
            (),
            None,
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
            self.logger.name,
            logging.INFO,
            filename,
            lineno,
            self._format_message(message, **kwargs),
            (),
            None,
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
            self.logger.name,
            logging.WARNING,
            filename,
            lineno,
            self._format_message(message, **kwargs),
            (),
            None,
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
            self.logger.name,
            logging.ERROR,
            filename,
            lineno,
            self._format_message(message, **kwargs),
            (),
            None,
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
            self.logger.name,
            logging.CRITICAL,
            filename,
            lineno,
            self._format_message(message, **kwargs),
            (),
            None,
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
            self.logger.name,
            logging.INFO,
            filename,
            lineno,
            self._format_message(f"{status} to MatrixOne", host=host, port=port, user=user, database=database),
            (),
            None,
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
            self.logger.name, logging.INFO, filename, lineno, f"{status} from MatrixOne", (), None
        )
        self.logger.handle(record)

    def _extract_sql_summary(self, sql: str) -> str:
        """
        Extract operation summary from SQL query

        Args::

            sql: SQL query string

        Returns::

            Summary string describing the operation
        """
        sql_stripped = sql.strip()
        sql_upper = sql_stripped.upper()

        # Extract operation type and key details
        import re

        # SELECT operations
        if sql_upper.startswith('SELECT'):
            match = re.search(r'FROM\s+([`"]?\w+[`"]?)', sql_upper, re.IGNORECASE)
            table = match.group(1).strip('`"') if match else '?'
            return f"SELECT FROM {table}"

        # INSERT operations
        elif sql_upper.startswith('INSERT'):
            match = re.search(r'INSERT\s+INTO\s+([`"]?\w+[`"]?)', sql_upper, re.IGNORECASE)
            table = match.group(1).strip('`"') if match else '?'
            # Count rows in batch insert by counting VALUES groups
            if 'VALUES' in sql_upper:
                # Count the number of value groups by counting '),' patterns
                values_count = sql_stripped.count('),(') + 1
                if values_count > 1:
                    return f"BATCH INSERT INTO {table} ({values_count} rows)"
            return f"INSERT INTO {table}"

        # UPDATE operations
        elif sql_upper.startswith('UPDATE'):
            match = re.search(r'UPDATE\s+([`"]?\w+[`"]?)', sql_upper, re.IGNORECASE)
            table = match.group(1).strip('`"') if match else '?'
            return f"UPDATE {table}"

        # DELETE operations
        elif sql_upper.startswith('DELETE'):
            match = re.search(r'DELETE\s+FROM\s+([`"]?\w+[`"]?)', sql_upper, re.IGNORECASE)
            table = match.group(1).strip('`"') if match else '?'
            return f"DELETE FROM {table}"

        # CREATE operations
        elif sql_upper.startswith('CREATE'):
            if 'INDEX' in sql_upper:
                match = re.search(r'ON\s+([`"]?\w+[`"]?)', sql_upper, re.IGNORECASE)
                table = match.group(1).strip('`"') if match else '?'
                return f"CREATE INDEX ON {table}"
            elif 'TABLE' in sql_upper:
                match = re.search(r'CREATE\s+TABLE\s+([`"]?\w+[`"]?)', sql_upper, re.IGNORECASE)
                table = match.group(1).strip('`"') if match else '?'
                return f"CREATE TABLE {table}"
            return "CREATE"

        # DROP operations
        elif sql_upper.startswith('DROP'):
            if 'TABLE' in sql_upper:
                match = re.search(r'DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([`"]?\w+[`"]?)', sql_upper, re.IGNORECASE)
                table = match.group(1).strip('`"') if match else '?'
                return f"DROP TABLE {table}"
            elif 'INDEX' in sql_upper:
                return "DROP INDEX"
            return "DROP"

        # SET operations
        elif sql_upper.startswith('SET'):
            return "SET VARIABLE"

        # Other operations
        else:
            # Return first 50 characters as fallback
            return sql_stripped[:50] + "..." if len(sql_stripped) > 50 else sql_stripped

    def _format_sql_for_log(
        self, sql: str, is_error: bool = False, is_slow: bool = False, override_mode: Optional[str] = None
    ) -> str:
        """
        Format SQL query for logging based on mode and query characteristics

        Args::

            sql: SQL query string
            is_error: Whether this is an error query
            is_slow: Whether this is a slow query
            override_mode: Optional mode override for this specific query

        Returns::

            Formatted SQL string for logging
        """
        # Use override mode if provided, otherwise use instance mode
        effective_mode = override_mode if override_mode is not None else self.sql_log_mode

        # Mode 'off': Don't log SQL
        if effective_mode == 'off':
            return ""

        # Errors and slow queries always show complete SQL for debugging
        if is_error or is_slow:
            return sql.strip()

        # Mode 'full': Always show complete SQL
        if effective_mode == 'full':
            return sql.strip()

        # Mode 'simple': Only show operation summary
        if effective_mode == 'simple':
            return self._extract_sql_summary(sql)

        # Mode 'auto': Smart formatting based on length
        if effective_mode == 'auto':
            sql_stripped = sql.strip()
            if len(sql_stripped) <= self.max_sql_display_length:
                # Short SQL: show fully
                return sql_stripped
            else:
                # Long SQL: show summary with length indicator
                summary = self._extract_sql_summary(sql)
                return f"{summary} [SQL length: {len(sql_stripped)} chars]"

        return sql.strip()

    def update_config(
        self,
        sql_log_mode: Optional[str] = None,
        slow_query_threshold: Optional[float] = None,
        max_sql_display_length: Optional[int] = None,
    ):
        """
        Dynamically update logger configuration at runtime.

        Args::

            sql_log_mode: New SQL logging mode ('off', 'auto', 'simple', 'full')
            slow_query_threshold: New threshold in seconds for slow query warnings
            max_sql_display_length: New maximum SQL length in auto mode before summarizing

        Example::

            # Enable full SQL logging for debugging
            client.logger.update_config(sql_log_mode='full')

            # Update multiple settings
            client.logger.update_config(
                sql_log_mode='auto',
                slow_query_threshold=2.0,
                max_sql_display_length=1000
            )
        """
        if sql_log_mode is not None:
            valid_modes = ['off', 'auto', 'simple', 'full']
            if sql_log_mode not in valid_modes:
                raise ValueError(f"Invalid sql_log_mode '{sql_log_mode}'. Must be one of {valid_modes}")
            self.sql_log_mode = sql_log_mode

        if slow_query_threshold is not None:
            if slow_query_threshold < 0:
                raise ValueError("slow_query_threshold must be non-negative")
            self.slow_query_threshold = slow_query_threshold

        if max_sql_display_length is not None:
            if max_sql_display_length < 1:
                raise ValueError("max_sql_display_length must be positive")
            self.max_sql_display_length = max_sql_display_length

    def log_query(
        self,
        query: str,
        execution_time: Optional[float] = None,
        affected_rows: Optional[int] = None,
        success: bool = True,
        override_sql_log_mode: Optional[str] = None,
    ):
        """
        Log SQL query execution with smart formatting.

        Args::

            query: SQL query string
            execution_time: Query execution time in seconds
            affected_rows: Number of rows affected
            success: Whether the query succeeded
            override_sql_log_mode: Temporarily override sql_log_mode for this query only
        """
        # Use override mode if provided, otherwise use instance mode
        effective_mode = override_sql_log_mode if override_sql_log_mode is not None else self.sql_log_mode

        # Skip logging if mode is 'off'
        if effective_mode == 'off':
            return

        # Determine if this is a slow query or error
        is_slow = execution_time is not None and execution_time >= self.slow_query_threshold
        is_error = not success

        # Format SQL based on mode and query characteristics
        display_sql = self._format_sql_for_log(query, is_error=is_error, is_slow=is_slow, override_mode=effective_mode)

        # Skip if no SQL to display
        if not display_sql:
            return

        # Build log message
        status_icon = "✓" if success else "✗"
        message_parts = [status_icon]

        # Add execution time if available
        if execution_time is not None:
            message_parts.append(f"{execution_time:.3f}s")

        # Add affected rows if available
        if affected_rows is not None:
            message_parts.append(f"{affected_rows} rows")

        # Add SQL with appropriate prefix
        if is_error:
            message_parts.append(f"[ERROR] {display_sql}")
        elif is_slow:
            message_parts.append(f"[SLOW] {display_sql}")
        else:
            message_parts.append(display_sql)

        message = " | ".join(message_parts)

        # Use findCaller to get the actual caller's file and line
        import sys

        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno

        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name,
            logging.ERROR if is_error else logging.INFO,
            filename,
            lineno,
            self._format_message(message),
            (),
            None,
        )

        self.logger.handle(record)

    def log_performance(self, operation: str, duration: float, **kwargs):
        """Log performance metrics (always enabled, control via log level)"""

        # Use findCaller to get the actual caller's file and line
        import sys

        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno

        kwargs["duration"] = f"{duration:.3f}s"
        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name,
            logging.INFO,
            filename,
            lineno,
            self._format_message(f"Performance: {operation}", **kwargs),
            (),
            None,
        )
        self.logger.handle(record)

    def log_error(self, error: Exception, context: Optional[str] = None, include_traceback: bool = False):
        """
        Log errors with context and optional traceback.

        Args:
            error: The exception object
            context: Optional context description (e.g., "Query execution", "Table creation")
            include_traceback: If True, include full traceback in log (useful for debugging)
        """
        error_type = type(error).__name__
        error_message = str(error)

        kwargs = {"error_type": error_type, "error_message": error_message}
        if context:
            kwargs["context"] = context

        # Use findCaller to get the actual caller's file and line
        import sys

        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno

        # Prepare error message
        error_msg = self._format_message(f"Error occurred: {error_type}", **kwargs)

        # Add traceback if requested (useful for debugging)
        exc_info = None
        if include_traceback:
            exc_info = sys.exc_info()

        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name,
            logging.ERROR,
            filename,
            lineno,
            error_msg,
            (),
            exc_info,  # Include exception info if requested
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
            self.logger.name,
            logging.INFO,
            filename,
            lineno,
            self._format_message(f"{status}: {action}", **kwargs),
            (),
            None,
        )
        self.logger.handle(record)

    def log_account_operation(
        self,
        operation: str,
        account_name: Optional[str] = None,
        user_name: Optional[str] = None,
        success: bool = True,
    ):
        """Log account management operations"""
        status = "✓ Account operation" if success else "✗ Account operation failed"
        kwargs = {"operation": operation}
        if account_name:
            kwargs["account"] = account_name
        if user_name:
            kwargs["user"] = user_name

        # Use findCaller to get the actual caller's file and line
        import sys

        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno

        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name,
            logging.INFO,
            filename,
            lineno,
            self._format_message(f"{status}: {operation}", **kwargs),
            (),
            None,
        )
        self.logger.handle(record)

    def log_snapshot_operation(
        self,
        operation: str,
        snapshot_name: Optional[str] = None,
        level: Optional[str] = None,
        success: bool = True,
    ):
        """Log snapshot operations"""
        status = "✓ Snapshot operation" if success else "✗ Snapshot operation failed"
        kwargs = {"operation": operation}
        if snapshot_name:
            kwargs["snapshot"] = snapshot_name
        if level:
            kwargs["level"] = level

        # Use findCaller to get the actual caller's file and line
        import sys

        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno

        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name,
            logging.INFO,
            filename,
            lineno,
            self._format_message(f"{status}: {operation}", **kwargs),
            (),
            None,
        )
        self.logger.handle(record)

    def log_pubsub_operation(
        self,
        operation: str,
        publication_name: Optional[str] = None,
        subscription_name: Optional[str] = None,
        success: bool = True,
    ):
        """Log PubSub operations"""
        status = "✓ PubSub operation" if success else "✗ PubSub operation failed"
        kwargs = {"operation": operation}
        if publication_name:
            kwargs["publication"] = publication_name
        if subscription_name:
            kwargs["subscription"] = subscription_name

        # Use findCaller to get the actual caller's file and line
        import sys

        frame = sys._getframe(1)
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno

        # Create a new record with the caller's info
        record = self.logger.makeRecord(
            self.logger.name,
            logging.INFO,
            filename,
            lineno,
            self._format_message(f"{status}: {operation}", **kwargs),
            (),
            None,
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


def create_default_logger(
    level: int = logging.INFO,
    format_string: Optional[str] = None,
    sql_log_mode: str = "auto",
    slow_query_threshold: float = 1.0,
    max_sql_display_length: int = 500,
) -> MatrixOneLogger:
    """
    Create a default MatrixOne logger

    Args::

        level: Logging level
        format_string: Custom format string
        sql_log_mode: SQL logging mode ('off', 'auto', 'simple', 'full')
        slow_query_threshold: Threshold in seconds for slow query warnings
        max_sql_display_length: Maximum SQL length in auto mode before summarizing

    Returns::

        MatrixOneLogger instance
    """
    return MatrixOneLogger(
        logger=None,
        level=level,
        format_string=format_string,
        sql_log_mode=sql_log_mode,
        slow_query_threshold=slow_query_threshold,
        max_sql_display_length=max_sql_display_length,
    )


def create_custom_logger(
    logger: logging.Logger,
    sql_log_mode: str = "auto",
    slow_query_threshold: float = 1.0,
    max_sql_display_length: int = 500,
) -> MatrixOneLogger:
    """
    Create MatrixOne logger from custom logger

    Args::

        logger: Custom logger instance
        sql_log_mode: SQL logging mode ('off', 'auto', 'simple', 'full')
        slow_query_threshold: Threshold in seconds for slow query warnings
        max_sql_display_length: Maximum SQL length in auto mode before summarizing

    Returns::

        MatrixOneLogger instance
    """
    return MatrixOneLogger(
        logger=logger,
        sql_log_mode=sql_log_mode,
        slow_query_threshold=slow_query_threshold,
        max_sql_display_length=max_sql_display_length,
    )

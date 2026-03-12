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
import re
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
        valid_modes = ['off', 'auto', 'simple', 'full']
        if sql_log_mode not in valid_modes:
            raise ValueError(f"Invalid sql_log_mode '{sql_log_mode}'. Must be one of {valid_modes}")

        self.sql_log_mode = sql_log_mode
        self.slow_query_threshold = slow_query_threshold
        self.max_sql_display_length = max_sql_display_length

        if logger is not None:
            self.logger = logger
            self._is_custom = True
        else:
            self.logger = self._create_default_logger(level, format_string)
            self._is_custom = False

    def _create_default_logger(self, level: int, format_string: Optional[str]) -> logging.Logger:
        """Create default logger with standard configuration"""
        logger = logging.getLogger("matrixone")

        if logger.handlers:
            return logger

        logger.setLevel(level)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)

        if format_string is None:
            format_string = "%(asctime)s|%(name)s|%(levelname)s|[%(filename)s:%(lineno)d]: %(message)s"

        formatter = logging.Formatter(format_string)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        logger.propagate = False

        return logger

    # ── Core emit helper ─────────────────────────────────────────────

    def _emit(self, level: int, message: str, exc_info=None, stack_depth: int = 2):
        """Emit a log record with the caller's file/line info.

        Args:
            level: logging level constant (e.g. logging.INFO)
            message: formatted message string
            exc_info: optional exception info tuple for tracebacks
            stack_depth: how many frames to skip (default 2 = caller of caller)
        """
        frame = sys._getframe(stack_depth)
        record = self.logger.makeRecord(
            self.logger.name,
            level,
            frame.f_code.co_filename,
            frame.f_lineno,
            message,
            (),
            exc_info,
        )
        self.logger.handle(record)

    # ── Standard level methods ───────────────────────────────────────

    def debug(self, message: str, **kwargs):
        """Log debug message"""
        self._emit(logging.DEBUG, self._format_message(message, **kwargs))

    def info(self, message: str, **kwargs):
        """Log info message"""
        self._emit(logging.INFO, self._format_message(message, **kwargs))

    def warning(self, message: str, **kwargs):
        """Log warning message"""
        self._emit(logging.WARNING, self._format_message(message, **kwargs))

    def error(self, message: str, **kwargs):
        """Log error message"""
        self._emit(logging.ERROR, self._format_message(message, **kwargs))

    def critical(self, message: str, **kwargs):
        """Log critical message"""
        self._emit(logging.CRITICAL, self._format_message(message, **kwargs))

    # ── Message formatting ───────────────────────────────────────────

    def _format_message(self, message: str, **kwargs) -> str:
        """Format message with additional context"""
        if not kwargs:
            return message
        context_str = " | ".join(f"{k}={v}" for k, v in kwargs.items())
        return f"{message} | {context_str}"

    # ── Domain-specific logging ──────────────────────────────────────

    def log_connection(self, host: str, port: int, user: str, database: str, success: bool = True):
        """Log connection events"""
        status = "✓ Connected" if success else "✗ Connection failed"
        self._emit(
            logging.INFO,
            self._format_message(f"{status} to MatrixOne", host=host, port=port, user=user, database=database),
        )

    def log_disconnection(self, success: bool = True):
        """Log disconnection events"""
        status = "✓ Disconnected" if success else "✗ Disconnection failed"
        self._emit(logging.INFO, f"{status} from MatrixOne")

    def log_error(self, error: Exception, context: Optional[str] = None, include_traceback: bool = False):
        """
        Log errors with context and optional traceback.

        Args:
            error: The exception object
            context: Optional context description (e.g., "Query execution", "Table creation")
            include_traceback: If True, include full traceback in log
        """
        error_type = type(error).__name__
        kwargs = {"error_type": error_type, "error_message": str(error)}
        if context:
            kwargs["context"] = context

        exc_info = sys.exc_info() if include_traceback else None
        self._emit(
            logging.ERROR,
            self._format_message(f"Error occurred: {error_type}", **kwargs),
            exc_info=exc_info,
        )

    # ── SQL query logging ────────────────────────────────────────────

    def _extract_sql_summary(self, sql: str) -> str:
        """Extract operation summary from SQL query"""
        sql_stripped = sql.strip()
        sql_upper = sql_stripped.upper()

        if sql_upper.startswith('SELECT'):
            match = re.search(r'FROM\s+([`"]?\w+[`"]?)', sql_upper, re.IGNORECASE)
            table = match.group(1).strip('`"') if match else '?'
            return f"SELECT FROM {table}"

        elif sql_upper.startswith('INSERT'):
            match = re.search(r'INSERT\s+INTO\s+([`"]?\w+[`"]?)', sql_upper, re.IGNORECASE)
            table = match.group(1).strip('`"') if match else '?'
            if 'VALUES' in sql_upper:
                values_count = sql_stripped.count('),(') + 1
                if values_count > 1:
                    return f"BATCH INSERT INTO {table} ({values_count} rows)"
            return f"INSERT INTO {table}"

        elif sql_upper.startswith('UPDATE'):
            match = re.search(r'UPDATE\s+([`"]?\w+[`"]?)', sql_upper, re.IGNORECASE)
            table = match.group(1).strip('`"') if match else '?'
            return f"UPDATE {table}"

        elif sql_upper.startswith('DELETE'):
            match = re.search(r'DELETE\s+FROM\s+([`"]?\w+[`"]?)', sql_upper, re.IGNORECASE)
            table = match.group(1).strip('`"') if match else '?'
            return f"DELETE FROM {table}"

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

        elif sql_upper.startswith('DROP'):
            if 'TABLE' in sql_upper:
                match = re.search(r'DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([`"]?\w+[`"]?)', sql_upper, re.IGNORECASE)
                table = match.group(1).strip('`"') if match else '?'
                return f"DROP TABLE {table}"
            elif 'INDEX' in sql_upper:
                return "DROP INDEX"
            return "DROP"

        elif sql_upper.startswith('SET'):
            return "SET VARIABLE"

        else:
            return sql_stripped[:50] + "..." if len(sql_stripped) > 50 else sql_stripped

    def _format_sql_for_log(
        self, sql: str, is_error: bool = False, is_slow: bool = False, log_mode: Optional[str] = None
    ) -> str:
        """Format SQL query for logging based on mode and query characteristics"""
        effective_mode = log_mode if log_mode is not None else self.sql_log_mode

        if effective_mode == 'off':
            return ""

        # Errors and slow queries always show complete SQL for debugging
        if is_error or is_slow:
            return sql.strip()

        if effective_mode == 'full':
            return sql.strip()

        if effective_mode == 'simple':
            return self._extract_sql_summary(sql)

        if effective_mode == 'auto':
            sql_stripped = sql.strip()
            if len(sql_stripped) <= self.max_sql_display_length:
                return sql_stripped
            else:
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
        log_mode: Optional[str] = None,
    ):
        """
        Log SQL query execution with smart formatting.

        Args::

            query: SQL query string
            execution_time: Query execution time in seconds
            affected_rows: Number of rows affected
            success: Whether the query succeeded
            log_mode: Temporarily override sql_log_mode for this query only
        """
        effective_mode = log_mode if log_mode is not None else self.sql_log_mode

        if effective_mode == 'off':
            return

        is_slow = execution_time is not None and execution_time >= self.slow_query_threshold
        is_error = not success

        display_sql = self._format_sql_for_log(query, is_error=is_error, is_slow=is_slow, log_mode=effective_mode)
        if not display_sql:
            return

        # Build log message
        status_icon = "✓" if success else "✗"
        message_parts = [status_icon]

        if execution_time is not None:
            message_parts.append(f"{execution_time:.3f}s")

        if affected_rows is not None:
            message_parts.append(f"{affected_rows} rows")

        if is_error:
            message_parts.append(f"[ERROR] {display_sql}")
        elif is_slow:
            message_parts.append(f"[SLOW] {display_sql}")
        else:
            message_parts.append(display_sql)

        self._emit(
            logging.ERROR if is_error else logging.INFO,
            self._format_message(" | ".join(message_parts)),
        )

    # ── Utility ──────────────────────────────────────────────────────

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

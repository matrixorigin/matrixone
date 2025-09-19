"""
MatrixOne SDK Exceptions
"""


class MatrixOneError(Exception):
    """Base exception for all MatrixOne SDK errors"""
    pass


class ConnectionError(MatrixOneError):
    """Raised when connection to MatrixOne fails"""
    pass


class QueryError(MatrixOneError):
    """Raised when SQL query execution fails"""
    pass


class ConfigurationError(MatrixOneError):
    """Raised when configuration is invalid"""
    pass


class SnapshotError(MatrixOneError):
    """Raised when snapshot operations fail"""
    pass


class CloneError(MatrixOneError):
    """Raised when clone operations fail"""
    pass


class MoCtlError(MatrixOneError):
    """Raised when mo_ctl operations fail"""
    pass


class RestoreError(MatrixOneError):
    """Raised when restore operations fail"""
    pass


class PitrError(MatrixOneError):
    """Raised when PITR operations fail"""
    pass


class PubSubError(MatrixOneError):
    """Raised when publish-subscribe operations fail"""
    pass


class AccountError(MatrixOneError):
    """Raised when account management operations fail"""
    pass

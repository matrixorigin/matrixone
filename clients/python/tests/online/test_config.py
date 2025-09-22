"""
Configuration for online tests
"""

import os
from typing import Optional, Tuple


class OnlineTestConfig:
    """Configuration for online tests"""

    def __init__(self):
        self.host = os.getenv('MATRIXONE_HOST', '127.0.0.1')
        self.port = int(os.getenv('MATRIXONE_PORT', '6001'))
        self.user = os.getenv('MATRIXONE_USER', 'root')
        self.password = os.getenv('MATRIXONE_PASSWORD', '111')
        self.database = os.getenv('MATRIXONE_DATABASE', 'test')

    def get_connection_params(self) -> Tuple[str, int, str, str, str]:
        """Get connection parameters as tuple"""
        return self.host, self.port, self.user, self.password, self.database

    def __str__(self):
        return f"OnlineTestConfig(host={self.host}, port={self.port}, user={self.user}, database={self.database})"


# Global configuration instance
online_config = OnlineTestConfig()

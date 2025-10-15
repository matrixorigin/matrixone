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
MatrixOne SDK Exceptions
"""


class MatrixOneError(Exception):
    """Base exception for all MatrixOne SDK errors"""


class ConnectionError(MatrixOneError):
    """Raised when connection to MatrixOne fails"""


class QueryError(MatrixOneError):
    """Raised when SQL query execution fails"""


class ConfigurationError(MatrixOneError):
    """Raised when configuration is invalid"""


class SnapshotError(MatrixOneError):
    """Raised when snapshot operations fail"""


class CloneError(MatrixOneError):
    """Raised when clone operations fail"""


class MoCtlError(MatrixOneError):
    """Raised when mo_ctl operations fail"""


class RestoreError(MatrixOneError):
    """Raised when restore operations fail"""


class PitrError(MatrixOneError):
    """Raised when PITR operations fail"""


class PubSubError(MatrixOneError):
    """Raised when publish-subscribe operations fail"""


class AccountError(MatrixOneError):
    """Raised when account management operations fail"""


class VersionError(MatrixOneError):
    """Raised when version compatibility check fails"""

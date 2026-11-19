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

from typing import Optional


class MatrixOneError(Exception):
    """Compatible exception with optional structured execution evidence.

    committed is True only for an acknowledged commit, False for a proven
    pre-commit failure, and None when unknown or not applicable. No field
    authorizes an automatic retry of a write.
    """

    def __init__(
        self,
        *args,
        orig=None,
        errno=None,
        sqlstate=None,
        phase=None,
        committed=None,
        rows=None,
        statement_acknowledged=None,
    ):
        super().__init__(*args)
        self._orig = orig
        self._errno = errno
        self._sqlstate = sqlstate
        self.phase = phase
        self.committed = committed
        self.rows = rows
        self.statement_acknowledged = statement_acknowledged

    @property
    def orig(self):
        if self._orig is not None:
            return self._orig
        if self.__cause__ is not None:
            return self.__cause__
        return None if self.__suppress_context__ else self.__context__

    @orig.setter
    def orig(self, value):
        self._orig = value

    def _source_detail(self, name):
        error = self.orig
        seen = {id(self)}
        while error is not None and id(error) not in seen:
            seen.add(id(error))
            value = (
                getattr(error, '_' + name, None) if isinstance(error, MatrixOneError) else getattr(error, name, None)
            )
            if value is not None:
                return value
            if name == 'errno' and error.args and isinstance(error.args[0], int):
                return error.args[0]
            source = getattr(error, 'orig', None) or error.__cause__
            error = source if source is not None else None if error.__suppress_context__ else error.__context__
        return None

    @property
    def errno(self):
        return self._errno if self._errno is not None else self._source_detail('errno')

    @errno.setter
    def errno(self, value):
        self._errno = value

    @property
    def sqlstate(self):
        return self._sqlstate if self._sqlstate is not None else self._source_detail('sqlstate')

    @sqlstate.setter
    def sqlstate(self, value):
        self._sqlstate = value


class ConnectionError(MatrixOneError):
    """Raised when connection to MatrixOne fails"""


class QueryError(MatrixOneError):
    """Raised when SQL query execution fails; orig and errno retain its source."""

    orig: Optional[Exception]
    errno: Optional[int]


class ConfigurationError(MatrixOneError):
    """Raised when configuration is invalid"""


class SnapshotError(MatrixOneError):
    """Raised when snapshot operations fail"""


class SnapshotNotFound(SnapshotError):
    """Snapshot lookup completed successfully and found no matching name."""


class CloneError(MatrixOneError):
    """Raised when clone operations fail"""


class BranchError(MatrixOneError):
    """Raised when branch operations fail"""


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


def annotate_execution_error(error, *, phase, rows=None, committed=None):
    """Attach known boundary evidence without changing the caught error class."""
    evidence = MatrixOneError(orig=error)
    if getattr(error, 'errno', None) is None:
        error.errno = evidence.errno
    if getattr(error, 'sqlstate', None) is None:
        error.sqlstate = evidence.sqlstate
    if getattr(error, 'phase', None) is None:
        error.phase = phase
    if getattr(error, 'rows', None) is None:
        error.rows = rows
    error.committed = committed
    return error

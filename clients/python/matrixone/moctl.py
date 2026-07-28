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
MatrixOne Control Operations (mo_ctl) Manager

Provides access to MatrixOne control operations that require sys tenant privileges.

IMPORTANT: All mo_ctl operations require 'sys' account privileges.
These operations cannot be executed with regular user accounts.

To use mo_ctl operations:
1. Connect with 'sys' account (e.g., 'sys#root' or account='sys')
2. Ensure proper administrative privileges
3. Operations may require specific tenant permissions

Example connection with sys account:
    client.connect(host, port, 'sys#root', password, database)
    # or
    client.connect(host, port, 'root', password, database, account='sys')
"""

import json
from typing import Any, Dict

from .exceptions import MatrixOneError


class MoCtlError(MatrixOneError):
    """Raised when mo_ctl operations fail"""


class MoCtlManager:
    """
    Manager for MatrixOne control operations (mo_ctl).

    This class provides access to MatrixOne's control operations through the
    mo_ctl SQL function. It allows programmatic execution of various
    MatrixOne administrative and maintenance operations.

    ⚠️  CRITICAL REQUIREMENT: All mo_ctl operations require 'sys' account privileges.
    These operations cannot be executed with regular user accounts.

    Key Features:

    - Programmatic access to mo_ctl commands
    - Table flush operations for data persistence
    - Checkpoint operations for data consistency
    - Integration with MatrixOne client operations
    - Error handling and result parsing

    Supported Operations:

    - Table flush operations (flush_table)
    - Incremental checkpoints (increment_checkpoint)
    - Global checkpoints (global_checkpoint)

    Usage Examples:

        # IMPORTANT: Connect with sys account first
        client = Client()
        client.connect(host, port, 'sys#root', password, database)

        # Initialize mo_ctl manager
        moctl = client.moctl

        # Flush a specific table
        result = moctl.flush_table("database_name", "table_name")

        # Force incremental checkpoint
        result = moctl.increment_checkpoint()

        # Force global checkpoint
        result = moctl.global_checkpoint()

    Requirements:
    - Must connect with 'sys' account (e.g., 'sys#root' or account='sys')
    - Requires appropriate administrative privileges
    - Operations may require specific tenant permissions

    Note: If you get permission errors, ensure you're connected with sys account:
        client.connect(host, port, 'sys#root', password, database)
        # or
        client.connect(host, port, 'root', password, database, account='sys')
    """

    def __init__(self, client):
        """
        Initialize MoCtlManager

        Args:

            client: MatrixOne client instance
        """
        self.client = client

    def _execute_moctl(self, method: str, target: str, params: str = "") -> Dict[str, Any]:
        """
        Execute mo_ctl command

        Args:

            method: Control method (e.g., 'dn')
            target: Target operation (e.g., 'flush', 'checkpoint')
            params: Parameters for the operation

        Returns:

            Parsed result from mo_ctl command

        Raises:

            MoCtlError: If the operation fails
        """
        try:
            # Build mo_ctl SQL command
            if params:
                sql = f"SELECT mo_ctl('{method}', '{target}', '{params}')"
            else:
                sql = f"SELECT mo_ctl('{method}', '{target}', '')"

            # Execute the command
            result = self.client.execute(sql)

            if not result.rows:
                raise MoCtlError(f"mo_ctl command returned no results: {sql}")

            # Parse the JSON result
            result_str = result.rows[0][0]
            parsed_result = json.loads(result_str)

            # Check for errors in the result
            if "result" in parsed_result and parsed_result["result"]:
                first_result = parsed_result["result"][0]
                if "returnStr" in first_result and first_result["returnStr"] != "OK":
                    raise MoCtlError(f"mo_ctl operation failed: {first_result['returnStr']}")

            return parsed_result

        except json.JSONDecodeError as e:
            raise MoCtlError(f"Failed to parse mo_ctl result: {e}")
        except Exception as e:
            raise MoCtlError(f"mo_ctl operation failed: {e}")

    def flush_table(self, database: str, table: str) -> Dict[str, Any]:
        """
        Force flush table.

        Force flush table `table` in database `database`. It returns after all blks in the table are flushed.

        ⚠️  Requires 'sys' account privileges.

        Args:

            database: Database name
            table: Table name

        Returns:

            Result of the flush operation

        Example:

            >>> client.moctl.flush_table('db1', 't')
            {'method': 'Flush', 'result': [{'returnStr': 'OK'}]}
        """
        table_ref = f"{database}.{table}"
        return self._execute_moctl("dn", "flush", table_ref)

    def increment_checkpoint(self) -> Dict[str, Any]:
        """
        Force incremental checkpoint.

        Flush all blks in DN, generate an Incremental Checkpoint and truncate WAL.

        ⚠️  Requires 'sys' account privileges.

        Returns:

            Result of the incremental checkpoint operation

        Example:

            >>> client.moctl.increment_checkpoint()
            {'method': 'Checkpoint', 'result': [{'returnStr': 'OK'}]}
        """
        return self._execute_moctl("dn", "checkpoint", "")

    def global_checkpoint(self) -> Dict[str, Any]:
        """
        Force global checkpoint.

        Generate a global checkpoint across all nodes.

        ⚠️  Requires 'sys' account privileges.

        Returns:

            Result of the global checkpoint operation

        Example:

            >>> client.moctl.global_checkpoint()
            {'method': 'GlobalCheckpoint', 'result': [{'returnStr': 'OK'}]}
        """
        return self._execute_moctl("dn", "globalcheckpoint", "")

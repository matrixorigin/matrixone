"""
MatrixOne Control Operations (mo_ctl) Manager
Provides access to MatrixOne control operations that require sys tenant privileges
"""

import json
from typing import Any, Dict

from .exceptions import MatrixOneError


class MoCtlError(MatrixOneError):
    """Raised when mo_ctl operations fail"""

    pass


class MoCtlManager:
    """Manager for MatrixOne control operations (mo_ctl)"""

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
        Force flush table

        Force flush table `table` in database `database`. It returns after all blks in the table are flushed.

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
        Force incremental checkpoint

        Flush all blks in DN, generate an Incremental Checkpoint and truncate WAL.

        Returns:
            Result of the incremental checkpoint operation

        Example:
            >>> client.moctl.increment_checkpoint()
            {'method': 'Checkpoint', 'result': [{'returnStr': 'OK'}]}
        """
        return self._execute_moctl("dn", "checkpoint", "")

    def global_checkpoint(self) -> Dict[str, Any]:
        """
        Force global checkpoint

        Generate a global checkpoint across all nodes.

        Returns:
            Result of the global checkpoint operation

        Example:
            >>> client.moctl.global_checkpoint()
            {'method': 'GlobalCheckpoint', 'result': [{'returnStr': 'OK'}]}
        """
        return self._execute_moctl("dn", "globalcheckpoint", "")

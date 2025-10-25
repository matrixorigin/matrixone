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
Data Export for MatrixOne

This module provides functionality to export query results to files or stages
using MatrixOne's SELECT ... INTO OUTFILE and SELECT ... INTO STAGE commands.
"""

from enum import Enum
from typing import Any, Optional


class ExportFormat(Enum):
    """Export file format options"""

    CSV = "csv"
    JSONLINE = "jsonline"


def _build_export_sql(
    query: str,
    output_path: str,
    format: ExportFormat = ExportFormat.CSV,
    fields_terminated_by: Optional[str] = None,
    fields_enclosed_by: Optional[str] = None,
    lines_terminated_by: Optional[str] = None,
    header: bool = False,
    max_file_size: Optional[int] = None,
    force_quote: Optional[list] = None,
    compression: Optional[str] = None,
) -> str:
    """
    Build export SQL statement (shared logic for sync and async).

    Args:
        query: SELECT query to execute (without INTO clause)
        output_path: Output file path or stage path (e.g., '/tmp/file.csv' or 'stage://stage_name/file.csv')
        format: Export file format
        fields_terminated_by: Field delimiter
        fields_enclosed_by: Field enclosure character
        lines_terminated_by: Line terminator
        header: Whether to include column headers
        max_file_size: Maximum size per file in bytes
        force_quote: List of column names/indices to always quote
        compression: Compression format

    Returns:
        Complete export SQL statement
    """
    # Determine quote style for output path based on fields_enclosed_by
    # If fields_enclosed_by contains a quote character, use double quotes for the path
    # to avoid quote conflicts in the SQL statement
    if fields_enclosed_by and fields_enclosed_by in ('"', "'"):
        path_quoted = f'"{output_path}"'
    else:
        path_quoted = f"'{output_path}'"

    # Build the export SQL
    sql_parts = [query.rstrip(';').strip(), "INTO OUTFILE", path_quoted]

    # Add format-specific options
    if format == ExportFormat.CSV:
        options = []
        if fields_terminated_by:
            options.append(f"fields terminated by '{fields_terminated_by}'")
        if fields_enclosed_by:
            # MatrixOne syntax: fields enclosed by '"' or fields enclosed by "'"
            # Note: MatrixOne doesn't support using both fields_terminated_by and
            # fields_enclosed_by simultaneously in current version
            options.append(f"fields enclosed by '{fields_enclosed_by}'")
        if lines_terminated_by:
            options.append(f"lines terminated by '{lines_terminated_by}'")

        if options:
            sql_parts.extend(options)

    # Note: Advanced options like HEADER, MAX_FILE_SIZE, FORCE_QUOTE, COMPRESSION are not yet
    # supported in current MatrixOne version for INTO OUTFILE. Commenting out for now.
    # TODO: Enable these options when MatrixOne adds support

    # # Add header option
    # if header:
    #     sql_parts.append("HEADER=TRUE")

    # # Add max file size
    # if max_file_size:
    #     sql_parts.append(f"MAX_FILE_SIZE {max_file_size}")

    # # Add force quote
    # if force_quote:
    #     if isinstance(force_quote, list):
    #         force_quote_str = ','.join(str(item) for item in force_quote)
    #         sql_parts.append(f"FORCE_QUOTE ({force_quote_str})")

    # # Add compression
    # if compression:
    #     sql_parts.append(f"COMPRESSION '{compression}'")

    return ' '.join(sql_parts)


class ExportManager:
    """
    Manager class for data export operations.

    This class provides methods to export query results to local files
    (INTO OUTFILE) or to external stages (INTO STAGE).
    """

    def __init__(self, client: Any):
        """
        Initialize ExportManager with a client instance.

        Args:
            client: MatrixOne client instance (Client or TransactionWrapper)
        """
        self.client = client

    def to_file(
        self,
        query: str,
        filepath: str,
        format: ExportFormat = ExportFormat.CSV,
        fields_terminated_by: Optional[str] = None,
        fields_enclosed_by: Optional[str] = None,
        lines_terminated_by: Optional[str] = None,
        header: bool = False,
        max_file_size: Optional[int] = None,
        force_quote: Optional[list] = None,
    ) -> Any:
        """
        Export query results to a local file using SELECT ... INTO OUTFILE.

        This method executes a SELECT query and writes the results to a file
        on the MatrixOne server's filesystem.

        Args:
            query: SELECT query to execute (without INTO clause)
            filepath: Absolute path on server filesystem where file will be created
            format: Export file format (CSV or JSONLINE)
            fields_terminated_by: Field delimiter (default: ',' for CSV)
            fields_enclosed_by: Field enclosure character (default: '"' for CSV)
            lines_terminated_by: Line terminator (default: '\\n')
            header: Whether to include column headers (default: False)
            max_file_size: Maximum size per file in bytes (for splitting large exports)
            force_quote: List of column names/indices to always quote

        Returns:
            ResultSet with export operation results

        Examples:
            >>> # Export to CSV file with headers
            >>> client.export.to_file(
            ...     query="SELECT * FROM orders WHERE order_date > '2025-01-01'",
            ...     filepath="/tmp/orders_export.csv",
            ...     format=ExportFormat.CSV,
            ...     header=True
            ... )

            >>> # Export to JSONLINE format
            >>> client.export.to_file(
            ...     query="SELECT * FROM users",
            ...     filepath="/tmp/users.jsonl",
            ...     format=ExportFormat.JSONLINE
            ... )

        Note:
            - The filepath must be on the MatrixOne server's filesystem
            - The MatrixOne server process must have write permissions
            - The file will be overwritten if it already exists
            - MatrixOne doesn't support fields_terminated_by and fields_enclosed_by simultaneously
        """
        # Build SQL using shared logic
        export_sql = _build_export_sql(
            query=query,
            output_path=filepath,
            format=format,
            fields_terminated_by=fields_terminated_by,
            fields_enclosed_by=fields_enclosed_by,
            lines_terminated_by=lines_terminated_by,
            header=header,
            max_file_size=max_file_size,
            force_quote=force_quote,
        )

        # Execute export
        return self.client.execute(export_sql)

    def to_stage(
        self,
        query: str,
        stage_name: str,
        filename: str,
        format: ExportFormat = ExportFormat.CSV,
        fields_terminated_by: Optional[str] = None,
        fields_enclosed_by: Optional[str] = None,
        lines_terminated_by: Optional[str] = None,
        header: bool = False,
        max_file_size: Optional[int] = None,
        force_quote: Optional[list] = None,
        compression: Optional[str] = None,
    ) -> Any:
        """
        Export query results to a stage using SELECT ... INTO STAGE.

        This method executes a SELECT query and writes the results to an
        external stage (S3, local filesystem, etc.).

        Args:
            query: SELECT query to execute (without INTO clause)
            stage_name: Name of the target stage
            filename: Filename to create in the stage
            format: Export file format (CSV or JSONLINE)
            fields_terminated_by: Field delimiter (default: ',' for CSV)
            fields_enclosed_by: Field enclosure character (default: '"' for CSV)
            lines_terminated_by: Line terminator (default: '\\n')
            header: Whether to include column headers (default: False)
            max_file_size: Maximum size per file in bytes (for splitting large exports)
            force_quote: List of column names/indices to always quote
            compression: Compression format (e.g., 'gzip', 'bzip2')

        Returns:
            ResultSet with export operation results

        Examples:
            >>> # Export to stage as CSV
            >>> client.export.to_stage(
            ...     query="SELECT * FROM orders",
            ...     stage_name="s3_stage",
            ...     filename="orders_backup.csv",
            ...     format=ExportFormat.CSV,
            ...     header=True,
            ...     compression="gzip"
            ... )

            >>> # Export aggregated results
            >>> client.export.to_stage(
            ...     query="SELECT product_id, SUM(quantity) as total FROM sales GROUP BY product_id",
            ...     stage_name="local_stage",
            ...     filename="sales_summary.jsonl",
            ...     format=ExportFormat.JSONLINE
            ... )

        Note:
            - The stage must exist before exporting
            - The stage must have appropriate write permissions
            - Use compression for large exports to save storage and transfer costs
            - MatrixOne doesn't support fields_terminated_by and fields_enclosed_by simultaneously
        """
        # MatrixOne uses special INTO OUTFILE syntax with stage:// protocol
        # Format: INTO OUTFILE 'stage://stage_name/filename.csv'
        stage_path = f"stage://{stage_name}/{filename}"

        # Build SQL using shared logic
        export_sql = _build_export_sql(
            query=query,
            output_path=stage_path,
            format=format,
            fields_terminated_by=fields_terminated_by,
            fields_enclosed_by=fields_enclosed_by,
            lines_terminated_by=lines_terminated_by,
            header=header,
            max_file_size=max_file_size,
            force_quote=force_quote,
            compression=compression,
        )

        # Execute export
        return self.client.execute(export_sql)


class AsyncExportManager:
    """
    Async manager class for data export operations.

    This class provides async methods to export query results to local files
    (INTO OUTFILE) or to external stages (INTO STAGE).
    """

    def __init__(self, client: Any):
        """
        Initialize AsyncExportManager with an async client instance.

        Args:
            client: MatrixOne async client instance (AsyncClient or AsyncTransactionWrapper)
        """
        self.client = client

    async def to_file(
        self,
        query: str,
        filepath: str,
        format: ExportFormat = ExportFormat.CSV,
        fields_terminated_by: Optional[str] = None,
        fields_enclosed_by: Optional[str] = None,
        lines_terminated_by: Optional[str] = None,
        header: bool = False,
        max_file_size: Optional[int] = None,
        force_quote: Optional[list] = None,
    ) -> Any:
        """Async version of to_file. See ExportManager.to_file for documentation."""
        # Build SQL using shared logic
        export_sql = _build_export_sql(
            query=query,
            output_path=filepath,
            format=format,
            fields_terminated_by=fields_terminated_by,
            fields_enclosed_by=fields_enclosed_by,
            lines_terminated_by=lines_terminated_by,
            header=header,
            max_file_size=max_file_size,
            force_quote=force_quote,
        )

        # Execute export asynchronously
        return await self.client.execute(export_sql)

    async def to_stage(
        self,
        query: str,
        stage_name: str,
        filename: str,
        format: ExportFormat = ExportFormat.CSV,
        fields_terminated_by: Optional[str] = None,
        fields_enclosed_by: Optional[str] = None,
        lines_terminated_by: Optional[str] = None,
        header: bool = False,
        max_file_size: Optional[int] = None,
        force_quote: Optional[list] = None,
        compression: Optional[str] = None,
    ) -> Any:
        """Async version of to_stage. See ExportManager.to_stage for documentation."""
        # MatrixOne uses special INTO OUTFILE syntax with stage:// protocol
        # Format: INTO OUTFILE 'stage://stage_name/filename.csv'
        stage_path = f"stage://{stage_name}/{filename}"

        # Build SQL using shared logic
        export_sql = _build_export_sql(
            query=query,
            output_path=stage_path,
            format=format,
            fields_terminated_by=fields_terminated_by,
            fields_enclosed_by=fields_enclosed_by,
            lines_terminated_by=lines_terminated_by,
            header=header,
            max_file_size=max_file_size,
            force_quote=force_quote,
            compression=compression,
        )

        # Execute export asynchronously
        return await self.client.execute(export_sql)


# Convenience type aliases
TransactionExportManager = ExportManager
AsyncTransactionExportManager = AsyncExportManager

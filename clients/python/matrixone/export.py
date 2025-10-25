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
from typing import Any, Optional, Union


def _query_to_sql(query: Union[str, Any]) -> str:
    """
    Convert various query types to SQL string.
    
    Args:
        query: Can be:
            - String: Returned as-is  
            - SQLAlchemy select() statement: Compiled to SQL
            - MatrixOneQuery object: Compiled via _build_sql()
            
    Returns:
        SQL string
        
    Examples::
    
        # String query
        sql = _query_to_sql("SELECT * FROM users")
        
        # SQLAlchemy select()
        from sqlalchemy import select
        stmt = select(User).where(User.age > 25)
        sql = _query_to_sql(stmt)
        
        # MatrixOneQuery
        query = client.query(User).filter(User.age > 25)
        sql = _query_to_sql(query)
    """
    # If it's already a string, return it
    if isinstance(query, str):
        return query.rstrip(';').strip()
    
    # If it has _build_sql method (MatrixOneQuery)
    if hasattr(query, '_build_sql'):
        sql, params = query._build_sql()
        # Replace parameters in the SQL
        if params:
            for param in params:
                if isinstance(param, str):
                    sql = sql.replace("?", f"'{param}'", 1)
                else:
                    sql = sql.replace("?", str(param), 1)
        return sql.rstrip(';').strip()
    
    # If it's a SQLAlchemy statement (has compile method)
    if hasattr(query, 'compile'):
        try:
            compiled = query.compile(compile_kwargs={"literal_binds": True})
            return str(compiled).rstrip(';').strip()
        except Exception as e:
            raise ValueError(f"Failed to compile SQLAlchemy statement: {e}")
    
    # Unsupported type
    raise TypeError(
        f"Unsupported query type: {type(query)}. "
        "Expected str, SQLAlchemy select(), or MatrixOneQuery object."
    )


class ExportFormat(Enum):
    """Export file format options"""

    CSV = "csv"
    JSONLINE = "jsonline"


def _build_export_sql(
    query: Union[str, Any],
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
        query: SELECT query to execute (without INTO clause). Can be:
            - String: Raw SQL
            - SQLAlchemy select() statement
            - MatrixOneQuery object
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
    # Convert query to SQL string
    query_sql = _query_to_sql(query)
    
    # Determine quote style for output path based on fields_enclosed_by
    # If fields_enclosed_by contains a quote character, use double quotes for the path
    # to avoid quote conflicts in the SQL statement
    if fields_enclosed_by and fields_enclosed_by in ('"', "'"):
        path_quoted = f'"{output_path}"'
    else:
        path_quoted = f"'{output_path}'"

    # Build the export SQL
    sql_parts = [query_sql, "INTO OUTFILE", path_quoted]

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
        query: Union[str, Any],
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
            query: SELECT query to execute. Can be:
                - String: Raw SQL (e.g., "SELECT * FROM users")
                - SQLAlchemy select(): stmt = select(User).where(User.age > 25)
                - MatrixOneQuery: query = client.query(User).filter(User.age > 25)
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
            >>> # Export with raw SQL
            >>> client.export.to_file(
            ...     query="SELECT * FROM orders WHERE order_date > '2025-01-01'",
            ...     filepath="/tmp/orders_export.csv",
            ...     format=ExportFormat.CSV,
            ...     header=True
            ... )

            >>> # Export with SQLAlchemy select()
            >>> from sqlalchemy import select
            >>> stmt = select(Order).where(Order.order_date > '2025-01-01')
            >>> client.export.to_file(
            ...     query=stmt,
            ...     filepath="/tmp/orders_export.csv",
            ...     format=ExportFormat.CSV,
            ...     header=True
            ... )
            
            >>> # Export with MatrixOne fulltext search
            >>> from sqlalchemy import select
            >>> from matrixone.sqlalchemy_ext import boolean_match
            >>> stmt = select(Article).where(
            ...     boolean_match("title", "content").must("python")
            ... )
            >>> client.export.to_file(query=stmt, filepath="/tmp/articles.csv")

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
        query: Union[str, Any],
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
            query: SELECT query to execute. Can be:
                - String: Raw SQL (e.g., "SELECT * FROM users")
                - SQLAlchemy select(): stmt = select(User).where(User.age > 25)
                - MatrixOneQuery: query = client.query(User).filter(User.age > 25)
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
            >>> # Export to stage with raw SQL
            >>> client.export.to_stage(
            ...     query="SELECT * FROM orders",
            ...     stage_name="s3_stage",
            ...     filename="orders_backup.csv",
            ...     format=ExportFormat.CSV,
            ...     header=True,
            ...     compression="gzip"
            ... )

            >>> # Export with SQLAlchemy select()
            >>> from sqlalchemy import select, func
            >>> stmt = select(Sale.product_id, func.sum(Sale.quantity).label('total')).group_by(Sale.product_id)
            >>> client.export.to_stage(
            ...     query=stmt,
            ...     stage_name="local_stage",
            ...     filename="sales_summary.jsonl",
            ...     format=ExportFormat.JSONLINE
            ... )
            
            >>> # Export with MatrixOne vector search
            >>> from sqlalchemy import select
            >>> query_vector = [0.1, 0.2, 0.3, ...]
            >>> stmt = (select(Document, Document.embedding.l2_distance(query_vector).label('distance'))
            ...        .order_by(Document.embedding.l2_distance(query_vector))
            ...        .limit(100))
            >>> client.export.to_stage(query=stmt, stage_name="results", filename="similar_docs.csv")

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
        query: Union[str, Any],
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
        query: Union[str, Any],
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

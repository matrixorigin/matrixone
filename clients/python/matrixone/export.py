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

This module provides pandas-style functionality to export query results to files
or stages using MatrixOne's SELECT ... INTO OUTFILE commands.
"""

import warnings
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
        f"Unsupported query type: {type(query)}. " "Expected str, SQLAlchemy select(), or MatrixOneQuery object."
    )


def _validate_csv_params(sep: str, quotechar: Optional[str], lineterminator: str) -> None:
    """
    Validate CSV export parameters.

    Args:
        sep: Field separator
        quotechar: Quote character
        lineterminator: Line terminator

    Raises:
        ValueError: If parameters are invalid or conflicting
    """
    if not sep:
        raise ValueError("sep cannot be empty")

    if len(sep) > 1:
        warnings.warn(
            f"sep is '{sep}' (length {len(sep)}). MatrixOne may only support single-character separators.", UserWarning
        )

    if quotechar and sep == quotechar:
        raise ValueError(f"sep and quotechar cannot be the same: '{sep}'")


def _build_csv_export_sql(
    query: Union[str, Any],
    output_path: str,
    sep: str = ',',
    quotechar: Optional[str] = None,
    lineterminator: str = '\n',
    header: bool = False,
) -> str:
    """
    Build CSV export SQL statement.

    Args:
        query: SELECT query to execute
        output_path: Output file path or stage path
        sep: Field separator (default: ',')
        quotechar: Quote character (default: None)
        lineterminator: Line terminator (default: '\n')
        header: Whether to include column headers (default: False)

    Returns:
        Complete export SQL statement
    """
    # Validate parameters
    _validate_csv_params(sep, quotechar, lineterminator)

    # Convert query to SQL string
    query_sql = _query_to_sql(query)

    # Build the export SQL
    sql_parts = [query_sql, "INTO OUTFILE", f"'{output_path}'"]

    # Add CSV options
    options = []

    # MatrixOne limitation: can't use both sep and quotechar simultaneously
    # If both are specified, use sep only
    if quotechar and sep != ',':
        # Use quotechar only if sep is not default (user wants quoting behavior)
        options.append(f"fields enclosed by '{quotechar}'")
    elif sep:
        # Use sep (default behavior)
        options.append(f"fields terminated by '{sep}'")

    # Add line terminator if not default
    if lineterminator != '\n':
        options.append(f"lines terminated by '{lineterminator}'")

    sql_parts.extend(options)

    # Note: HEADER option not yet supported in MatrixOne
    if header:
        warnings.warn(
            "header=True is not yet supported by MatrixOne's INTO OUTFILE. " "Headers will not be included in the output.",
            UserWarning,
        )

    return ' '.join(sql_parts)


def _build_jsonl_export_sql(
    query: Union[str, Any],
    output_path: str,
) -> str:
    """
    Build JSONL export SQL statement.

    Args:
        query: SELECT query to execute
        output_path: Output file path or stage path

    Returns:
        Complete export SQL statement
    """
    # Convert query to SQL string
    query_sql = _query_to_sql(query)

    # Build the export SQL with JSONL format
    # MatrixOne syntax: INTO OUTFILE 'path' FORMAT 'jsonline'
    return f"{query_sql} INTO OUTFILE '{output_path}'"


class ExportManager:
    """
    Manager class for data export operations (pandas-style interface).

    This class provides pandas-like methods to export query results to local
    files or external stages.

    Examples::

        # CSV export (pandas-style)
        client.export.to_csv('data.csv', query, sep='|', header=True)

        # JSONL export
        client.export.to_jsonl('data.jsonl', query)

        # Export to S3 stage
        client.export.to_csv('stage://s3_stage/data.csv', query)
    """

    def __init__(self, client: Any):
        """
        Initialize ExportManager with a client instance.

        Args:
            client: MatrixOne client instance (Client or Session)
        """
        self.client = client

    def to_csv(
        self,
        path: str,
        query: Union[str, Any],
        *,
        sep: str = ',',
        quotechar: Optional[str] = None,
        lineterminator: str = '\n',
        header: bool = False,
        **kwargs,
    ) -> Any:
        """
        Export query results to CSV file (pandas-style).

        This method exports query results to a CSV file on the MatrixOne server's
        filesystem or to an external stage.

        Args:
            path: Output file path. Can be:
                - Local path: '/tmp/data.csv'
                - Stage path: 'stage://stage_name/data.csv'
            query: SELECT query to execute. Can be:
                - Raw SQL string: "SELECT * FROM users"
                - SQLAlchemy select(): select(User).where(User.age > 25)
                - MatrixOneQuery: client.query(User).filter(User.age > 25)
            sep: Field separator/delimiter (default: ',')
            quotechar: Character to quote fields containing special characters
            lineterminator: Line terminator (default: '\\n')
            header: Include column headers (not yet supported by MatrixOne)
            **kwargs: Reserved for future options

        Returns:
            Query execution result

        Raises:
            ValueError: If parameters are invalid
            TypeError: If query type is unsupported

        Examples::

            # Basic CSV export with defaults
            client.export.to_csv('/tmp/users.csv', "SELECT * FROM users")

            # Custom separator (TSV)
            client.export.to_csv('/tmp/users.tsv', query, sep='\\t')

            # With SQLAlchemy
            from sqlalchemy import select
            stmt = select(User).where(User.age > 25)
            client.export.to_csv('/tmp/adults.csv', stmt, sep='|')

            # Export to stage
            client.export.to_csv('stage://s3_stage/backup.csv', query)

            # With MatrixOne query builder
            query = client.query(Order).filter(Order.status == 'completed')
            client.export.to_csv('/tmp/orders.csv', query, sep=',')

            # Using session
            with client.session() as session:
                query = session.query(User).filter(User.active == True)
                session.export.to_csv('/tmp/active_users.csv', query)

        Note:
            - For local paths, the MatrixOne server must have write permissions
            - For stage paths, use format: 'stage://stage_name/filename.csv'
            - MatrixOne doesn't support using sep and quotechar simultaneously
            - header=True is not yet supported by MatrixOne
        """
        # Build SQL
        export_sql = _build_csv_export_sql(
            query=query,
            output_path=path,
            sep=sep,
            quotechar=quotechar,
            lineterminator=lineterminator,
            header=header,
        )

        # Execute export
        return self.client.execute(export_sql)

    def to_jsonl(self, path: str, query: Union[str, Any], **kwargs) -> Any:
        """
        Export query results to JSONL file (JSON Lines format).

        Each line in the output file is a valid JSON object representing one row.

        Args:
            path: Output file path. Can be:
                - Local path: '/tmp/data.jsonl'
                - Stage path: 'stage://stage_name/data.jsonl'
            query: SELECT query to execute (str, SQLAlchemy select(), or MatrixOneQuery)
            **kwargs: Reserved for future options

        Returns:
            Query execution result

        Examples::

            # Basic JSONL export
            client.export.to_jsonl('/tmp/users.jsonl', "SELECT * FROM users")

            # With SQLAlchemy
            from sqlalchemy import select
            stmt = select(User).where(User.verified == True)
            client.export.to_jsonl('/tmp/verified.jsonl', stmt)

            # Export to stage
            client.export.to_jsonl('stage://s3_stage/data.jsonl', query)

            # Complex query with joins
            query = '''
                SELECT u.name, o.total
                FROM users u
                JOIN orders o ON u.id = o.user_id
            '''
            client.export.to_jsonl('/tmp/user_orders.jsonl', query)

        Note:
            - JSONL format: one JSON object per line, no array wrapper
            - Each line is a complete, valid JSON object
            - Suitable for streaming and big data processing
        """
        # Build SQL
        export_sql = _build_jsonl_export_sql(
            query=query,
            output_path=path,
        )

        # Execute export
        return self.client.execute(export_sql)

    def to_csv_stage(
        self,
        stage_name: str,
        filename: str,
        query: Union[str, Any],
        *,
        sep: str = ',',
        quotechar: Optional[str] = None,
        lineterminator: str = '\n',
        header: bool = False,
        **kwargs,
    ) -> Any:
        """
        Export query results to CSV file in an external stage.

        This is a convenience method for stage exports that doesn't require
        the 'stage://' protocol prefix.

        Args:
            stage_name: Name of the external stage
            filename: Name of the file to create in the stage
            query: SELECT query to execute (str, SQLAlchemy select(), or MatrixOneQuery)
            sep: Field separator/delimiter (default: ',')
            quotechar: Character to quote fields containing special characters
            lineterminator: Line terminator (default: '\\n')
            header: Include column headers (not yet supported by MatrixOne)
            **kwargs: Reserved for future options

        Returns:
            Query execution result

        Examples::

            # Export to S3 stage
            client.export.to_csv_stage('s3_stage', 'data.csv', "SELECT * FROM users")

            # With custom separator
            client.export.to_csv_stage('backup_stage', 'data.tsv', query, sep='\t')

            # With SQLAlchemy
            from sqlalchemy import select
            stmt = select(User).where(User.active == True)
            client.export.to_csv_stage('active_stage', 'active_users.csv', stmt)

        Note:
            - The stage must exist before calling this method
            - Use client.stage.create_s3() or client.stage.create_local() to create stages
            - MatrixOne doesn't support using sep and quotechar simultaneously
        """
        # Build stage path
        stage_path = f"stage://{stage_name}/{filename}"

        # Delegate to to_csv method
        return self.to_csv(
            path=stage_path,
            query=query,
            sep=sep,
            quotechar=quotechar,
            lineterminator=lineterminator,
            header=header,
            **kwargs,
        )

    def to_jsonl_stage(
        self,
        stage_name: str,
        filename: str,
        query: Union[str, Any],
        **kwargs,
    ) -> Any:
        """
        Export query results to JSONL file in an external stage.

        This is a convenience method for stage exports that doesn't require
        the 'stage://' protocol prefix.

        Args:
            stage_name: Name of the external stage
            filename: Name of the file to create in the stage
            query: SELECT query to execute (str, SQLAlchemy select(), or MatrixOneQuery)
            **kwargs: Reserved for future options

        Returns:
            Query execution result

        Examples::

            # Export to S3 stage
            client.export.to_jsonl_stage('s3_stage', 'data.jsonl', "SELECT * FROM users")

            # With SQLAlchemy
            from sqlalchemy import select
            stmt = select(User).where(User.verified == True)
            client.export.to_jsonl_stage('verified_stage', 'verified.jsonl', stmt)

        Note:
            - The stage must exist before calling this method
            - Use client.stage.create_s3() or client.stage.create_local() to create stages
        """
        # Build stage path
        stage_path = f"stage://{stage_name}/{filename}"

        # Delegate to to_jsonl method
        return self.to_jsonl(path=stage_path, query=query, **kwargs)


class AsyncExportManager:
    """
    Async manager class for data export operations (pandas-style interface).

    Provides async versions of all export methods from ExportManager.

    Examples::

        # Async CSV export
        await client.export.to_csv('data.csv', query, sep='|')

        # Async JSONL export
        await client.export.to_jsonl('data.jsonl', query)
    """

    def __init__(self, client: Any):
        """
        Initialize AsyncExportManager with an async client instance.

        Args:
            client: MatrixOne async client instance (AsyncClient or AsyncSession)
        """
        self.client = client

    async def to_csv(
        self,
        path: str,
        query: Union[str, Any],
        *,
        sep: str = ',',
        quotechar: Optional[str] = None,
        lineterminator: str = '\n',
        header: bool = False,
        **kwargs,
    ) -> Any:
        """
        Async export query results to CSV file.

        See ExportManager.to_csv() for detailed documentation.

        Examples::

            # Async CSV export
            await client.export.to_csv('/tmp/users.csv', "SELECT * FROM users")

            # With custom options
            await client.export.to_csv(
                '/tmp/data.tsv',
                query,
                sep='\\t',
                lineterminator='\\r\\n'
            )

            # Using async session
            async with client.session() as session:
                query = session.query(User).filter(User.active == True)
                await session.export.to_csv('/tmp/active.csv', query)
        """
        # Build SQL
        export_sql = _build_csv_export_sql(
            query=query,
            output_path=path,
            sep=sep,
            quotechar=quotechar,
            lineterminator=lineterminator,
            header=header,
        )

        # Execute export asynchronously
        return await self.client.execute(export_sql)

    async def to_jsonl(self, path: str, query: Union[str, Any], **kwargs) -> Any:
        """
        Async export query results to JSONL file.

        See ExportManager.to_jsonl() for detailed documentation.

        Examples::

            # Async JSONL export
            await client.export.to_jsonl('/tmp/data.jsonl', query)

            # Export to stage
            await client.export.to_jsonl('stage://s3/backup.jsonl', query)
        """
        # Build SQL
        export_sql = _build_jsonl_export_sql(
            query=query,
            output_path=path,
        )

        # Execute export asynchronously
        return await self.client.execute(export_sql)

    async def to_csv_stage(
        self,
        stage_name: str,
        filename: str,
        query: Union[str, Any],
        *,
        sep: str = ',',
        quotechar: Optional[str] = None,
        lineterminator: str = '\n',
        header: bool = False,
        **kwargs,
    ) -> Any:
        """
        Async export query results to CSV file in an external stage.

        This is a convenience method for stage exports that doesn't require
        the 'stage://' protocol prefix.

        Args:
            stage_name: Name of the external stage
            filename: Name of the file to create in the stage
            query: SELECT query to execute (str, SQLAlchemy select(), or MatrixOneQuery)
            sep: Field separator/delimiter (default: ',')
            quotechar: Character to quote fields containing special characters
            lineterminator: Line terminator (default: '\\n')
            header: Include column headers (not yet supported by MatrixOne)
            **kwargs: Reserved for future options

        Returns:
            Query execution result

        Examples::

            # Async export to S3 stage
            await client.export.to_csv_stage('s3_stage', 'data.csv', "SELECT * FROM users")

            # With custom separator
            await client.export.to_csv_stage('backup_stage', 'data.tsv', query, sep='\t')

            # With SQLAlchemy
            from sqlalchemy import select
            stmt = select(User).where(User.active == True)
            await client.export.to_csv_stage('active_stage', 'active_users.csv', stmt)
        """
        # Build stage path
        stage_path = f"stage://{stage_name}/{filename}"

        # Delegate to to_csv method
        return await self.to_csv(
            path=stage_path,
            query=query,
            sep=sep,
            quotechar=quotechar,
            lineterminator=lineterminator,
            header=header,
            **kwargs,
        )

    async def to_jsonl_stage(
        self,
        stage_name: str,
        filename: str,
        query: Union[str, Any],
        **kwargs,
    ) -> Any:
        """
        Async export query results to JSONL file in an external stage.

        This is a convenience method for stage exports that doesn't require
        the 'stage://' protocol prefix.

        Args:
            stage_name: Name of the external stage
            filename: Name of the file to create in the stage
            query: SELECT query to execute (str, SQLAlchemy select(), or MatrixOneQuery)
            **kwargs: Reserved for future options

        Returns:
            Query execution result

        Examples::

            # Async export to S3 stage
            await client.export.to_jsonl_stage('s3_stage', 'data.jsonl', "SELECT * FROM users")

            # With SQLAlchemy
            from sqlalchemy import select
            stmt = select(User).where(User.verified == True)
            await client.export.to_jsonl_stage('verified_stage', 'verified.jsonl', stmt)
        """
        # Build stage path
        stage_path = f"stage://{stage_name}/{filename}"

        # Delegate to to_jsonl method
        return await self.to_jsonl(path=stage_path, query=query, **kwargs)

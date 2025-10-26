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
MatrixOne Load Data Manager - Provides high-level LOAD DATA operations
"""

from typing import Optional, List, Dict, Union
from enum import Enum


class LoadDataFormat(str, Enum):
    """Supported file formats for LOAD DATA"""

    CSV = 'csv'
    JSONLINE = 'jsonline'
    PARQUET = 'parquet'


class CompressionFormat(str, Enum):
    """Supported compression formats"""

    NONE = 'none'
    GZIP = 'gzip'
    GZ = 'gz'
    BZIP2 = 'bzip2'
    BZ2 = 'bz2'
    LZ4 = 'lz4'
    TAR_GZ = 'tar.gz'
    TAR_BZ2 = 'tar.bz2'


class JsonDataStructure(str, Enum):
    """JSONLINE data structure types"""

    OBJECT = 'object'
    ARRAY = 'array'


class BaseLoadDataManager:
    """
    Base class for Load Data management containing shared SQL building logic.

    This class contains all SQL generation methods that are common between
    synchronous and asynchronous implementations.
    """

    def __init__(self, client):
        """Initialize base load data manager"""
        self.client = client

    def _build_load_data_sql(
        self,
        file_path: str,
        table_name: str,
        fields_terminated_by: str = ",",
        fields_enclosed_by: Optional[str] = None,
        fields_optionally_enclosed: bool = False,
        fields_escaped_by: Optional[str] = None,
        lines_terminated_by: Optional[str] = None,
        lines_starting_by: Optional[str] = None,
        ignore_lines: int = 0,
        character_set: Optional[str] = None,
        parallel: bool = False,
        columns: Optional[List[str]] = None,
        local: bool = False,
        format: Optional[str] = None,
        jsondata: Optional[str] = None,
        compression: Optional[str] = None,
        set_clause: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Build the LOAD DATA SQL statement.

        Args:
            See LoadDataManager.from_file() for parameter descriptions
            local (bool): Whether to use LOCAL keyword

        Returns:
            str: Complete LOAD DATA SQL statement
        """
        # Check if we need to use brace syntax for JSONLINE or Parquet
        use_brace_syntax = format in ('jsonline', 'parquet')

        if use_brace_syntax:
            return self._build_load_data_sql_with_braces(
                file_path=file_path,
                table_name=table_name,
                format=format,
                jsondata=jsondata,
                compression=compression,
                local=local,
            )
        else:
            return self._build_load_data_sql_standard(
                file_path=file_path,
                table_name=table_name,
                fields_terminated_by=fields_terminated_by,
                fields_enclosed_by=fields_enclosed_by,
                fields_optionally_enclosed=fields_optionally_enclosed,
                fields_escaped_by=fields_escaped_by,
                lines_terminated_by=lines_terminated_by,
                lines_starting_by=lines_starting_by,
                ignore_lines=ignore_lines,
                character_set=character_set,
                parallel=parallel,
                columns=columns,
                local=local,
                set_clause=set_clause,
            )

    def _build_load_data_sql_standard(
        self,
        file_path: str,
        table_name: str,
        fields_terminated_by: str = ",",
        fields_enclosed_by: Optional[str] = None,
        fields_optionally_enclosed: bool = False,
        fields_escaped_by: Optional[str] = None,
        lines_terminated_by: Optional[str] = None,
        lines_starting_by: Optional[str] = None,
        ignore_lines: int = 0,
        character_set: Optional[str] = None,
        parallel: bool = False,
        columns: Optional[List[str]] = None,
        local: bool = False,
        set_clause: Optional[Dict[str, str]] = None,
    ) -> str:
        """Build standard LOAD DATA SQL statement (CSV format)."""
        sql_parts = []

        # LOAD DATA [LOCAL] INFILE
        if local:
            sql_parts.append("LOAD DATA LOCAL INFILE")
        else:
            sql_parts.append("LOAD DATA INFILE")

        # File path
        sql_parts.append(f"'{file_path}'")

        # INTO TABLE
        sql_parts.append(f"INTO TABLE {table_name}")

        # CHARACTER SET
        if character_set:
            sql_parts.append(f"CHARACTER SET {character_set}")

        # FIELDS options
        fields_options = []
        if fields_terminated_by:
            fields_options.append(f"TERMINATED BY '{fields_terminated_by}'")
        if fields_enclosed_by:
            if fields_optionally_enclosed:
                fields_options.append(f"OPTIONALLY ENCLOSED BY '{fields_enclosed_by}'")
            else:
                fields_options.append(f"ENCLOSED BY '{fields_enclosed_by}'")
        if fields_escaped_by:
            fields_options.append(f"ESCAPED BY '{fields_escaped_by}'")

        if fields_options:
            sql_parts.append("FIELDS " + " ".join(fields_options))

        # LINES options
        lines_options = []
        if lines_starting_by:
            lines_options.append(f"STARTING BY '{lines_starting_by}'")
        if lines_terminated_by:
            lines_options.append(f"TERMINATED BY '{lines_terminated_by}'")

        if lines_options:
            sql_parts.append("LINES " + " ".join(lines_options))

        # IGNORE LINES
        if ignore_lines > 0:
            sql_parts.append(f"IGNORE {ignore_lines} LINES")

        # Column list
        if columns:
            column_list = ", ".join(columns)
            sql_parts.append(f"({column_list})")

        # SET clause
        if set_clause:
            set_parts = [f"{col}={expr}" for col, expr in set_clause.items()]
            sql_parts.append("SET " + ", ".join(set_parts))

        # PARALLEL option
        if parallel:
            sql_parts.append("PARALLEL 'true'")

        return " ".join(sql_parts)

    def _build_load_data_sql_with_braces(
        self,
        file_path: str,
        table_name: str,
        format: str,
        jsondata: Optional[str] = None,
        compression: Optional[str] = None,
        local: bool = False,
    ) -> str:
        """Build LOAD DATA SQL with brace syntax for JSONLINE/Parquet."""
        sql_parts = []

        # LOAD DATA [LOCAL] INFILE
        if local:
            sql_parts.append("LOAD DATA LOCAL INFILE")
        else:
            sql_parts.append("LOAD DATA INFILE")

        # Build brace parameters
        brace_params = [f"'filepath'='{file_path}'"]
        brace_params.append(f"'format'='{format}'")

        if jsondata:
            brace_params.append(f"'jsondata'='{jsondata}'")

        if compression:
            brace_params.append(f"'compression'='{compression}'")

        # Combine into brace syntax
        sql_parts.append("{" + ", ".join(brace_params) + "}")

        # INTO TABLE
        sql_parts.append(f"INTO TABLE {table_name}")

        return " ".join(sql_parts)

    def _build_load_data_inline_sql(
        self,
        data: str,
        table_name: str,
        format: str = 'csv',
        delimiter: str = ",",
        enclosed_by: Optional[str] = None,
        jsontype: Optional[str] = None,
    ) -> str:
        """Build LOAD DATA INLINE SQL statement."""
        sql_parts = []

        # LOAD DATA INLINE
        sql_parts.append("LOAD DATA INLINE")

        # FORMAT and DATA (comma-separated)
        inline_params = []
        inline_params.append(f"FORMAT='{format}'")

        # DATA - escape single quotes in data
        escaped_data = data.replace("'", "''")
        inline_params.append(f"DATA='{escaped_data}'")

        # JSONTYPE (for JSONLINE format)
        if jsontype and format == 'jsonline':
            inline_params.append(f"JSONTYPE='{jsontype}'")

        # Combine FORMAT, DATA, JSONTYPE with commas
        sql_parts.append(", ".join(inline_params))

        # INTO TABLE
        sql_parts.append(f"INTO TABLE {table_name}")

        # FIELDS options (for CSV)
        if format == 'csv':
            fields_options = []
            if delimiter:
                fields_options.append(f"TERMINATED BY '{delimiter}'")
            if enclosed_by:
                fields_options.append(f"ENCLOSED BY '{enclosed_by}'")

            if fields_options:
                sql_parts.append("FIELDS " + " ".join(fields_options))

        return " ".join(sql_parts)


class LoadDataManager(BaseLoadDataManager):
    """
    Manager for LOAD DATA operations in MatrixOne.

    This class provides a flexible and user-friendly interface for loading data
    from files into MatrixOne tables. It supports various data formats (CSV, JSON,
    Parquet), compression formats, and advanced options like parallel loading.

    Key Features:

    - Load data from local files or stage files
    - Support for various delimiters and enclosures
    - Character set conversion
    - Parallel loading for large files
    - Compression support (gzip, tar.gz, tar.bz2)
    - Column mapping and transformations
    - Transaction support

    This class is typically accessed through the Client's `load_data` property:

    Examples::

        # Basic CSV loading
        client.load_data.from_file('/path/to/data.csv', 'users')

        # Load with custom delimiter
        client.load_data.from_file('/path/to/data.txt', 'users',
                                     fields_terminated_by='|')

        # Load with header row (skip first line)
        client.load_data.from_file('/path/to/data.csv', 'users',
                                     ignore_lines=1)

        # Load pipe-delimited with quoted fields
        client.load_data.from_file('/path/to/data.txt', 'orders',
                                     fields_terminated_by='|',
                                     fields_enclosed_by='"')
    """

    def __init__(self, client, executor=None):
        """
        Initialize LoadDataManager.

        Args:
            client: Client object that provides execute() method
            executor: Optional executor (e.g., session) for executing SQL.
                     If None, uses client.execute
        """
        super().__init__(client)
        self.executor = executor

    def _get_executor(self):
        """Get the executor for SQL execution (session or client)"""
        return self.executor if self.executor else self.client

    def from_csv(
        self,
        file_path: str,
        table_name_or_model,
        delimiter: str = ",",
        enclosed_by: Optional[str] = None,
        optionally_enclosed: bool = False,
        escaped_by: Optional[str] = None,
        ignore_lines: int = 0,
        columns: Optional[List[str]] = None,
        character_set: Optional[str] = None,
        parallel: bool = False,
        compression: Optional[Union[str, CompressionFormat]] = None,
        set_clause: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        """
        Load CSV (Comma-Separated Values) data from a file into a table.

        This is the most commonly used data loading interface, providing a simplified
        API for CSV files with sensible defaults and the most frequently used options.

        CSV Format:
        -----------
        CSV is a plain text format where:
        - Each line represents one row
        - Fields are separated by a delimiter (default: comma)
        - Fields may be enclosed in quotes to handle special characters

        Common Use Cases:
        -----------------
        - **Data imports**: Import data from Excel, databases, or other sources
        - **Bulk loading**: Load large datasets efficiently
        - **ETL pipelines**: Extract-Transform-Load workflows
        - **Data migration**: Move data between systems

        Simplified interface for CSV files with commonly used options.

        Args:
            file_path (str): Path to the CSV file
            table_name_or_model: Table name (str) or SQLAlchemy model class
            delimiter (str): Field delimiter. Default: ','
            enclosed_by (str, optional): Character enclosing field values (e.g. '"')
            optionally_enclosed (bool): If True, use OPTIONALLY ENCLOSED BY. Default: False
            escaped_by (str, optional): Escape character
            ignore_lines (int): Number of header lines to skip. Default: 0
            columns (list, optional): List of column names to load
            character_set (str, optional): Character set (e.g. 'utf8')
            parallel (bool): Enable parallel loading. Default: False
            compression (str or CompressionFormat, optional): Compression format
            set_clause (dict, optional): Column transformations

        Returns:
            ResultSet: Load results with affected_rows

        Examples::

            # Basic CSV
            >>> client.load_data.from_csv('data.csv', User)

            # CSV with header
            >>> client.load_data.from_csv('data.csv', User, ignore_lines=1)

            # Custom delimiter with quotes
            >>> client.load_data.from_csv('data.txt', User,
            ...     delimiter='|', enclosed_by='"')

            # Optionally enclosed fields (some fields quoted, some not)
            >>> client.load_data.from_csv('data.csv', User,
            ...     enclosed_by='"', optionally_enclosed=True)
        """
        return self.from_file(
            file_path=file_path,
            table_name_or_model=table_name_or_model,
            fields_terminated_by=delimiter,
            fields_enclosed_by=enclosed_by,
            fields_optionally_enclosed=optionally_enclosed,
            fields_escaped_by=escaped_by,
            ignore_lines=ignore_lines,
            columns=columns,
            character_set=character_set,
            parallel=parallel,
            compression=compression,
            set_clause=set_clause,
            **kwargs,
        )

    def from_tsv(
        self, file_path: str, table_name_or_model, ignore_lines: int = 0, columns: Optional[List[str]] = None, **kwargs
    ):
        """
        Load TSV (Tab-Separated Values) data from a file.

        TSV Format:
        -----------
        TSV is similar to CSV but uses tab characters ('\\t') as delimiters.
        It's commonly used for:
        - **Log files**: Application logs, server logs
        - **Data exports**: Database exports, system reports
        - **Scientific data**: Research data, measurements
        - **Text processing**: NLP datasets, corpus files

        Advantages over CSV:
        - No need for quoting (tabs rarely appear in data)
        - Better readability in text editors with tab stops
        - Standard format for many Unix tools (cut, awk, etc.)

        Args:
            file_path (str): Path to the TSV file
            table_name_or_model: Table name (str) or SQLAlchemy model class
            ignore_lines (int): Number of header lines to skip. Default: 0
            columns (list, optional): List of column names to load into
            **kwargs: Additional options

        Returns:
            ResultSet: Load results with affected_rows

        Examples::

            >>> # Basic TSV loading
            >>> client.load_data.from_tsv('logs.tsv', Log)

            >>> # TSV with header row
            >>> client.load_data.from_tsv('data.tsv', User, ignore_lines=1)

            >>> # Load into specific columns
            >>> client.load_data.from_tsv('data.tsv', User,
            ...     columns=['id', 'name', 'email'])
        """
        return self.from_csv(
            file_path=file_path,
            table_name_or_model=table_name_or_model,
            delimiter='\\t',
            ignore_lines=ignore_lines,
            columns=columns,
            **kwargs,
        )

    def from_jsonline(
        self,
        file_path: str,
        table_name_or_model,
        structure: Union[str, JsonDataStructure] = JsonDataStructure.OBJECT,
        compression: Optional[Union[str, CompressionFormat]] = None,
        **kwargs,
    ):
        """
        Load JSONLINE (JSON Lines, also called NDJSON) data from a file.

        JSONLINE Format:
        ----------------
        JSONLINE is a format where each line is a valid JSON value (usually an object).
        Unlike regular JSON arrays, JSONLINE is:
        - **Streamable**: Process one line at a time, no need to load entire file
        - **Appendable**: Add new records by appending lines
        - **Fault-tolerant**: One corrupted line doesn't break the entire file
        - **Efficient**: Suitable for very large datasets

        Common Use Cases:
        -----------------
        - **Log aggregation**: Application logs, access logs, event streams
        - **API responses**: RESTful API data exports
        - **Big data**: Large datasets from Hadoop, Spark, etc.
        - **Event sourcing**: Event streams, message queues
        - **Machine learning**: Training datasets, feature stores

        Two Structure Types:
        --------------------
        1. **OBJECT** (default): {"id":1,"name":"Alice","age":30}
           - Most common and readable
           - Field names included in each line

        2. **ARRAY**: [1,"Alice",30]
           - More compact (no field names)
           - Column order must match table

        Args:
            file_path (str): Path to the JSONLINE file (.jsonl, .ndjson)
            table_name_or_model: Table name (str) or SQLAlchemy model class
            structure (str or JsonDataStructure): JSON structure format
                - JsonDataStructure.OBJECT or 'object': JSON objects (default)
                - JsonDataStructure.ARRAY or 'array': JSON arrays
            compression (str or CompressionFormat, optional): Compression format
            **kwargs: Additional options

        Returns:
            ResultSet: Load results with affected_rows

        Examples::

            # Object structure (most common)
            >>> client.load_data.from_jsonline('events.jsonl', Event)

            # Array structure
            >>> client.load_data.from_jsonline('data.jsonl', User,
            ...     structure=JsonDataStructure.ARRAY)

            # Compressed file
            >>> client.load_data.from_jsonline('events.jsonl.gz', Event,
            ...     compression=CompressionFormat.GZIP)
        """
        # Convert enum to string if needed
        structure_str = structure.value if isinstance(structure, JsonDataStructure) else structure
        compression_str = compression.value if isinstance(compression, CompressionFormat) else compression

        return self.from_file(
            file_path=file_path,
            table_name_or_model=table_name_or_model,
            format=LoadDataFormat.JSONLINE.value,
            jsondata=structure_str,
            compression=compression_str,
            **kwargs,
        )

    def from_parquet(self, file_path: str, table_name_or_model, **kwargs):
        """
        Load Parquet data from a file.

        Args:
            file_path (str): Path to the Parquet file
            table_name_or_model: Table name (str) or SQLAlchemy model class

        Returns:
            ResultSet: Load results with affected_rows

        Important - MatrixOne Parquet Requirements:
            MatrixOne currently has specific requirements for Parquet files.

            Currently NOT Supported (as of this version):
                ❌ Nullable columns (INT64(optional), STRING(optional), etc.)
                ❌ Dictionary encoding (indexed pages)
                ❌ Parquet 2.0 features
                ❌ Column statistics metadata
                ❌ Compressed Parquet files (GZIP, SNAPPY, etc.)

            If you encounter errors like:
                - "indexed INT64 page is not yet implemented"
                - "load INT64(optional) to BIGINT NULL is not yet implemented"

            You must generate Parquet files with the following PyArrow settings:

            Required Settings (Mandatory):
                1. Schema columns must be non-nullable: nullable=False
                2. Disable dictionary encoding: use_dictionary=False
                3. Disable compression: compression='none'
                4. Disable statistics: write_statistics=False
                5. Use Parquet 1.0 format: data_page_version='1.0'

            Note: These limitations are temporary. Support for nullable columns, dictionary
            encoding, compression, and Parquet 2.0 features is planned for future releases.
            Stay tuned for updates!

            Example::

                import pyarrow as pa
                import pyarrow.parquet as pq

                # Define non-nullable schema (REQUIRED)
                schema = pa.schema([
                    pa.field('id', pa.int64(), nullable=False),
                    pa.field('name', pa.string(), nullable=False)
                ])

                # Create table
                table = pa.table({
                    'id': pa.array([1, 2, 3], type=pa.int64()),
                    'name': pa.array(['Alice', 'Bob', 'Charlie'], type=pa.string())
                }, schema=schema)

                # Write with MatrixOne-compatible options (REQUIRED)
                pq.write_table(
                    table, 'data.parq',
                    compression='none',          # REQUIRED
                    use_dictionary=False,        # REQUIRED
                    write_statistics=False,      # REQUIRED
                    data_page_version='1.0'      # REQUIRED
                )

                # Now load the file
                client.load_data.from_parquet('data.parq', User)

        Examples::

            >>> client.load_data.from_parquet('data.parq', 'users')
            >>> client.load_data.from_parquet('data.parquet', 'sales')
        """
        return self.from_file(
            file_path=file_path, table_name_or_model=table_name_or_model, format=LoadDataFormat.PARQUET.value, **kwargs
        )

    def from_inline(
        self,
        data: str,
        table_name_or_model,
        format: Union[str, LoadDataFormat] = LoadDataFormat.CSV,
        delimiter: str = ",",
        enclosed_by: Optional[str] = None,
        jsontype: Optional[Union[str, JsonDataStructure]] = None,
        **kwargs,
    ):
        """
        Load data from an inline string directly into a table.

        This method uses LOAD DATA INLINE to load data from a string without
        needing to create a temporary file.

        Args:
            data (str): The data string to load
            table_name_or_model: Table name (str) or SQLAlchemy model class
            format (str or LoadDataFormat): Data format. Default: LoadDataFormat.CSV
            delimiter (str): Field delimiter (for CSV). Default: ','
            enclosed_by (str, optional): Character enclosing fields (for CSV)
            jsontype (str or JsonDataStructure, optional): JSON structure (for JSONLINE)

        Returns:
            ResultSet: Load results with affected_rows

        Examples::

            # Basic CSV inline
            >>> result = client.load_data.from_inline(
            ...     "1,Alice\\n2,Bob\\n",
            ...     User
            ... )

            # CSV with custom delimiter
            >>> result = client.load_data.from_inline(
            ...     "1|Alice\\n2|Bob\\n",
            ...     User,
            ...     delimiter='|'
            ... )

            # JSONLINE inline
            >>> result = client.load_data.from_inline(
            ...     '{"id":1,"name":"Alice"}',
            ...     User,
            ...     format=LoadDataFormat.JSONLINE,
            ...     jsontype=JsonDataStructure.OBJECT
            ... )
        """
        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            table_name = table_name_or_model.__tablename__
        else:
            table_name = table_name_or_model

        # Validate parameters
        if not data or not isinstance(data, str):
            raise ValueError("data must be a non-empty string")

        if not table_name or not isinstance(table_name, str):
            raise ValueError("table_name must be a non-empty string")

        # Convert enums to strings
        format_str = format.value if isinstance(format, LoadDataFormat) else format
        jsontype_str = jsontype.value if isinstance(jsontype, JsonDataStructure) else jsontype

        # Build SQL
        sql = self._build_load_data_inline_sql(
            data=data,
            table_name=table_name,
            format=format_str,
            delimiter=delimiter,
            enclosed_by=enclosed_by,
            jsontype=jsontype_str,
        )

        return self._get_executor().execute(sql)

    def from_csv_inline(
        self, data: str, table_name_or_model, delimiter: str = ",", enclosed_by: Optional[str] = None, **kwargs
    ):
        """
        Load CSV data from an inline string.

        Simplified interface for inline CSV data.

        Args:
            data (str): CSV data string
            table_name_or_model: Table name (str) or SQLAlchemy model class
            delimiter (str): Field delimiter. Default: ','
            enclosed_by (str, optional): Character enclosing fields

        Returns:
            ResultSet: Load results with affected_rows

        Examples::

            >>> result = client.load_data.from_csv_inline(
            ...     "1,Alice,alice@example.com\\n2,Bob,bob@example.com\\n",
            ...     User
            ... )
        """
        return self.from_inline(
            data=data,
            table_name_or_model=table_name_or_model,
            format=LoadDataFormat.CSV,
            delimiter=delimiter,
            enclosed_by=enclosed_by,
            **kwargs,
        )

    def from_jsonline_inline(
        self, data: str, table_name_or_model, structure: Union[str, JsonDataStructure] = JsonDataStructure.OBJECT, **kwargs
    ):
        """
        Load JSONLINE data from an inline string.

        Args:
            data (str): JSONLINE data string
            table_name_or_model: Table name (str) or SQLAlchemy model class
            structure (str or JsonDataStructure): JSON structure. Default: JsonDataStructure.OBJECT

        Returns:
            ResultSet: Load results with affected_rows

        Examples::

            >>> result = client.load_data.from_jsonline_inline(
            ...     '{"id":1,"name":"Alice"}',
            ...     User
            ... )
        """
        return self.from_inline(
            data=data, table_name_or_model=table_name_or_model, format=LoadDataFormat.JSONLINE, jsontype=structure, **kwargs
        )

    def from_stage(
        self,
        stage_name: str,
        filepath: str,
        table_name_or_model,
        format: Union[str, LoadDataFormat] = LoadDataFormat.CSV,
        delimiter: str = ",",
        enclosed_by: Optional[str] = None,
        jsondata: Optional[Union[str, JsonDataStructure]] = None,
        compression: Optional[Union[str, CompressionFormat]] = None,
        **kwargs,
    ):
        """
        Load data from a stage file.

        Stage is a named external location that references external cloud storage (S3, OSS) or
        local file system paths. This method provides a convenient way to load data from stages.

        Args:
            stage_name (str): Stage name (without 'stage://' prefix)
            filepath (str): File path within the stage (e.g., 'data.csv', 'folder/file.parq')
            table_name_or_model: Table name (str) or SQLAlchemy model class
            format (str or LoadDataFormat): Data format. Default: LoadDataFormat.CSV
            delimiter (str): Field delimiter for CSV/TSV. Default: ','
            enclosed_by (str, optional): Field enclosure character
            jsondata (str or JsonDataStructure, optional): JSON structure for JSONLINE
            compression (str or CompressionFormat, optional): Compression format
            **kwargs: Additional options (lines_starting_by, lines_terminated_by, etc.)

        Returns:
            ResultSet: Load results with affected_rows

        Example:
            >>> # Load CSV from stage
            >>> result = client.load_data.from_stage(
            ...     'mystage',
            ...     'users.csv',
            ...     User,
            ...     format=LoadDataFormat.CSV
            ... )

            >>> # Load Parquet from stage
            >>> result = client.load_data.from_stage(
            ...     'parqstage',
            ...     'data/users.parq',
            ...     User,
            ...     format=LoadDataFormat.PARQUET
            ... )

            >>> # Load compressed CSV from stage
            >>> result = client.load_data.from_stage(
            ...     'mystage',
            ...     'data.csv.gz',
            ...     User,
            ...     compression=CompressionFormat.GZIP
            ... )
        """
        # Construct stage URL
        stage_url = f"stage://{stage_name}/{filepath}"

        # Convert delimiter to fields_terminated_by for from_file
        if delimiter and 'fields_terminated_by' not in kwargs:
            kwargs['fields_terminated_by'] = delimiter
        if enclosed_by and 'fields_enclosed_by' not in kwargs:
            kwargs['fields_enclosed_by'] = enclosed_by

        # Use from_file with the stage URL
        return self.from_file(
            stage_url, table_name_or_model, format=format, jsondata=jsondata, compression=compression, **kwargs
        )

    def from_stage_csv(
        self,
        stage_name: str,
        filepath: str,
        table_name_or_model,
        delimiter: str = ",",
        enclosed_by: Optional[str] = None,
        compression: Optional[Union[str, CompressionFormat]] = None,
        **kwargs,
    ):
        """
        Load CSV data from a stage file.

        This is a simplified interface for loading CSV files from an external stage.
        A stage is a named external storage location (filesystem, S3, etc.) that you
        create once and can reuse for multiple load operations.

        Why use stages?
        ---------------
        - **Centralized storage**: Keep all data files in one location
        - **Access control**: Manage permissions at the stage level
        - **Reusability**: Load multiple files from the same stage
        - **Performance**: Direct loading from cloud storage (S3, etc.)

        Args:
            stage_name (str): Stage name (without 'stage://' prefix)
                Example: 'my_data_stage', 's3_stage'
            filepath (str): File path within the stage (relative to stage URL)
                Example: 'users.csv', 'data/2024/users.csv'
            table_name_or_model: Table name (str) or SQLAlchemy model class
            delimiter (str): Field delimiter. Default: ','
            enclosed_by (str, optional): Field enclosure character (e.g., '"')
            compression (str or CompressionFormat, optional): Compression format
                Supported: GZIP, BZIP2, LZ4, etc.
            **kwargs: Additional options (ignore_lines, columns, set_clause, etc.)

        Returns:
            ResultSet: Load results with affected_rows

        Prerequisites:
            You must create the stage first:
            >>> client.execute("CREATE STAGE my_stage URL='file:///path/to/data/'")

        Examples:
            >>> # Basic CSV from stage
            >>> result = client.load_data.from_stage_csv(
            ...     'mystage',
            ...     'users.csv',
            ...     User
            ... )

            >>> # CSV with custom delimiter
            >>> result = client.load_data.from_stage_csv(
            ...     'mystage',
            ...     'data.txt',
            ...     User,
            ...     delimiter='|'
            ... )

            >>> # Skip header row
            >>> result = client.load_data.from_stage_csv(
            ...     'mystage',
            ...     'users.csv',
            ...     User,
            ...     ignore_lines=1
            ... )
        """
        return self.from_stage(
            stage_name,
            filepath,
            table_name_or_model,
            format=LoadDataFormat.CSV,
            delimiter=delimiter,
            enclosed_by=enclosed_by,
            compression=compression,
            **kwargs,
        )

    def from_stage_tsv(
        self,
        stage_name: str,
        filepath: str,
        table_name_or_model,
        compression: Optional[Union[str, CompressionFormat]] = None,
        **kwargs,
    ):
        """
        Load TSV (Tab-Separated Values) data from a stage file.

        TSV is a common format for data interchange, especially for log files
        and data exports. This method automatically uses tab ('\\t') as the delimiter.

        A stage is a named external storage location that you create once and
        reuse for loading multiple files.

        Args:
            stage_name (str): Stage name (without 'stage://' prefix)
                Example: 'log_stage', 'export_stage'
            filepath (str): File path within the stage (relative to stage URL)
                Example: 'logs.tsv', 'exports/2024-01/data.tsv'
            table_name_or_model: Table name (str) or SQLAlchemy model class
            compression (str or CompressionFormat, optional): Compression format
                Useful for compressed TSV files (.tsv.gz, .tsv.bz2)
            **kwargs: Additional options (ignore_lines, columns, set_clause, etc.)

        Returns:
            ResultSet: Load results with affected_rows

        Prerequisites:
            Create the stage first:
            >>> client.execute("CREATE STAGE log_stage URL='file:///var/logs/'")

        Examples:
            >>> # Basic TSV from stage
            >>> result = client.load_data.from_stage_tsv(
            ...     'log_stage',
            ...     'app.tsv',
            ...     Log
            ... )

            >>> # Compressed TSV
            >>> result = client.load_data.from_stage_tsv(
            ...     'log_stage',
            ...     'app.tsv.gz',
            ...     Log,
            ...     compression=CompressionFormat.GZIP
            ... )
        """
        return self.from_stage(
            stage_name,
            filepath,
            table_name_or_model,
            format=LoadDataFormat.CSV,
            delimiter='\t',
            compression=compression,
            **kwargs,
        )

    def from_stage_jsonline(
        self,
        stage_name: str,
        filepath: str,
        table_name_or_model,
        structure: Union[str, JsonDataStructure] = JsonDataStructure.OBJECT,
        compression: Optional[Union[str, CompressionFormat]] = None,
        **kwargs,
    ):
        """
        Load JSONLINE (JSON Lines) data from a stage file.

        JSONLINE format stores one JSON object per line, making it ideal for:
        - Streaming data (logs, events, metrics)
        - Large datasets that don't fit in memory
        - Append-only data collections

        A stage is a named external storage location (filesystem, S3, etc.) that
        provides centralized, reusable access to data files.

        Args:
            stage_name (str): Stage name (without 'stage://' prefix)
                Example: 'event_stage', 's3_logs'
            filepath (str): File path within the stage (relative to stage URL)
                Example: 'events.jsonl', 'logs/2024-10/events.jsonl'
            table_name_or_model: Table name (str) or SQLAlchemy model class
            structure (str or JsonDataStructure): JSON structure format
                - JsonDataStructure.OBJECT: {"id":1,"name":"Alice"} (default)
                - JsonDataStructure.ARRAY: [1,"Alice",30]
            compression (str or CompressionFormat, optional): Compression format
                Useful for .jsonl.gz, .jsonl.bz2 files
            **kwargs: Additional options (columns, set_clause, etc.)

        Returns:
            ResultSet: Load results with affected_rows

        Prerequisites:
            Create the stage first:
            >>> client.execute("CREATE STAGE event_stage URL='s3://my-bucket/events/'")

        Examples:
            >>> # Load JSONLINE with object structure
            >>> result = client.load_data.from_stage_jsonline(
            ...     'event_stage',
            ...     'user_events.jsonl',
            ...     Event,
            ...     structure=JsonDataStructure.OBJECT
            ... )

            >>> # Load JSONLINE with array structure
            >>> result = client.load_data.from_stage_jsonline(
            ...     'event_stage',
            ...     'metrics.jsonl',
            ...     Metric,
            ...     structure=JsonDataStructure.ARRAY
            ... )

            >>> # Load compressed JSONLINE
            >>> result = client.load_data.from_stage_jsonline(
            ...     'event_stage',
            ...     'events.jsonl.gz',
            ...     Event,
            ...     compression=CompressionFormat.GZIP
            ... )
        """
        return self.from_stage(
            stage_name,
            filepath,
            table_name_or_model,
            format=LoadDataFormat.JSONLINE,
            jsondata=structure,
            compression=compression,
            **kwargs,
        )

    def from_stage_parquet(self, stage_name: str, filepath: str, table_name_or_model, **kwargs):
        """
        Load Parquet data from a stage file.

        Args:
            stage_name (str): Stage name (without 'stage://' prefix)
            filepath (str): File path within the stage
            table_name_or_model: Table name (str) or SQLAlchemy model class
            **kwargs: Additional options

        Returns:
            ResultSet: Load results with affected_rows

        Important - MatrixOne Parquet Requirements:
            MatrixOne currently has specific requirements for Parquet files.

            Currently NOT Supported (as of this version):
                ❌ Nullable columns (INT64(optional), STRING(optional), etc.)
                ❌ Dictionary encoding (indexed pages)
                ❌ Parquet 2.0 features
                ❌ Column statistics metadata
                ❌ Compressed Parquet files (GZIP, SNAPPY, etc.)

            If you encounter errors like:
                - "indexed INT64 page is not yet implemented"
                - "load INT64(optional) to BIGINT NULL is not yet implemented"

            You must generate Parquet files with the following PyArrow settings:

            Required Settings (Mandatory):
                1. Schema columns must be non-nullable: nullable=False
                2. Disable dictionary encoding: use_dictionary=False
                3. Disable compression: compression='none'
                4. Disable statistics: write_statistics=False
                5. Use Parquet 1.0 format: data_page_version='1.0'

            Note: These limitations are temporary. Support for nullable columns, dictionary
            encoding, compression, and Parquet 2.0 features is planned for future releases.
            Stay tuned for updates!

            Example::

                import pyarrow as pa
                import pyarrow.parquet as pq

                # Define non-nullable schema (REQUIRED)
                schema = pa.schema([
                    pa.field('id', pa.int64(), nullable=False),
                    pa.field('name', pa.string(), nullable=False)
                ])

                # Create and write table with MatrixOne-compatible options
                table = pa.table({'id': [1, 2], 'name': ['A', 'B']}, schema=schema)
                pq.write_table(
                    table, 'data.parq',
                    compression='none',          # REQUIRED
                    use_dictionary=False,        # REQUIRED
                    write_statistics=False,      # REQUIRED
                    data_page_version='1.0'      # REQUIRED
                )

        Example:
            >>> result = client.load_data.from_stage_parquet(
            ...     'parqstage',
            ...     'data.parq',
            ...     User
            ... )
        """
        return self.from_stage(stage_name, filepath, table_name_or_model, format=LoadDataFormat.PARQUET, **kwargs)

    def from_file(
        self,
        file_path: str,
        table_name_or_model,
        fields_terminated_by: str = ",",
        fields_enclosed_by: Optional[str] = None,
        fields_optionally_enclosed: bool = False,
        fields_escaped_by: Optional[str] = None,
        lines_terminated_by: Optional[str] = None,
        lines_starting_by: Optional[str] = None,
        ignore_lines: int = 0,
        character_set: Optional[str] = None,
        parallel: bool = False,
        columns: Optional[List[str]] = None,
        format: Optional[Union[str, LoadDataFormat]] = None,
        jsondata: Optional[Union[str, JsonDataStructure]] = None,
        compression: Optional[Union[str, CompressionFormat]] = None,
        set_clause: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        """
        Load data from a file into a table.

        This is the main method for loading data from files. It supports all
        standard LOAD DATA INFILE options and provides a clean Python interface.

        Args:
            file_path (str): Path to the file to load. Can be:
                - Local file path: '/path/to/data.csv'
                - Stage file: 'stage://stage_name/path/to/file'

            table_name_or_model: Either a table name (str) or SQLAlchemy model class

            fields_terminated_by (str): Character(s) that separate fields.
                Default: ',' (comma). Common values:
                - ',' for CSV files
                - '\\t' for TSV files
                - '|' for pipe-delimited
                - '*' or any custom delimiter

            fields_enclosed_by (str, optional): Character that encloses field values.
                Used when fields contain the delimiter character.
                Common values: '"' or "'"

            fields_optionally_enclosed (bool): If True, use OPTIONALLY ENCLOSED BY.
                This means only some fields may be enclosed, not all.
                Default: False

            fields_escaped_by (str, optional): Escape character for special characters.
                Default: '\\' (backslash) when enclosed_by is specified.

            lines_terminated_by (str, optional): Character(s) that terminate lines.
                Default: '\\n'. For Windows files: '\\r\\n'

            lines_starting_by (str, optional): Prefix that identifies lines to load.
                Lines not starting with this prefix are ignored.

            ignore_lines (int): Number of lines to skip at the beginning of the file.
                Useful for skipping header rows. Default: 0

            character_set (str, optional): Character set of the input file.
                Examples: 'utf8', 'utf-8', 'utf-16', 'gbk'

            parallel (bool): Enable parallel loading for large files.
                Default: False. Set to True for faster loading of large files.

            columns (list, optional): List of column names to load data into.
                When specified, only these columns are populated. Example:
                ['col1', 'col2', 'col3']

            format (str or LoadDataFormat, optional): File format. Supported values:
                - LoadDataFormat.CSV or 'csv' (default)
                - LoadDataFormat.JSONLINE or 'jsonline'
                - LoadDataFormat.PARQUET or 'parquet'

            jsondata (str or JsonDataStructure, optional): JSON data structure (for JSONLINE).
                - JsonDataStructure.OBJECT or 'object': JSON objects (one per line)
                - JsonDataStructure.ARRAY or 'array': JSON arrays (one per line)
                Required when format is JSONLINE

            compression (str or CompressionFormat, optional): Compression format.
                - CompressionFormat.NONE or 'none': No compression (default)
                - CompressionFormat.GZIP or 'gzip': Gzip compression
                - CompressionFormat.BZIP2 or 'bzip2': Bzip2 compression
                - CompressionFormat.LZ4 or 'lz4': LZ4 compression
                - CompressionFormat.TAR_GZ or 'tar.gz': Tar+Gzip
                - CompressionFormat.TAR_BZ2 or 'tar.bz2': Tar+Bzip2

            set_clause (dict, optional): Column transformations.
                Dictionary of column transformations, e.g.:
                {'col1': 'NULLIF(col1, "null")', 'col2': 'NULLIF(col2, 1)'}

            **kwargs: Additional options for future extensions

        Returns:
            ResultSet: Object containing load results with affected_rows count

        Raises:
            ValueError: If parameters are invalid
            QueryError: If file loading fails

        Examples::

            # Basic CSV loading
            >>> result = client.load_data.from_file('/path/to/users.csv', 'users')
            >>> print(f"Loaded {result.affected_rows} rows")

            # CSV with header row
            >>> result = client.load_data.from_file(
            ...     '/path/to/data.csv',
            ...     'products',
            ...     ignore_lines=1
            ... )

            # Pipe-delimited with quoted fields
            >>> result = client.load_data.from_file(
            ...     '/path/to/data.txt',
            ...     'orders',
            ...     fields_terminated_by='|',
            ...     fields_enclosed_by='"',
            ...     fields_escaped_by='\\\\'
            ... )

            # Tab-separated values (TSV)
            >>> result = client.load_data.from_file(
            ...     '/path/to/data.tsv',
            ...     'logs',
            ...     fields_terminated_by='\\t'
            ... )

            # Load with character set conversion
            >>> result = client.load_data.from_file(
            ...     '/path/to/data.csv',
            ...     'users',
            ...     character_set='utf-8'
            ... )

            # Load specific columns only
            >>> result = client.load_data.from_file(
            ...     '/path/to/data.csv',
            ...     'users',
            ...     columns=['name', 'email', 'age']
            ... )

            # Parallel loading for large files
            >>> result = client.load_data.from_file(
            ...     '/path/to/large_data.csv',
            ...     'big_table',
            ...     parallel=True
            ... )

            # Load with lines starting by filter
            >>> result = client.load_data.from_file(
            ...     '/path/to/data.txt',
            ...     'filtered_data',
            ...     lines_starting_by='DATA:'
            ... )

            # Load JSONLINE format (object)
            >>> result = client.load_data.from_file(
            ...     '/path/to/data.jl',
            ...     'users',
            ...     format=LoadDataFormat.JSONLINE,
            ...     jsondata=JsonDataStructure.OBJECT
            ... )

            # Load JSONLINE format (array) with compression
            >>> result = client.load_data.from_file(
            ...     '/path/to/data.jl.gz',
            ...     'users',
            ...     format=LoadDataFormat.JSONLINE,
            ...     jsondata=JsonDataStructure.ARRAY,
            ...     compression=CompressionFormat.GZIP
            ... )

            # Load Parquet format
            >>> result = client.load_data.from_file(
            ...     '/path/to/data.parq',
            ...     'users',
            ...     format=LoadDataFormat.PARQUET
            ... )

            # Load with SET clause (NULLIF)
            >>> result = client.load_data.from_file(
            ...     '/path/to/data.csv',
            ...     'users',
            ...     set_clause={
            ...         'col1': 'NULLIF(col1, "null")',
            ...         'col2': 'NULLIF(col2, 1)'
            ...     }
            ... )
        """
        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            table_name = table_name_or_model.__tablename__
        else:
            table_name = table_name_or_model

        # Validate required parameters
        if not file_path or not isinstance(file_path, str):
            raise ValueError("file_path must be a non-empty string")

        if not table_name or not isinstance(table_name, str):
            raise ValueError("table_name must be a non-empty string")

        if not isinstance(ignore_lines, int) or ignore_lines < 0:
            raise ValueError("ignore_lines must be a non-negative integer")

        # Convert enums to strings if needed
        format_str = format.value if isinstance(format, LoadDataFormat) else format
        jsondata_str = jsondata.value if isinstance(jsondata, JsonDataStructure) else jsondata
        compression_str = compression.value if isinstance(compression, CompressionFormat) else compression

        # Build the LOAD DATA SQL statement
        sql = self._build_load_data_sql(
            file_path=file_path,
            table_name=table_name,
            fields_terminated_by=fields_terminated_by,
            fields_enclosed_by=fields_enclosed_by,
            fields_optionally_enclosed=fields_optionally_enclosed,
            fields_escaped_by=fields_escaped_by,
            lines_terminated_by=lines_terminated_by,
            lines_starting_by=lines_starting_by,
            ignore_lines=ignore_lines,
            character_set=character_set,
            parallel=parallel,
            columns=columns,
            format=format_str,
            jsondata=jsondata_str,
            compression=compression_str,
            set_clause=set_clause,
        )

        # Execute the LOAD DATA statement
        return self._get_executor().execute(sql)

    def from_local_file(self, file_path: str, table_name_or_model, **kwargs):
        """
        Load data from a local file using LOAD DATA LOCAL INFILE.

        This method uses the LOCAL keyword, which allows loading files from
        the client machine rather than the server machine.

        Args:
            file_path (str): Path to the local file
            table_name_or_model: Either a table name (str) or SQLAlchemy model class
            **kwargs: Same options as from_file()

        Returns:
            ResultSet: Object containing load results

        Examples::

            # Load from client machine
            >>> result = client.load_data.from_local_file(
            ...     '/local/path/to/data.csv',
            ...     'users',
            ...     ignore_lines=1
            ... )
        """
        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            table_name = table_name_or_model.__tablename__
        else:
            table_name = table_name_or_model

        # Build SQL with LOCAL keyword
        sql = self._build_load_data_sql(file_path=file_path, table_name=table_name, local=True, **kwargs)

        return self._get_executor().execute(sql)


class AsyncLoadDataManager(BaseLoadDataManager):
    """
    Async manager for LOAD DATA operations in MatrixOne.

    This class provides async/await support for LOAD DATA operations.
    All SQL building logic is inherited from BaseLoadDataManager.

    Examples::

        # Basic async CSV loading
        await client.load_data.from_csv('/path/to/data.csv', User)

        # Load from stage
        await client.load_data.from_stage_csv('mystage', 'data.csv', Product)
    """

    def __init__(self, client, executor=None):
        """
        Initialize AsyncLoadDataManager.

        Args:
            client: Client object
            executor: Optional executor (e.g., async session) for executing SQL.
                     If None, uses client.execute
        """
        super().__init__(client)
        self.executor = executor

    def _get_executor(self):
        """Get the executor for SQL execution (session or client)"""
        return self.executor if self.executor else self.client

    async def from_csv(
        self,
        file_path: str,
        table_name_or_model,
        delimiter: str = ",",
        enclosed_by: Optional[str] = None,
        optionally_enclosed: bool = False,
        escaped_by: Optional[str] = None,
        ignore_lines: int = 0,
        columns: Optional[List[str]] = None,
        character_set: Optional[str] = None,
        parallel: bool = False,
        compression: Optional[Union[str, CompressionFormat]] = None,
        set_clause: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        """Async version of from_csv"""
        return await self.from_file(
            file_path=file_path,
            table_name_or_model=table_name_or_model,
            fields_terminated_by=delimiter,
            fields_enclosed_by=enclosed_by,
            fields_optionally_enclosed=optionally_enclosed,
            fields_escaped_by=escaped_by,
            ignore_lines=ignore_lines,
            columns=columns,
            character_set=character_set,
            parallel=parallel,
            compression=compression,
            set_clause=set_clause,
            **kwargs,
        )

    async def from_tsv(
        self, file_path: str, table_name_or_model, ignore_lines: int = 0, columns: Optional[List[str]] = None, **kwargs
    ):
        """Async version of from_tsv"""
        return await self.from_csv(
            file_path=file_path,
            table_name_or_model=table_name_or_model,
            delimiter='\t',
            ignore_lines=ignore_lines,
            columns=columns,
            **kwargs,
        )

    async def from_jsonline(
        self,
        file_path: str,
        table_name_or_model,
        structure: Union[str, JsonDataStructure] = JsonDataStructure.OBJECT,
        compression: Optional[Union[str, CompressionFormat]] = None,
        **kwargs,
    ):
        """Async version of from_jsonline"""
        structure_str = structure.value if isinstance(structure, JsonDataStructure) else structure
        compression_str = compression.value if isinstance(compression, CompressionFormat) else compression

        return await self.from_file(
            file_path=file_path,
            table_name_or_model=table_name_or_model,
            format=LoadDataFormat.JSONLINE.value,
            jsondata=structure_str,
            compression=compression_str,
            **kwargs,
        )

    async def from_parquet(self, file_path: str, table_name_or_model, **kwargs):
        """Async version of from_parquet"""
        return await self.from_file(
            file_path=file_path, table_name_or_model=table_name_or_model, format=LoadDataFormat.PARQUET.value, **kwargs
        )

    async def from_stage(
        self,
        stage_name: str,
        filepath: str,
        table_name_or_model,
        format: Union[str, LoadDataFormat] = LoadDataFormat.CSV,
        delimiter: str = ",",
        enclosed_by: Optional[str] = None,
        jsondata: Optional[Union[str, JsonDataStructure]] = None,
        compression: Optional[Union[str, CompressionFormat]] = None,
        **kwargs,
    ):
        """Async version of from_stage"""
        stage_url = f"stage://{stage_name}/{filepath}"

        if delimiter and 'fields_terminated_by' not in kwargs:
            kwargs['fields_terminated_by'] = delimiter
        if enclosed_by and 'fields_enclosed_by' not in kwargs:
            kwargs['fields_enclosed_by'] = enclosed_by

        return await self.from_file(
            stage_url, table_name_or_model, format=format, jsondata=jsondata, compression=compression, **kwargs
        )

    async def from_stage_csv(
        self,
        stage_name: str,
        filepath: str,
        table_name_or_model,
        delimiter: str = ",",
        enclosed_by: Optional[str] = None,
        compression: Optional[Union[str, CompressionFormat]] = None,
        **kwargs,
    ):
        """Async version of from_stage_csv"""
        return await self.from_stage(
            stage_name,
            filepath,
            table_name_or_model,
            format=LoadDataFormat.CSV,
            delimiter=delimiter,
            enclosed_by=enclosed_by,
            compression=compression,
            **kwargs,
        )

    async def from_stage_tsv(
        self,
        stage_name: str,
        filepath: str,
        table_name_or_model,
        compression: Optional[Union[str, CompressionFormat]] = None,
        **kwargs,
    ):
        """Async version of from_stage_tsv"""
        return await self.from_stage(
            stage_name,
            filepath,
            table_name_or_model,
            format=LoadDataFormat.CSV,
            delimiter='\t',
            compression=compression,
            **kwargs,
        )

    async def from_stage_jsonline(
        self,
        stage_name: str,
        filepath: str,
        table_name_or_model,
        structure: Union[str, JsonDataStructure] = JsonDataStructure.OBJECT,
        compression: Optional[Union[str, CompressionFormat]] = None,
        **kwargs,
    ):
        """Async version of from_stage_jsonline"""
        return await self.from_stage(
            stage_name,
            filepath,
            table_name_or_model,
            format=LoadDataFormat.JSONLINE,
            jsondata=structure,
            compression=compression,
            **kwargs,
        )

    async def from_stage_parquet(self, stage_name: str, filepath: str, table_name_or_model, **kwargs):
        """Async version of from_stage_parquet"""
        return await self.from_stage(stage_name, filepath, table_name_or_model, format=LoadDataFormat.PARQUET, **kwargs)

    async def from_file(
        self,
        file_path: str,
        table_name_or_model,
        fields_terminated_by: str = ",",
        fields_enclosed_by: Optional[str] = None,
        fields_optionally_enclosed: bool = False,
        fields_escaped_by: Optional[str] = None,
        lines_terminated_by: Optional[str] = None,
        lines_starting_by: Optional[str] = None,
        ignore_lines: int = 0,
        character_set: Optional[str] = None,
        parallel: bool = False,
        columns: Optional[List[str]] = None,
        format: Optional[Union[str, LoadDataFormat]] = None,
        jsondata: Optional[Union[str, JsonDataStructure]] = None,
        compression: Optional[Union[str, CompressionFormat]] = None,
        set_clause: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        """Async version of from_file - builds SQL and executes asynchronously"""
        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            table_name = table_name_or_model.__tablename__
        else:
            table_name = table_name_or_model

        # Validate required parameters
        if not file_path or not isinstance(file_path, str):
            raise ValueError("file_path must be a non-empty string")

        if not table_name or not isinstance(table_name, str):
            raise ValueError("table_name must be a non-empty string")

        if not isinstance(ignore_lines, int) or ignore_lines < 0:
            raise ValueError("ignore_lines must be a non-negative integer")

        # Convert enums to strings if needed
        format_str = format.value if isinstance(format, LoadDataFormat) else format
        jsondata_str = jsondata.value if isinstance(jsondata, JsonDataStructure) else jsondata
        compression_str = compression.value if isinstance(compression, CompressionFormat) else compression

        # Build the LOAD DATA SQL statement using base class
        sql = self._build_load_data_sql(
            file_path=file_path,
            table_name=table_name,
            fields_terminated_by=fields_terminated_by,
            fields_enclosed_by=fields_enclosed_by,
            fields_optionally_enclosed=fields_optionally_enclosed,
            fields_escaped_by=fields_escaped_by,
            lines_terminated_by=lines_terminated_by,
            lines_starting_by=lines_starting_by,
            ignore_lines=ignore_lines,
            character_set=character_set,
            parallel=parallel,
            columns=columns,
            format=format_str,
            jsondata=jsondata_str,
            compression=compression_str,
            set_clause=set_clause,
        )

        # Execute asynchronously
        return await self._get_executor().execute(sql)

    async def from_local_file(self, file_path: str, table_name_or_model, **kwargs):
        """Async version of from_local_file"""
        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            table_name = table_name_or_model.__tablename__
        else:
            table_name = table_name_or_model

        # Build SQL with LOCAL keyword
        sql = self._build_load_data_sql(file_path=file_path, table_name=table_name, local=True, **kwargs)

        return await self._get_executor().execute(sql)

    async def from_inline(
        self,
        data: str,
        table_name_or_model,
        format: Union[str, LoadDataFormat] = LoadDataFormat.CSV,
        delimiter: str = ",",
        enclosed_by: Optional[str] = None,
        jsontype: Optional[Union[str, JsonDataStructure]] = None,
        **kwargs,
    ):
        """Async version of from_inline"""
        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            table_name = table_name_or_model.__tablename__
        else:
            table_name = table_name_or_model

        # Validate parameters
        if not data or not isinstance(data, str):
            raise ValueError("data must be a non-empty string")

        if not table_name or not isinstance(table_name, str):
            raise ValueError("table_name must be a non-empty string")

        # Convert enums to strings
        format_str = format.value if isinstance(format, LoadDataFormat) else format
        jsontype_str = jsontype.value if isinstance(jsontype, JsonDataStructure) else jsontype

        # Build SQL using base class
        sql = self._build_load_data_inline_sql(
            data=data,
            table_name=table_name,
            format=format_str,
            delimiter=delimiter,
            enclosed_by=enclosed_by,
            jsontype=jsontype_str,
        )

        return await self._get_executor().execute(sql)

    async def from_csv_inline(
        self, data: str, table_name_or_model, delimiter: str = ",", enclosed_by: Optional[str] = None, **kwargs
    ):
        """Async version of from_csv_inline"""
        return await self.from_inline(
            data=data,
            table_name_or_model=table_name_or_model,
            format=LoadDataFormat.CSV,
            delimiter=delimiter,
            enclosed_by=enclosed_by,
            **kwargs,
        )

    async def from_jsonline_inline(
        self, data: str, table_name_or_model, structure: Union[str, JsonDataStructure] = JsonDataStructure.OBJECT, **kwargs
    ):
        """Async version of from_jsonline_inline"""
        return await self.from_inline(
            data=data, table_name_or_model=table_name_or_model, format=LoadDataFormat.JSONLINE, jsontype=structure, **kwargs
        )

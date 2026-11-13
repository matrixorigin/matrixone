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
import re

from ._sql_literals import sql_identifier, sql_string_token
from ._utils import stage_file_uri


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

    _CSV_ALIASES = {
        'format': 'read_csv',
        'fields_terminated_by': 'sep',
        'fields_enclosed_by': 'quotechar',
        'fields_optionally_enclosed': 'quoting',
        'fields_escaped_by': 'escapechar',
        'ignore_lines': 'skiprows',
        'columns': 'names',
        'character_set': 'encoding',
        'file_path': 'filepath_or_buffer',
        'table_name_or_model': 'table',
    }
    _JSON_ALIASES = {
        'format': 'read_json',
        'jsondata': 'orient',
        'file_path': 'filepath_or_buffer',
        'table_name_or_model': 'table',
    }
    _PARQUET_ALIASES = {'format': 'read_parquet', 'file_path': 'filepath_or_buffer', 'table_name_or_model': 'table'}

    @staticmethod
    def _reject_wrapper_options(kwargs, aliases):
        for name, alternative in aliases.items():
            if name in kwargs:
                raise ValueError(f"{name} is controlled by {alternative}; use {alternative} or from_file directly")

    @staticmethod
    def _validate_set_clause(assignments):
        # The loader can name columns but cannot introduce LOAD row variables.
        identifier = r"(?:[A-Za-z_][A-Za-z0-9_]*|`(?:``|[^`])+`)"
        literal = r"(?:'(?:''|[^'\\])*'|\"(?:\"\"|[^\"\\])*\"|[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))"
        pattern = re.compile(r"\s*NULLIF\s*\(\s*(" + identifier + r")\s*,\s*" + literal + r"\s*\)\s*", re.I)
        if not isinstance(assignments, dict):
            raise ValueError('set_clause must map columns to NULLIF(column, literal)')
        for column, expression in assignments.items():
            match = pattern.fullmatch(expression) if isinstance(expression, str) else None
            source = match.group(1) if match else ''
            if source.startswith('`'):
                source = source[1:-1].replace('``', '`')
            if not match or not isinstance(column, str) or source.casefold() != column.casefold():
                raise ValueError('SET supports only NULLIF(target_column, literal); row variables are not supported')

    def _build_load_data_sql(
        self,
        file_path,
        table_name,
        fields_terminated_by=',',
        fields_enclosed_by=None,
        fields_optionally_enclosed=False,
        fields_escaped_by=None,
        lines_terminated_by=None,
        lines_starting_by=None,
        ignore_lines=0,
        character_set=None,
        parallel=False,
        columns=None,
        local=False,
        format=None,
        jsondata=None,
        compression=None,
        set_clause=None,
    ):
        if local and (
            'local_infile' not in self.client._connection_params
            or self.client._connection_params['local_infile'] is False
        ):
            raise ValueError('LOCAL INFILE is disabled or the adopted engine is not certified')
        format = (format or 'csv').lower()
        if format not in ('csv', 'jsonline', 'parquet'):
            raise ValueError('Unsupported LOAD format')
        if compression is not None:
            compression = compression.lower()
            if compression not in {item.value for item in CompressionFormat}:
                raise ValueError('Unsupported LOAD compression')
        if jsondata is not None and (format != 'jsonline' or jsondata not in ('object', 'array')):
            raise ValueError('jsondata requires JSONLINE object or array format')
        if format != 'csv' and any(
            (
                fields_terminated_by != ',',
                fields_enclosed_by is not None,
                fields_optionally_enclosed,
                fields_escaped_by is not None,
                lines_terminated_by is not None,
                lines_starting_by is not None,
            )
        ):
            raise ValueError('FIELDS/LINES options require CSV format')
        if format == 'parquet' and (ignore_lines or set_clause):
            raise ValueError('Parquet IGNORE LINES and SET are not qualified; use columns for projection')
        tail = self._build_load_tail(
            table_name,
            fields_terminated_by if format == 'csv' else None,
            fields_enclosed_by,
            fields_optionally_enclosed,
            fields_escaped_by,
            lines_terminated_by,
            lines_starting_by,
            ignore_lines,
            character_set,
            parallel,
            columns,
            set_clause,
        )
        prefix = 'LOAD DATA ' + ('LOCAL ' if local else '') + 'INFILE '
        if format != 'csv' or compression is not None:
            options = ["'filepath'=" + sql_string_token(file_path)]
            if format != 'csv':
                options.append("'format'=" + sql_string_token(format))
            if jsondata is not None:
                options.append("'jsondata'=" + sql_string_token(jsondata))
            if compression is not None:
                options.append("'compression'=" + sql_string_token(compression))
            source = '{' + ', '.join(options) + '}'
        else:
            source = sql_string_token(file_path)
        return prefix + source + ' ' + tail

    def _build_load_tail(
        self,
        table_name,
        fields_terminated_by=',',
        fields_enclosed_by=None,
        fields_optionally_enclosed=False,
        fields_escaped_by=None,
        lines_terminated_by=None,
        lines_starting_by=None,
        ignore_lines=0,
        character_set=None,
        parallel=False,
        columns=None,
        set_clause=None,
    ):
        if isinstance(ignore_lines, bool) or not isinstance(ignore_lines, int) or ignore_lines < 0:
            raise ValueError('ignore_lines must be a non-negative integer')
        if columns is not None and (not isinstance(columns, (list, tuple)) or not columns):
            raise ValueError('columns must be a nonempty list of identifiers')
        parts = ['INTO TABLE ' + sql_identifier(table_name, qualified=True)]
        if character_set:
            parts.append('CHARACTER SET ' + sql_identifier(character_set))
        fields = []
        if fields_terminated_by is not None:
            fields.append('TERMINATED BY ' + sql_string_token(fields_terminated_by))
        if fields_enclosed_by is not None:
            fields.append(
                ('OPTIONALLY ' if fields_optionally_enclosed else '')
                + 'ENCLOSED BY '
                + sql_string_token(fields_enclosed_by)
            )
        elif fields_optionally_enclosed:
            raise ValueError('OPTIONALLY ENCLOSED requires a quote character')
        if fields_escaped_by is not None:
            fields.append('ESCAPED BY ' + sql_string_token(fields_escaped_by))
        if fields:
            parts.append('FIELDS ' + ' '.join(fields))
        lines = []
        if lines_starting_by is not None:
            lines.append('STARTING BY ' + sql_string_token(lines_starting_by))
        if lines_terminated_by is not None:
            lines.append('TERMINATED BY ' + sql_string_token(lines_terminated_by))
        if lines:
            parts.append('LINES ' + ' '.join(lines))
        if ignore_lines:
            parts.append('IGNORE %d LINES' % ignore_lines)
        if columns is not None:
            parts.append('(' + ', '.join(sql_identifier(c) for c in columns) + ')')
        if set_clause is not None:
            self._validate_set_clause(set_clause)
            if set_clause:
                parts.append('SET ' + ', '.join(sql_identifier(k) + '=' + v for k, v in set_clause.items()))
        if parallel:
            parts.append("PARALLEL 'true'")
        return ' '.join(parts)

    def _build_load_data_sql_standard(self, file_path, table_name, **options):
        return self._build_load_data_sql(file_path, table_name, **options)

    def _build_load_data_sql_with_braces(
        self, file_path, table_name, format, jsondata=None, compression=None, local=False, **options
    ):
        return self._build_load_data_sql(
            file_path, table_name, format=format, jsondata=jsondata, compression=compression, local=local, **options
        )

    def _build_load_data_inline_sql(
        self,
        data,
        table_name,
        format='csv',
        delimiter=',',
        enclosed_by=None,
        jsontype=None,
        ignore_lines=0,
        columns=None,
        escaped_by=None,
        optionally_enclosed=False,
    ):
        if format not in ('csv', 'jsonline'):
            raise ValueError('Inline LOAD requires csv or jsonline')
        if jsontype is not None and (format != 'jsonline' or jsontype not in ('object', 'array')):
            raise ValueError('JSONTYPE requires JSONLINE object or array format')
        source = 'LOAD DATA INLINE FORMAT=' + sql_string_token(format) + ', DATA=' + sql_string_token(data)
        if jsontype:
            source += ', JSONTYPE=' + sql_string_token(jsontype)
        tail = self._build_load_tail(
            table_name,
            delimiter if format == 'csv' else None,
            enclosed_by,
            optionally_enclosed,
            escaped_by,
            ignore_lines=ignore_lines,
            columns=columns,
        )
        return source + ' ' + tail

    @staticmethod
    def _inline_options(options):
        aliases = {
            'skiprows': 'ignore_lines',
            'names': 'columns',
            'escapechar': 'escaped_by',
            'quoting': 'optionally_enclosed',
        }
        result = {}
        allowed = {'ignore_lines', 'columns', 'escaped_by', 'optionally_enclosed'}
        for key, value in options.items():
            key = aliases.get(key, key)
            if key not in allowed:
                raise ValueError('Unsupported inline LOAD option: ' + key)
            if key in result:
                raise ValueError('Duplicate inline LOAD option: ' + key)
            result[key] = value
        return result


class LoadDataManager(BaseLoadDataManager):
    """
    Synchronous data loading manager for MatrixOne bulk data operations.

    This class provides a comprehensive and high-performance interface for loading
    large volumes of data from files into MatrixOne tables. It supports multiple
    data formats, compression formats, and advanced loading options for optimal
    performance and flexibility.

    Key Features:

    - **Multiple data formats**: CSV, TSV, JSON, JSONLines, Parquet
    - **Flexible delimiters**: Customizable field and line terminators
    - **Compression support**: gzip, bzip2, tar.gz, tar.bz2, lz4, lzo
    - **Parallel loading**: High-performance parallel data loading
    - **Character set conversion**: Support for various character encodings
    - **Column mapping**: Map file columns to table columns
    - **Data transformation**: Apply SET clauses for column transformations
    - **Stage integration**: Load from external stages (S3, local filesystem)
    - **Transaction-aware**: Full integration with transaction contexts
    - **Header handling**: Skip header rows automatically

    Executor Pattern:

    - If executor is None, uses self.client.execute (default client-level executor)
    - If executor is provided (e.g., session), uses executor.execute (transaction-aware)
    - All operations can participate in transactions when used via session

    Supported Data Formats:

    - **CSV**: Comma-Separated Values (most common)
    - **TSV**: Tab-Separated Values
    - **JSON**: JSON objects (one per file)
    - **JSONLines**: JSON Lines format (one JSON object per line)
    - **Parquet**: Apache Parquet columnar format

    Usage Examples::

        from matrixone import Client
        from matrixone.orm import Column, Integer, String, VectorType

        client = Client(host='localhost', port=6001, user='root', password='111', database='test')

        # Define ORM models (recommended approach)
        class User(Base):
            __tablename__ = 'users'
            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            email = Column(String(255))
            age = Column(Integer)

        class Order(Base):
            __tablename__ = 'orders'
            id = Column(Integer, primary_key=True)
            user_id = Column(Integer)
            amount = Column(Float)

        # ========================================
        # Basic CSV Loading with ORM Models
        # ========================================

        # Load CSV using ORM model (recommended)
        client.load_data.from_csv('/path/to/users.csv', User)

        # CSV with header row (skip first line)
        client.load_data.from_csv(
            '/path/to/users.csv',
            User,
            ignore_lines=1  # Skip header
        )

        # Custom delimiter and quote character
        client.load_data.from_csv(
            '/path/to/data.txt',
            Order,
            delimiter='|',
            enclosed_by='"',
            optionally_enclosed=True
        )

        # ========================================
        # Advanced Loading Options
        # ========================================

        # Load compressed file
        client.load_data.from_csv(
            '/path/to/data.csv.gz',
            User,
            compression='gzip'
        )

        # Parallel loading for large files (high performance)
        client.load_data.from_csv(
            '/path/to/large_data.csv',
            User,
            parallel=True  # Enables parallel loading
        )

        # Column mapping and transformation
        client.load_data.from_csv(
            '/path/to/data.csv',
            User,
            columns=['name', 'email', 'age'],
            set_clause={'status': "NULLIF(status, 'null')"}
        )

        # ========================================
        # Different File Formats
        # ========================================

        # Load TSV (Tab-Separated Values)
        client.load_data.from_tsv('/path/to/data.tsv', User)

        # Load JSON Lines format
        client.load_data.from_jsonlines('/path/to/data.jsonl', User)

        # ========================================
        # Load from External Stages
        # ========================================

        # Load from S3 stage using ORM model
        client.load_data.from_stage_csv('production_s3', 'users.csv', User)

        # Load from local filesystem stage
        client.load_data.from_stage_csv('local_data', 'orders.csv', Order)

        # Load from stage with custom options
        client.load_data.from_stage(
            'stage://my_stage/data.csv',
            User,
            file_format='csv',
            delimiter=','
        )

        # ========================================
        # Transactional Data Loading
        # ========================================

        # Using within a transaction (all loads atomic)
        with client.session() as session:
            # Load multiple files atomically using ORM models
            session.load_data.from_csv('/path/to/users.csv', User)
            session.load_data.from_csv('/path/to/orders.csv', Order)

            # Insert additional data within same transaction
            session.execute(insert(User).values(name='Admin', email='admin@example.com'))
            # All operations commit together

    Performance Tips:

    - Use `parallel=True` for large files (>100MB) to enable parallel loading
    - Use compressed files (gzip, bzip2) to reduce I/O and transfer time
    - Specify `columns` parameter to skip unnecessary columns
    - Use appropriate `character_set` to avoid encoding overhead
    - For very large datasets, consider loading in batches

    Common Use Cases:

    - **Data migration**: Import data from legacy systems
    - **ETL pipelines**: Load transformed data into MatrixOne
    - **Data warehouse loading**: Bulk load fact and dimension tables
    - **Log file ingestion**: Import application or server logs
    - **CSV imports**: Load data from spreadsheets or exports

    See Also:

        - StageManager: For managing external stages
        - Client.insert: For small-scale data insertion
        - Client.batch_insert: For programmatic batch inserts
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

    def read_csv(
        self,
        filepath_or_buffer: str,
        table: Union[str, type],
        sep: str = ",",
        quotechar: Optional[str] = None,
        quoting: bool = False,
        escapechar: Optional[str] = None,
        skiprows: int = 0,
        names: Optional[List[str]] = None,
        encoding: Optional[str] = None,
        parallel: bool = False,
        compression: Optional[Union[str, CompressionFormat]] = None,
        set_clause: Optional[Dict[str, str]] = None,
        inline: bool = False,
        **kwargs,
    ):
        """
        Read CSV (Comma-Separated Values) file into MatrixOne table (pandas-style).

        This method provides a pandas-compatible interface for loading CSV files,
        with parameter names and behavior matching pandas.read_csv() where applicable.

        Args:
            filepath_or_buffer (str): File path, stage path, or inline data
                - Local path: '/path/to/data.csv'
                - Stage path: 'stage://stage_name/file.csv'
                - Inline data: CSV string (requires inline=True)
            table (str or Model): Table name or SQLAlchemy model class
            sep (str): Field separator/delimiter. Default: ',' (pandas: sep)
            quotechar (str, optional): Character for quoting fields (pandas: quotechar)
            quoting (bool): If True, use OPTIONALLY ENCLOSED BY. Default: False
            escapechar (str, optional): Escape character (pandas: escapechar)
            skiprows (int): Number of header lines to skip. Default: 0 (pandas: skiprows)
            names (list, optional): Column names to load into (pandas: names)
            encoding (str, optional): Character encoding (pandas: encoding)
            parallel (bool): Enable parallel loading. Default: False
            compression (str or CompressionFormat, optional): Compression format
            set_clause (dict, optional): NULLIF(target_column, literal) assignments; row variables are unsupported
            inline (bool): If True, treat filepath_or_buffer as inline data. Default: False

        Returns:
            ResultSet: Load results with affected_rows

        Examples::

            # Basic CSV (pandas-style)
            client.load_data.read_csv('data.csv', table='users')

            # CSV with header (pandas-style)
            client.load_data.read_csv('data.csv', table='users', skiprows=1)

            # Custom separator (pandas-style)
            client.load_data.read_csv('data.txt', table='users', sep='|')

            # With quote character (pandas-style)
            client.load_data.read_csv('data.csv', table='users',
                quotechar='"', quoting=True)

            # Load from stage
            client.load_data.read_csv('stage://s3_stage/data.csv', table='users')

            # Inline CSV
            csv_data = "1,Alice\\n2,Bob\\n"
            client.load_data.read_csv(csv_data, table='users', inline=True)

            # With ORM model
            client.load_data.read_csv('data.csv', table=User, skiprows=1)
        """
        self._reject_wrapper_options(kwargs, self._CSV_ALIASES)
        # Handle inline data
        if inline:
            if encoding is not None or parallel or compression is not None or set_clause is not None:
                raise ValueError('Inline CSV does not support encoding, parallel, compression or SET')
            return self.read_csv_inline(
                data=filepath_or_buffer,
                table=table,
                sep=sep,
                quotechar=quotechar,
                skiprows=skiprows,
                names=names,
                escapechar=escapechar,
                quoting=quoting,
                **kwargs,
            )

        # Call from_file with mapped parameters
        return self.from_file(
            file_path=filepath_or_buffer,
            table_name_or_model=table,
            fields_terminated_by=sep,
            fields_enclosed_by=quotechar,
            fields_optionally_enclosed=quoting,
            fields_escaped_by=escapechar,
            ignore_lines=skiprows,
            columns=names,
            character_set=encoding,
            parallel=parallel,
            compression=compression,
            set_clause=set_clause,
            **kwargs,
        )

    def read_csv_stage(
        self,
        stage_name: str,
        filename: str,
        table: Union[str, type],
        sep: str = ",",
        quotechar: Optional[str] = None,
        quoting: bool = False,
        escapechar: Optional[str] = None,
        skiprows: int = 0,
        names: Optional[List[str]] = None,
        encoding: Optional[str] = None,
        parallel: bool = False,
        compression: Optional[Union[str, CompressionFormat]] = None,
        set_clause: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        """
        Read CSV file from external stage into MatrixOne table (pandas-style convenience method).

        This is a convenience method that doesn't require the 'stage://' protocol prefix.

        Args:
            stage_name (str): Name of the external stage
            filename (str): Filename within the stage
            table (str or Model): Table name or SQLAlchemy model class
            sep (str): Field separator. Default: ','
            quotechar (str, optional): Character for quoting fields
            quoting (bool): Use OPTIONALLY ENCLOSED BY. Default: False
            skiprows (int): Number of header lines to skip. Default: 0
            names (list, optional): Column names
            encoding (str, optional): Character encoding
            parallel (bool): Enable parallel loading
            compression (str or CompressionFormat, optional): Compression format
            set_clause (dict, optional): NULLIF(target_column, literal) assignments; row variables are unsupported

        Returns:
            ResultSet: Load results

        Examples::

            # Load from S3 stage
            client.load_data.read_csv_stage('s3_stage', 'data.csv', table='users')

            # With options
            client.load_data.read_csv_stage('backup_stage', 'data.tsv',
                table='users', sep='\\t', skiprows=1)
        """
        # Build stage path
        stage_path = stage_file_uri(stage_name, filename)

        # Delegate to read_csv
        return self.read_csv(
            filepath_or_buffer=stage_path,
            table=table,
            sep=sep,
            quotechar=quotechar,
            quoting=quoting,
            escapechar=escapechar,
            skiprows=skiprows,
            names=names,
            encoding=encoding,
            parallel=parallel,
            compression=compression,
            set_clause=set_clause,
            **kwargs,
        )

    def read_csv_inline(
        self,
        data: str,
        table: Union[str, type],
        sep: str = ",",
        quotechar: Optional[str] = None,
        **kwargs,
    ):
        """
        Read CSV data from inline string into MatrixOne table (pandas-style).

        Args:
            data (str): CSV data string
            table (str or Model): Table name or SQLAlchemy model class
            sep (str): Field separator. Default: ','
            quotechar (str, optional): Character for quoting fields

        Returns:
            ResultSet: Load results

        Examples::

            csv_data = "1,Alice\\n2,Bob\\n"
            client.load_data.read_csv_inline(csv_data, table='users')
        """
        # Handle model class input
        if hasattr(table, '__tablename__'):
            table_name = table.__tablename__
        else:
            table_name = table

        # Build SQL
        sql = self._build_load_data_inline_sql(
            data=data,
            table_name=table_name,
            format='csv',
            delimiter=sep,
            enclosed_by=quotechar,
            **self._inline_options(kwargs),
        )

        return self._get_executor().execute(sql)

    def read_json(
        self,
        filepath_or_buffer: str,
        table: Union[str, type],
        lines: bool = True,
        orient: str = 'records',
        compression: Optional[Union[str, CompressionFormat]] = None,
        inline: bool = False,
        **kwargs,
    ):
        """
        Read JSON file into MatrixOne table (pandas-style).

        This method provides a pandas-compatible interface for loading JSON/JSONL files,
        matching pandas.read_json() parameter names.

        Args:
            filepath_or_buffer (str): File path, stage path, or inline data
                - Local path: '/path/to/data.jsonl'
                - Stage path: 'stage://stage_name/file.jsonl'
                - Inline data: JSON string (requires inline=True)
            table (str or Model): Table name or SQLAlchemy model class
            lines (bool): If True, read file as JSON Lines (one object per line).
                Default: True (pandas: lines)
            orient (str): JSON structure format (pandas: orient)
                - 'records': List of dicts like [{'col1': val1}, {'col2': val2}]
                - 'values': List of lists like [[val1, val2], [val3, val4]]
                Default: 'records'
            compression (str or CompressionFormat, optional): Compression format
            inline (bool): If True, treat filepath_or_buffer as inline data. Default: False

        Returns:
            ResultSet: Load results with affected_rows

        Examples::

            # JSON Lines with objects (pandas-style)
            client.load_data.read_json('events.jsonl', table='events', lines=True)

            # JSON Lines with arrays
            client.load_data.read_json('data.jsonl', table='users',
                lines=True, orient='values')

            # From stage
            client.load_data.read_json('stage://s3/events.jsonl', table='events')

            # Compressed JSON Lines
            client.load_data.read_json('events.jsonl.gz', table='events',
                compression='gzip')

            # Inline JSON
            json_data = '{"id":1,"name":"Alice"}\\n{"id":2,"name":"Bob"}\\n'
            client.load_data.read_json(json_data, table='users', inline=True)
        """
        self._reject_wrapper_options(kwargs, self._JSON_ALIASES)
        if lines is not True:
            raise ValueError('Native JSON loading requires lines=True (JSON Lines)')
        # Handle inline data
        if inline:
            return self.read_json_inline(data=filepath_or_buffer, table=table, orient=orient, **kwargs)

        # Map pandas orient to MatrixOne jsondata
        structure_str = _json_structure(orient)

        # Convert compression enum if needed
        compression_str = compression.value if isinstance(compression, CompressionFormat) else compression

        return self.from_file(
            file_path=filepath_or_buffer,
            table_name_or_model=table,
            format=LoadDataFormat.JSONLINE.value,
            jsondata=structure_str,
            compression=compression_str,
            **kwargs,
        )

    def read_parquet(self, filepath_or_buffer: str, table: Union[str, type], **kwargs):
        """
        Read Parquet file into MatrixOne table (pandas-style).

        This method provides a pandas-compatible interface for loading Parquet files,
        matching pandas.read_parquet() behavior.

        Args:
            filepath_or_buffer (str): File path or stage path
                - Local path: '/path/to/data.parquet'
                - Stage path: 'stage://stage_name/file.parquet'
            table (str or Model): Table name or SQLAlchemy model class

        Returns:
            ResultSet: Load results with affected_rows

        MatrixOne Parquet Support:

            Fully Supported Features:
                - All compression formats (NONE, SNAPPY, GZIP, LZ4, ZSTD, Brotli)
                - Parquet 1.0 and 2.0 formats
                - Dictionary encoding for supported scalar types
                - Column statistics (write_statistics=True/False)
                - Nullable columns (nullable=True/False, NULL values supported)
                - Large files, wide tables, empty files

            Supported Data Types:
                - INT32/INT64/UINT32 -> INT/BIGINT/INT UNSIGNED
                - FLOAT32/FLOAT64 -> FLOAT/DOUBLE
                - STRING -> VARCHAR (not TEXT!)
                - BOOLEAN -> BOOL
                - DATE32/DATE64 -> DATE
                - TIME32/TIME64 -> TIME
                - TIMESTAMP(tz='UTC') -> TIMESTAMP (UTC timezone required!)

            Not Supported:
                - INT8/INT16 types (use INT32 or INT64)
                - large_string type (use pa.string())
                - TIMESTAMP without timezone (must add tz='UTC')
                - BINARY, DECIMAL types
                - Complex types (List, Struct, Map)

            Recommended Settings:
                compression='snappy'        # Enable compression
                use_dictionary=True         # Supported for supported scalar types
                write_statistics=True       # Enable statistics
                data_page_version='2.0'     # Use Parquet 2.0

            Common Errors:
                - "load STRING(required) to TEXT NULL is not yet implemented"
                  -> Use VARCHAR in table definition, not TEXT
                - "load TIMESTAMP(isAdjustedToUTC=false..."
                  -> Use pa.timestamp('ms', tz='UTC')

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

                # Write with MatrixOne-compatible options (Verified 2025)
                pq.write_table(
                    table, 'data.parq',
                    compression='snappy',        # ✅ All compression supported!
                    use_dictionary=True,         # ✅ Dictionary encoding supported!
                    write_statistics=True,       # ✅ Supported! (Double-checked)
                    data_page_version='2.0'      # ✅ Parquet 2.0 supported!
                )

                # Now load the file
                client.load_data.from_parquet('data.parq', User)

        Examples::

            # Basic Parquet (pandas-style)
            client.load_data.read_parquet('data.parquet', table='users')

            # From stage
            client.load_data.read_parquet('stage://s3/data.parquet', table='users')

            # With ORM model
            client.load_data.read_parquet('data.parquet', table=User)
        """
        self._reject_wrapper_options(kwargs, self._PARQUET_ALIASES)
        return self.from_file(
            file_path=filepath_or_buffer, table_name_or_model=table, format=LoadDataFormat.PARQUET.value, **kwargs
        )

    def read_json_stage(
        self,
        stage_name: str,
        filename: str,
        table: Union[str, type],
        lines: bool = True,
        orient: str = 'records',
        compression: Optional[Union[str, CompressionFormat]] = None,
        **kwargs,
    ):
        """
        Read JSON file from external stage into MatrixOne table (pandas-style convenience method).

        Args:
            stage_name (str): Name of the external stage
            filename (str): Filename within the stage
            table (str or Model): Table name or SQLAlchemy model class
            lines (bool): If True, read as JSON Lines. Default: True
            orient (str): JSON structure. Default: 'records'
            compression (str or CompressionFormat, optional): Compression format

        Returns:
            ResultSet: Load results

        Examples::

            client.load_data.read_json_stage('s3_stage', 'events.jsonl', table='events')
        """
        stage_path = stage_file_uri(stage_name, filename)
        return self.read_json(
            filepath_or_buffer=stage_path,
            table=table,
            lines=lines,
            orient=orient,
            compression=compression,
            **kwargs,
        )

    def read_parquet_stage(
        self,
        stage_name: str,
        filename: str,
        table: Union[str, type],
        **kwargs,
    ):
        """
        Read Parquet file from external stage into MatrixOne table (pandas-style convenience method).

        Args:
            stage_name (str): Name of the external stage
            filename (str): Filename within the stage
            table (str or Model): Table name or SQLAlchemy model class

        Returns:
            ResultSet: Load results

        Examples::

            client.load_data.read_parquet_stage('s3_stage', 'data.parquet', table='users')
        """
        stage_path = stage_file_uri(stage_name, filename)
        return self.read_parquet(filepath_or_buffer=stage_path, table=table, **kwargs)

    def read_json_inline(
        self,
        data: str,
        table: Union[str, type],
        orient: str = 'records',
        **kwargs,
    ):
        """
        Read JSON data from inline string into MatrixOne table (pandas-style).

        Args:
            data (str): JSON data string
            table (str or Model): Table name or SQLAlchemy model class
            orient (str): JSON structure. Default: 'records'

        Returns:
            ResultSet: Load results

        Examples::

            json_data = '{"id":1,"name":"Alice"}\\n'
            client.load_data.read_json_inline(json_data, table='users')
        """
        # Handle model class input
        if hasattr(table, '__tablename__'):
            table_name = table.__tablename__
        else:
            table_name = table

        # Map orient to jsontype
        jsontype_str = _json_structure(orient)

        # Build SQL
        sql = self._build_load_data_inline_sql(
            data=data,
            table_name=table_name,
            format='jsonline',
            jsontype=jsontype_str,
            **self._inline_options(kwargs),
        )

        return self._get_executor().execute(sql)

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

            set_clause (dict, optional): NULLIF(target_column, literal) assignments; row variables are unsupported.
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

        if kwargs:
            raise ValueError('Unknown LOAD options: ' + ', '.join(sorted(kwargs)))
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

            # Default SDK connections authorize only this file for this statement.
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

        from ._session_execution import _LiteralSQL

        statement = _LiteralSQL(sql).execution_options(_matrixone_local_path=file_path)
        return self._get_executor().execute(statement)


class AsyncLoadDataManager(BaseLoadDataManager):
    """
    Asynchronous data loading manager for MatrixOne bulk data operations.

    Provides the same comprehensive bulk data loading functionality as LoadDataManager
    but with full async/await support for non-blocking I/O operations. Ideal for
    high-concurrency applications and async web frameworks requiring data loading.

    Key Features:

    - **Non-blocking operations**: All load operations use async/await
    - **Async file loading**: Load data files without blocking the event loop
    - **Concurrent loading**: Load multiple files concurrently
    - **Multiple formats**: CSV, TSV, JSON, JSONLines, Parquet (async)
    - **Async stage integration**: Load from external stages asynchronously
    - **Transaction-aware**: Full integration with async transaction contexts
    - **Executor pattern**: Works with both async client and async session

    Executor Pattern:

    - If executor is None, uses self.client.execute (default async client-level executor)
    - If executor is provided (e.g., async session), uses executor.execute (async transaction-aware)
    - All operations are non-blocking and use async/await
    - Enables concurrent data loading operations

    Usage Examples::

        from matrixone import AsyncClient
        from matrixone.orm import Column, Integer, String, Float, Base
        from sqlalchemy import insert
        import asyncio

        # Define ORM models (recommended approach)
        class User(Base):
            __tablename__ = 'users'
            id = Column(Integer, primary_key=True)
            name = Column(String(100))
            email = Column(String(255))
            age = Column(Integer)

        class Order(Base):
            __tablename__ = 'orders'
            id = Column(Integer, primary_key=True)
            user_id = Column(Integer)
            amount = Column(Float)

        class Product(Base):
            __tablename__ = 'products'
            id = Column(Integer, primary_key=True)
            name = Column(String(200))
            price = Column(Float)

        async def main():
            client = AsyncClient()
            await client.connect(host='localhost', port=6001, user='root', password='111', database='test')

            # ========================================
            # Basic Async CSV Loading with ORM
            # ========================================

            # Load CSV using ORM model (recommended)
            await client.load_data.from_csv('/path/to/users.csv', User)

            # CSV with header row
            await client.load_data.from_csv(
                '/path/to/users.csv',
                User,
                ignore_lines=1  # Skip header
            )

            # Custom delimiter and quote character (async)
            await client.load_data.from_csv(
                '/path/to/data.txt',
                Order,
                delimiter='|',
                enclosed_by='"'
            )

            # ========================================
            # Advanced Loading Options
            # ========================================

            # Parallel async loading (high performance)
            await client.load_data.from_csv(
                '/path/to/large_data.csv',
                User,
                parallel=True
            )

            # Load compressed file asynchronously
            await client.load_data.from_csv(
                '/path/to/data.csv.gz',
                User,
                compression='gzip'
            )

            # Column mapping with transformation
            await client.load_data.from_csv(
                '/path/to/data.csv',
                User,
                columns=['name', 'email', 'age'],
                set_clause={'status': "NULLIF(status, 'null')"}
            )

            # Load JSON Lines format asynchronously
            await client.load_data.from_jsonlines('/path/to/data.jsonl', User)

            # ========================================
            # Load from External Stages
            # ========================================

            # Load from S3 stage using ORM model
            await client.load_data.from_stage_csv('production_s3', 'users.csv', User)

            # Load from local stage
            await client.load_data.from_stage_csv('local_data', 'orders.csv', Order)

            # ========================================
            # Concurrent Loading (High Performance)
            # ========================================

            # Concurrent loading of multiple files using ORM models
            await asyncio.gather(
                client.load_data.from_csv('/path/to/users.csv', User),
                client.load_data.from_csv('/path/to/orders.csv', Order),
                client.load_data.from_csv('/path/to/products.csv', Product)
            )

            # ========================================
            # Transactional Async Loading
            # ========================================

            # Using within async transaction
            async with client.session() as session:
                # Load multiple files atomically using ORM models
                await session.load_data.from_csv('/path/to/users.csv', User)
                await session.load_data.from_csv('/path/to/orders.csv', Order)

                # Insert additional data within same transaction using SQLAlchemy
                await session.execute(insert(User).values(name='Admin', email='admin@example.com'))
                # All operations commit together

            await client.disconnect()

        asyncio.run(main())

    Performance Benefits:

    - **Non-blocking I/O**: Don't block the event loop during file loading
    - **Concurrent loading**: Load multiple files simultaneously
    - **Async web frameworks**: Perfect for FastAPI, aiohttp, etc.
    - **High concurrency**: Handle many load operations concurrently

    Use Cases:

    - **Async ETL pipelines**: Non-blocking data transformation and loading
    - **Real-time data ingestion**: Load data without blocking request handling
    - **Async web services**: Load data in response to API calls
    - **Concurrent batch loading**: Load multiple files in parallel
    - **High-throughput applications**: Maximize I/O throughput

    See Also:

        - AsyncClient: For async database operations
        - AsyncStageManager: For async stage management
        - LoadDataManager: For synchronous data loading
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

    async def read_csv(
        self,
        filepath_or_buffer: str,
        table: Union[str, type],
        sep: str = ",",
        quotechar: Optional[str] = None,
        quoting: bool = False,
        escapechar: Optional[str] = None,
        skiprows: int = 0,
        names: Optional[List[str]] = None,
        encoding: Optional[str] = None,
        parallel: bool = False,
        compression: Optional[Union[str, CompressionFormat]] = None,
        set_clause: Optional[Dict[str, str]] = None,
        inline: bool = False,
        **kwargs,
    ):
        """Async version of read_csv (pandas-style)"""
        self._reject_wrapper_options(kwargs, self._CSV_ALIASES)
        # Handle inline data
        if inline:
            if encoding is not None or parallel or compression is not None or set_clause is not None:
                raise ValueError('Inline CSV does not support encoding, parallel, compression or SET')
            return await self.read_csv_inline(
                data=filepath_or_buffer,
                table=table,
                sep=sep,
                quotechar=quotechar,
                skiprows=skiprows,
                names=names,
                escapechar=escapechar,
                quoting=quoting,
                **kwargs,
            )

        return await self.from_file(
            file_path=filepath_or_buffer,
            table_name_or_model=table,
            fields_terminated_by=sep,
            fields_enclosed_by=quotechar,
            fields_optionally_enclosed=quoting,
            fields_escaped_by=escapechar,
            ignore_lines=skiprows,
            columns=names,
            character_set=encoding,
            parallel=parallel,
            compression=compression,
            set_clause=set_clause,
            **kwargs,
        )

    async def read_json(
        self,
        filepath_or_buffer: str,
        table: Union[str, type],
        lines: bool = True,
        orient: str = 'records',
        compression: Optional[Union[str, CompressionFormat]] = None,
        inline: bool = False,
        **kwargs,
    ):
        """Async version of read_json (pandas-style)"""
        self._reject_wrapper_options(kwargs, self._JSON_ALIASES)
        if lines is not True:
            raise ValueError('Native JSON loading requires lines=True (JSON Lines)')
        # Handle inline data
        if inline:
            return await self.read_json_inline(data=filepath_or_buffer, table=table, orient=orient, **kwargs)

        # Map pandas orient to MatrixOne jsondata
        structure_str = _json_structure(orient)

        compression_str = compression.value if isinstance(compression, CompressionFormat) else compression

        return await self.from_file(
            file_path=filepath_or_buffer,
            table_name_or_model=table,
            format=LoadDataFormat.JSONLINE.value,
            jsondata=structure_str,
            compression=compression_str,
            **kwargs,
        )

    async def read_parquet(self, filepath_or_buffer: str, table: Union[str, type], **kwargs):
        """Async version of read_parquet (pandas-style)"""
        self._reject_wrapper_options(kwargs, self._PARQUET_ALIASES)
        return await self.from_file(
            file_path=filepath_or_buffer, table_name_or_model=table, format=LoadDataFormat.PARQUET.value, **kwargs
        )

    async def read_csv_stage(
        self,
        stage_name: str,
        filename: str,
        table: Union[str, type],
        sep: str = ",",
        quotechar: Optional[str] = None,
        quoting: bool = False,
        escapechar: Optional[str] = None,
        skiprows: int = 0,
        names: Optional[List[str]] = None,
        encoding: Optional[str] = None,
        parallel: bool = False,
        compression: Optional[Union[str, CompressionFormat]] = None,
        set_clause: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        """Async version of read_csv_stage (pandas-style)"""
        stage_path = stage_file_uri(stage_name, filename)
        return await self.read_csv(
            filepath_or_buffer=stage_path,
            table=table,
            sep=sep,
            quotechar=quotechar,
            quoting=quoting,
            escapechar=escapechar,
            skiprows=skiprows,
            names=names,
            encoding=encoding,
            parallel=parallel,
            compression=compression,
            set_clause=set_clause,
            **kwargs,
        )

    async def read_json_stage(
        self,
        stage_name: str,
        filename: str,
        table: Union[str, type],
        lines: bool = True,
        orient: str = 'records',
        compression: Optional[Union[str, CompressionFormat]] = None,
        **kwargs,
    ):
        """Async version of read_json_stage (pandas-style)"""
        stage_path = stage_file_uri(stage_name, filename)
        return await self.read_json(
            filepath_or_buffer=stage_path,
            table=table,
            lines=lines,
            orient=orient,
            compression=compression,
            **kwargs,
        )

    async def read_parquet_stage(
        self,
        stage_name: str,
        filename: str,
        table: Union[str, type],
        **kwargs,
    ):
        """Async version of read_parquet_stage (pandas-style)"""
        stage_path = stage_file_uri(stage_name, filename)
        return await self.read_parquet(filepath_or_buffer=stage_path, table=table, **kwargs)

    async def read_csv_inline(
        self,
        data: str,
        table: Union[str, type],
        sep: str = ",",
        quotechar: Optional[str] = None,
        **kwargs,
    ):
        """Async version of read_csv_inline (pandas-style)"""
        # Handle model class input
        if hasattr(table, '__tablename__'):
            table_name = table.__tablename__
        else:
            table_name = table

        # Build SQL
        sql = self._build_load_data_inline_sql(
            data=data,
            table_name=table_name,
            format='csv',
            delimiter=sep,
            enclosed_by=quotechar,
            **self._inline_options(kwargs),
        )

        return await self._get_executor().execute(sql)

    async def read_json_inline(
        self,
        data: str,
        table: Union[str, type],
        orient: str = 'records',
        **kwargs,
    ):
        """Async version of read_json_inline (pandas-style)"""
        # Handle model class input
        if hasattr(table, '__tablename__'):
            table_name = table.__tablename__
        else:
            table_name = table

        # Map orient to jsontype
        jsontype_str = _json_structure(orient)

        # Build SQL
        sql = self._build_load_data_inline_sql(
            data=data,
            table_name=table_name,
            format='jsonline',
            jsontype=jsontype_str,
            **self._inline_options(kwargs),
        )

        return await self._get_executor().execute(sql)

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

        if kwargs:
            raise ValueError('Unknown LOAD options: ' + ', '.join(sorted(kwargs)))
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

        from ._session_execution import _LiteralSQL

        statement = _LiteralSQL(sql).execution_options(_matrixone_local_path=file_path)
        return await self._get_executor().execute(statement)


def _json_structure(orient):
    if orient == 'records':
        return 'object'
    if orient == 'values':
        return 'array'
    raise ValueError('JSON orient must be records or values')

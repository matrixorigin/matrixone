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

from typing import Optional, List, Dict, Any, Union
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


class LoadDataManager:
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
    
    def __init__(self, client):
        """
        Initialize LoadDataManager.
        
        Args:
            client: Client object that provides execute() method
        """
        self.client = client
    
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
        **kwargs
    ):
        """
        Load CSV data from a file into a table.
        
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
            **kwargs
        )
    
    def from_tsv(
        self,
        file_path: str,
        table_name_or_model,
        ignore_lines: int = 0,
        columns: Optional[List[str]] = None,
        **kwargs
    ):
        """
        Load TSV (Tab-Separated Values) data from a file.
        
        Args:
            file_path (str): Path to the TSV file
            table_name_or_model: Table name (str) or SQLAlchemy model class
            ignore_lines (int): Number of header lines to skip. Default: 0
            columns (list, optional): List of column names to load
            
        Returns:
            ResultSet: Load results with affected_rows
            
        Examples::
        
            >>> client.load_data.from_tsv('data.tsv', 'logs')
            >>> client.load_data.from_tsv('data.tsv', 'logs', ignore_lines=1)
        """
        return self.from_csv(
            file_path=file_path,
            table_name_or_model=table_name_or_model,
            delimiter='\\t',
            ignore_lines=ignore_lines,
            columns=columns,
            **kwargs
        )
    
    def from_jsonline(
        self,
        file_path: str,
        table_name_or_model,
        structure: Union[str, JsonDataStructure] = JsonDataStructure.OBJECT,
        compression: Optional[Union[str, CompressionFormat]] = None,
        **kwargs
    ):
        """
        Load JSONLINE data from a file.
        
        Args:
            file_path (str): Path to the JSONLINE file
            table_name_or_model: Table name (str) or SQLAlchemy model class
            structure (str or JsonDataStructure): JSON structure. Default: JsonDataStructure.OBJECT
                - JsonDataStructure.OBJECT or 'object': JSON objects
                - JsonDataStructure.ARRAY or 'array': JSON arrays
            compression (str or CompressionFormat, optional): Compression format
            
        Returns:
            ResultSet: Load results with affected_rows
            
        Examples::
        
            # JSONLINE with objects
            >>> client.load_data.from_jsonline('data.jl', 'users')
            
            # JSONLINE with arrays
            >>> client.load_data.from_jsonline('data.jl', 'users', 
            ...     structure=JsonDataStructure.ARRAY)
            
            # Compressed JSONLINE
            >>> client.load_data.from_jsonline('data.jl.gz', 'users', 
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
            **kwargs
        )
    
    def from_parquet(
        self,
        file_path: str,
        table_name_or_model,
        **kwargs
    ):
        """
        Load Parquet data from a file.
        
        Args:
            file_path (str): Path to the Parquet file
            table_name_or_model: Table name (str) or SQLAlchemy model class
            
        Returns:
            ResultSet: Load results with affected_rows
            
        Examples::
        
            >>> client.load_data.from_parquet('data.parq', 'users')
            >>> client.load_data.from_parquet('data.parquet', 'sales')
        """
        return self.from_file(
            file_path=file_path,
            table_name_or_model=table_name_or_model,
            format=LoadDataFormat.PARQUET.value,
            **kwargs
        )
    
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
        **kwargs
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
            set_clause=set_clause
        )
        
        # Execute the LOAD DATA statement
        return self.client.execute(sql)
    
    def from_local_file(
        self,
        file_path: str,
        table_name_or_model,
        **kwargs
    ):
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
        sql = self._build_load_data_sql(
            file_path=file_path,
            table_name=table_name,
            local=True,
            **kwargs
        )
        
        return self.client.execute(sql)
    
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
        set_clause: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Build the LOAD DATA SQL statement.
        
        Args:
            See from_file() for parameter descriptions
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
                local=local
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
                set_clause=set_clause
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
        set_clause: Optional[Dict[str, str]] = None
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
        local: bool = False
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


class TransactionLoadDataManager(LoadDataManager):
    """
    Load Data Manager for transaction context.
    
    This manager executes LOAD DATA operations within a transaction,
    providing atomicity for bulk data loading operations.
    
    Examples::
    
        # Load data within transaction
        with client.transaction() as tx:
            tx.load_data.from_file('/path/to/data1.csv', 'table1')
            tx.load_data.from_file('/path/to/data2.csv', 'table2')
            # Both loads succeed or both roll back
    """
    
    def __init__(self, transaction_wrapper):
        """
        Initialize TransactionLoadDataManager.
        
        Args:
            transaction_wrapper: TransactionWrapper instance
        """
        self.transaction_wrapper = transaction_wrapper
        super().__init__(transaction_wrapper)


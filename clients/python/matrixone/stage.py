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
Stage Management for MatrixOne

This module provides comprehensive stage management capabilities for MatrixOne,
including creating, modifying, and querying external storage locations (stages).
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Union


@dataclass
class Stage:
    """
    Represents a MatrixOne external stage.
    
    A stage is a named external storage location (filesystem, S3, cloud storage)
    that can be used to load data from or export data to.
    
    Attributes:
        name: Stage name
        url: Storage location URL (file://, s3://, etc.)
        credentials: Authentication credentials (for S3, cloud storage)
        status: Stage status (enabled/disabled)
        comment: Optional description
        created_time: When the stage was created
        modified_time: When the stage was last modified
    """
    
    name: str
    url: str
    credentials: Optional[Dict[str, str]] = None
    status: Optional[str] = None
    comment: Optional[str] = None
    created_time: Optional[datetime] = None
    modified_time: Optional[datetime] = None
    _client: Optional[Any] = field(default=None, repr=False)  # Reference to client for load_data
    
    def __repr__(self):
        return f"Stage(name='{self.name}', url='{self.url}', status='{self.status}')"
    
    def load_csv(self, filepath: str, table: Any, **kwargs) -> Any:
        """
        Load CSV file from this stage.
        
        Args:
            filepath: File path relative to stage location
            table: SQLAlchemy table model or table name
            **kwargs: Additional load options
            
        Returns:
            ResultSet with load operation results
            
        Examples:
            >>> stage = client.stage.get('my_stage')
            >>> result = stage.load_csv('users.csv', User)
            >>> print(f"Loaded {result.affected_rows} rows")
        """
        if self._client is None:
            raise ValueError("Stage object must be associated with a client to load data")
        from .load_data import LoadDataManager
        return LoadDataManager(self._client).from_stage_csv(self.name, filepath, table, **kwargs)
    
    def load_tsv(self, filepath: str, table: Any, **kwargs) -> Any:
        """Load TSV file from this stage"""
        if self._client is None:
            raise ValueError("Stage object must be associated with a client to load data")
        from .load_data import LoadDataManager
        return LoadDataManager(self._client).from_stage_tsv(self.name, filepath, table, **kwargs)
    
    def load_json(self, filepath: str, table: Any, **kwargs) -> Any:
        """Load JSONLINE file from this stage"""
        if self._client is None:
            raise ValueError("Stage object must be associated with a client to load data")
        from .load_data import LoadDataManager
        return LoadDataManager(self._client).from_stage_jsonline(self.name, filepath, table, **kwargs)
    
    def load_parquet(self, filepath: str, table: Any, **kwargs) -> Any:
        """Load Parquet file from this stage"""
        if self._client is None:
            raise ValueError("Stage object must be associated with a client to load data")
        from .load_data import LoadDataManager
        return LoadDataManager(self._client).from_stage_parquet(self.name, filepath, table, **kwargs)
    
    def load_files(self, file_mappings: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """
        Load multiple files from this stage in batch.
        
        Args:
            file_mappings: Dictionary mapping file paths to table models
            **kwargs: Additional parameters for load operations
            
        Returns:
            Dictionary mapping file paths to ResultSet objects
            
        Examples:
            >>> stage = client.stage.get('my_stage')
            >>> results = stage.load_files({
            ...     'users.csv': User,
            ...     'events.jsonl': Event,
            ...     'orders.parq': Order
            ... })
            >>> print(f"Loaded {results['users.csv'].affected_rows} users")
        """
        if self._client is None:
            raise ValueError("Stage object must be associated with a client to load data")
        
        results = {}
        for filepath, table_model in file_mappings.items():
            try:
                # Auto-detect file type by extension
                if filepath.endswith('.csv'):
                    results[filepath] = self.load_csv(filepath, table_model, **kwargs)
                elif filepath.endswith('.tsv'):
                    results[filepath] = self.load_tsv(filepath, table_model, **kwargs)
                elif filepath.endswith(('.jsonl', '.json')):
                    results[filepath] = self.load_json(filepath, table_model, **kwargs)
                elif filepath.endswith(('.parquet', '.parq')):
                    results[filepath] = self.load_parquet(filepath, table_model, **kwargs)
                else:
                    # Fallback to generic file loading
                    from .load_data import LoadDataManager
                    results[filepath] = LoadDataManager(self._client).from_stage(
                        self.name, filepath, table_model, **kwargs
                    )
            except Exception as e:
                # Store error in results for debugging
                results[filepath] = f"Error: {e}"
        
        return results


class StageManager:
    """
    Manager for MatrixOne Stage operations.
    
    A Stage is a named external storage location (like S3, filesystem, or cloud storage)
    that stores data files. Stages provide centralized, secure, and reusable access
    to external data sources.
    
    Key Concepts:
    =============
    
    What is a Stage?
    ----------------
    A stage is a pointer to an external storage location where:
    - Data files are stored (CSV, JSONLINE, Parquet, etc.)
    - Files can be loaded into tables
    - Query results can be exported
    
    Stage Types:
    ------------
    1. **File System Stage**: Local or network filesystem
       >>> CREATE STAGE fs_stage URL='file:///data/files/'
    
    2. **S3 Stage**: Amazon S3 or compatible (MinIO, etc.)
       >>> CREATE STAGE s3_stage URL='s3://bucket/path/'
       ... CREDENTIALS={'AWS_KEY_ID'='xxx', 'AWS_SECRET_KEY'='xxx'}
    
    3. **Sub-Stage**: Stage based on another stage
       >>> CREATE STAGE sub_stage URL='stage://parent_stage/subdir/'
    
    Why Use Stages?
    ---------------
    - **Centralized**: One location for all data files
    - **Reusable**: Create once, use for multiple tables
    - **Secure**: Credentials managed at stage level
    - **Efficient**: Direct loading from cloud storage
    - **Organized**: Logical grouping of related data files
    
    Typical Workflow:
    -----------------
    1. Create a stage pointing to your data location:
       >>> stage_mgr.create('my_stage', 'file:///data/')
    
    2. Upload files to that location (outside MatrixOne)
    
    3. Load data from stage:
       >>> client.load_data.from_stage_csv('my_stage', 'users.csv', User)
    
    4. Reuse the same stage for other files:
       >>> client.load_data.from_stage_csv('my_stage', 'orders.csv', Order)
    
    This class is typically accessed through the Client's `stage` property:
    
    Examples::
    
        # Create a file system stage
        client.stage.create('my_stage', 'file:///path/to/data/')
        
        # Create an S3 stage
        client.stage.create(
            's3_stage',
            's3://my-bucket/data/',
            credentials={
                'AWS_KEY_ID': 'AKIAIOSFODNN7EXAMPLE',
                'AWS_SECRET_KEY': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                'AWS_REGION': 'us-east-1'
            }
        )
        
        # List all stages
        stages = client.stage.list()
        for stage in stages:
            print(f"{stage.name}: {stage.url}")
        
        # Load data from stage
        client.load_data.from_stage_csv('my_stage', 'data.csv', User)
    """
    
    def __init__(self, client):
        """
        Initialize StageManager.
        
        Args:
            client: Client object that provides execute() method
        """
        self.client = client
    
    def create(
        self,
        name: str,
        url: str,
        credentials: Optional[Dict[str, str]] = None,
        comment: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> Stage:
        """
        Create a new external stage.
        
        Args:
            name (str): Stage name
            url (str): Storage location URL
                Supported protocols:
                - file:///path/to/data/ - Local or network filesystem
                - s3://bucket/path/ - Amazon S3
                - stage://parent_stage/subdir/ - Sub-stage
            credentials (dict, optional): Authentication credentials
                For S3/cloud storage:
                {
                    'AWS_KEY_ID': 'your_key_id',
                    'AWS_SECRET_KEY': 'your_secret_key',
                    'AWS_REGION': 'us-east-1',  # optional
                    'PROVIDER': 'minio'  # optional: minio, aws, etc.
                }
            comment (str, optional): Stage description
            if_not_exists (bool): If True, don't error if stage already exists
        
        Returns:
            Stage: Created stage object
        
        Examples:
            >>> # File system stage
            >>> stage = client.stage.create(
            ...     'local_data',
            ...     'file:///var/data/imports/'
            ... )
            
            >>> # S3 stage with credentials
            >>> stage = client.stage.create(
            ...     'production_data',
            ...     's3://my-bucket/data/',
            ...     credentials={
                    'AWS_KEY_ID': 'AKIAIOSFODNN7EXAMPLE',
            ...         'AWS_SECRET_KEY': 'wJalrXUtnFEMI/...',
            ...         'AWS_REGION': 'us-east-1'
            ...     },
            ...     comment='Production data stage'
            ... )
            
            >>> # Sub-stage (stage based on another stage)
            >>> stage = client.stage.create(
            ...     'archive_2024',
            ...     'stage://production_data/archive/2024/'
            ... )
        """
        # Build CREATE STAGE SQL
        sql_parts = ["CREATE STAGE"]
        
        if if_not_exists:
            sql_parts.append("IF NOT EXISTS")
        
        sql_parts.append(name)
        sql_parts.append(f"URL='{url}'")
        
        # Add credentials if provided
        if credentials:
            cred_pairs = [f"'{k}'='{v}'" for k, v in credentials.items()]
            sql_parts.append(f"CREDENTIALS={{{', '.join(cred_pairs)}}}")
        
        # Add comment if provided
        if comment:
            sql_parts.append(f"COMMENT='{comment}'")
        
        sql = " ".join(sql_parts)
        self.client.execute(sql)
        
        # Return Stage object with client reference
        return Stage(
            name=name,
            url=url,
            credentials=credentials,
            status='enabled',
            comment=comment,
            created_time=datetime.now(),
            _client=self.client,
        )
    
    def alter(
        self,
        name: str,
        url: Optional[str] = None,
        credentials: Optional[Dict[str, str]] = None,
        comment: Optional[str] = None,
        enable: Optional[bool] = None,
        if_exists: bool = False,
    ) -> Stage:
        """
        Modify an existing stage.
        
        You can update URL, credentials, comment, or enable/disable the stage.
        At least one of url, credentials, comment, or enable must be provided.
        
        Args:
            name (str): Stage name to modify
            url (str, optional): New storage location URL
            credentials (dict, optional): New authentication credentials
            comment (str, optional): New comment/description
            enable (bool, optional): Enable (True) or disable (False) the stage
            if_exists (bool): If True, don't error if stage doesn't exist
        
        Returns:
            Stage: Updated stage object
        
        Examples:
            >>> # Update URL
            >>> client.stage.alter('my_stage', url='s3://new-bucket/data/')
            
            >>> # Update credentials
            >>> client.stage.alter(
            ...     's3_stage',
            ...     credentials={'AWS_KEY_ID': 'new_key'}
            ... )
            
            >>> # Update comment
            >>> client.stage.alter('my_stage', comment='Updated description')
            
            >>> # Disable stage
            >>> client.stage.alter('my_stage', enable=False)
            
            >>> # Update multiple properties
            >>> client.stage.alter(
            ...     'my_stage',
            ...     url='s3://new-bucket/',
            ...     comment='Migrated to new bucket',
            ...     enable=True
            ... )
        """
        if not any([url, credentials, comment, enable is not None]):
            raise ValueError("At least one of url, credentials, comment, or enable must be provided")
        
        # Build ALTER STAGE SQL
        sql_parts = ["ALTER STAGE"]
        
        if if_exists:
            sql_parts.append("IF EXISTS")
        
        sql_parts.append(name)
        sql_parts.append("SET")
        
        set_clauses = []
        
        if url is not None:
            set_clauses.append(f"URL='{url}'")
        
        if credentials is not None:
            cred_pairs = [f"'{k}'='{v}'" for k, v in credentials.items()]
            set_clauses.append(f"CREDENTIALS={{{', '.join(cred_pairs)}}}")
        
        if comment is not None:
            set_clauses.append(f"COMMENT='{comment}'")
        
        if enable is not None:
            set_clauses.append(f"ENABLE={'TRUE' if enable else 'FALSE'}")
        
        # Join with space for ALTER STAGE SET syntax
        sql = " ".join(sql_parts) + " " + " ".join(set_clauses)
        self.client.execute(sql)
        
        # Return updated Stage object (fetch from database to get current state)
        return self.get(name)
    
    def drop(self, name: str, if_exists: bool = False) -> None:
        """
        Delete an external stage.
        
        Args:
            name (str): Stage name to delete
            if_exists (bool): If True, don't error if stage doesn't exist
        
        Examples:
            >>> # Drop stage
            >>> client.stage.drop('my_stage')
            
            >>> # Drop with IF EXISTS
            >>> client.stage.drop('my_stage', if_exists=True)
        """
        sql_parts = ["DROP STAGE"]
        
        if if_exists:
            sql_parts.append("IF EXISTS")
        
        sql_parts.append(name)
        sql = " ".join(sql_parts)
        
        self.client.execute(sql)
    
    def show(self, like_pattern: Optional[str] = None) -> List[Stage]:
        """
        Show stages with optional pattern matching.
        
        Args:
            like_pattern (str, optional): SQL LIKE pattern for filtering stages
                Example: 'my_stage%', '%prod%'
        
        Returns:
            List[Stage]: List of matching stages
        
        Examples:
            >>> # Show all stages
            >>> stages = client.stage.show()
            
            >>> # Show stages matching pattern
            >>> stages = client.stage.show(like_pattern='prod%')
            >>> for stage in stages:
            ...     print(f"{stage.name}: {stage.url}")
        """
        sql = "SHOW STAGES"
        
        if like_pattern:
            sql += f" LIKE '{like_pattern}'"
        
        result = self.client.execute(sql)
        
        stages = []
        for row in result.rows:
            # Parse SHOW STAGES output
            # Expected columns: stage_name, url, status, comment, created_time, modified_time
            stage = Stage(
                name=row[0],
                url=row[1] if len(row) > 1 else None,
                status=row[2] if len(row) > 2 else None,
                comment=row[3] if len(row) > 3 else None,
                created_time=row[4] if len(row) > 4 else None,
                _client=self.client,
            )
            stages.append(stage)
        
        return stages
    
    def list(self) -> List[Stage]:
        """
        List all stages by querying mo_catalog.mo_stages system table.
        
        Returns:
            List[Stage]: List of all stages
        
        Examples:
            >>> # Get all stages
            >>> stages = client.stage.list()
            >>> for stage in stages:
            ...     print(f"{stage.name}: {stage.url}")
        """
        sql = "SELECT stage_name, url, stage_status, comment, created_time FROM mo_catalog.mo_stages"
        result = self.client.execute(sql)
        
        stages = []
        for row in result.rows:
            stage = Stage(
                name=row[0],
                url=row[1] if len(row) > 1 else None,
                status=row[2] if len(row) > 2 else None,
                comment=row[3] if len(row) > 3 else None,
                created_time=row[4] if len(row) > 4 else None,
                _client=self.client,
            )
            stages.append(stage)
        
        return stages
    
    def get(self, name: str) -> Stage:
        """
        Get details of a specific stage.
        
        Args:
            name (str): Stage name
        
        Returns:
            Stage: Stage object with details
        
        Raises:
            ValueError: If stage not found
        
        Examples:
            >>> stage = client.stage.get('my_stage')
            >>> print(f"URL: {stage.url}")
            >>> print(f"Status: {stage.status}")
        """
        # Query from system table for accurate information
        sql = f"SELECT stage_name, url, stage_status, comment, created_time FROM mo_catalog.mo_stages WHERE stage_name = '{name}'"
        result = self.client.execute(sql)
        
        if not result.rows:
            raise ValueError(f"Stage '{name}' not found")
        
        row = result.rows[0]
        return Stage(
            name=row[0],
            url=row[1] if len(row) > 1 else None,
            status=row[2] if len(row) > 2 else None,
            comment=row[3] if len(row) > 3 else None,
            created_time=row[4] if len(row) > 4 else None,
            _client=self.client,
        )
    
    def exists(self, name: str) -> bool:
        """
        Check if a stage exists.
        
        Args:
            name (str): Stage name
        
        Returns:
            bool: True if stage exists, False otherwise
        
        Examples:
            >>> if client.stage.exists('my_stage'):
            ...     print("Stage exists")
            ... else:
            ...     print("Stage does not exist")
        """
        try:
            self.get(name)
            return True
        except ValueError:
            return False
    
    def create_local(self, name: str, path: str, comment: Optional[str] = None, 
                     if_not_exists: bool = False) -> "Stage":
        """
        Create a local file system stage with simplified path handling.
        
        Args:
            name: Stage name
            path: Local file system path (supports ~, relative, absolute paths)
            comment: Optional comment
            if_not_exists: Create only if not exists
            
        Returns:
            Created Stage object
            
        Examples:
            >>> # Relative path
            >>> stage = client.stage.create_local('data', './data/')
            
            >>> # User home directory
            >>> stage = client.stage.create_local('home', '~/data/')
            
            >>> # Absolute path
            >>> stage = client.stage.create_local('data', '/var/data/')
        """
        import os
        
        # Expand user home directory
        if path.startswith('~'):
            path = os.path.expanduser(path)
        
        # Convert to absolute path
        path = os.path.abspath(path)
        
        # Ensure path ends with /
        if not path.endswith('/'):
            path += '/'
        
        # Add file:// prefix
        url = f'file://{path}'
        
        return self.create(name, url, comment=comment, if_not_exists=if_not_exists)
    
    def create_s3(self, name: str, bucket_path: str, 
                  aws_key: Optional[str] = None, aws_secret: Optional[str] = None,
                  aws_region: Optional[str] = None, profile: Optional[str] = None,
                  comment: Optional[str] = None, if_not_exists: bool = False) -> "Stage":
        """
        Create an S3 stage with simplified AWS credentials.
        
        Args:
            name: Stage name
            bucket_path: S3 bucket and path (e.g., 'my-bucket/data/')
            aws_key: AWS access key (optional, can use environment variables)
            aws_secret: AWS secret key (optional, can use environment variables)
            aws_region: AWS region (optional, can use environment variables)
            profile: AWS profile name (optional)
            comment: Optional comment
            if_not_exists: Create only if not exists
            
        Returns:
            Created Stage object
            
        Examples:
            >>> # Using explicit credentials
            >>> stage = client.stage.create_s3('prod', 'my-bucket/data/',
            ...                                aws_key='AKIA...', aws_secret='...')
            
            >>> # Using environment variables
            >>> stage = client.stage.create_s3('prod', 'my-bucket/data/')
            
            >>> # Using AWS profile
            >>> stage = client.stage.create_s3('prod', 'my-bucket/data/', profile='default')
        """
        import os
        
        # Build S3 URL
        if not bucket_path.startswith('s3://'):
            bucket_path = f's3://{bucket_path}'
        
        credentials = {}
        
        # Handle AWS credentials
        if aws_key and aws_secret:
            credentials['AWS_KEY_ID'] = aws_key
            credentials['AWS_SECRET_KEY'] = aws_secret
            if aws_region:
                credentials['AWS_REGION'] = aws_region
        elif profile:
            # Use AWS profile (MatrixOne will handle this)
            credentials['AWS_PROFILE'] = profile
        else:
            # Try environment variables
            aws_key = os.getenv('AWS_ACCESS_KEY_ID') or os.getenv('AWS_KEY_ID')
            aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY') or os.getenv('AWS_SECRET_KEY')
            aws_region = os.getenv('AWS_DEFAULT_REGION') or os.getenv('AWS_REGION')
            
            if aws_key and aws_secret:
                credentials['AWS_KEY_ID'] = aws_key
                credentials['AWS_SECRET_KEY'] = aws_secret
                if aws_region:
                    credentials['AWS_REGION'] = aws_region
        
        return self.create(name, bucket_path, credentials=credentials, 
                          comment=comment, if_not_exists=if_not_exists)


class TransactionStageManager(StageManager):
    """
    Stage Manager for transaction context.
    
    This manager executes stage operations within a transaction.
    
    Examples::
    
        # Create and use stage within transaction
        with client.transaction() as tx:
            tx.stage.create('temp_stage', 'file:///tmp/data/')
            tx.load_data.from_stage_csv('temp_stage', 'data.csv', User)
    """
    
    pass  # Inherits all methods from StageManager


class AsyncStageManager(StageManager):
    """
    Async manager for MatrixOne Stage operations.
    
    This class extends StageManager to provide async/await support.
    All SQL building logic is inherited - only execute() calls are async.
    
    Examples::
    
        # Create stage asynchronously
        stage = await client.stage.create('my_stage', 'file:///data/')
        
        # List stages
        stages = await client.stage.list()
        
        # Load data from stage
        await stage.load_data.from_csv('users.csv', User)
    """
    
    async def create(self, *args, **kwargs):
        """Async version of create"""
        # Build SQL using parent logic
        sql = self._build_create_sql(*args, **kwargs)
        await self.client.execute(sql)
        
        # Build and return Stage object
        name = args[0] if args else kwargs.get('name')
        url = args[1] if len(args) > 1 else kwargs.get('url')
        credentials = args[2] if len(args) > 2 else kwargs.get('credentials')
        comment = kwargs.get('comment')
        
        return Stage(
            name=name,
            url=url,
            credentials=credentials,
            status='enabled',
            comment=comment,
            created_time=datetime.now(),
            _client=self.client,
        )
    
    def _build_create_sql(self, name, url, credentials=None, comment=None, if_not_exists=False):
        """Build CREATE STAGE SQL"""
        sql_parts = ["CREATE STAGE"]
        
        if if_not_exists:
            sql_parts.append("IF NOT EXISTS")
        
        sql_parts.append(name)
        sql_parts.append(f"URL='{url}'")
        
        if credentials:
            cred_pairs = [f"'{k}'='{v}'" for k, v in credentials.items()]
            sql_parts.append(f"CREDENTIALS={{{', '.join(cred_pairs)}}}")
        
        if comment:
            sql_parts.append(f"COMMENT='{comment}'")
        
        return " ".join(sql_parts)
    
    async def alter(self, *args, **kwargs):
        """Async version of alter"""
        sql = self._build_alter_sql(*args, **kwargs)
        await self.client.execute(sql)
        
        # Return updated Stage
        name = args[0] if args else kwargs.get('name')
        return await self.get(name)
    
    def _build_alter_sql(self, name, url=None, credentials=None, comment=None, enable=None, if_exists=False):
        """Build ALTER STAGE SQL"""
        if not any([url, credentials, comment, enable is not None]):
            raise ValueError("At least one of url, credentials, comment, or enable must be provided")
        
        sql_parts = ["ALTER STAGE"]
        
        if if_exists:
            sql_parts.append("IF EXISTS")
        
        sql_parts.append(name)
        sql_parts.append("SET")
        
        set_clauses = []
        
        if url is not None:
            set_clauses.append(f"URL='{url}'")
        
        if credentials is not None:
            cred_pairs = [f"'{k}'='{v}'" for k, v in credentials.items()]
            set_clauses.append(f"CREDENTIALS={{{', '.join(cred_pairs)}}}")
        
        if comment is not None:
            set_clauses.append(f"COMMENT='{comment}'")
        
        if enable is not None:
            set_clauses.append(f"ENABLE={'TRUE' if enable else 'FALSE'}")
        
        return " ".join(sql_parts) + " " + " ".join(set_clauses)
    
    async def drop(self, name, if_exists=False):
        """Async version of drop"""
        sql = self._build_drop_sql(name, if_exists)
        await self.client.execute(sql)
    
    def _build_drop_sql(self, name, if_exists=False):
        """Build DROP STAGE SQL"""
        sql_parts = ["DROP STAGE"]
        
        if if_exists:
            sql_parts.append("IF EXISTS")
        
        sql_parts.append(name)
        return " ".join(sql_parts)
    
    async def show(self, like_pattern=None):
        """Async version of show"""
        sql = "SHOW STAGES"
        
        if like_pattern:
            sql += f" LIKE '{like_pattern}'"
        
        result = await self.client.execute(sql)
        
        stages = []
        for row in result.rows:
            stage = Stage(
                name=row[0],
                url=row[1] if len(row) > 1 else None,
                status=row[2] if len(row) > 2 else None,
                comment=row[3] if len(row) > 3 else None,
                created_time=row[4] if len(row) > 4 else None,
                _client=self.client,
            )
            stages.append(stage)
        
        return stages
    
    async def list(self):
        """Async version of list"""
        sql = "SELECT stage_name, url, stage_status, comment, created_time FROM mo_catalog.mo_stages"
        result = await self.client.execute(sql)
        
        stages = []
        for row in result.rows:
            stage = Stage(
                name=row[0],
                url=row[1] if len(row) > 1 else None,
                status=row[2] if len(row) > 2 else None,
                comment=row[3] if len(row) > 3 else None,
                created_time=row[4] if len(row) > 4 else None,
                _client=self.client,
            )
            stages.append(stage)
        
        return stages
    
    async def get(self, name):
        """Async version of get"""
        sql = f"SELECT stage_name, url, stage_status, comment, created_time FROM mo_catalog.mo_stages WHERE stage_name = '{name}'"
        result = await self.client.execute(sql)
        
        if not result.rows:
            raise ValueError(f"Stage '{name}' not found")
        
        row = result.rows[0]
        return Stage(
            name=row[0],
            url=row[1] if len(row) > 1 else None,
            status=row[2] if len(row) > 2 else None,
            comment=row[3] if len(row) > 3 else None,
            created_time=row[4] if len(row) > 4 else None,
            modified_time=row[5] if len(row) > 5 else None,
            _client=self.client,
        )
    
    async def exists(self, name):
        """Async version of exists"""
        try:
            await self.get(name)
            return True
        except ValueError:
            return False


class AsyncTransactionStageManager(AsyncStageManager):
    """
    Async Stage Manager for transaction context.
    
    Executes stage operations within an async transaction.
    
    Examples::
    
        async with client.transaction() as tx:
            await tx.stage.create('temp_stage', 'file:///tmp/')
            await tx.load_data.from_stage_csv('temp_stage', 'data.csv', User)
    """
    
    pass  # Inherits all async methods


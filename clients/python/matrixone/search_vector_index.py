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
SearchVectorIndex - A Pinecone-compatible vector search interface for MatrixOne

This module provides a high-level interface for vector search operations that is
compatible with Pinecone's API, making it easy to migrate from Pinecone to MatrixOne.

Key Features:
- Pinecone-compatible API for seamless migration
- Support for both IVF and HNSW vector indexes
- Metadata filtering with complex query syntax
- Vector upsert and delete operations (IVF only)
- Synchronous and asynchronous operation support
- Automatic index type detection and configuration

Supported Operations:
- Vector similarity search with multiple distance metrics
- Metadata filtering with Pinecone-compatible syntax
- Vector upsert (insert/update) operations
- Vector deletion by ID
- Index statistics and information

Index Types:
- IVF (Inverted File): Supports full CRUD operations, good for frequent updates
- HNSW (Hierarchical Navigable Small World): Read-only, optimized for search performance

Usage Example:

    # Get a Pinecone-compatible index
    index = client.get_pinecone_index("my_table", "embedding_column")

    # Query vectors with metadata filtering
    results = index.query(
        vector=[0.1, 0.2, 0.3, ...],
        top_k=10,
        include_metadata=True,
        filter={"category": "technology", "price": {"$gte": 100}}
    )

    # Process results
    for match in results.matches:
        print(f"ID: {match.id}, Score: {match.score}")
        print(f"Metadata: {match.metadata}")

    # Upsert vectors (IVF index only)
    index.upsert([
        {"id": "doc1", "embedding": [0.1, 0.2, ...], "title": "Document 1"},
        {"id": "doc2", "embedding": [0.3, 0.4, ...], "title": "Document 2"}
    ])

    # Delete vectors (IVF index only)
    index.delete(["doc1", "doc2"])
"""

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class VectorMatch:
    """
    Represents a single vector search match result.

    Attributes:

        id: Unique identifier for the vector (primary key value as string)
        score: Similarity score (lower is more similar for L2 distance)
        metadata: Dictionary containing all metadata fields from the table
        values: Optional vector values if include_values=True in query
    """

    id: str
    score: float
    metadata: Dict[str, Any]
    values: Optional[List[float]] = None


@dataclass
class QueryResponse:
    """
    Represents a query response compatible with Pinecone API.

    Attributes:

        matches: List of VectorMatch objects containing search results
        namespace: Namespace identifier (empty string for MatrixOne)
        usage: Optional usage statistics (e.g., {"read_units": 10})
    """

    matches: List[VectorMatch]
    namespace: str = ""
    usage: Optional[Dict[str, Any]] = None


class PineconeCompatibleIndex:
    """
    A Pinecone-compatible vector search interface for MatrixOne.

    This class provides a high-level interface for vector search operations
    that is compatible with Pinecone's API, making it easy to migrate from
    Pinecone to MatrixOne.

    Features:
    - Vector similarity search with multiple distance metrics (L2, cosine, inner product)
    - Metadata filtering with Pinecone-compatible filter syntax
    - Vector upsert and delete operations (IVF index only)
    - Support for both synchronous and asynchronous operations
    - Automatic index type detection (IVF/HNSW)
    - Case-insensitive column name handling

    Supported Index Types:
    - IVF (Inverted File): Supports upsert/delete operations, good for frequent updates
    - HNSW (Hierarchical Navigable Small World): Read-only, optimized for search performance

    Example:

        # Get a Pinecone-compatible index
        index = client.get_pinecone_index("my_table", "embedding_column")

        # Query vectors
        results = index.query(
            vector=[0.1, 0.2, 0.3, ...],
            top_k=10,
            include_metadata=True,
            filter={"category": "technology", "price": {"$gte": 100}}
        )

        # Upsert vectors (IVF index only)
        index.upsert([
            {"id": "doc1", "embedding": [0.1, 0.2, ...], "title": "Document 1"},
            {"id": "doc2", "embedding": [0.3, 0.4, ...], "title": "Document 2"}
        ])

        # Delete vectors (IVF index only)
        index.delete(["doc1", "doc2"])
    """

    def __init__(self, client, table_name: str, vector_column: str):
        """
        Initialize PineconeCompatibleIndex.

        Args:

            client: MatrixOne client instance (Client or AsyncClient)
            table_name: Name of the table containing vectors
            vector_column: Name of the vector column containing embeddings

        Note:

            The table must already exist and contain a vector column.
            The primary key column will be automatically detected.
            Metadata columns are all non-primary-key, non-vector columns.
        """
        self.client = client
        self.table_name = table_name
        self.vector_column = vector_column
        self._index_info = None
        self._metadata_columns = None  # Will be auto-detected
        self._id_column = None  # Will be auto-detected as primary key

    @property
    def metadata_columns(self):
        """Get metadata columns (all columns except id and vector columns)"""
        # Check if this is an async client
        if hasattr(self.client, "execute") and hasattr(self.client.execute, "__call__"):
            import asyncio

            if asyncio.iscoroutinefunction(self.client.execute):
                raise RuntimeError("Use _get_metadata_columns_async() for async clients")
        return self._get_metadata_columns()

    def _get_id_column(self):
        """Get the primary key column name"""
        if self._id_column is not None:
            return self._id_column

        # Check if this is an async client
        if hasattr(self.client, "execute") and hasattr(self.client.execute, "__call__"):
            # Check if execute returns a coroutine (async client)
            import asyncio

            if asyncio.iscoroutinefunction(self.client.execute):
                raise RuntimeError("Use _get_id_column_async() for async clients")

        # Get table schema to find primary key
        schema_result = self.client.execute(f"DESCRIBE {self.table_name}")
        if not schema_result.rows:
            # Fallback to 'id' if table not found
            self._id_column = "id"
            return self._id_column

        # Find primary key column
        for row in schema_result.rows:
            column_name = row[0]
            key_info = row[3] if len(row) > 3 else ""  # Key column
            if "PRI" in key_info.upper():
                self._id_column = column_name
                return self._id_column

        # Fallback to 'id' if no primary key found
        self._id_column = "id"
        return self._id_column

    async def _get_id_column_async(self):
        """Get the primary key column name - async version"""
        if self._id_column is not None:
            return self._id_column

        # Get table schema to find primary key
        schema_result = await self.client.execute(f"DESCRIBE {self.table_name}")
        if not schema_result.rows:
            # Fallback to 'id' if table not found
            self._id_column = "id"
            return self._id_column

        # Find primary key column
        for row in schema_result.rows:
            column_name = row[0]
            key_info = row[3] if len(row) > 3 else ""  # Key column
            if "PRI" in key_info.upper():
                self._id_column = column_name
                return self._id_column

        # Fallback to 'id' if no primary key found
        self._id_column = "id"
        return self._id_column

    def _get_metadata_columns(self):
        """Get metadata columns (all columns except id and vector columns)"""
        if self._metadata_columns is not None:
            return self._metadata_columns

        # Get table schema
        schema_result = self.client.execute(f"DESCRIBE {self.table_name}")
        if not schema_result.rows:
            self._metadata_columns = []
            return self._metadata_columns

        # Extract column names, excluding id and vector columns
        all_columns = [row[0] for row in schema_result.rows]
        id_column = self._get_id_column()
        self._metadata_columns = [
            col for col in all_columns if col.lower() not in [id_column.lower(), self.vector_column.lower()]
        ]
        return self._metadata_columns

    async def _get_metadata_columns_async(self):
        """Get metadata columns (all columns except id and vector columns) - async version"""
        if self._metadata_columns is not None:
            return self._metadata_columns

        # Get table schema
        schema_result = await self.client.execute(f"DESCRIBE {self.table_name}")
        if not schema_result.rows:
            self._metadata_columns = []
            return self._metadata_columns

        # Extract column names, excluding id and vector columns
        all_columns = [row[0] for row in schema_result.rows]
        id_column = await self._get_id_column_async()
        self._metadata_columns = [
            col for col in all_columns if col.lower() not in [id_column.lower(), self.vector_column.lower()]
        ]
        return self._metadata_columns

    async def _get_index_info_async(self):
        """Get index information for async client"""
        if self._index_info is not None:
            return self._index_info

        # Get table schema
        schema_result = await self.client.execute(f"SHOW CREATE TABLE {self.table_name}")
        if not schema_result.rows:
            raise ValueError(f"Table {self.table_name} not found")

        create_sql = schema_result.rows[0][1]  # Second column contains CREATE statement

        # Parse vector index information from CREATE statement
        self._index_info = self._parse_index_info(create_sql)
        return self._index_info

    def _get_index_info(self):
        """Get index information for sync client"""
        if self._index_info is not None:
            return self._index_info

        # Get table schema
        schema_result = self.client.execute(f"SHOW CREATE TABLE {self.table_name}")
        if not schema_result.rows:
            raise ValueError(f"Table {self.table_name} not found")

        create_sql = schema_result.rows[0][1]  # Second column contains CREATE statement

        # Parse vector index information from CREATE statement
        self._index_info = self._parse_index_info(create_sql)
        return self._index_info

    def _parse_index_info(self, create_sql: str) -> Dict[str, Any]:
        """
        Parse vector index information from CREATE TABLE statement.

        Args:

            create_sql: CREATE TABLE SQL statement

        Returns:

            Dictionary containing index information
        """
        index_info = {
            "algorithm": "ivf",
            "metric": "l2",
            "dimensions": None,
            "parameters": {},
        }  # default  # default

        # Extract vector column definition
        vector_pattern = rf"`?{self.vector_column}`?\s+vec(?:f32|f64)\s*\(\s*(\d+)\s*\)"
        vector_match = re.search(vector_pattern, create_sql, re.IGNORECASE)
        if vector_match:
            index_info["dimensions"] = int(vector_match.group(1))

        # Extract index creation statements - match both CREATE INDEX and KEY definitions
        index_pattern = (
            r"(?:CREATE\s+(?:INDEX|VECTOR\s+INDEX)\s+(\w+)\s+ON\s+\w+\s*\([^)]+\)\s+USING\s+(\w+)"
            r"(?:\s+WITH\s+\(([^)]+)\))?|KEY\s+`?(\w+)`?\s+USING\s+(\w+)\s+\([^)]+\)\s+([^,\n]+))"
        )
        index_matches = re.findall(index_pattern, create_sql, re.IGNORECASE | re.MULTILINE)

        for match in index_matches:
            # Handle both CREATE INDEX and KEY formats
            if len(match) == 3:  # CREATE INDEX format
                algorithm, params = match[1], match[2]
            else:  # KEY format
                algorithm, params = match[4], match[5]

            if self.vector_column in create_sql:
                index_info["algorithm"] = algorithm.lower()

                # Parse parameters
                if params:
                    # Parse parameters like "m = 16  ef_construction = 200  ef_search = 50  op_type 'vector_l2_ops'"
                    param_pairs = re.findall(r"(\w+)\s*=\s*([^,\s]+)", params)
                    for key, value in param_pairs:
                        # Convert string values to appropriate types
                        value = value.strip().strip("'\"")
                        if value.isdigit():
                            index_info["parameters"][key] = int(value)
                        elif value.replace(".", "").isdigit():
                            index_info["parameters"][key] = float(value)
                        else:
                            index_info["parameters"][key] = value

                    # Parse op_type parameter
                    op_type_match = re.search(r"op_type\s+'([^']+)'", params)
                    if op_type_match:
                        op_type = op_type_match.group(1)
                        if "cosine" in op_type.lower():
                            index_info["metric"] = "cosine"
                        elif "ip" in op_type.lower():
                            index_info["metric"] = "ip"
                        else:
                            index_info["metric"] = "l2"
                break

        return index_info

    def _parse_pinecone_filter(self, filter_dict: Dict[str, Any]) -> tuple:
        """
        Parse Pinecone-compatible filter into SQL WHERE conditions and parameters.

        Args:

            filter_dict: Pinecone filter dictionary

        Returns:

            Tuple of (where_conditions, where_params)
        """
        if not filter_dict:
            return [], []

        where_conditions = []
        where_params = []

        def parse_condition(key: str, value: Any) -> str:
            """Parse a single filter condition"""
            if isinstance(value, dict):
                # Handle operators like $eq, $in, $gte, etc.
                if "$eq" in value:
                    where_params.append(value["$eq"])
                    return f"{key} = ?"
                elif "$ne" in value:
                    where_params.append(value["$ne"])
                    return f"{key} != ?"
                elif "$in" in value:
                    if not value["$in"]:  # Empty list
                        return "1=0"  # Always false condition
                    placeholders = ",".join(["?" for _ in value["$in"]])
                    where_params.extend(value["$in"])
                    return f"{key} IN ({placeholders})"
                elif "$nin" in value:
                    if not value["$nin"]:  # Empty list
                        return "1=1"  # Always true condition
                    placeholders = ",".join(["?" for _ in value["$nin"]])
                    where_params.extend(value["$nin"])
                    return f"{key} NOT IN ({placeholders})"
                elif "$gt" in value:
                    where_params.append(value["$gt"])
                    return f"{key} > ?"
                elif "$gte" in value:
                    where_params.append(value["$gte"])
                    return f"{key} >= ?"
                elif "$lt" in value:
                    where_params.append(value["$lt"])
                    return f"{key} < ?"
                elif "$lte" in value:
                    where_params.append(value["$lte"])
                    return f"{key} <= ?"
                elif "$and" in value:
                    # Handle nested $and conditions
                    and_conditions = []
                    for condition in value["$and"]:
                        for sub_key, sub_value in condition.items():
                            and_conditions.append(parse_condition(sub_key, sub_value))
                    return f"({' AND '.join(and_conditions)})"
                elif "$or" in value:
                    # Handle nested $or conditions
                    or_conditions = []
                    for condition in value["$or"]:
                        for sub_key, sub_value in condition.items():
                            or_conditions.append(parse_condition(sub_key, sub_value))
                    return f"({' OR '.join(or_conditions)})"
                else:
                    raise ValueError(f"Unsupported operator in filter: {list(value.keys())}")
            else:
                # Direct value comparison (equivalent to $eq)
                where_params.append(value)
                return f"{key} = ?"

        def parse_nested_condition(condition_dict: dict) -> str:
            """Parse a nested condition that might contain $and or $or"""
            if "$and" in condition_dict:
                and_conditions = []
                for condition in condition_dict["$and"]:
                    and_conditions.append(parse_nested_condition(condition))
                return f"({' AND '.join(and_conditions)})"
            elif "$or" in condition_dict:
                or_conditions = []
                for condition in condition_dict["$or"]:
                    or_conditions.append(parse_nested_condition(condition))
                return f"({' OR '.join(or_conditions)})"
            else:
                # This is a simple condition, parse it normally
                conditions = []
                for key, value in condition_dict.items():
                    conditions.append(parse_condition(key, value))
                return " AND ".join(conditions)

        # Parse top-level conditions
        for key, value in filter_dict.items():
            if key == "$and":
                # Handle top-level $and
                and_conditions = []
                for condition in value:
                    and_conditions.append(parse_nested_condition(condition))
                where_conditions.append(f"({' AND '.join(and_conditions)})")
            elif key == "$or":
                # Handle top-level $or
                or_conditions = []
                for condition in value:
                    or_conditions.append(parse_nested_condition(condition))
                where_conditions.append(f"({' OR '.join(or_conditions)})")
            else:
                condition = parse_condition(key, value)
                where_conditions.append(condition)

        return where_conditions, where_params

    def query(
        self,
        vector: List[float],
        top_k: int = 10,
        include_metadata: bool = True,
        include_values: bool = False,
        filter: Optional[Dict[str, Any]] = None,
        namespace: str = "",
    ) -> QueryResponse:
        """
        Query the vector index using similarity search (Pinecone-compatible API).

        Performs vector similarity search and returns the most similar vectors
        based on the configured distance metric (L2, cosine, or inner product).

        Args:

            vector: Query vector for similarity search. Must match the dimension
                   of vectors in the index.
            top_k: Maximum number of results to return (default: 10)
            include_metadata: Whether to include metadata fields in results (default: True)
            include_values: Whether to include vector values in results (default: False)
            filter: Optional metadata filter using Pinecone-compatible syntax:
                    - Equality: {"category": "technology"} or {"category": {"$eq": "technology"}}
                    - Not Equal: {"status": {"$ne": "inactive"}}
                    - Greater Than: {"price": {"$gt": 100}}
                    - Greater Than or Equal: {"price": {"$gte": 100}}
                    - Less Than: {"price": {"$lt": 500}}
                    - Less Than or Equal: {"price": {"$lte": 500}}
                    - In: {"status": {"$in": ["active", "pending", "review"]}}
                    - Not In: {"category": {"$nin": ["deprecated", "archived"]}}
                    - Logical AND: {"$and": [{"category": "tech"}, {"price": {"$gt": 50}}]}
                    - Logical OR: {"$or": [{"status": "active"}, {"priority": "high"}]}
                    - Nested conditions: {"$and": [{"$or": [{"a": 1}, {"b": 2}]}, {"c": 3}]}
            namespace: Namespace identifier (not used in MatrixOne, kept for compatibility)

        Returns:

            QueryResponse: Object containing:
                - matches: List of VectorMatch objects with id, score, metadata, and optional values
                - namespace: Namespace (empty string for MatrixOne)
                - usage: Dictionary with read_units count

        Example:

            # Basic similarity search
            results = index.query([0.1, 0.2, 0.3], top_k=5)

            # Simple equality filter
            results = index.query(
                vector=[0.1, 0.2, 0.3],
                filter={"category": "technology"}
            )

            # Comparison operators
            results = index.query(
                vector=[0.1, 0.2, 0.3],
                filter={"price": {"$gte": 100, "$lt": 500}}
            )

            # In/Not In operators
            results = index.query(
                vector=[0.1, 0.2, 0.3],
                filter={"status": {"$in": ["active", "pending"]}}
            )

            # Logical AND/OR operators
            results = index.query(
                vector=[0.1, 0.2, 0.3],
                filter={
                    "$and": [
                        {"category": {"$in": ["tech", "science"]}},
                        {"$or": [{"price": {"$lt": 100}}, {"discount": True}]}
                    ]
                }
            )

            # Complex nested conditions
            results = index.query(
                vector=[0.1, 0.2, 0.3],
                filter={
                    "$and": [
                        {"$or": [{"priority": "high"}, {"urgent": True}]},
                        {"status": {"$ne": "archived"}},
                        {"created_date": {"$gte": "2024-01-01"}}
                    ]
                }
            )

        Raises:

            ValueError: If vector dimension doesn't match index dimension
            RuntimeError: If used with async client (use query_async instead)
        """
        index_info = self._get_index_info()

        # Parse filter if provided
        where_conditions, where_params = self._parse_pinecone_filter(filter)

        # Build similarity search query
        id_column = self._get_id_column()
        select_columns = [id_column]
        if include_metadata:
            metadata_columns = self._get_metadata_columns()
            select_columns.extend(metadata_columns)
        if include_values:
            select_columns.append(self.vector_column)

        # Use the appropriate similarity search method based on algorithm
        if index_info["algorithm"] == "hnsw":
            results = self.client.vector_ops.similarity_search(
                self.table_name,
                vector_column=self.vector_column,
                query_vector=vector,
                limit=top_k,
                distance_type=index_info.get("metric", "l2"),
                select_columns=select_columns,
                where_conditions=where_conditions,
                where_params=where_params,
            )
        else:  # default to IVF
            results = self.client.vector_ops.similarity_search(
                self.table_name,
                vector_column=self.vector_column,
                query_vector=vector,
                limit=top_k,
                distance_type=index_info.get("metric", "l2"),
                select_columns=select_columns,
                where_conditions=where_conditions,
                where_params=where_params,
            )

        # Convert results to MatrixOne format (using real primary key)
        matches = []
        for row in results:
            # Use the actual primary key value and column name
            pk_value = row[0]  # Primary key value (can be any type)
            score = float(row[-1]) if len(row) > 1 else 0.0  # Last column is usually score

            # Extract metadata (including primary key as a field)
            metadata = {}
            if include_metadata:
                metadata_columns = self._get_metadata_columns()
                for i, col in enumerate(metadata_columns):
                    if i + 1 < len(row):
                        metadata[col] = row[i + 1]

                # Add primary key to metadata with its real column name
                id_column = self._get_id_column()
                metadata[id_column] = pk_value

            # Extract vector values if requested
            values = None
            if include_values and self.vector_column in select_columns:
                # Find vector column index case-insensitively
                vector_idx = next(i for i, col in enumerate(select_columns) if col.lower() == self.vector_column.lower())
                if vector_idx < len(row):
                    values = row[vector_idx]

            # Use primary key value as the match ID (convert to string for compatibility)
            matches.append(VectorMatch(id=str(pk_value), score=score, metadata=metadata, values=values))

        return QueryResponse(matches=matches, namespace=namespace, usage={"read_units": len(matches)})

    async def query_async(
        self,
        vector: List[float],
        top_k: int = 10,
        include_metadata: bool = True,
        include_values: bool = False,
        filter: Optional[Dict[str, Any]] = None,
        namespace: str = "",
    ) -> QueryResponse:
        """
        Async version of query method.

        Args:

            vector: Query vector
            top_k: Number of results to return
            include_metadata: Whether to include metadata in results
            include_values: Whether to include vector values in results
            filter: Optional metadata filter (Pinecone-compatible)
            namespace: Namespace (not used in MatrixOne)

        Returns:

            QueryResponse object with matches
        """
        index_info = await self._get_index_info_async()

        # Parse filter if provided
        where_conditions, where_params = self._parse_pinecone_filter(filter)

        # Build similarity search query
        id_column = await self._get_id_column_async()
        select_columns = [id_column]
        if include_metadata:
            metadata_columns = await self._get_metadata_columns_async()
            select_columns.extend(metadata_columns)
        if include_values:
            select_columns.append(self.vector_column)

        # Use unified SQL builder for async queries
        from .sql_builder import DistanceFunction, build_vector_similarity_query

        # Convert metric to distance function enum
        metric = index_info.get("metric", "l2")
        if metric == "l2":
            distance_func = DistanceFunction.L2
        elif metric == "cosine":
            distance_func = DistanceFunction.COSINE
        elif metric == "ip":
            distance_func = DistanceFunction.INNER_PRODUCT
        else:
            distance_func = DistanceFunction.L2

        # Build query using unified SQL builder
        sql = build_vector_similarity_query(
            table_name=self.table_name,
            vector_column=self.vector_column,
            query_vector=vector,
            distance_func=distance_func,
            limit=top_k,
            select_columns=select_columns,
            where_conditions=where_conditions,
            where_params=where_params,
        )

        # Execute query
        result = await self.client.execute(sql)
        results = result.rows

        # Convert results to MatrixOne format (using real primary key)
        matches = []
        for row in results:
            # Use the actual primary key value and column name
            pk_value = row[0]  # Primary key value (can be any type)
            score = float(row[-1]) if len(row) > 1 else 0.0  # Last column is usually score

            # Extract metadata (including primary key as a field)
            metadata = {}
            if include_metadata:
                metadata_columns = await self._get_metadata_columns_async()
                for i, col in enumerate(metadata_columns):
                    if i + 1 < len(row):
                        metadata[col] = row[i + 1]

                # Add primary key to metadata with its real column name
                id_column = await self._get_id_column_async()
                metadata[id_column] = pk_value

            # Extract vector values if requested
            values = None
            if include_values and self.vector_column in select_columns:
                # Find vector column index case-insensitively
                vector_idx = next(i for i, col in enumerate(select_columns) if col.lower() == self.vector_column.lower())
                if vector_idx < len(row):
                    values = row[vector_idx]

            # Use primary key value as the match ID (convert to string for compatibility)
            matches.append(VectorMatch(id=str(pk_value), score=score, metadata=metadata, values=values))

        return QueryResponse(matches=matches, namespace=namespace, usage={"read_units": len(matches)})

    def delete(self, ids: List[Any], namespace: str = ""):
        """
        Delete vectors by their primary key IDs (IVF index only).

        Removes vectors from the index based on their primary key values.
        This operation is only supported for IVF indexes, not HNSW indexes.

        Args:

            ids: List of primary key values to delete. Can be any type (str, int, etc.)
                 that matches the primary key column type.
            namespace: Namespace identifier (not used in MatrixOne, kept for compatibility)

        Returns:

            None

        Example:

            # Delete vectors by ID
            index.delete(["doc1", "doc2", "doc3"])

            # Delete vectors with integer IDs
            index.delete([1, 2, 3, 4, 5])

            # Delete a single vector
            index.delete(["single_doc_id"])

        Raises:

            ValueError: If the index type is HNSW (not supported for delete operations)
            RuntimeError: If used with async client (use delete_async instead)

        Note:

            - Only IVF indexes support delete operations
            - HNSW indexes are read-only and do not support upsert/delete
            - IDs must match the primary key column type and values
            - Non-existent IDs are silently ignored (no error raised)
        """
        index_info = self._get_index_info()

        # Check if index type supports delete operations
        if index_info["algorithm"] == "hnsw":
            raise ValueError(
                "HNSW index does not support delete operations. " "Only IVF index supports INSERT/UPDATE/DELETE operations."
            )

        if ids:
            # Use unified SQL builder for DELETE
            from .sql_builder import build_delete_query

            id_column = self._get_id_column()
            placeholders = ",".join(["?" for _ in ids])
            where_condition = f"{id_column} IN ({placeholders})"

            sql, params = build_delete_query(
                table_name=self.table_name, where_conditions=[where_condition], where_params=ids
            )
            self.client.execute(sql, params)

    async def delete_async(self, ids: List[Any], namespace: str = ""):
        """
        Async version of delete method.

        Args:

            ids: List of vector IDs to delete (can be any type: str, int, etc.)
            namespace: Namespace (not used in MatrixOne)

        Raises:

            ValueError: If the index type is HNSW (not supported for delete operations)
        """
        index_info = await self._get_index_info_async()

        # Check if index type supports delete operations
        if index_info["algorithm"] == "hnsw":
            raise ValueError(
                "HNSW index does not support delete operations. " "Only IVF index supports INSERT/UPDATE/DELETE operations."
            )

        if ids:
            # Use unified SQL builder for DELETE
            from .sql_builder import build_delete_query

            id_column = await self._get_id_column_async()
            placeholders = ",".join(["?" for _ in ids])
            where_condition = f"{id_column} IN ({placeholders})"

            sql, params = build_delete_query(
                table_name=self.table_name, where_conditions=[where_condition], where_params=ids
            )
            await self.client.execute(sql, params)

    def describe_index_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive index statistics (Pinecone-compatible API).

        Returns detailed information about the vector index including dimensions,
        vector count, and namespace information.

        Returns:

            Dict: Index statistics containing:
                - dimension: Vector dimension size
                - index_fullness: Index fullness ratio (always 0.0 for MatrixOne)
                - total_vector_count: Total number of vectors in the index
                - namespaces: Dictionary with namespace information:
                    - "": Default namespace with vector_count

        Example:

            stats = index.describe_index_stats()
            print(f"Index has {stats['total_vector_count']} vectors")
            print(f"Vector dimension: {stats['dimension']}")
            print(f"Namespace vector count: {stats['namespaces']['']['vector_count']}")

        Note:

            - index_fullness is always 0.0 as MatrixOne doesn't use this concept
            - Only the default namespace ("") is supported
            - Vector count is the total number of rows in the table
        """
        # Get table row count using unified SQL builder
        from .sql_builder import build_select_query

        sql = build_select_query(table_name=self.table_name, select_columns=["COUNT(*)"])
        count_result = self.client.execute(sql)
        total_vector_count = count_result.rows[0][0] if count_result.rows else 0

        index_info = self._get_index_info()

        return {
            "dimension": index_info.get("dimensions", 0),
            "index_fullness": 0.0,  # Not applicable to MatrixOne
            "total_vector_count": total_vector_count,
            "namespaces": {"": {"vector_count": total_vector_count}},
        }

    async def describe_index_stats_async(self) -> Dict[str, Any]:
        """
        Async version of describe_index_stats method.

        Returns:

            Dictionary with index statistics
        """
        # Get table row count using unified SQL builder
        from .sql_builder import build_select_query

        sql = build_select_query(table_name=self.table_name, select_columns=["COUNT(*)"])
        count_result = await self.client.execute(sql)
        total_vector_count = count_result.rows[0][0] if count_result.rows else 0

        index_info = await self._get_index_info_async()

        return {
            "dimension": index_info.get("dimensions", 0),
            "index_fullness": 0.0,  # Not applicable to MatrixOne
            "total_vector_count": total_vector_count,
            "namespaces": {"": {"vector_count": total_vector_count}},
        }

    def upsert(self, vectors: List[Dict[str, Any]], namespace: str = ""):
        """
        Upsert vectors into the index (IVF index only).

        Inserts new vectors or updates existing ones based on the primary key.
        This operation is only supported for IVF indexes, not HNSW indexes.

        Args:

            vectors: List of vector dictionaries to upsert. Each vector dict must contain:
                - Primary key field: Value for the primary key column (required)
                - Vector field: Vector values as a list of floats (required)
                - Additional fields: Any metadata fields to store
            namespace: Namespace identifier (not used in MatrixOne, kept for compatibility)

        Returns:

            Dict: Statistics about the upsert operation:
                - upserted_count: Number of vectors successfully upserted

        Example:

            # Upsert vectors with metadata
            vectors = [
                {
                    "id": "doc1",  # Primary key field
                    "embedding": [0.1, 0.2, 0.3, 0.4],  # Vector field
                    "title": "Document 1",
                    "category": "technology",
                    "price": 99.99
                },
                {
                    "id": "doc2",
                    "embedding": [0.5, 0.6, 0.7, 0.8],
                    "title": "Document 2",
                    "category": "science",
                    "price": 149.99
                }
            ]
            result = index.upsert(vectors)
            print(f"Upserted {result['upserted_count']} vectors")

        Raises:

            ValueError: If the index type is HNSW (not supported for upsert operations)
            ValueError: If a vector is missing required fields (primary key or vector)
            RuntimeError: If used with async client (use upsert_async instead)

        Note:

            - Only IVF indexes support upsert operations
            - HNSW indexes are read-only and do not support upsert/delete
            - Vector dimensions must match the index configuration
            - Primary key values must be unique within the table
        """
        if not vectors:
            return {"upserted_count": 0}

        # Get the actual primary key column name
        id_column = self._get_id_column()

        # Process each vector individually for proper upsert behavior
        for vector in vectors:
            # Check if primary key field exists
            if id_column not in vector:
                raise ValueError(f"Each vector must have '{id_column}' field (primary key)")

            # Check if vector field exists
            if self.vector_column not in vector:
                raise ValueError(f"Each vector must have '{self.vector_column}' field (vector values)")

            # Prepare data - use all fields from the vector dict
            data = dict(vector)

            # Build upsert SQL using INSERT ... ON DUPLICATE KEY UPDATE
            columns = list(data.keys())
            columns_str = ", ".join(columns)

            # Format values - use proper vector format
            formatted_values = []
            for col in columns:
                value = data[col]
                if isinstance(value, list):
                    # Format vector as string with proper escaping
                    vector_str = "[" + ",".join(map(str, value)) + "]"
                    formatted_values.append(f"'{vector_str}'")
                else:
                    formatted_values.append(f"'{value}'")
            values_str = "(" + ", ".join(formatted_values) + ")"

            # Build ON DUPLICATE KEY UPDATE clause
            update_clauses = []
            for col in columns:
                if col != id_column:  # Don't update the primary key
                    update_clauses.append(f"{col} = VALUES({col})")
            update_str = ", ".join(update_clauses)

            # Execute upsert SQL
            sql = (
                f"INSERT INTO {self.table_name} ({columns_str}) VALUES {values_str} " f"ON DUPLICATE KEY UPDATE {update_str}"
            )
            self.client.execute(sql)

        return {"upserted_count": len(vectors)}

    async def upsert_async(self, vectors: List[Dict[str, Any]], namespace: str = ""):
        """
        Async version of upsert method.

        Args:

            vectors: List of vectors to upsert. Each vector should be a dict with:
                - Primary key field: Value for the primary key column (required)
                - Vector field: Vector values (required)
                - Other fields: Any additional metadata fields
            namespace: Namespace (not used in MatrixOne)

        Returns:

            Dict with upsert statistics
        """
        if not vectors:
            return {"upserted_count": 0}

        # Get the actual primary key column name
        id_column = await self._get_id_column_async()

        # Process each vector individually for proper upsert behavior
        for vector in vectors:
            # Check if primary key field exists
            if id_column not in vector:
                raise ValueError(f"Each vector must have '{id_column}' field (primary key)")

            # Check if vector field exists
            if self.vector_column not in vector:
                raise ValueError(f"Each vector must have '{self.vector_column}' field (vector values)")

            # Prepare data - use all fields from the vector dict
            data = dict(vector)

            # Build upsert SQL using INSERT ... ON DUPLICATE KEY UPDATE
            columns = list(data.keys())
            columns_str = ", ".join(columns)

            # Format values - use proper vector format
            formatted_values = []
            for col in columns:
                value = data[col]
                if isinstance(value, list):
                    # Format vector as string with proper escaping
                    vector_str = "[" + ",".join(map(str, value)) + "]"
                    formatted_values.append(f"'{vector_str}'")
                else:
                    formatted_values.append(f"'{value}'")
            values_str = "(" + ", ".join(formatted_values) + ")"

            # Build ON DUPLICATE KEY UPDATE clause
            update_clauses = []
            for col in columns:
                if col != id_column:  # Don't update the primary key
                    update_clauses.append(f"{col} = VALUES({col})")
            update_str = ", ".join(update_clauses)

            # Execute upsert SQL
            sql = (
                f"INSERT INTO {self.table_name} ({columns_str}) VALUES {values_str} " f"ON DUPLICATE KEY UPDATE {update_str}"
            )
            await self.client.execute(sql)

        return {"upserted_count": len(vectors)}

    def batch_insert(self, vectors: List[Dict[str, Any]], namespace: str = ""):
        """
        Batch insert vectors (Pinecone-compatible API).

        Args:

            vectors: List of vectors to insert. Each vector should be a dict with:
                - Primary key field: Value for the primary key column (required)
                - Vector field: Vector values (required)
                - Other fields: Any additional metadata fields
            namespace: Namespace (not used in MatrixOne)

        Returns:

            Dict with insert statistics
        """
        if not vectors:
            return {"inserted_count": 0}

        # Get the actual primary key column name
        id_column = self._get_id_column()

        # Prepare data for batch insert
        batch_data = []
        for vector in vectors:
            # Check if primary key field exists
            if id_column not in vector:
                raise ValueError(f"Each vector must have '{id_column}' field (primary key)")

            # Check if vector field exists
            if self.vector_column not in vector:
                raise ValueError(f"Each vector must have '{self.vector_column}' field (vector values)")

            # Prepare row data
            row_data = dict(vector)
            batch_data.append(row_data)

        # Use client's batch_insert method
        self.client.batch_insert(self.table_name, batch_data)

        return {"inserted_count": len(vectors)}

    async def batch_insert_async(self, vectors: List[Dict[str, Any]], namespace: str = ""):
        """
        Async version of batch_insert method.

        Args:

            vectors: List of vectors to insert. Each vector should be a dict with:
                - Primary key field: Value for the primary key column (required)
                - Vector field: Vector values (required)
                - Other fields: Any additional metadata fields
            namespace: Namespace (not used in MatrixOne)

        Returns:

            Dict with insert statistics
        """
        if not vectors:
            return {"inserted_count": 0}

        # Get the actual primary key column name
        id_column = await self._get_id_column_async()

        # Prepare data for batch insert
        batch_data = []
        for vector in vectors:
            # Check if primary key field exists
            if id_column not in vector:
                raise ValueError(f"Each vector must have '{id_column}' field (primary key)")

            # Check if vector field exists
            if self.vector_column not in vector:
                raise ValueError(f"Each vector must have '{self.vector_column}' field (vector values)")

            # Prepare row data
            row_data = dict(vector)
            batch_data.append(row_data)

        # Use client's batch_insert_async method
        await self.client.batch_insert_async(self.table_name, batch_data)

        return {"inserted_count": len(vectors)}

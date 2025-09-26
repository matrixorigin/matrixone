"""
SearchVectorIndex - A Pinecone-compatible vector search interface for MatrixOne
"""

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class VectorMatch:
    """Represents a vector search match result"""

    id: str
    score: float
    metadata: Dict[str, Any]
    values: Optional[List[float]] = None


@dataclass
class QueryResponse:
    """Represents a query response compatible with Pinecone"""

    matches: List[VectorMatch]
    namespace: str = ""
    usage: Optional[Dict[str, Any]] = None


class PineconeCompatibleIndex:
    """
    A Pinecone-compatible vector search interface for MatrixOne.

    This class provides a high-level interface for vector search operations
    that is compatible with Pinecone's API, making it easy to migrate from
    Pinecone to MatrixOne.
    """

    def __init__(self, client, table_name: str, vector_column: str):
        """
        Initialize PineconeCompatibleIndex.

        Args:
            client: MatrixOne client instance
            table_name: Name of the table containing vectors
            vector_column: Name of the vector column
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
        index_info = {"algorithm": "ivf", "metric": "l2", "dimensions": None, "parameters": {}}  # default  # default

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
        Query the vector index (Pinecone-compatible API).

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
            results = self.client.vector_query.similarity_search(
                table_name=self.table_name,
                vector_column=self.vector_column,
                query_vector=vector,
                limit=top_k,
                distance_type=index_info.get("metric", "l2"),
                select_columns=select_columns,
                where_conditions=where_conditions,
                where_params=where_params,
            )
        else:  # default to IVF
            results = self.client.vector_query.similarity_search(
                table_name=self.table_name,
                vector_column=self.vector_column,
                query_vector=vector,
                limit=top_k,
                distance_type=index_info.get("metric", "l2"),
                select_columns=select_columns,
                where_conditions=where_conditions,
                where_params=where_params,
            )

        # Convert results to Pinecone format
        matches = []
        for row in results:
            match_id = str(row[0])  # ID column
            score = float(row[-1]) if len(row) > 1 else 0.0  # Last column is usually score

            # Extract metadata
            metadata = {}
            if include_metadata:
                metadata_columns = self._get_metadata_columns()
                for i, col in enumerate(metadata_columns):
                    if i + 1 < len(row):
                        metadata[col] = row[i + 1]

            # Extract vector values if requested
            values = None
            if include_values and self.vector_column in select_columns:
                # Find vector column index case-insensitively
                vector_idx = next(
                    i for i, col in enumerate(select_columns) if col.lower() == self.vector_column.lower()
                )
                if vector_idx < len(row):
                    values = row[vector_idx]

            matches.append(VectorMatch(id=match_id, score=score, metadata=metadata, values=values))

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
        from .sql_builder import (DistanceFunction,
                                  build_vector_similarity_query)

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

        # Convert results to Pinecone format
        matches = []
        for row in results:
            match_id = str(row[0])  # ID column
            score = float(row[-1]) if len(row) > 1 else 0.0  # Last column is usually score

            # Extract metadata
            metadata = {}
            if include_metadata:
                metadata_columns = await self._get_metadata_columns_async()
                for i, col in enumerate(metadata_columns):
                    if i + 1 < len(row):
                        metadata[col] = row[i + 1]

            # Extract vector values if requested
            values = None
            if include_values and self.vector_column in select_columns:
                # Find vector column index case-insensitively
                vector_idx = next(
                    i for i, col in enumerate(select_columns) if col.lower() == self.vector_column.lower()
                )
                if vector_idx < len(row):
                    values = row[vector_idx]

            matches.append(VectorMatch(id=match_id, score=score, metadata=metadata, values=values))

        return QueryResponse(matches=matches, namespace=namespace, usage={"read_units": len(matches)})

    def delete(self, ids: List[Any], namespace: str = ""):
        """
        Delete vectors by IDs (Pinecone-compatible API).

        Args:
            ids: List of vector IDs to delete (can be any type: str, int, etc.)
            namespace: Namespace (not used in MatrixOne)

        Raises:
            ValueError: If the index type is HNSW (not supported for delete operations)
        """
        index_info = self._get_index_info()

        # Check if index type supports delete operations
        if index_info["algorithm"] == "hnsw":
            raise ValueError(
                "HNSW index does not support delete operations. "
                "Only IVF index supports INSERT/UPDATE/DELETE operations."
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
                "HNSW index does not support delete operations. "
                "Only IVF index supports INSERT/UPDATE/DELETE operations."
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
        Get index statistics (Pinecone-compatible API).

        Returns:
            Dictionary with index statistics
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

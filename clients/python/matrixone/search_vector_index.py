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
import json
import math
import numbers
import asyncio
from dataclasses import dataclass
from ._sql_literals import sql_identifier, sql_literal, substitute_parameters

from typing import Any, Dict, List, Optional
from .exceptions import QueryError


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

    @property
    def metadata_columns(self):
        """Get metadata columns (all columns except id and vector columns)"""
        # Check if this is an async client
        if hasattr(self.client, "execute") and hasattr(self.client.execute, "__call__"):
            import asyncio

            if asyncio.iscoroutinefunction(self.client.execute):
                raise RuntimeError("Use _get_metadata_columns_async() for async clients")
        return self._get_metadata_columns()

    @staticmethod
    def _schema_columns(rows):
        primary = [row[0] for row in rows if len(row) > 3 and 'PRI' in str(row[3]).upper()]
        if len(primary) != 1:
            raise ValueError('Pinecone handles require a single primary key column')
        return primary[0], [row[0] for row in rows]

    def _get_id_column(self):
        return self._schema_columns(
            self.client.execute('DESCRIBE ' + sql_identifier(self.table_name, qualified=True)).rows
        )[0]

    async def _get_id_column_async(self):
        result = await self.client.execute('DESCRIBE ' + sql_identifier(self.table_name, qualified=True))
        return self._schema_columns(result.rows)[0]

    def _get_metadata_columns(self):
        result = self.client.execute('DESCRIBE ' + sql_identifier(self.table_name, qualified=True))
        primary, columns = self._schema_columns(result.rows)
        return [c for c in columns if c.lower() not in (primary.lower(), self.vector_column.lower())]

    async def _get_metadata_columns_async(self):
        result = await self.client.execute('DESCRIBE ' + sql_identifier(self.table_name, qualified=True))
        primary, columns = self._schema_columns(result.rows)
        return [c for c in columns if c.lower() not in (primary.lower(), self.vector_column.lower())]

    def _get_index_info(self):
        result = self.client.execute('SHOW CREATE TABLE ' + sql_identifier(self.table_name, qualified=True))
        if not result.rows:
            raise ValueError('Table not found: ' + self.table_name)
        return self._parse_index_info(result.rows[0][1])

    async def _get_index_info_async(self):
        result = await self.client.execute('SHOW CREATE TABLE ' + sql_identifier(self.table_name, qualified=True))
        if not result.rows:
            raise ValueError('Table not found: ' + self.table_name)
        return self._parse_index_info(result.rows[0][1])

    @staticmethod
    def _table_definitions(sql):
        """Split SHOW CREATE's outer definition list, respecting quoted tokens."""
        depth = 0
        quote = None
        start = None
        definitions = []
        i = 0
        while i < len(sql):
            ch = sql[i]
            if quote:
                if ch == quote:
                    if i + 1 < len(sql) and sql[i + 1] == quote:
                        i += 2
                        continue
                    quote = None
                elif ch == '\\' and quote != '`':
                    i += 2
                    continue
            elif ch in ("'", '"', '`'):
                quote = ch
            elif ch == '(':
                depth += 1
                if depth == 1:
                    start = i + 1
            elif ch == ')':
                depth -= 1
                if depth == 0 and start is not None:
                    definitions.append(sql[start:i].strip())
                    return definitions
            elif ch == ',' and depth == 1:
                definitions.append(sql[start:i].strip())
                start = i + 1
            i += 1
        raise ValueError('Malformed SHOW CREATE TABLE definition')

    def _parse_index_info(self, create_sql):
        info = dict(algorithm=None, metric='l2', dimensions=None, parameters={})
        identifier = r'(?:`(?:``|[^`])+`|[A-Za-z_][\w$]*)'
        column_name = self.vector_column
        vector_pattern = (
            r'(?:`'
            + re.escape(column_name.replace('`', '``'))
            + r'`|\b'
            + re.escape(column_name)
            + r'\b)\s+vec(?:f32|f64)\s*\(\s*(\d+)\s*\)'
        )
        definitions = self._table_definitions(create_sql)
        for definition in definitions:
            match = re.match(vector_pattern, definition, re.IGNORECASE)
            if match:
                info['dimensions'] = int(match.group(1))
        patterns = [
            r'(?:KEY|INDEX)\s+' + identifier + r'\s+USING\s+(\w+)\s*\(([^)]+)\)\s*([^,\n]*)',
            r'CREATE\s+(?:VECTOR\s+)?INDEX\s+'
            + identifier
            + r'\s+ON\s+'
            + identifier
            + r'\s*\(([^)]+)\)\s+USING\s+(\w+)\s*(?:WITH\s*\(([^)]+)\))?',
        ]
        candidates = []
        for number, pattern in enumerate(patterns):
            for definition in definitions:
                match = re.match(pattern, definition, re.IGNORECASE)
                if match is None:
                    continue
                algorithm, columns, params = match.groups()
                if number == 1:
                    columns, algorithm = algorithm, columns
                if algorithm.lower() not in ('ivf', 'ivfflat', 'hnsw'):
                    continue
                indexed = [value.strip().strip('`').replace('``', '`') for value in columns.split(',')]
                if len(indexed) != 1 or indexed[0].lower() != column_name.lower():
                    continue
                params = params or ''
                op = re.search(r"op_type\s*(?:=\s*)?['\"]([^'\"]+)['\"]", params, re.IGNORECASE)
                operators = {'vector_l2_ops': 'l2', 'vector_cosine_ops': 'cosine', 'vector_ip_ops': 'ip'}
                if op and op.group(1).lower() not in operators:
                    raise ValueError('Unsupported vector index operator: ' + op.group(1))
                metric = operators[op.group(1).lower()] if op else 'l2'
                parsed = {}
                for key, value in re.findall(r'(\w+)\s*=\s*([^,\s]+)', params):
                    value = value.strip("'\"")
                    parsed[key] = int(value) if value.isdigit() else value
                candidates.append(
                    dict(algorithm=algorithm.lower(), metric=metric, dimensions=info['dimensions'], parameters=parsed)
                )
        if len({item['metric'] for item in candidates}) > 1:
            raise ValueError('Multiple vector indexes with different metrics; select an unambiguous vector column')
        return candidates[0] if candidates else info

    @staticmethod
    def _distance_type(info):
        try:
            return {'l2': 'l2', 'cosine': 'cosine', 'ip': 'inner_product'}[info['metric']]
        except KeyError as error:
            raise ValueError('Unknown vector index metric') from error

    def _parse_pinecone_filter(self, filter_dict):
        if filter_dict is None:
            return [], []
        parameters = []
        comparisons = {'$eq': '=', '$ne': '!=', '$gt': '>', '$gte': '>=', '$lt': '<', '$lte': '<='}

        def parse(mapping, depth=0):
            if not isinstance(mapping, dict) or depth > 64:
                raise ValueError('Filter must be a mapping with at most 64 nested levels')
            conditions = []
            for key, value in mapping.items():
                if key in ('$and', '$or'):
                    if not isinstance(value, (list, tuple)) or not value or any(not item for item in value):
                        raise ValueError('Logical filter requires nonempty mapping operands')
                    children = []
                    for item in value:
                        parts = parse(item, depth + 1)
                        children.append('(' + ' AND '.join(parts) + ')' if len(parts) > 1 else parts[0])
                    conditions.append('(' + (' AND ' if key == '$and' else ' OR ').join(children) + ')')
                    continue
                if not isinstance(key, str) or key.startswith('$'):
                    raise ValueError('Unsupported operator in filter: ' + str(key))
                field = sql_identifier(key)
                operators = value if isinstance(value, dict) else {'$eq': value}
                if not operators:
                    raise ValueError('A field filter requires at least one operator')
                for op, operand in operators.items():
                    if op in comparisons:
                        if operand is None and op in ('$eq', '$ne'):
                            conditions.append(field + (' IS NULL' if op == '$eq' else ' IS NOT NULL'))
                        else:
                            conditions.append(field + ' ' + comparisons[op] + ' ?')
                            parameters.append(operand)
                    elif op in ('$in', '$nin'):
                        if not isinstance(operand, (list, tuple)):
                            raise ValueError(op + ' requires a list')
                        if not operand:
                            conditions.append('1=0' if op == '$in' else '1=1')
                        else:
                            conditions.append(
                                field + (' IN (' if op == '$in' else ' NOT IN (') + ','.join('?' for _ in operand) + ')'
                            )
                            parameters.extend(operand)
                    else:
                        raise ValueError('Unsupported operator in filter: ' + str(op))
            return conditions

        conditions = parse(filter_dict)
        return conditions, parameters

    @staticmethod
    def _numeric_vector(value, dimensions=None):
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (ValueError, TypeError) as error:
                raise ValueError('Malformed vector text') from error
        if not isinstance(value, (list, tuple)) or not value:
            raise ValueError('Vector values must be a nonempty numeric list')
        if any(isinstance(v, bool) or not isinstance(v, numbers.Real) or not math.isfinite(v) for v in value):
            raise ValueError('Vector values must contain only finite numbers')
        if dimensions is not None and len(value) != dimensions:
            raise ValueError('Vector dimensions do not match the column')
        return [float(v) for v in value]

    def _materialize_matches(self, results, id_column, columns, info, include_values, include_metadata, namespace):
        metadata_columns = [c for c in columns if c.lower() not in (id_column.lower(), self.vector_column.lower())]
        matches = []
        for row in results:
            try:
                values = self._numeric_vector(row[self.vector_column], info['dimensions']) if include_values else None
                if row[id_column] is None:
                    raise ValueError('Vector response has no primary key')
                score = float(row['distance'])
                if not math.isfinite(score):
                    raise ValueError('Vector response score must be finite')
                metadata = {column: row[column] for column in metadata_columns} if include_metadata else {}
                matches.append(VectorMatch(id=str(row[id_column]), score=score, metadata=metadata, values=values))
            except (ValueError, TypeError, KeyError, OverflowError) as error:
                raise QueryError('Invalid vector response: ' + str(error)) from error
        return QueryResponse(matches=matches, namespace=namespace, usage={"read_units": len(matches)})

    def _prepare_upserts(self, vectors, schema_rows, info):
        id_column, known_columns = self._schema_columns(schema_rows)
        if info['algorithm'] == 'hnsw':
            raise ValueError('HNSW index does not support upsert operations')
        statements = []
        for vector in vectors:
            if not isinstance(vector, dict):
                raise ValueError('Each vector must be a mapping')
            if id_column not in vector or vector[id_column] is None:
                raise ValueError("Each vector must have '%s' field (primary key)" % id_column)
            if self.vector_column not in vector:
                raise ValueError("Each vector must have '%s' field (vector values)" % self.vector_column)
            if any(column not in known_columns for column in vector):
                raise ValueError('Unknown upsert column')
            data = dict(vector)
            data[self.vector_column] = self._numeric_vector(data[self.vector_column], info['dimensions'])
            columns = list(data)
            names = [sql_identifier(column) for column in columns]
            updates = ', '.join(
                '%s=VALUES(%s)' % (name, name) for column, name in zip(columns, names) if column != id_column
            )
            sql = 'INSERT INTO ' + sql_identifier(self.table_name, qualified=True)
            sql += (
                ' ('
                + ', '.join(names)
                + ') VALUES ('
                + ', '.join(sql_literal(data[column]) for column in columns)
                + ')'
            )
            sql += ' ON DUPLICATE KEY UPDATE ' + updates
            statements.append(sql)
        return statements

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
        # Parse filter if provided
        where_conditions, where_params = self._parse_pinecone_filter(filter)

        where_clause = substitute_parameters(' AND '.join(where_conditions), where_params) if where_conditions else None

        # Build similarity search query
        schema = self.client.execute('DESCRIBE ' + sql_identifier(self.table_name, qualified=True))
        id_column, columns = self._schema_columns(schema.rows)
        index_info = self._get_index_info()
        select_columns = [id_column]
        if include_metadata:
            select_columns.extend(
                c for c in columns if c.lower() not in (id_column.lower(), self.vector_column.lower())
            )
        if include_values:
            select_columns.append(self.vector_column)

        # Use VectorManager.similarity_search with new API
        results = self.client.vector_ops.similarity_search(
            self.table_name,
            vector_column=self.vector_column,
            query_vector=vector,
            distance_type=self._distance_type(index_info),
            limit=top_k,
            select_columns=select_columns,
            where_clause=where_clause,
        )

        # Convert results to Pinecone format
        return self._materialize_matches(
            results, id_column, columns, index_info, include_values, include_metadata, namespace
        )

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
        # Parse filter if provided
        where_conditions, where_params = self._parse_pinecone_filter(filter)

        where_clause = substitute_parameters(' AND '.join(where_conditions), where_params) if where_conditions else None

        # Build similarity search query
        schema = await self.client.execute('DESCRIBE ' + sql_identifier(self.table_name, qualified=True))
        id_column, columns = self._schema_columns(schema.rows)
        index_info = await self._get_index_info_async()
        select_columns = [id_column]
        if include_metadata:
            select_columns.extend(
                c for c in columns if c.lower() not in (id_column.lower(), self.vector_column.lower())
            )
        if include_values:
            select_columns.append(self.vector_column)

        # Use AsyncVectorManager.similarity_search with new API
        results = await self.client.vector_ops.similarity_search(
            self.table_name,
            vector_column=self.vector_column,
            query_vector=vector,
            distance_type=self._distance_type(index_info),
            limit=top_k,
            select_columns=select_columns,
            where_clause=where_clause,
        )

        # Convert results to Pinecone format
        return self._materialize_matches(
            results, id_column, columns, index_info, include_values, include_metadata, namespace
        )

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
            return {'upserted_count': 0}
        from .client import Client

        if not isinstance(self.client, Client):
            raise ValueError('upsert requires a Client that owns its transaction')
        schema = self.client.execute('DESCRIBE ' + sql_identifier(self.table_name, qualified=True))
        statements = self._prepare_upserts(vectors, schema.rows, self._get_index_info())
        with self.client.session() as session:
            for sql in statements:
                session.execute(sql)
        return {'upserted_count': len(statements)}

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
            return {'upserted_count': 0}
        from .async_client import AsyncClient

        if not isinstance(self.client, AsyncClient):
            raise ValueError('upsert_async requires an AsyncClient that owns its transaction')

        async def run():
            schema = await self.client.execute('DESCRIBE ' + sql_identifier(self.table_name, qualified=True))
            info = await self._get_index_info_async()
            statements = self._prepare_upserts(vectors, schema.rows, info)
            async with self.client.session() as session:
                for sql in statements:
                    await session.execute(sql)
            return {'upserted_count': len(statements)}

        return await asyncio.wait_for(run(), timeout=self.client.query_timeout)

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

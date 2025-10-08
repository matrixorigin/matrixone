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
Fulltext index support for SQLAlchemy integration with MatrixOne.
"""

from typing import Any, List, Union

from sqlalchemy import Index, text
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import CreateIndex


class FulltextAlgorithmType:
    """Enum-like class for fulltext algorithm types."""

    TF_IDF = "TF-IDF"
    BM25 = "BM25"


class FulltextModeType:
    """Enum-like class for fulltext search modes."""

    NATURAL_LANGUAGE = "natural language mode"
    BOOLEAN = "boolean mode"
    QUERY_EXPANSION = "query expansion mode"


class FulltextIndex(Index):
    """
    SQLAlchemy Index for fulltext columns with MatrixOne-specific syntax.

    Specialized class for fulltext indexes with type safety and clear API.

    Usage Examples

    1. Class Methods (Recommended for one-time operations)::

        # Create index using class method
        success = FulltextIndex.create_index(
            engine=engine,
            table_name='my_table',
            name='ftidx_content',
            columns=['title', 'content'],
            algorithm=FulltextAlgorithmType.BM25
        )

        # Drop index using class method
        success = FulltextIndex.drop_index(
            engine=engine,
            table_name='my_table',
            name='ftidx_content'
        )

        # Create index within existing transaction
        with engine.begin() as conn:
            success = FulltextIndex.create_index_in_transaction(
                connection=conn,
                table_name='my_table',
                name='ftidx_content',
                columns=['title', 'content']
            )

        # Drop index within existing transaction
        with engine.begin() as conn:
            success = FulltextIndex.drop_index_in_transaction(
                connection=conn,
                table_name='my_table',
                name='ftidx_content'
            )

    2. Instance Methods (Useful for reusable index configurations)::

        # Create index object
        index = FulltextIndex('ftidx_content', ['title', 'content'], algorithm=FulltextAlgorithmType.BM25)

        # Create index using instance method
        success = index.create(engine, 'my_table')

        # Drop index using instance method
        success = index.drop(engine, 'my_table')

        # Create index within existing transaction
        with engine.begin() as conn:
            success = index.create_in_transaction(conn, 'my_table')

        # Drop index within existing transaction
        with engine.begin() as conn:
            success = index.drop_in_transaction(conn, 'my_table')

    3. SQLAlchemy ORM Integration::

        # In table definition
        class Document(Base):
            __tablename__ = 'documents'
            id = Column(Integer, primary_key=True)
            title = Column(String)
            content = Column(Text)

            # Define fulltext index in table
            __table_args__ = (FulltextIndex('ftidx_doc', ['title', 'content']),)

        # Or create index separately
        FulltextIndex.create_index(engine, 'documents', 'ftidx_doc', ['title', 'content'])

    4. Using client.fulltext_index.create() method::

        # Using client.fulltext_index.create() method
        client.fulltext_index.create(
            'my_table', 'ftidx_content', ['title', 'content'],
            algorithm=FulltextAlgorithmType.BM25
        )

        # Using client.fulltext_index.create_in_transaction() method
        with client.transaction() as tx:
            client.fulltext_index.create_in_transaction(
                tx, 'my_table', 'ftidx_content', ['title', 'content']
            )
    """

    def __init__(
        self,
        name: str,
        columns: Union[str, List[str]],
        algorithm: str = FulltextAlgorithmType.TF_IDF,
    ):
        """
        Initialize FulltextIndex.

        Args:

            name: Index name
            columns: Column(s) to index (string or list of strings)
            algorithm: Fulltext algorithm type (TF-IDF or BM25)
        """
        if isinstance(columns, str):
            columns = [columns]

        self.algorithm = algorithm
        self._column_names = columns.copy()  # Store column names for easy access
        super().__init__(name, *columns)

    def get_columns(self):
        """Get column names as a list"""
        return self._column_names.copy()

    def _create_index_sql(self, table_name: str) -> str:
        """Generate the CREATE INDEX SQL for fulltext index."""
        columns_str = ", ".join(self._column_names)
        return f"CREATE FULLTEXT INDEX {self.name} ON {table_name} ({columns_str})"

    @classmethod
    def create_index(
        cls,
        engine,
        table_name: str,
        name: str,
        columns: Union[str, List[str]],
        algorithm: str = FulltextAlgorithmType.TF_IDF,
    ) -> bool:
        """
        Create a fulltext index using ORM-style method.

        Args:

            engine: SQLAlchemy engine
            table_name: Target table name
            name: Index name
            columns: Column(s) to index
            algorithm: Fulltext algorithm type

        Returns:

            bool: True if successful, False otherwise
        """
        try:
            if isinstance(columns, str):
                columns = [columns]

            columns_str = ", ".join(columns)
            sql = f"CREATE FULLTEXT INDEX {name} ON {table_name} ({columns_str})"

            with engine.begin() as conn:
                conn.execute(text(sql))

            return True
        except Exception as e:
            print(f"Failed to create fulltext index: {e}")
            return False

    @classmethod
    def create_index_in_transaction(
        cls,
        connection,
        table_name: str,
        name: str,
        columns: Union[str, List[str]],
        algorithm: str = FulltextAlgorithmType.TF_IDF,
    ) -> bool:
        """
        Create a fulltext index within an existing transaction.

        Args:

            connection: SQLAlchemy connection
            table_name: Target table name
            name: Index name
            columns: Column(s) to index
            algorithm: Fulltext algorithm type

        Returns:

            bool: True if successful, False otherwise
        """
        try:
            if isinstance(columns, str):
                columns = [columns]

            columns_str = ", ".join(columns)
            sql = f"CREATE FULLTEXT INDEX {name} ON {table_name} ({columns_str})"

            connection.execute(text(sql))
            return True
        except Exception as e:
            print(f"Failed to create fulltext index in transaction: {e}")
            return False

    @classmethod
    def drop_index(cls, engine, table_name: str, name: str) -> bool:
        """
        Drop a fulltext index using ORM-style method.

        Args:

            engine: SQLAlchemy engine
            table_name: Target table name
            name: Index name

        Returns:

            bool: True if successful, False otherwise
        """
        try:
            sql = f"DROP INDEX {name} ON {table_name}"

            with engine.begin() as conn:
                conn.execute(text(sql))

            return True
        except Exception as e:
            print(f"Failed to drop fulltext index: {e}")
            return False

    @classmethod
    def drop_index_in_transaction(cls, connection, table_name: str, name: str) -> bool:
        """
        Drop a fulltext index within an existing transaction.

        Args:

            connection: SQLAlchemy connection
            table_name: Target table name
            name: Index name

        Returns:

            bool: True if successful, False otherwise
        """
        try:
            sql = f"DROP INDEX {name} ON {table_name}"
            connection.execute(text(sql))
            return True
        except Exception as e:
            print(f"Failed to drop fulltext index in transaction: {e}")
            return False

    def create(self, engine, table_name: str) -> bool:
        """
        Create this fulltext index using ORM-style method.

        Args:

            engine: SQLAlchemy engine
            table_name: Target table name

        Returns:

            bool: True if successful, False otherwise
        """
        try:
            sql = self._create_index_sql(table_name)

            with engine.begin() as conn:
                conn.execute(text(sql))

            return True
        except Exception as e:
            print(f"Failed to create fulltext index: {e}")
            return False

    def drop(self, engine, table_name: str) -> bool:
        """
        Drop this fulltext index using ORM-style method.

        Args:

            engine: SQLAlchemy engine
            table_name: Target table name

        Returns:

            bool: True if successful, False otherwise
        """
        try:
            sql = f"DROP INDEX {self.name} ON {table_name}"

            with engine.begin() as conn:
                conn.execute(text(sql))

            return True
        except Exception as e:
            print(f"Failed to drop fulltext index: {e}")
            return False

    def create_in_transaction(self, connection, table_name: str) -> bool:
        """
        Create this fulltext index within an existing transaction.

        Args:

            connection: SQLAlchemy connection
            table_name: Target table name

        Returns:

            bool: True if successful, False otherwise
        """
        try:
            sql = self._create_index_sql(table_name)
            connection.execute(text(sql))
            return True
        except Exception as e:
            print(f"Failed to create fulltext index in transaction: {e}")
            return False

    def drop_in_transaction(self, connection, table_name: str) -> bool:
        """
        Drop this fulltext index within an existing transaction.

        Args:

            connection: SQLAlchemy connection
            table_name: Target table name

        Returns:

            bool: True if successful, False otherwise
        """
        try:
            sql = f"DROP INDEX {self.name} ON {table_name}"
            connection.execute(text(sql))
            return True
        except Exception as e:
            print(f"Failed to drop fulltext index in transaction: {e}")
            return False


class FulltextSearchBuilder:
    """
    Builder class for fulltext search queries.

    Provides a fluent interface for building MATCH...AGAINST queries.
    """

    def __init__(self, table_name: str, columns: Union[str, List[str]]):
        """
        Initialize FulltextSearchBuilder.

        Args:

            table_name: Table to search in
            columns: Column(s) to search in
        """
        self.table_name = table_name
        if isinstance(columns, str):
            columns = [columns]
        self.columns = columns
        self.search_term = None
        self.search_mode = FulltextModeType.NATURAL_LANGUAGE
        self.include_score = False
        self.where_conditions = []
        self.order_clause = None
        self.limit_value = None
        self.offset_value = None

    @property
    def with_score(self):
        """Get the with_score setting for backward compatibility"""
        return self.include_score

    @property
    def mode(self):
        """Get the search mode for backward compatibility"""
        return self.search_mode

    @property
    def order_by(self):
        """Get the order by clause for backward compatibility"""
        return self.order_clause

    def search(self, term: str) -> "FulltextSearchBuilder":
        """
        Set the search term.

        Args:

            term: Search term

        Returns:

            FulltextSearchBuilder: Self for chaining
        """
        self.search_term = term
        return self

    def set_mode(self, mode: str) -> "FulltextSearchBuilder":
        """
        Set the search mode.

        Args:

            mode: Search mode (natural language, boolean, query expansion)

        Returns:

            FulltextSearchBuilder: Self for chaining
        """
        self.search_mode = mode
        return self

    def set_with_score(self, include_score: bool = True) -> "FulltextSearchBuilder":
        """
        Include relevance score in results.

        Args:

            include_score: Whether to include score

        Returns:

            FulltextSearchBuilder: Self for chaining
        """
        self.include_score = include_score
        return self

    def where(self, condition: str) -> "FulltextSearchBuilder":
        """
        Add WHERE condition.

        Args:

            condition: WHERE condition

        Returns:

            FulltextSearchBuilder: Self for chaining
        """
        self.where_conditions.append(condition)
        return self

    def set_order_by(self, column: str, direction: str = "DESC") -> "FulltextSearchBuilder":
        """
        Set ORDER BY clause.

        Args:

            column: Column to order by
            direction: Order direction (ASC/DESC)

        Returns:

            FulltextSearchBuilder: Self for chaining
        """
        self.order_clause = f"{column} {direction}"
        return self

    def limit(self, count: int) -> "FulltextSearchBuilder":
        """
        Set LIMIT clause.

        Args:

            count: Number of rows to limit

        Returns:

            FulltextSearchBuilder: Self for chaining
        """
        self.limit_value = count
        return self

    def offset(self, count: int) -> "FulltextSearchBuilder":
        """
        Set OFFSET clause.

        Args:

            count: Number of rows to offset

        Returns:

            FulltextSearchBuilder: Self for chaining
        """
        self.offset_value = count
        return self

    def build_sql(self) -> str:
        """
        Build the SQL query using unified SQL builder.

        Returns:

            str: SQL query string
        """
        if not self.search_term:
            raise ValueError("Search term is required")

        from ..sql_builder import MatrixOneSQLBuilder

        builder = MatrixOneSQLBuilder()

        # Build SELECT clause
        columns_str = ", ".join(self.columns)
        # MatrixOne doesn't support "IN NATURAL_LANGUAGE" syntax, use simple AGAINST
        if self.search_mode == FulltextModeType.NATURAL_LANGUAGE or self.search_mode == "natural language mode":
            match_clause = f"MATCH({columns_str}) AGAINST('{self.search_term}')"
        elif self.search_mode == FulltextModeType.BOOLEAN or self.search_mode == "boolean mode":
            match_clause = f"MATCH({columns_str}) AGAINST('{self.search_term}' IN BOOLEAN MODE)"
        elif self.search_mode == FulltextModeType.QUERY_EXPANSION or self.search_mode == "query expansion mode":
            match_clause = f"MATCH({columns_str}) AGAINST('{self.search_term}' WITH QUERY EXPANSION)"
        else:
            # Default to simple AGAINST for unknown modes
            match_clause = f"MATCH({columns_str}) AGAINST('{self.search_term}')"

        if self.include_score:
            builder.select("*", f"{match_clause} AS score")
        else:
            builder.select_all()

        # Build FROM clause
        builder.from_table(self.table_name)

        # Build WHERE clause with MATCH AGAINST
        builder.where(match_clause)

        # Add additional WHERE conditions
        for condition in self.where_conditions:
            builder.where(condition)

        # Add ORDER BY clause
        if self.order_clause:
            builder.order_by(self.order_clause)
        elif self.include_score:
            builder.order_by("score DESC")

        # Add LIMIT/OFFSET clause
        if self.limit_value:
            builder.limit(self.limit_value)
            if self.offset_value:
                builder.offset(self.offset_value)

        return builder.build_with_parameter_substitution()

    def execute(self, connection) -> Any:
        """
        Execute the search query.

        Args:

            connection: Database connection

        Returns:

            Query result
        """
        sql = self.build_sql()
        return connection.execute(text(sql))


# Convenience functions
def create_fulltext_index(
    engine,
    table_name: str,
    name: str,
    columns: Union[str, List[str]],
    algorithm: str = FulltextAlgorithmType.TF_IDF,
) -> bool:
    """
    Convenience function to create a fulltext index.

    Args:

        engine: SQLAlchemy engine
        table_name: Target table name
        name: Index name
        columns: Column(s) to index
        algorithm: Fulltext algorithm type

    Returns:

        bool: True if successful, False otherwise
    """
    return FulltextIndex.create_index(engine, table_name, name, columns, algorithm)


def fulltext_search_builder(table_name: str, columns: Union[str, List[str]]) -> FulltextSearchBuilder:
    """
    Convenience function to create a fulltext search builder.

    Args:

        table_name: Table to search in
        columns: Column(s) to search in

    Returns:

        FulltextSearchBuilder: Search builder instance
    """
    return FulltextSearchBuilder(table_name, columns)


# Register SQLAlchemy compiler for FulltextIndex to generate FULLTEXT DDL
@compiles(CreateIndex)
def compile_create_index(element, compiler, **kw):
    """
    Custom compiler for CREATE INDEX that handles FulltextIndex specially.

    This function intercepts SQLAlchemy's CREATE INDEX statement generation
    and adds the FULLTEXT keyword for FulltextIndex instances.
    """
    index = element.element

    # Check if this is a FulltextIndex
    if isinstance(index, FulltextIndex):
        # Generate FULLTEXT index DDL
        columns_str = ", ".join(col.name for col in index.columns)
        return f"CREATE FULLTEXT INDEX {index.name} ON {index.table.name} ({columns_str})"

    # Default behavior for regular indexes
    return compiler.visit_create_index(element, **kw)

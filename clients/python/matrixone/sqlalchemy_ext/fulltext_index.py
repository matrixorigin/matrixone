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


def _exec_sql_safe(connection, sql: str):
    """
    Execute SQL safely, bypassing SQLAlchemy's bind parameter parsing.

    This prevents JSON strings like {"a":1} from being incorrectly parsed as :1 bind params.
    Uses exec_driver_sql() when available, falls back to text() for testing/compatibility.
    """
    if hasattr(connection, 'exec_driver_sql'):
        # Escape % to %% for pymysql's format string handling
        escaped_sql = sql.replace('%', '%%')
        return connection.exec_driver_sql(escaped_sql)
    else:
        # Fallback for testing or older SQLAlchemy versions
        return connection.execute(text(sql))


class FulltextAlgorithmType:
    """
    Enum-like class for fulltext algorithm types.

    MatrixOne supports two main fulltext relevancy algorithms:

    Attributes:
        TF_IDF (str): Term Frequency-Inverse Document Frequency

            * Traditional information retrieval algorithm
            * Good for specific use cases with proven reliability
            * Formula: TF(term) × IDF(term)
            * Use case: Academic search, technical documentation

        BM25 (str): Best Matching 25 (Okapi BM25)

            * Modern probabilistic ranking algorithm
            * Generally superior to TF-IDF for diverse content
            * Handles document length normalization better
            * Use case: General-purpose search, modern applications
            * Recommended as default for new applications

    Note:
        The algorithm is set at runtime via SQL command, not in the index DDL.

    Examples::

        # Set algorithm to BM25
        client.execute('SET ft_relevancy_algorithm = "BM25"')

        # Create index with BM25 reference
        index = FulltextIndex("ftidx_content", ["title", "content"],
                             algorithm=FulltextAlgorithmType.BM25)

        # Perform searches with BM25 scoring
        result = client.query(Article).filter(
            boolean_match(Article.content).must("search term")
        ).execute()
    """

    TF_IDF = "TF-IDF"
    BM25 = "BM25"


class FulltextParserType:
    """
    Enum-like class for fulltext parser types.

    MatrixOne supports specialized parsers for different content types.

    Attributes:
        JSON (str): Parser for JSON documents

            * Indexes JSON values (not keys)
            * Suitable for text/varchar/json columns containing JSON data
            * Use case: Product details, user profiles, metadata
            * Example SQL: CREATE FULLTEXT INDEX idx ON table (col) WITH PARSER json

        NGRAM (str): Parser for Chinese and Asian languages

            * N-gram based tokenization for languages without word delimiters
            * Better word segmentation for Chinese, Japanese, Korean, etc.
            * Use case: Chinese articles, mixed language content
            * Example SQL: CREATE FULLTEXT INDEX idx ON table (col) WITH PARSER ngram

    Examples::

        # Using JSON parser in ORM
        class Product(Base):
            __tablename__ = "products"
            details = Column(Text)
            __table_args__ = (
                FulltextIndex("ftidx_json", "details",
                             parser=FulltextParserType.JSON),
            )

        # Using NGRAM parser for Chinese content
        class ChineseArticle(Base):
            __tablename__ = "chinese_articles"
            title = Column(String(200))
            body = Column(Text)
            __table_args__ = (
                FulltextIndex("ftidx_chinese", ["title", "body"],
                             parser=FulltextParserType.NGRAM),
            )

        # Using parser in create_index method
        FulltextIndex.create_index(
            engine, 'products', 'ftidx_json', 'details',
            parser=FulltextParserType.JSON
        )
    """

    JSON = "json"
    NGRAM = "ngram"


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
        parser: str = None,
    ):
        """
        Initialize FulltextIndex.

        Args:
            name (str): Index name (e.g., 'ftidx_content', 'idx_search')

            columns (str or list): Column(s) to index

                * Single column: "content" or ["content"]
                * Multiple columns: ["title", "content"]

            algorithm (str): Fulltext algorithm type (stored but not part of DDL)

                * FulltextAlgorithmType.TF_IDF (default): Traditional TF-IDF
                * FulltextAlgorithmType.BM25: Modern BM25 ranking
                * Note: Set via SET ft_relevancy_algorithm at runtime

            parser (str, optional): Parser type for specialized content

                * None (default): Standard text parser
                * FulltextParserType.JSON: Parse JSON documents
                * FulltextParserType.NGRAM: N-gram for Chinese/Asian languages

        Examples::

            # Basic fulltext index (no parser)
            index = FulltextIndex("ftidx_content", "content")

            # Multiple columns with BM25
            index = FulltextIndex("ftidx_search", ["title", "content"],
                                 algorithm=FulltextAlgorithmType.BM25)

            # JSON parser for JSON content
            index = FulltextIndex("ftidx_json", "json_data",
                                 parser=FulltextParserType.JSON)

            # NGRAM parser for Chinese content
            index = FulltextIndex("ftidx_chinese", ["title", "body"],
                                 parser=FulltextParserType.NGRAM)

            # Combined: Multiple columns with JSON parser
            index = FulltextIndex("ftidx_multi_json", ["json1", "json2"],
                                 parser=FulltextParserType.JSON)
        """
        if isinstance(columns, str):
            columns = [columns]

        self.algorithm = algorithm
        self.parser = parser
        self._column_names = columns.copy()  # Store column names for easy access
        super().__init__(name, *columns)

    def get_columns(self):
        """Get column names as a list"""
        return self._column_names.copy()

    def _create_index_sql(self, table_name: str) -> str:
        """Generate the CREATE INDEX SQL for fulltext index."""
        columns_str = ", ".join(self._column_names)
        sql = f"CREATE FULLTEXT INDEX {self.name} ON {table_name} ({columns_str})"
        if self.parser:
            sql += f" WITH PARSER {self.parser}"
        return sql

    @classmethod
    def create_index(
        cls,
        engine,
        table_name: str,
        name: str,
        columns: Union[str, List[str]],
        algorithm: str = FulltextAlgorithmType.TF_IDF,
        parser: str = None,
    ) -> bool:
        """
        Create a fulltext index using class method.

        This method creates a fulltext index on specified columns with optional
        parser support for specialized content types (JSON, Chinese text, etc.).

        Args:
            engine: SQLAlchemy engine instance
            table_name (str): Target table name (e.g., 'articles', 'documents')
            name (str): Index name (e.g., 'ftidx_content', 'idx_search')
            columns (str or list): Column(s) to index

                * Single: "content" or ["content"]
                * Multiple: ["title", "content", "summary"]

            algorithm (str): Algorithm type (stored for reference, not in DDL)

                * FulltextAlgorithmType.TF_IDF (default)
                * FulltextAlgorithmType.BM25
                * Set via: SET ft_relevancy_algorithm = "BM25"

            parser (str, optional): Parser type for specialized content

                * None (default): Standard parser
                * FulltextParserType.JSON: For JSON documents
                * FulltextParserType.NGRAM: For Chinese/Asian languages

        Returns:
            bool: True if succeeded, False otherwise

        Examples::

            # Basic fulltext index
            FulltextIndex.create_index(
                engine, 'articles', 'ftidx_content', 'content'
            )

            # Multiple columns with BM25
            FulltextIndex.create_index(
                engine, 'articles', 'ftidx_search', ['title', 'content'],
                algorithm=FulltextAlgorithmType.BM25
            )

            # JSON parser
            FulltextIndex.create_index(
                engine, 'products', 'ftidx_json', 'details',
                parser=FulltextParserType.JSON
            )

            # NGRAM parser
            FulltextIndex.create_index(
                engine, 'chinese_articles', 'ftidx_chinese', ['title', 'body'],
                parser=FulltextParserType.NGRAM
            )
        """
        try:
            if isinstance(columns, str):
                columns = [columns]

            columns_str = ", ".join(columns)
            sql = f"CREATE FULLTEXT INDEX {name} ON {table_name} ({columns_str})"
            if parser:
                sql += f" WITH PARSER {parser}"

            with engine.begin() as conn:
                _exec_sql_safe(conn, sql)

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
        parser: str = None,
    ) -> bool:
        """
        Create a fulltext index within an existing transaction.

        Use this method when you need to create a fulltext index as part of a
        larger transaction, ensuring atomic operations.

        Args:
            connection: Active SQLAlchemy connection within a transaction
            table_name (str): Target table name
            name (str): Index name
            columns (str or list): Column(s) to index

                * Single: "content" or ["content"]
                * Multiple: ["title", "content"]

            algorithm (str): Algorithm type (stored for reference)

                * FulltextAlgorithmType.TF_IDF (default)
                * FulltextAlgorithmType.BM25

            parser (str, optional): Parser type

                * None (default): Standard parser
                * FulltextParserType.JSON: For JSON documents
                * FulltextParserType.NGRAM: For Chinese/Asian languages

        Returns:
            bool: True if succeeded, False otherwise

        Examples::

            # Basic usage within transaction
            with engine.begin() as conn:
                FulltextIndex.create_index_in_transaction(
                    conn, 'articles', 'ftidx_content', 'content'
                )

            # With JSON parser
            with engine.begin() as conn:
                FulltextIndex.create_index_in_transaction(
                    conn, 'products', 'ftidx_json', 'details',
                    parser=FulltextParserType.JSON
                )

            # With NGRAM parser
            with engine.begin() as conn:
                FulltextIndex.create_index_in_transaction(
                    conn, 'chinese_docs', 'ftidx_chinese', ['title', 'body'],
                    parser=FulltextParserType.NGRAM
                )
        """
        try:
            if isinstance(columns, str):
                columns = [columns]

            columns_str = ", ".join(columns)
            sql = f"CREATE FULLTEXT INDEX {name} ON {table_name} ({columns_str})"
            if parser:
                sql += f" WITH PARSER {parser}"

            _exec_sql_safe(connection, sql)
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
                _exec_sql_safe(conn, sql)

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
            _exec_sql_safe(connection, sql)
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
                _exec_sql_safe(conn, sql)

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
                _exec_sql_safe(conn, sql)

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
            _exec_sql_safe(connection, sql)
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
            _exec_sql_safe(connection, sql)
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
        return _exec_sql_safe(connection, sql)


# Convenience functions
def create_fulltext_index(
    engine,
    table_name: str,
    name: str,
    columns: Union[str, List[str]],
    algorithm: str = FulltextAlgorithmType.TF_IDF,
    parser: str = None,
) -> bool:
    """
    Convenience function to create a fulltext index.

    Args:

        engine: SQLAlchemy engine
        table_name: Target table name
        name: Index name
        columns: Column(s) to index
        algorithm: Fulltext algorithm type
        parser: Parser type for fulltext index (json, ngram, or None)

    Returns:

        bool: True if successful, False otherwise
    """
    return FulltextIndex.create_index(engine, table_name, name, columns, algorithm, parser)


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
    and adds the FULLTEXT keyword and parser clause for FulltextIndex instances.
    """
    index = element.element

    # Check if this is a FulltextIndex
    if isinstance(index, FulltextIndex):
        # Generate FULLTEXT index DDL
        columns_str = ", ".join(col.name for col in index.columns)
        sql = f"CREATE FULLTEXT INDEX {index.name} ON {index.table.name} ({columns_str})"
        if hasattr(index, 'parser') and index.parser:
            sql += f" WITH PARSER {index.parser}"
        return sql

    # Default behavior for regular indexes
    return compiler.visit_create_index(element, **kw)

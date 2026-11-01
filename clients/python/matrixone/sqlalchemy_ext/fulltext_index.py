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

from .._sql_literals import string_grammar_token
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

        # Create the index; select BM25 when executing a search
        index = FulltextIndex("ftidx_content", ["title", "content"],
                             algorithm=FulltextAlgorithmType.TF_IDF)

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
            algorithm=FulltextAlgorithmType.TF_IDF
        )

        # Drop index using class method
        success = FulltextIndex.drop_index(
            engine=engine,
            table_name='my_table',
            name='ftidx_content'
        )

    2. Instance Methods (Useful for reusable index configurations)::

        # Create index object
        index = FulltextIndex('ftidx_content', ['title', 'content'], algorithm=FulltextAlgorithmType.TF_IDF)

        # Create index using instance method
        success = index.create(engine, 'my_table')

        # Drop index using instance method
        success = index.drop(engine, 'my_table')

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

    4. Using client.fulltext_index manager (recommended)::

        # Auto-commit mode
        client.fulltext_index.create(
            'my_table', 'ftidx_content', ['title', 'content'],
            algorithm=FulltextAlgorithmType.TF_IDF
        )

        # Transaction context mode
        with client.session() as session:
            session.fulltext_index.create(
                'my_table', 'ftidx_content', ['title', 'content']
            )
            session.commit()
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

            algorithm (str): Compatibility TF-IDF default only; ranking is query-scoped

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
                                 algorithm=FulltextAlgorithmType.TF_IDF)

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

        # Validate columns parameter
        if not columns:
            raise ValueError("FulltextIndex requires at least one column")

        if not all(isinstance(col, str) for col in columns):
            raise TypeError("All columns must be strings")

        self.validate_algorithm(algorithm)
        if parser not in (None, FulltextParserType.JSON, FulltextParserType.NGRAM):
            raise ValueError('Unsupported fulltext parser')
        self.algorithm = algorithm
        self.parser = parser
        self._column_names = columns.copy()  # Store column names for easy access
        super().__init__(name, *columns)

    @staticmethod
    def validate_algorithm(algorithm):
        if algorithm != FulltextAlgorithmType.TF_IDF:
            raise ValueError(
                'Index DDL cannot configure ranking; use the search builder with_algorithm() '
                'for BM25. The default TF-IDF argument is retained for compatibility.'
            )

    def get_columns(self):
        """Get column names as a list"""
        return self._column_names.copy()

    def _create_index_sql(self, table_name: str) -> str:
        """Generate the CREATE INDEX SQL for fulltext index."""
        from .._sql_literals import sql_object_name

        columns_str = ", ".join(sql_object_name(name, force=True) for name in self._column_names)
        sql = "CREATE FULLTEXT INDEX %s ON %s (%s)" % (
            sql_object_name(self.name, force=True),
            sql_object_name(table_name, qualified=True, force=True),
            columns_str,
        )
        if self.parser:
            sql += f" WITH PARSER {self.parser}"
        return sql

    @classmethod
    def create_index(
        cls,
        bind,
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
            bind: SQLAlchemy connection (from session.connection() or engine.begin())
            table_name (str): Target table name (e.g., 'articles', 'documents')
            name (str): Index name (e.g., 'ftidx_content', 'idx_search')
            columns (str or list): Column(s) to index

                * Single: "content" or ["content"]
                * Multiple: ["title", "content", "summary"]

            algorithm (str): Compatibility TF-IDF default only; ranking is query-scoped

                * FulltextAlgorithmType.TF_IDF (default)
                * FulltextAlgorithmType.BM25
                * Set via: SET ft_relevancy_algorithm = "BM25"

            parser (str, optional): Parser type for specialized content

                * None (default): Standard parser
                * FulltextParserType.JSON: For JSON documents
                * FulltextParserType.NGRAM: For Chinese/Asian languages

        Returns:
            bool: True on acknowledgement; database errors propagate

        Examples::

            # With session connection (recommended)
            with client.session() as session:
                conn = session.connection()
                FulltextIndex.create_index(
                    conn, 'articles', 'ftidx_content', 'content'
                )
                session.commit()

            # With async session connection
            async with async_client.session() as session:
                conn = await session.connection()
                await conn.run_sync(
                    lambda sync_conn: FulltextIndex.create_index(
                        sync_conn, 'articles', 'ftidx_content', 'content'
                    )
                )
                await session.commit()
        """
        index = cls(name, columns, algorithm=algorithm, parser=parser)
        _exec_sql_safe(bind, index._create_index_sql(table_name))
        return True

    @classmethod
    def drop_index(cls, bind, table_name: str, name: str) -> bool:
        """
        Drop a fulltext index using ORM-style method.

        Args:

            bind: SQLAlchemy connection (from session.connection() or engine.begin())
            table_name: Target table name
            name: Index name

        Returns:

            bool: True on acknowledgement; database errors propagate
        """
        from .._sql_literals import sql_object_name

        sql = "DROP INDEX %s ON %s" % (
            sql_object_name(name, force=True),
            sql_object_name(table_name, qualified=True, force=True),
        )
        # Directly use the provided connection
        _exec_sql_safe(bind, sql)
        return True

    def create(self, engine, table_name: str) -> bool:
        """
        Create this fulltext index using ORM-style method.

        Args:

            engine: SQLAlchemy engine
            table_name: Target table name

        Returns:

            bool: True on acknowledgement; database errors propagate
        """
        sql = self._create_index_sql(table_name)
        with engine.begin() as conn:
            _exec_sql_safe(conn, sql)
        return True

    def drop(self, engine, table_name: str) -> bool:
        """
        Drop this fulltext index using ORM-style method.

        Args:

            engine: SQLAlchemy engine
            table_name: Target table name

        Returns:

            bool: True on acknowledgement; database errors propagate
        """
        from .._sql_literals import sql_object_name

        sql = "DROP INDEX %s ON %s" % (
            sql_object_name(self.name, force=True),
            sql_object_name(table_name, qualified=True, force=True),
        )
        with engine.begin() as conn:
            _exec_sql_safe(conn, sql)
        return True


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

        from .fulltext_search import _match_columns, _match_mode

        columns_str = _match_columns(self.columns)
        match_clause = (
            f"MATCH({columns_str}) AGAINST({string_grammar_token(self.search_term)}{_match_mode(self.search_mode)})"
        )

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
        if self.limit_value is not None:
            builder.limit(self.limit_value)
        if self.offset_value is not None:
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

        bool: True on acknowledgement; database errors propagate
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
        # Validate table is bound
        if index.table is None:
            raise ValueError(
                f"FulltextIndex '{index.name}' is not bound to a table. "
                "Ensure the index is defined in __table_args__ or bound to a table."
            )

        # Generate FULLTEXT index DDL
        columns_str = ", ".join(compiler.preparer.quote(col.name) for col in index.columns)
        sql = f"CREATE FULLTEXT INDEX {compiler.preparer.quote(index.name)} ON {compiler.preparer.format_table(index.table)} ({columns_str})"
        if hasattr(index, 'parser') and index.parser:
            sql += f" WITH PARSER {index.parser}"
        return sql

    # Default behavior for regular indexes
    return compiler.visit_create_index(element, **kw)

"""
Advanced Fulltext Search Builder for MatrixOne

This module provides an Elasticsearch-like query builder for MatrixOne fulltext search,
with chainable methods and comprehensive search capabilities.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Optional

if TYPE_CHECKING:
    from ..client import Client


class FulltextSearchMode:
    """Enum-like class for fulltext search modes."""

    NATURAL_LANGUAGE = "natural language mode"
    BOOLEAN = "boolean mode"
    QUERY_EXPANSION = "query expansion mode"


class FulltextSearchAlgorithm:
    """Enum-like class for fulltext search algorithms."""

    TF_IDF = "TF-IDF"
    BM25 = "BM25"


class FulltextTerm:
    """Represents a single search term with optional modifiers."""

    def __init__(self, term: str, modifier: str = ""):
        self.term = term
        self.modifier = modifier  # +, -, ~, etc.

    def __str__(self) -> str:
        return f"{self.modifier}{self.term}"


class FulltextQuery:
    """Represents a fulltext query with terms and phrases."""

    def __init__(self):
        self.terms: List[FulltextTerm] = []
        self.phrases: List[str] = []
        self.wildcards: List[str] = []

    def add_term(
        self, term: str, required: bool = False, excluded: bool = False, proximity: Optional[int] = None
    ) -> "FulltextQuery":
        """
        Add a search term to the query.

        Args:
            term: The search term
            required: Whether the term is required (+) - must contain this term
            excluded: Whether the term is excluded (-) - must not contain this term
            proximity: Proximity modifier for boolean mode (not supported in MatrixOne)

        Returns:
            FulltextQuery: Self for chaining

        Examples:
            # Required term: +machine
            .add_term("machine", required=True)

            # Excluded term: -java
            .add_term("java", excluded=True)

            # Optional term: learning
            .add_term("learning")
        """
        modifier = ""
        if required:
            modifier = "+"
        elif excluded:
            modifier = "-"

        # Note: proximity not supported in MatrixOne, but < > operators are supported
        if proximity is not None:
            modifier = f"{modifier}(<{term} >{proximity})"

        self.terms.append(FulltextTerm(term, modifier))
        return self

    def add_phrase(self, phrase: str) -> "FulltextQuery":
        """
        Add an exact phrase to the query.

        Args:
            phrase: The exact phrase to search for (wrapped in double quotes)

        Returns:
            FulltextQuery: Self for chaining

        Examples:
            # Exact phrase: "machine learning"
            .add_phrase("machine learning")
        """
        self.phrases.append(f'"{phrase}"')
        return self

    def add_wildcard(self, pattern: str) -> "FulltextQuery":
        """
        Add a wildcard pattern to the query.

        Args:
            pattern: Wildcard pattern with * suffix (e.g., "test*", "neural*")

        Returns:
            FulltextQuery: Self for chaining

        Examples:
            # Prefix match: neural*
            .add_wildcard("neural*")
        """
        self.wildcards.append(pattern)
        return self

    def build(self) -> str:
        """Build the final query string."""
        parts = []

        # Add terms
        for term in self.terms:
            parts.append(str(term))

        # Add phrases
        parts.extend(self.phrases)

        # Add wildcards
        parts.extend(self.wildcards)

        return " ".join(parts)


class FulltextSearchBuilder:
    """
    Elasticsearch-like fulltext search builder for MatrixOne.

    Provides a chainable interface for building complex fulltext queries
    with support for various search modes, filters, and sorting.

    Boolean Mode Operators:
        +word     : Required term (must contain)
        -word     : Excluded term (must not contain)
        ~word     : Lower weight term (reduces relevance score)
        <word     : Lower weight term (reduces relevance score)
        >word     : Higher weight term (increases relevance score)
        word      : Optional term (may contain)
        "phrase"  : Exact phrase match
        word*     : Wildcard prefix match
        (word1 word2) : Grouping (contains any of the words)

    Note: MatrixOne supports +, -, ~, *, "", (), <, > operators.
          All boolean mode operators are fully supported.

    Search Modes:
        - NATURAL_LANGUAGE: Automatic stopword removal, stemming, relevance scoring
        - BOOLEAN: Full control with operators, no automatic processing
        - QUERY_EXPANSION: Not supported in MatrixOne (auto-expands query with related terms)

    Examples:
        # Natural language search
        results = client.fulltext_search() \\
            .table("articles") \\
            .columns(["title", "content"]) \\
            .with_mode(FulltextSearchMode.NATURAL_LANGUAGE) \\
            .query("machine learning") \\
            .with_score() \\
            .limit(10) \\
            .execute()

        # Boolean search with complex terms
        results = client.fulltext_search() \\
            .table("articles") \\
            .columns(["title", "content"]) \\
            .with_mode(FulltextSearchMode.BOOLEAN) \\
            .add_term("machine", required=True) \\
            .add_term("learning", required=True) \\
            .add_term("python", excluded=True) \\
            .add_phrase("deep learning") \\
            .add_wildcard("neural*") \\
            .where("category = 'AI'") \\
            .order_by("score", "DESC") \\
            .limit(20) \\
            .execute()

        # Complex boolean query equivalent to: +red -(<blue >is)
        results = client.fulltext_search() \\
            .table("src") \\
            .columns(["body", "title"]) \\
            .with_mode(FulltextSearchMode.BOOLEAN) \\
            .add_term("red", required=True) \\
            .add_term("blue", excluded=True) \\
            .add_term("is", excluded=True) \\
            .execute()

        # Weight operators example: +machine <java >python
        results = client.fulltext_search() \\
            .table("articles") \\
            .columns(["title", "content"]) \\
            .with_mode(FulltextSearchMode.BOOLEAN) \\
            .query("+machine <java >python") \\
            .with_score() \\
            .execute()
    """

    def __init__(self, client: "Client"):
        self.client = client
        self._table_name: Optional[str] = None
        self._columns: List[str] = []
        self._search_mode = FulltextSearchMode.NATURAL_LANGUAGE
        self._algorithm = FulltextSearchAlgorithm.BM25
        self._query_obj = FulltextQuery()
        self._include_score = False
        self._where_conditions: List[str] = []
        self._order_by: Optional[str] = None
        self._limit_value: Optional[int] = None
        self._offset_value: Optional[int] = None
        self._select_columns: List[str] = ["*"]

    def table(self, table_name: str) -> "FulltextSearchBuilder":
        """
        Set the target table for the search.

        Args:
            table_name: Name of the table to search

        Returns:
            FulltextSearchBuilder: Self for chaining
        """
        self._table_name = table_name
        return self

    def columns(self, columns: List[str]) -> "FulltextSearchBuilder":
        """
        Set the columns to search in.

        Args:
            columns: List of column names to search

        Returns:
            FulltextSearchBuilder: Self for chaining
        """
        self._columns = columns
        return self

    def with_mode(self, mode: str) -> "FulltextSearchBuilder":
        """
        Set the search mode.

        Args:
            mode: Search mode
                - FulltextSearchMode.NATURAL_LANGUAGE: Automatic processing, user-friendly
                - FulltextSearchMode.BOOLEAN: Full control with operators
                - FulltextSearchMode.QUERY_EXPANSION: Not supported in MatrixOne

        Returns:
            FulltextSearchBuilder: Self for chaining

        Examples:
            # Natural language mode (default)
            .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)

            # Boolean mode for complex queries
            .with_mode(FulltextSearchMode.BOOLEAN)
        """
        self._search_mode = mode
        return self

    def with_algorithm(self, algorithm: str) -> "FulltextSearchBuilder":
        """
        Set the search algorithm.

        Args:
            algorithm: Search algorithm
                - FulltextSearchAlgorithm.TF_IDF: Traditional TF-IDF scoring
                - FulltextSearchAlgorithm.BM25: Modern BM25 scoring (recommended)

        Returns:
            FulltextSearchBuilder: Self for chaining

        Examples:
            # Use BM25 algorithm (recommended)
            .with_algorithm(FulltextSearchAlgorithm.BM25)

            # Use TF-IDF algorithm
            .with_algorithm(FulltextSearchAlgorithm.TF_IDF)
        """
        self._algorithm = algorithm
        return self

    def query(self, query_string: str) -> "FulltextSearchBuilder":
        """
        Set a simple query string (resets previous terms).

        Args:
            query_string: The search query (natural language or boolean syntax)

        Returns:
            FulltextSearchBuilder: Self for chaining

        Examples:
            # Natural language query
            .query("machine learning algorithms")

            # Boolean query
            .query("+machine +learning -java")

        Note: This method resets any previously added terms, phrases, or wildcards.
        """
        self._query_obj = FulltextQuery()
        self._query_obj.add_term(query_string)
        return self

    def add_term(
        self, term: str, required: bool = False, excluded: bool = False, proximity: Optional[int] = None
    ) -> "FulltextSearchBuilder":
        """
        Add a search term to the query.

        Args:
            term: The search term
            required: Whether the term is required (+) - must contain this term
            excluded: Whether the term is excluded (-) - must not contain this term
            proximity: Proximity modifier for boolean mode (not supported in MatrixOne)

        Returns:
            FulltextSearchBuilder: Self for chaining

        Examples:
            # Required term: +machine
            .add_term("machine", required=True)

            # Excluded term: -java
            .add_term("java", excluded=True)

            # Optional term: learning
            .add_term("learning")

            # Complex query: +machine +learning -java
            .add_term("machine", required=True)
            .add_term("learning", required=True)
            .add_term("java", excluded=True)
        """
        self._query_obj.add_term(term, required, excluded, proximity)
        return self

    def add_phrase(self, phrase: str) -> "FulltextSearchBuilder":
        """
        Add an exact phrase to the query.

        Args:
            phrase: The exact phrase to search for (wrapped in double quotes)

        Returns:
            FulltextSearchBuilder: Self for chaining

        Examples:
            # Exact phrase: "machine learning"
            .add_phrase("machine learning")

            # Multiple phrases
            .add_phrase("deep learning")
            .add_phrase("neural networks")
        """
        self._query_obj.add_phrase(phrase)
        return self

    def add_wildcard(self, pattern: str) -> "FulltextSearchBuilder":
        """
        Add a wildcard pattern to the query.

        Args:
            pattern: Wildcard pattern with * suffix (e.g., "test*", "neural*")

        Returns:
            FulltextSearchBuilder: Self for chaining

        Examples:
            # Prefix match: neural*
            .add_wildcard("neural*")

            # Multiple wildcards
            .add_wildcard("machine*")
            .add_wildcard("learn*")
        """
        self._query_obj.add_wildcard(pattern)
        return self

    def with_score(self, include: bool = True) -> "FulltextSearchBuilder":
        """
        Include relevance score in results.

        Args:
            include: Whether to include the score

        Returns:
            FulltextSearchBuilder: Self for chaining
        """
        self._include_score = include
        return self

    def select(self, columns: List[str]) -> "FulltextSearchBuilder":
        """
        Set the columns to select in the result.

        Args:
            columns: List of column names to select

        Returns:
            FulltextSearchBuilder: Self for chaining
        """
        self._select_columns = columns
        return self

    def where(self, condition: str) -> "FulltextSearchBuilder":
        """
        Add a WHERE condition.

        Args:
            condition: WHERE condition

        Returns:
            FulltextSearchBuilder: Self for chaining
        """
        self._where_conditions.append(condition)
        return self

    def order_by(self, column: str, direction: str = "DESC") -> "FulltextSearchBuilder":
        """
        Set ORDER BY clause.

        Args:
            column: Column to order by
            direction: Order direction (ASC/DESC)

        Returns:
            FulltextSearchBuilder: Self for chaining
        """
        self._order_by = f"{column} {direction}"
        return self

    def limit(self, count: int) -> "FulltextSearchBuilder":
        """
        Set LIMIT clause.

        Args:
            count: Number of results to return

        Returns:
            FulltextSearchBuilder: Self for chaining
        """
        self._limit_value = count
        return self

    def offset(self, count: int) -> "FulltextSearchBuilder":
        """
        Set OFFSET clause.

        Args:
            count: Number of results to skip

        Returns:
            FulltextSearchBuilder: Self for chaining
        """
        self._offset_value = count
        return self

    def _build_sql(self) -> str:
        """Build the final SQL query."""
        if not self._table_name:
            raise ValueError("Table name is required")

        if not self._columns:
            raise ValueError("Search columns are required")

        query_string = self._query_obj.build()
        if not query_string:
            raise ValueError("Query is required")

        # Build SELECT clause
        select_parts = self._select_columns.copy()
        if self._include_score:
            columns_str = ", ".join(self._columns)
            select_parts.append(f"MATCH({columns_str}) AGAINST('{query_string}' IN {self._search_mode}) AS score")

        select_clause = f"SELECT {', '.join(select_parts)}"

        # Build FROM clause
        from_clause = f"FROM {self._table_name}"

        # Build WHERE clause
        where_parts = []

        # Add fulltext search condition
        columns_str = ", ".join(self._columns)
        fulltext_condition = f"MATCH({columns_str}) AGAINST('{query_string}' IN {self._search_mode})"
        where_parts.append(fulltext_condition)

        # Add additional WHERE conditions
        where_parts.extend(self._where_conditions)

        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

        # Build ORDER BY clause
        order_clause = f"ORDER BY {self._order_by}" if self._order_by else ""

        # Build LIMIT clause
        limit_clause = f"LIMIT {self._limit_value}" if self._limit_value else ""

        # Build OFFSET clause
        offset_clause = f"OFFSET {self._offset_value}" if self._offset_value else ""

        # Combine all clauses
        sql_parts = [select_clause, from_clause, where_clause, order_clause, limit_clause, offset_clause]
        return " ".join(filter(None, sql_parts))

    def execute(self) -> Any:
        """
        Execute the fulltext search query.

        Returns:
            Query results
        """
        sql = self._build_sql()
        return self.client.execute(sql)

    def explain(self) -> str:
        """
        Get the SQL query that would be executed.

        Returns:
            SQL query string
        """
        return self._build_sql()


class FulltextIndexManager:
    """Manager for fulltext index operations."""

    def __init__(self, client: "Client"):
        self.client = client

    def create(
        self, table_name: str, name: str, columns: List[str], algorithm: str = FulltextSearchAlgorithm.BM25
    ) -> bool:
        """
        Create a fulltext index.

        Args:
            table_name: Name of the table
            name: Name of the index
            columns: List of columns to index
            algorithm: Search algorithm to use

        Returns:
            bool: True if successful
        """
        from .fulltext_index import FulltextIndex

        # Set the algorithm
        self.client.execute(f'SET ft_relevancy_algorithm = "{algorithm}"')

        # Create the index
        success = FulltextIndex.create_index(
            engine=self.client.get_sqlalchemy_engine(),
            table_name=table_name,
            name=name,
            columns=columns,
            algorithm=algorithm,
        )

        return success

    def drop(self, table_name: str, name: str) -> bool:
        """
        Drop a fulltext index.

        Args:
            table_name: Name of the table
            name: Name of the index

        Returns:
            bool: True if successful
        """
        from .fulltext_index import FulltextIndex

        success = FulltextIndex.drop_index(engine=self.client.get_sqlalchemy_engine(), table_name=table_name, name=name)

        return success

    def search(self) -> "FulltextSearchBuilder":
        """
        Create a new fulltext search builder.

        Returns:
            FulltextSearchBuilder: New search builder instance
        """
        return FulltextSearchBuilder(self.client)

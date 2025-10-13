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
Advanced Fulltext Search Builder for MatrixOne

This module provides an Elasticsearch-like query builder for MatrixOne fulltext search,
with chainable methods and comprehensive search capabilities.

## Column Matching Requirements

**CRITICAL**: The columns specified in MATCH() must exactly match the columns
defined in the FULLTEXT index. This is a MatrixOne requirement.

Examples:
- If your index is: `FULLTEXT(title, content, tags)`
- Your MATCH() must be: `MATCH(title, content, tags) AGAINST(...)`
- NOT: `MATCH(title) AGAINST(...)` or `MATCH(title, content) AGAINST(...)`

## MatrixOne Limitations

1. **Multiple MATCH() Functions**: MatrixOne does not support multiple
   MATCH() functions in the same query.

   ❌ WRONG: `WHERE MATCH(...) AND MATCH(...)`
   ✅ CORRECT: Use chained filter() calls or combine terms in single MATCH()

2. **Complex Nested Groups**: Some complex nested syntaxes are not supported.

   ❌ WRONG: `'+learning -basic (+machine AI) (+deep neural)'`
   ✅ CORRECT: `'+learning -basic +machine +deep'`

## Supported Boolean Mode Operators

### Group-level operators (applied to entire groups):
- `+(group)`: Group must be present
- `-(group)`: Group must not be present

### Element-level operators:
- `+term`: Term must contain (required)
- `-term`: Term must not contain (excluded)
- `term`: Term optional (should contain)
- `"phrase"`: Exact phrase match
- `term*`: Prefix match

### Weight operators (within groups/elements):
- `term`: Optional term with normal positive weight boost
- `>term`: Higher relevance weight for term (high positive boost)
- `<term`: Lower relevance weight for term (low positive boost)
- `~term`: Reduced/suppressed relevance weight (negative or minimal boost)

### Weight Operator Comparison:
- `encourage("tutorial")` → `tutorial` : Encourages documents with "tutorial"
- `discourage("legacy")` → `~legacy` : Discourages documents with "legacy"

Both are optional (don't filter documents) but affect ranking differently.

### MatrixOne Example:
- `'+red -(<blue >is)'`: Must have 'red', must NOT have group containing 'blue' (low weight) and 'is' (high weight)

## Supported Modes

- **NATURAL LANGUAGE**: Default full-text search
- **BOOLEAN**: Advanced boolean operators
- **QUERY EXPANSION**: Automatic query expansion (limited support)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Optional

from sqlalchemy import Boolean
from sqlalchemy.sql import and_, not_, or_, text
from sqlalchemy.sql.elements import ClauseElement

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


class FulltextElement:
    """Represents a single fulltext element (term, phrase, prefix, etc.)."""

    def __init__(self, content: str, operator: str = "", weight_modifier: str = ""):
        self.content = content
        self.operator = operator  # "+", "-", "", etc.
        self.weight_modifier = weight_modifier  # ">", "<", "~", etc.

    def build(self) -> str:
        """Build the element string."""
        if self.weight_modifier:
            return f"{self.operator}{self.weight_modifier}{self.content}"
        return f"{self.operator}{self.content}"


class FulltextGroup:
    """Represents a group of fulltext elements for building nested boolean queries.

    This class is used to create groups of terms that can be combined with
    group-level operators (+, -, ~, no prefix) in MatrixOne's boolean mode.

    Element-level Methods (within groups):
        - medium(): Add terms with medium weight (no operators)
        - high(): Add terms with high weight (>term)
        - low(): Add terms with low weight (<term)
        - phrase(): Add exact phrase matches ("phrase")
        - prefix(): Add prefix matches (term*)

    Group Types:
        - "or": OR semantics (default) - any term in group can match
        - "and": AND semantics - all terms in group must match
        - "not": NOT semantics - none of the terms in group can match

    Usage with Group-level Operators:
        # Create groups and apply group-level operators
        query.must(group().medium("java", "kotlin"))           # +(java kotlin)
        query.encourage(group().medium("tutorial", "guide"))   # (tutorial guide)
        query.discourage(group().medium("old", "outdated"))    # ~(old outdated)
        query.must_not(group().medium("spam", "junk"))         # -(spam junk)

    Element-level Weight Operators (inside groups):
        # MatrixOne syntax: '+red -(<blue >is)'
        group().low("blue").high("is")
        # Used as: query.must("red").must_not(group().low("blue").high("is"))

    Important Notes:
        - Use medium() for normal terms inside groups (no operators)
        - Use high()/low() for element-level weight control
        - Group-level operators (+, -, ~) are applied by the parent query builder
    """

    def __init__(self, group_type: str = "or"):
        self.elements: List[FulltextElement] = []
        self.groups: List["FulltextGroup"] = []
        self.group_type = group_type  # "or", "and", "not"
        self.is_tilde = False  # Whether this group has tilde weight

    def must(self, *terms: str) -> "FulltextGroup":
        """Add required terms (only for top-level, groups should use medium() instead)."""
        for term in terms:
            # Groups don't use +/- operators on elements, only at group level
            if self.group_type in ["or", "not"]:
                # Inside groups, elements should not have +/- operators
                self.elements.append(FulltextElement(term, ""))
            else:
                # Top-level (main group) can use + operator
                self.elements.append(FulltextElement(term, "+"))
        return self

    def must_not(self, *terms: str) -> "FulltextGroup":
        """Add excluded terms (only for top-level, groups should use medium() instead)."""
        for term in terms:
            # Groups don't use +/- operators on elements, only at group level
            if self.group_type in ["or", "not"]:
                # Inside groups, elements should not have +/- operators
                self.elements.append(FulltextElement(term, ""))
            else:
                # Top-level (main group) can use - operator
                self.elements.append(FulltextElement(term, "-"))
        return self

    def encourage(self, *terms: str) -> "FulltextGroup":
        """Add terms that should be encouraged (normal positive weight).

        These terms are optional - documents without them can still match,
        but documents containing them will get normal positive scoring boost.

        Args::

            *terms: Terms to add with normal positive weight

        Example::

            # Documents with 'python' get normal positive boost
            group.encourage("python")  # Generates: python
        """
        for term in terms:
            # Optional terms never have operators
            self.elements.append(FulltextElement(term, ""))
        return self

    def medium(self, *terms: str) -> "FulltextGroup":
        """Add terms with medium/normal weight (no operators)."""
        for term in terms:
            self.elements.append(FulltextElement(term, ""))
        return self

    def phrase(self, phrase: str) -> "FulltextGroup":
        """Add a phrase search."""
        self.elements.append(FulltextElement(f'"{phrase}"', ""))
        return self

    def prefix(self, prefix: str) -> "FulltextGroup":
        """Add a prefix search."""
        self.elements.append(FulltextElement(f"{prefix}*", ""))
        return self

    def boost(self, term: str, weight: float) -> "FulltextGroup":
        """Add a boosted term."""
        self.elements.append(FulltextElement(f"{term}^{weight}", ""))
        return self

    def high(self, *terms: str) -> "FulltextGroup":
        """Add terms with high weight (>term)."""
        for term in terms:
            self.elements.append(FulltextElement(term, "", ">"))
        return self

    def low(self, *terms: str) -> "FulltextGroup":
        """Add terms with low weight (<term)."""
        for term in terms:
            self.elements.append(FulltextElement(term, "", "<"))
        return self

    def add_group(self, *groups: "FulltextGroup") -> "FulltextGroup":
        """Add nested groups."""
        for group in groups:
            self.groups.append(group)
        return self

    def add_tilde_group(self, group: "FulltextGroup") -> "FulltextGroup":
        """Add a group with tilde weight (~group)."""
        group.is_tilde = True
        self.groups.append(group)
        return self

    def build(self) -> str:
        """Build the group string."""
        parts = []

        # Add elements
        for element in self.elements:
            parts.append(element.build())

        # Add nested groups with appropriate prefix based on group type
        for group in self.groups:
            group_str = group.build()
            if group_str:
                if group.is_tilde:
                    # For tilde groups, use ~(<content>) format
                    parts.append(f"~({group_str})")
                elif group.group_type == "not":
                    # For NOT groups, use -(<content>) format
                    parts.append(f"-({group_str})")
                elif group.group_type == "and":
                    # For AND groups, use +(<content>) format
                    parts.append(f"+({group_str})")
                else:  # or (default)
                    # For OR groups, just use (<content>) format
                    parts.append(f"({group_str})")

        return " ".join(parts)


class FulltextQueryBuilder:
    """Builder for constructing fulltext boolean queries.

    This class provides a chainable API for building complex fulltext search queries
    that are compatible with MatrixOne's MATCH() AGAINST() syntax.

    Core Methods:
        - must(): Required terms/groups (+ operator)
        - must_not(): Excluded terms/groups (- operator)
        - encourage(): Optional terms/groups with normal weight (no prefix)
        - discourage(): Optional terms/groups with reduced weight (~ operator)

    Examples::

        # Basic usage
        query.must("python")                    # +python
        query.encourage("tutorial")             # tutorial
        query.discourage("legacy")              # ~legacy
        query.must_not("deprecated")            # -deprecated

        # Group usage
        query.must(group().medium("java", "kotlin"))           # +(java kotlin)
        query.encourage(group().medium("tutorial", "guide"))   # (tutorial guide)
        query.must_not(group().medium("spam", "junk"))         # -(spam junk)

    Note: Group-level operators (+, -, ~) applied to entire groups. Element-level operators (>, <)
    applied within groups using high(), low()
    """

    def __init__(self):
        self.main_group = FulltextGroup("and")  # Main group with AND semantics

    def must(self, *items) -> "FulltextQueryBuilder":
        """Add required terms or groups (+ operator at group level).

        Documents MUST contain these terms/groups to match. This is equivalent
        to the '+' operator in MatrixOne's boolean mode syntax.

        Args::

            *items: Can be strings (terms) or FulltextGroup objects

        Examples::

            # Required term - documents must contain 'python'
            query.must("python")  # Generates: +python

            # Required group - documents must contain either 'java' OR 'kotlin'
            query.must(group().medium("java", "kotlin"))  # Generates: +(java kotlin)

            # Multiple required terms
            query.must("python", "programming")  # Generates: +python +programming

            # Unpack list to search multiple terms
            words = ["python", "programming"]
            query.must(*words)  # Correct: unpacks the list

        Raises::

            TypeError: If a list or tuple is passed directly without unpacking

        Returns::

            FulltextQueryBuilder: Self for method chaining
        """
        for item in items:
            # Validate parameter types and provide friendly error messages
            if isinstance(item, (list, tuple)):
                raise TypeError(
                    f"must() received a {type(item).__name__} object, but expected individual terms. "
                    f"To search multiple terms, use the unpacking operator: must(*terms) instead of must(terms). "
                    f"Example: must(*{list(item)[:3]}) or must({', '.join(repr(str(t)) for t in list(item)[:3])})"
                )
            if isinstance(item, FulltextGroup):
                item.group_type = "and"  # Force group to be required
                self.main_group.add_group(item)
            else:
                self.main_group.must(item)
        return self

    def must_not(self, *items) -> "FulltextQueryBuilder":
        """Add excluded terms or groups (- operator at group level).

        Documents MUST NOT contain these terms/groups to match. This is equivalent
        to the '-' operator in MatrixOne's boolean mode syntax.

        Args::

            *items: Can be strings (terms) or FulltextGroup objects

        Examples::

            # Excluded term - documents must not contain 'deprecated'
            query.must_not("deprecated")  # Generates: -deprecated

            # Excluded group - documents must not contain 'spam' OR 'junk'
            query.must_not(group().medium("spam", "junk"))  # Generates: -(spam junk)

            # Multiple excluded terms
            query.must_not("spam", "junk")  # Generates: -spam -junk

            # Unpack list to exclude multiple terms
            words = ["spam", "junk"]
            query.must_not(*words)  # Correct: unpacks the list

        Raises::

            TypeError: If a list or tuple is passed directly without unpacking

        Returns::

            FulltextQueryBuilder: Self for method chaining
        """
        for item in items:
            # Validate parameter types and provide friendly error messages
            if isinstance(item, (list, tuple)):
                raise TypeError(
                    f"must_not() received a {type(item).__name__} object, but expected individual terms. "
                    f"To exclude multiple terms, use the unpacking operator: must_not(*terms) instead of must_not(terms). "
                    f"Example: must_not(*{list(item)[:3]}) or must_not({', '.join(repr(str(t)) for t in list(item)[:3])})"
                )
            if isinstance(item, FulltextGroup):
                item.group_type = "not"  # Force group to be excluded
                self.main_group.add_group(item)
            else:
                self.main_group.must_not(item)
        return self

    def encourage(self, *items) -> "FulltextQueryBuilder":
        """Add terms or groups that should be encouraged (normal positive weight).

        Documents can match without these terms, but containing them will
        INCREASE the relevance score. This provides normal positive weight boost.

        Args::

            *items: Can be strings (terms) or FulltextGroup objects

        Examples::

            # Encourage documents with 'tutorial'
            query.encourage("tutorial")  # Generates: tutorial

            # Encourage documents with 'beginner' OR 'intro'
            query.encourage(group().medium("beginner", "intro"))  # Generates: (beginner intro)

            # Multiple encouraged terms
            query.encourage("tutorial", "guide")  # Generates: tutorial guide

            # Unpack list to encourage multiple terms
            words = ["tutorial", "guide"]
            query.encourage(*words)  # Correct: unpacks the list

        Weight Comparison:
            - encourage("term"): Normal positive boost (encourages term)
            - discourage("term"): Reduced/negative boost (discourages term)

        Raises::

            TypeError: If a list or tuple is passed directly without unpacking

        Returns::

            FulltextQueryBuilder: Self for method chaining
        """
        for item in items:
            # Validate parameter types and provide friendly error messages
            if isinstance(item, (list, tuple)):
                raise TypeError(
                    f"encourage() received a {type(item).__name__} object, but expected individual terms. "
                    f"To encourage multiple terms, use the unpacking operator: encourage(*terms) "
                    f"instead of encourage(terms). "
                    f"Example: encourage(*{list(item)[:3]}) or "
                    f"encourage({', '.join(repr(str(t)) for t in list(item)[:3])})"
                )
            if isinstance(item, FulltextGroup):
                item.group_type = "or"  # Force group to be optional
                self.main_group.add_group(item)
            else:
                self.main_group.encourage(item)
        return self

    def discourage(self, *items) -> "FulltextQueryBuilder":
        """Add terms or groups that should be discouraged (~ operator at group level).

        Documents can match without these terms, but containing them will
        DECREASE the relevance score. This provides reduced or negative weight boost,
        effectively discouraging documents that contain these terms.

        Args::

            *items: Can be strings (terms) or FulltextGroup objects

        Examples::

            # Discourage documents with 'legacy'
            query.discourage("legacy")  # Generates: ~legacy

            # Discourage documents with 'old' OR 'outdated'
            query.discourage(group().medium("old", "outdated"))  # Generates: ~(old outdated)

            # Multiple discouraged terms
            query.discourage("legacy", "deprecated")  # Generates: ~legacy ~deprecated

            # Unpack list to discourage multiple terms
            words = ["legacy", "deprecated"]
            query.discourage(*words)  # Correct: unpacks the list

        Weight Comparison:
            - encourage("term"): Normal positive boost (encourages term)
            - discourage("term"): Reduced/negative boost (discourages term)

        Use Cases:
            # Search Python content, but discourage legacy versions
            query.must("python").encourage("3.11").discourage("2.7")

            # Find tutorials, but avoid outdated content
            query.must("tutorial").discourage(group().medium("old", "deprecated"))

        Raises::

            TypeError: If a list or tuple is passed directly without unpacking

        Returns::

            FulltextQueryBuilder: Self for method chaining
        """
        for item in items:
            # Validate parameter types and provide friendly error messages
            if isinstance(item, (list, tuple)):
                raise TypeError(
                    f"discourage() received a {type(item).__name__} object, but expected individual terms. "
                    f"To discourage multiple terms, use the unpacking operator: discourage(*terms) "
                    f"instead of discourage(terms). "
                    f"Example: discourage(*{list(item)[:3]}) or "
                    f"discourage({', '.join(repr(str(t)) for t in list(item)[:3])})"
                )
            if isinstance(item, FulltextGroup):
                # Apply tilde to the entire group
                self.main_group.add_tilde_group(item)
            else:
                # Apply tilde to individual term
                self.main_group.elements.append(FulltextElement(item, "", "~"))
        return self

    def phrase(self, phrase: str) -> "FulltextQueryBuilder":
        """Add a phrase search to the main group."""
        self.main_group.phrase(phrase)
        return self

    def prefix(self, prefix: str) -> "FulltextQueryBuilder":
        """Add a prefix search to the main group."""
        self.main_group.prefix(prefix)
        return self

    def boost(self, term: str, weight: float) -> "FulltextQueryBuilder":
        """Add a boosted term to the main group."""
        self.main_group.boost(term, weight)
        return self

    def group(self, *builders: "FulltextQueryBuilder") -> "FulltextQueryBuilder":
        """Add nested query builders as groups (OR semantics)."""
        for builder in builders:
            # Convert builder to group and add to main group
            group = FulltextGroup("or")
            # Add all elements from the builder's main group
            group.elements.extend(builder.main_group.elements)
            group.groups.extend(builder.main_group.groups)
            self.main_group.groups.append(group)
        return self

    def build(self) -> str:
        """Build the final query string."""
        return self.main_group.build()

    def as_sql(
        self,
        table: str,
        columns: List[str],
        mode: str = FulltextSearchMode.BOOLEAN,
        include_score: bool = False,
        select_columns: Optional[List[str]] = None,
        where_conditions: Optional[List[str]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> str:
        """Build a complete SQL query with optional AS score support.

        This method generates a full SQL query similar to FulltextSearchBuilder but using
        the query built by FulltextQueryBuilder.

        Args::

            table: Table name to search in
            columns: List of columns to search in (must match FULLTEXT index)
            mode: Search mode (BOOLEAN, NATURAL_LANGUAGE, etc.)
            include_score: Whether to include relevance score in results
            select_columns: Columns to select (default: all columns "*")
            where_conditions: Additional WHERE conditions
            order_by: ORDER BY clause (e.g., "score DESC")
            limit: LIMIT value
            offset: OFFSET value

        Returns::

            str: Complete SQL query

        Examples::

            .. code-block:: python

            # Basic query with score
            query = FulltextQueryBuilder().must("python").encourage("tutorial")
            sql = query.as_sql("articles", ["title", "content"], include_score=True)
            # SELECT *, MATCH(title, content) AGAINST('+python tutorial' IN boolean mode) AS score
            # FROM articles WHERE MATCH(title, content) AGAINST('+python tutorial' IN boolean mode)

            # Query with custom columns and ORDER BY score
            sql = query.as_sql("articles", ["title", "content"],
                              select_columns=["id", "title"], include_score=True,
                              order_by="score DESC", limit=10)
        """
        query_string = self.build()
        if not query_string:
            raise ValueError("Query is required - add at least one search term")

        if not table:
            raise ValueError("Table name is required")

        if not columns:
            raise ValueError("Search columns are required")

        # Build columns string for MATCH()
        columns_str = ", ".join(columns)

        # Build SELECT clause
        if select_columns:
            select_parts = select_columns.copy()
        else:
            select_parts = ["*"]

        if include_score:
            score_expr = f"MATCH({columns_str}) AGAINST('{query_string}' IN {mode}) AS score"
            select_parts.append(score_expr)

        select_clause = f"SELECT {', '.join(select_parts)}"

        # Build FROM clause
        from_clause = f"FROM {table}"

        # Build WHERE clause
        where_parts = []

        # Add fulltext search condition
        fulltext_condition = f"MATCH({columns_str}) AGAINST('{query_string}' IN {mode})"
        where_parts.append(fulltext_condition)

        # Add additional WHERE conditions
        if where_conditions:
            where_parts.extend(where_conditions)

        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

        # Build ORDER BY clause
        order_clause = f"ORDER BY {order_by}" if order_by else ""

        # Build LIMIT clause
        limit_clause = f"LIMIT {limit}" if limit else ""

        # Build OFFSET clause
        offset_clause = f"OFFSET {offset}" if offset else ""

        # Combine all clauses
        sql_parts = [
            select_clause,
            from_clause,
            where_clause,
            order_clause,
            limit_clause,
            offset_clause,
        ]
        return " ".join(filter(None, sql_parts))

    def as_score_sql(self, table: str, columns: List[str], mode: str = FulltextSearchMode.BOOLEAN) -> str:
        """Convenient method to generate SQL with score included.

        This is equivalent to calling as_sql() with include_score=True.

        Args::

            table: Table name to search in
            columns: List of columns to search in
            mode: Search mode

        Returns::

            str: Complete SQL query with AS score

        Example::

            query = FulltextQueryBuilder().must("python").encourage("tutorial")
            sql = query.as_score_sql("articles", ["title", "content"])
            # Generates SQL with AS score automatically included
        """
        return self.as_sql(table, columns, mode, include_score=True)


class FulltextFilter(ClauseElement):
    """Advanced fulltext filter for integrating fulltext search with ORM queries.

    This class wraps FulltextQueryBuilder to provide seamless integration
    with MatrixOne ORM's filter() method, allowing fulltext search to be
    combined with other SQL conditions.

    Core Methods (Group-level operators):
        - must(): Required terms/groups (+ operator)
        - must_not(): Excluded terms/groups (- operator)
        - encourage(): Optional terms/groups with normal weight (no prefix)
        - discourage(): Optional terms/groups with reduced weight (~ operator)

    Parameter Types:
        - str: Single term (e.g., "python")
        - FulltextGroup: Group of terms (e.g., group().medium("java", "kotlin"))

    Usage with ORM:

    .. code-block:: python

        # Basic fulltext filter
        results = client.query(Article).filter(
            boolean_match("title", "content").must("python").encourage("tutorial")
        ).all()

        # Combined with other conditions
        results = client.query(Article).filter(
            boolean_match("title", "content").must("python")
        ).filter(
            Article.category == "Programming"
        ).all()

        # Complex fulltext with groups
        results = client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .must("programming")
            .must(group().medium("python", "java"))
            .discourage(group().medium("legacy", "deprecated"))
        ).all()

    Weight Operator Examples

    .. code-block:: python

        # Encourage tutorials, discourage legacy content
        boolean_match("title", "content")
            .must("python")
            .encourage("tutorial")   # Boost documents with 'tutorial'
            .discourage("legacy")    # Lower ranking for 'legacy' documents

    Supported MatrixOne Boolean Mode Operators:
        Group-level: +, -, ~, (no prefix) - applied to entire groups/terms
        Element-level: >, < - applied within groups using high(), low()
        Other: "phrase", term* - exact phrases and prefix matching
        Complex: +red -(<blue >is) - nested groups with mixed operators

    Important MatrixOne Requirements:
        **Column Matching**: The columns specified must exactly match
        the columns defined in the FULLTEXT index. If your index is
        `FULLTEXT(title, content, tags)`, you must include all three columns.

        **Limitations**:
        - Only one MATCH() function per query is supported
        - Complex nested groups may have syntax restrictions
        - Use fulltext_and/fulltext_or for combining with other conditions
    """

    def __init__(self, columns: List[str], mode: str = FulltextSearchMode.BOOLEAN):
        super().__init__()
        self.columns = columns
        self.mode = mode
        self.query_builder = FulltextQueryBuilder()
        self._natural_query = None  # Store natural language query separately
        # Set SQLAlchemy type info for compatibility
        self.type = Boolean()

    def columns(self, *columns: str) -> "FulltextFilter":
        """Set the columns to search in."""
        self.columns = list(columns)
        return self

    def must(self, *items) -> "FulltextFilter":
        """Add required terms or groups (+ operator at group level)."""
        self.query_builder.must(*items)
        return self

    def must_not(self, *items) -> "FulltextFilter":
        """Add excluded terms or groups (- operator at group level)."""
        self.query_builder.must_not(*items)
        return self

    def encourage(self, *items) -> "FulltextFilter":
        """Add terms or groups that should be encouraged (normal positive weight)."""
        self.query_builder.encourage(*items)
        return self

    def phrase(self, *phrases: str) -> "FulltextFilter":
        """Add exact phrases - equivalent to "phrase"."""
        self.query_builder.phrase(*phrases)
        return self

    def prefix(self, *terms: str) -> "FulltextFilter":
        """Add prefix terms - equivalent to term*."""
        self.query_builder.prefix(*terms)
        return self

    def boost(self, term: str, weight: float) -> "FulltextFilter":
        """Add a boosted term (term^weight)."""
        self.query_builder.boost(term, weight)
        return self

    def discourage(self, *items) -> "FulltextFilter":
        """Add terms or groups that should be discouraged (~ operator at group level)."""
        self.query_builder.discourage(*items)
        return self

    def set_natural_query(self, query: str) -> "FulltextFilter":
        """Set natural language query string (used for NATURAL_LANGUAGE mode)."""
        self._natural_query = query
        return self

    def group(self, *filters: "FulltextFilter") -> "FulltextFilter":
        """Add nested query groups (OR semantics)."""
        builders = [f.query_builder for f in filters]
        self.query_builder.group(*builders)
        return self

    def natural_language(self) -> "FulltextFilter":
        """Set to natural language mode."""
        self.mode = FulltextSearchMode.NATURAL_LANGUAGE
        return self

    def boolean_mode(self) -> "FulltextFilter":
        """Set to boolean mode."""
        self.mode = FulltextSearchMode.BOOLEAN
        return self

    def query_expansion(self) -> "FulltextFilter":
        """Set to query expansion mode."""
        self.mode = FulltextSearchMode.QUERY_EXPANSION
        return self

    def compile(self, compile_kwargs=None):
        """Compile to SQL expression for use in filter() method."""
        if not self.columns:
            raise ValueError("Columns must be specified")

        columns_str = ", ".join(self.columns)

        # For natural language mode, use the stored natural query if available
        if self.mode == FulltextSearchMode.NATURAL_LANGUAGE and self._natural_query:
            query_string = self._natural_query
        else:
            query_string = self.query_builder.build()

        if not query_string:
            raise ValueError("Query cannot be empty")

        if self.mode == FulltextSearchMode.NATURAL_LANGUAGE:
            return f"MATCH({columns_str}) AGAINST('{query_string}')"
        elif self.mode == FulltextSearchMode.BOOLEAN:
            return f"MATCH({columns_str}) AGAINST('{query_string}' IN BOOLEAN MODE)"
        elif self.mode == FulltextSearchMode.QUERY_EXPANSION:
            return f"MATCH({columns_str}) AGAINST('{query_string}' WITH QUERY EXPANSION)"
        else:
            return f"MATCH({columns_str}) AGAINST('{query_string}')"

    def _compiler_dispatch(self, visitor, **kw):
        """SQLAlchemy compiler dispatch method for complete compatibility."""
        # Generate the MATCH() AGAINST() SQL
        sql_text = self.compile()
        # Return a text clause that SQLAlchemy can handle
        return visitor.process(text(sql_text), **kw)

    def label(self, name: str):
        """Create a labeled version for use in SELECT clauses.

        This allows using fulltext expressions as selectable columns with aliases:

        Args::

            name: The alias name for the column

        Returns::

            A SQLAlchemy labeled expression

        Examples::

            .. code-block:: python

            # Use as a SELECT column with score
            query(Article, Article.id,
                  boolean_match("title", "content").must("python").label("score"))

            # Multiple fulltext scores
            query(Article, Article.id,
                  boolean_match("title", "content").must("python").label("relevance"),
                  boolean_match("tags").must("programming").label("tag_score"))

        Generated SQL:

        .. code-block:: sql

            SELECT articles.id,
                   MATCH(title, content) AGAINST('+python' IN BOOLEAN MODE) AS score
            FROM articles
        """
        # Create a text expression that can be labeled
        sql_text = self.compile()
        text_expr = text(sql_text)

        # Create a custom labeled expression
        class FulltextLabel:
            def __init__(self, text_expr, name):
                self.text_expr = text_expr
                self.name = name

            def __str__(self):
                # For ORM integration, return only the expression without AS
                # The ORM will add the AS alias part
                return sql_text

            # Make it compatible with SQLAlchemy's compilation
            def compile(self, compile_kwargs=None):
                # For standalone use, include AS
                return f"{sql_text} AS {self.name}"

            def _compiler_dispatch(self, visitor, **kw):
                # For SQLAlchemy integration, return only the expression
                # SQLAlchemy will handle the AS alias
                return sql_text

        return FulltextLabel(text_expr, name)

    def __str__(self):
        """String representation for debugging."""
        return f"FulltextFilter({self.columns}, mode={self.mode})"

    def __repr__(self):
        """Detailed representation for debugging."""
        return f"FulltextFilter(columns={self.columns}, mode='{self.mode}', query='{self.query_builder.build()}')"

    def as_text(self):
        """Convert to SQLAlchemy text() object for compatibility with and_(), or_(), etc."""
        return text(self.compile())

    @classmethod
    def _create_and(cls, *conditions):
        """Helper to create AND expressions with FulltextFilter support."""
        processed_conditions = []
        for condition in conditions:
            if isinstance(condition, cls):
                processed_conditions.append(condition.as_text())
            else:
                processed_conditions.append(condition)
        return and_(*processed_conditions)

    @classmethod
    def _create_or(cls, *conditions):
        """Helper to create OR expressions with FulltextFilter support."""
        processed_conditions = []
        for condition in conditions:
            if isinstance(condition, cls):
                processed_conditions.append(condition.as_text())
            else:
                processed_conditions.append(condition)
        return or_(*processed_conditions)


# Convenience functions for common use cases


def boolean_match(*columns) -> FulltextFilter:
    """Create a boolean mode fulltext filter for specified columns.

    This is the main entry point for creating fulltext search queries that integrate
    seamlessly with MatrixOne ORM's filter() method.

    Args::

        *columns: Column names or SQLAlchemy Column objects to search against

    Returns::

        FulltextFilter: A chainable filter object

    Examples::

        # Basic search - must contain 'python'
        boolean_match("title", "content").must("python")

        # Multiple conditions
        boolean_match("title", "content")
            .must("python")
            .encourage("tutorial")
            .discourage("legacy")

        # Group search - either 'python' or 'java'
        boolean_match("title", "content").must(group().medium("python", "java"))

        # Using SQLAlchemy Column objects
        boolean_match(Article.title, Article.content).must("python")

    Note: The columns specified must exactly match the FULLTEXT index columns. For example, if your
    index is FULLTEXT(title, content, tags), you must use boolean_match("title", "content", "tags")
    """
    # Convert columns to strings
    column_names = []
    for col in columns:
        if hasattr(col, 'name'):
            # SQLAlchemy Column object
            column_names.append(col.name)
        elif hasattr(col, '__tablename__') and hasattr(col, 'name'):
            # Model attribute
            column_names.append(col.name)
        else:
            # String column name
            column_names.append(str(col))

    return FulltextFilter(column_names, FulltextSearchMode.BOOLEAN)


def natural_match(*columns, query: str) -> FulltextFilter:
    """
    Create a natural language mode fulltext filter for specified columns.

    Natural language mode provides user-friendly search with automatic processing:
    - Stopword removal (e.g., 'the', 'a', 'an')
    - Stemming and variations
    - Relevance scoring based on TF-IDF or BM25 algorithm
    - Best for end-user search interfaces

    Args:
        *columns: Column names or SQLAlchemy Column objects to search against
            - Must exactly match the columns in your fulltext index
            - Can be strings or Column objects
        query: Natural language query string
            - User-friendly search terms
            - Automatically processed for best results
            - Multi-word queries are supported

    Important - Column Matching:
        The columns specified in MATCH() must exactly match the columns defined in
        the FULLTEXT index. Mismatches will cause errors.

        Examples:
            If index is: FULLTEXT(title, content)
            - ✅ natural_match("title", "content", query="...")  - Correct
            - ❌ natural_match("title", query="...")             - Error (partial)
            - ❌ natural_match("content", query="...")           - Error (partial)

            If index is: FULLTEXT(content)
            - ✅ natural_match("content", query="...")           - Correct
            - ❌ natural_match("title", "content", query="...")  - Error (extra column)

    Parser Compatibility:
        Works with all parser types:
        - Default parser: Standard text tokenization
        - JSON parser: Searches JSON values within documents
        - NGRAM parser: Chinese and Asian language tokenization

    Returns:
        FulltextFilter: A fulltext filter object for use in queries

    Examples::

        # Basic natural language search
        result = client.query("articles.id", "articles.title", "articles.content").filter(
            natural_match("title", "content", query="machine learning")
        ).execute()

        # Using with ORM models
        result = client.query(Article).filter(
            natural_match(Article.title, Article.content, query="artificial intelligence")
        ).execute()

        # Single column search
        result = client.query(Article).filter(
            natural_match(Article.content, query="python programming")
        ).execute()

        # With relevance scoring
        result = client.query(
            Article.id,
            Article.title,
            Article.content,
            natural_match(Article.content, query="deep learning").label("score")
        ).execute()

        # JSON parser - searching within JSON documents
        result = client.query(Product).filter(
            natural_match(Product.details, query="Dell laptop")
        ).execute()

        # NGRAM parser - Chinese content search
        result = client.query(ChineseArticle).filter(
            natural_match(ChineseArticle.title, ChineseArticle.body, query="神雕侠侣")
        ).execute()

        # Combined with SQL filters
        result = client.query(Article).filter(
            natural_match(Article.content, query="programming tutorial")
        ).filter(Article.category == "Education").execute()
    """
    # Convert columns to strings
    column_names = []
    for col in columns:
        if hasattr(col, 'name'):
            # SQLAlchemy Column object
            column_names.append(col.name)
        elif hasattr(col, '__tablename__') and hasattr(col, 'name'):
            # Model attribute
            column_names.append(col.name)
        else:
            # String column name
            column_names.append(str(col))

    return FulltextFilter(column_names, FulltextSearchMode.NATURAL_LANGUAGE).set_natural_query(query)


def group() -> FulltextGroup:
    """Create a new query group builder with OR semantics between elements.

    Creates a group where elements have OR relationship. The group-level semantics
    (required, excluded, optional, reduced weight) are determined by how it's used:
    - must(group()) → +(...)  - group is required
    - must_not(group()) → -(...) - group is excluded
    - encourage(group()) → (...) - group is optional with normal weight
    - discourage(group()) → ~(...) - group is optional with reduced weight

    Element-level Methods (use inside groups):
        - medium(): Add terms with medium weight (no operators)
        - high(): Add terms with high weight (>term)
        - low(): Add terms with low weight (<term)
        - phrase(): Add exact phrase matches ("phrase")
        - prefix(): Add prefix matches (term*)

    IMPORTANT: Inside groups, do NOT use must()/must_not() as they add +/- operators.
    Use medium() for plain terms or high()/low() for element-level weight control.

    Examples
        # Required group - must contain 'java' OR 'kotlin'
        query.must(group().medium("java", "kotlin"))  # +(java kotlin)

        # Excluded group - must not contain 'spam' OR 'junk'
        query.must_not(group().medium("spam", "junk"))  # -(spam junk)

        # Optional group with normal weight
        query.encourage(group().medium("tutorial", "guide"))  # (tutorial guide)

        # Optional group with reduced weight
        query.discourage(group().medium("old", "outdated"))  # ~(old outdated)

        # Complex MatrixOne style with element-level weights
        query.must("red").must_not(group().low("blue").high("is"))
        # Generates: '+red -(<blue >is)'
    """
    return FulltextGroup("or")


# Import generic logical adapters at the end to avoid circular imports
try:
    from .adapters import logical_and, logical_not, logical_or
except ImportError:
    # Fallback implementations if adapters module is not available
    def logical_and(*conditions):
        processed_conditions = []
        for condition in conditions:
            if hasattr(condition, 'compile') and callable(getattr(condition, 'compile')):
                processed_conditions.append(text(f"({condition.compile()})"))
            else:
                processed_conditions.append(condition)
        return and_(*processed_conditions)

    def logical_or(*conditions):
        processed_conditions = []
        for condition in conditions:
            if hasattr(condition, 'compile') and callable(getattr(condition, 'compile')):
                processed_conditions.append(text(f"({condition.compile()})"))
            else:
                processed_conditions.append(condition)
        return or_(*processed_conditions)

    def logical_not(condition):
        if hasattr(condition, 'compile') and callable(getattr(condition, 'compile')):
            return text(f"NOT ({condition.compile()})")
        else:
            return not_(condition)


# Remove old FulltextTerm and FulltextQuery classes as they are replaced by FulltextQueryBuilder


class FulltextSearchBuilder:
    """
    Elasticsearch-like fulltext search builder for MatrixOne.

    Provides a chainable interface for building complex fulltext queries
    with support for various search modes, filters, and sorting.

    Boolean Mode Operators:
        - ``+word`` : Required term (must contain)
        - ``-word`` : Excluded term (must not contain)
        - ``~word`` : Lower weight term (reduces relevance score)
        - ``<word`` : Lower weight term (reduces relevance score)
        - ``>word`` : Higher weight term (increases relevance score)
        - word : Optional term (may contain)
        - ``"phrase"`` : Exact phrase match
        - ``word*`` : Wildcard prefix match
        - (word1 word2) : Grouping (contains any of the words)

    Note: MatrixOne supports all boolean mode operators.

    Search Modes:
        - NATURAL_LANGUAGE: Automatic stopword removal, stemming, relevance scoring
        - BOOLEAN: Full control with operators, no automatic processing
        - QUERY_EXPANSION: Not supported in MatrixOne

    Examples::

        # Natural language search
        results = client.fulltext_search()
            .table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
            .query("machine learning")
            .with_score()
            .limit(10)
            .execute()

        # Boolean search with complex terms
        results = client.fulltext_search()
            .table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .add_term("machine", required=True)
            .add_term("learning", required=True)
            .where("category = 'AI'")
            .order_by("score", "DESC")
            .limit(20)
            .execute()
    """

    def __init__(self, client: "Client"):
        self.client = client
        self._table_name: Optional[str] = None
        self._columns: List[str] = []
        self._search_mode = FulltextSearchMode.NATURAL_LANGUAGE
        self._algorithm = FulltextSearchAlgorithm.BM25
        self._query_obj = FulltextQueryBuilder()
        self._include_score = False
        self._where_conditions: List[str] = []
        self._order_by: Optional[str] = None
        self._limit_value: Optional[int] = None
        self._offset_value: Optional[int] = None
        self._select_columns: List[str] = ["*"]

    def table(self, table_name: str) -> "FulltextSearchBuilder":
        """
        Set the target table for the search.

        Args::

            table_name: Name of the table to search

        Returns::

            FulltextSearchBuilder: Self for chaining
        """
        self._table_name = table_name
        return self

    def columns(self, columns: List[str]) -> "FulltextSearchBuilder":
        """
        Set the columns to search in.

        Args::

            columns: List of column names to search

        Returns::

            FulltextSearchBuilder: Self for chaining
        """
        self._columns = columns
        return self

    def with_mode(self, mode: str) -> "FulltextSearchBuilder":
        """
        Set the search mode.

        Args::

            mode: Search mode
                - FulltextSearchMode.NATURAL_LANGUAGE: Automatic processing, user-friendly
                - FulltextSearchMode.BOOLEAN: Full control with operators
                - FulltextSearchMode.QUERY_EXPANSION: Not supported in MatrixOne

        Returns::

            FulltextSearchBuilder: Self for chaining

        Examples::

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

        Args::

            algorithm: Search algorithm
                - FulltextSearchAlgorithm.TF_IDF: Traditional TF-IDF scoring
                - FulltextSearchAlgorithm.BM25: Modern BM25 scoring (recommended)

        Returns::

            FulltextSearchBuilder: Self for chaining

        Examples::

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

        Args::

            query_string: The search query (natural language or boolean syntax)

        Returns::

            FulltextSearchBuilder: Self for chaining

        Examples::

            # Natural language query
            .query("machine learning algorithms")

            # Boolean query
            .query("+machine +learning -java")

        Note: This method resets any previously added terms, phrases, or wildcards.
        """
        self._query_obj = FulltextQueryBuilder()
        self._query_obj.encourage(query_string)
        return self

    def add_term(
        self,
        term: str,
        required: bool = False,
        excluded: bool = False,
        proximity: Optional[int] = None,
    ) -> "FulltextSearchBuilder":
        """
        Add a search term to the query.

        Args::

            term: The search term
            required: Whether the term is required (+) - must contain this term
            excluded: Whether the term is excluded (-) - must not contain this term
            proximity: Proximity modifier for boolean mode (not supported in MatrixOne)

        Returns::

            FulltextSearchBuilder: Self for chaining

        Examples::

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
        if required:
            self._query_obj.must(term)
        elif excluded:
            self._query_obj.must_not(term)
        else:
            self._query_obj.encourage(term)
        return self

    def add_phrase(self, phrase: str) -> "FulltextSearchBuilder":
        """
        Add an exact phrase to the query.

        Args::

            phrase: The exact phrase to search for (wrapped in double quotes)

        Returns::

            FulltextSearchBuilder: Self for chaining

        Examples::

            # Exact phrase: "machine learning"
            .add_phrase("machine learning")

            # Multiple phrases
            .add_phrase("deep learning")
            .add_phrase("neural networks")
        """
        self._query_obj.phrase(phrase)
        return self

    def add_wildcard(self, pattern: str) -> "FulltextSearchBuilder":
        """
        Add a wildcard pattern to the query.

        Args::

            pattern: Wildcard pattern with * suffix (e.g., "test*", "neural*")

        Returns::

            FulltextSearchBuilder: Self for chaining

        Examples::

            # Prefix match: neural*
            .add_wildcard("neural*")

            # Multiple wildcards
            .add_wildcard("machine*")
            .add_wildcard("learn*")
        """
        self._query_obj.prefix(pattern.rstrip('*'))
        return self

    def with_score(self, include: bool = True) -> "FulltextSearchBuilder":
        """
        Include relevance score in results.

        Args::

            include: Whether to include the score

        Returns::

            FulltextSearchBuilder: Self for chaining
        """
        self._include_score = include
        return self

    def select(self, columns: List[str]) -> "FulltextSearchBuilder":
        """
        Set the columns to select in the result.

        Args::

            columns: List of column names to select

        Returns::

            FulltextSearchBuilder: Self for chaining
        """
        self._select_columns = columns
        return self

    def where(self, condition: str) -> "FulltextSearchBuilder":
        """
        Add a WHERE condition.

        Args::

            condition: WHERE condition

        Returns::

            FulltextSearchBuilder: Self for chaining
        """
        self._where_conditions.append(condition)
        return self

    def order_by(self, column: str, direction: str = "DESC") -> "FulltextSearchBuilder":
        """
        Set ORDER BY clause.

        Args::

            column: Column to order by
            direction: Order direction (ASC/DESC)

        Returns::

            FulltextSearchBuilder: Self for chaining
        """
        self._order_by = f"{column} {direction}"
        return self

    def limit(self, count: int) -> "FulltextSearchBuilder":
        """
        Set LIMIT clause.

        Args::

            count: Number of results to return

        Returns::

            FulltextSearchBuilder: Self for chaining
        """
        self._limit_value = count
        return self

    def offset(self, count: int) -> "FulltextSearchBuilder":
        """
        Set OFFSET clause.

        Args::

            count: Number of results to skip

        Returns::

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
        sql_parts = [
            select_clause,
            from_clause,
            where_clause,
            order_clause,
            limit_clause,
            offset_clause,
        ]
        return " ".join(filter(None, sql_parts))

    def execute(self) -> Any:
        """
        Execute the fulltext search query.

        Returns::

            Query results
        """
        sql = self._build_sql()
        return self.client.execute(sql)

    def explain(self) -> str:
        """
        Get the SQL query that would be executed.

        Returns::

            SQL query string
        """
        return self._build_sql()


class FulltextIndexManager:
    """Manager for fulltext index operations."""

    def __init__(self, client: "Client"):
        self.client = client

    def create(
        self,
        table_name: str,
        name: str,
        columns: List[str],
        algorithm: str = FulltextSearchAlgorithm.BM25,
    ) -> bool:
        """
        Create a fulltext index.

        Args::

            table_name: Name of the table
            name: Name of the index
            columns: List of columns to index
            algorithm: Search algorithm to use

        Returns::

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

        Args::

            table_name: Name of the table
            name: Name of the index

        Returns::

            bool: True if successful
        """
        from .fulltext_index import FulltextIndex

        success = FulltextIndex.drop_index(engine=self.client.get_sqlalchemy_engine(), table_name=table_name, name=name)

        return success

    def search(self) -> "FulltextSearchBuilder":
        """
        Create a new fulltext search builder.

        Returns::

            FulltextSearchBuilder: New search builder instance
        """
        return FulltextSearchBuilder(self.client)

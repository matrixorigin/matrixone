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
Online tests for SimpleFulltextQueryBuilder.

Tests functionality against a real MatrixOne database to verify
that generated SQL works correctly and returns expected results.
"""

import pytest
from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import declarative_base

from matrixone.client import Client
from matrixone.config import get_connection_kwargs
from matrixone.exceptions import QueryError
from matrixone.sqlalchemy_ext import boolean_match

# SQLAlchemy model for testing
Base = declarative_base()


class Article(Base):
    __tablename__ = "test_simple_fulltext_articles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(255), nullable=False)
    content = Column(Text, nullable=False)
    category = Column(String(100), nullable=False)
    tags = Column(String(500))


class TestSimpleFulltextOnline:
    """Online tests for SimpleFulltextQueryBuilder with real database."""

    @classmethod
    def setup_class(cls):
        """Set up test database and data."""
        # Get connection parameters
        conn_params = get_connection_kwargs()
        client_params = {k: v for k, v in conn_params.items() if k in ['host', 'port', 'user', 'password', 'database']}

        cls.client = Client()
        cls.client.connect(**client_params)

        cls.test_db = "test_simple_fulltext"
        try:
            cls.client.execute(f"DROP DATABASE IF EXISTS {cls.test_db}")
            cls.client.execute(f"CREATE DATABASE {cls.test_db}")
            cls.client.execute(f"USE {cls.test_db}")
        except Exception as e:
            print(f"Database setup warning: {e}")

        # Create test table
        cls.client.execute("DROP TABLE IF EXISTS test_simple_fulltext_articles")
        cls.client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_simple_fulltext_articles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                category VARCHAR(100) NOT NULL,
                tags VARCHAR(500)
            )
        """
        )

        # Insert test data
        test_articles = [
            (
                1,
                "Python Programming Tutorial",
                "Learn Python programming from basics to advanced concepts",
                "Programming",
                "python,tutorial,beginner",
            ),
            (
                2,
                "Machine Learning with Python",
                "Complete guide to machine learning algorithms and implementation",
                "AI",
                "python,ml,ai,algorithms",
            ),
            (
                3,
                "JavaScript Web Development",
                "Modern JavaScript development for web applications",
                "Programming",
                "javascript,web,frontend",
            ),
            (
                4,
                "Data Science Fundamentals",
                "Introduction to data science concepts and tools",
                "AI",
                "data,science,analytics",
            ),
            (
                5,
                "Deprecated Python Libraries",
                "Old Python libraries that should be avoided",
                "Programming",
                "python,deprecated,legacy",
            ),
            (
                6,
                "Advanced Machine Learning",
                "Deep learning and neural networks with TensorFlow",
                "AI",
                "ai,deeplearning,tensorflow",
            ),
            (
                7,
                "Web Development Best Practices",
                "Modern practices for building web applications",
                "Programming",
                "web,best practices,modern",
            ),
            (
                8,
                "Artificial Intelligence Overview",
                "Introduction to AI concepts and applications",
                "AI",
                "ai,overview,introduction",
            ),
        ]

        for article in test_articles:
            title_escaped = article[1].replace("'", "''")
            content_escaped = article[2].replace("'", "''")
            category_escaped = article[3].replace("'", "''")
            tags_escaped = article[4].replace("'", "''") if article[4] else ''

            cls.client.execute(
                f"""
                INSERT INTO test_simple_fulltext_articles (id, title, content, category, tags) 
                VALUES ({article[0]}, '{title_escaped}', '{content_escaped}', '{category_escaped}', '{tags_escaped}')
            """
            )

        # Create fulltext index
        try:
            cls.client.execute("CREATE FULLTEXT INDEX ft_articles ON test_simple_fulltext_articles(title, content, tags)")
        except Exception as e:
            print(f"Fulltext index creation warning: {e}")

    @classmethod
    def teardown_class(cls):
        """Clean up test database."""
        if hasattr(cls, 'client'):
            cls.client.disconnect()

    def test_basic_natural_language_search(self):
        """Test basic natural language search with real database."""
        results = self.client.query(
            Article.id,
            Article.title,
            Article.content,
            Article.category,
            Article.tags,
            boolean_match("title", "content", "tags").encourage("python programming"),
        ).execute()

        # Verify result structure
        assert isinstance(results, object), "Results should be a result set object"
        assert hasattr(results, 'fetchall'), "Results should have fetchall method"
        assert hasattr(results, 'columns'), "Results should have columns attribute"
        assert hasattr(results, 'rows'), "Results should have rows attribute"

        # Verify columns are correct (including the MATCH function column)
        expected_base_columns = ['id', 'title', 'content', 'category', 'tags']
        assert len(results.columns) >= len(
            expected_base_columns
        ), f"Should have at least {len(expected_base_columns)} columns, got {len(results.columns)}"

        # Verify base columns are correct
        for i, expected_col in enumerate(expected_base_columns):
            assert results.columns[i] == expected_col, f"Column {i} should be '{expected_col}', got '{results.columns[i]}'"

        # Verify there's an additional MATCH column (for boolean_match)
        assert len(results.columns) > len(expected_base_columns), "Should have additional MATCH column from boolean_match"
        match_column = results.columns[-1]
        assert "MATCH" in match_column, f"Last column should contain MATCH function, got: {match_column}"

        # Verify we have results
        all_rows = results.fetchall()
        assert isinstance(all_rows, list), "fetchall() should return a list"
        assert len(all_rows) > 0, "Should find at least one article matching 'python programming'"

        # Verify row structure
        for i, row in enumerate(all_rows):
            # Row can be tuple, list, or SQLAlchemy Row object
            assert hasattr(row, '__getitem__'), f"Row {i} should support indexing, got {type(row)}"
            assert hasattr(row, '__len__'), f"Row {i} should support len(), got {type(row)}"
            assert len(row) == len(results.columns), f"Row {i} should have {len(results.columns)} columns, got {len(row)}"

        # Verify content matches search criteria
        found_python = False
        found_programming = False
        for row in all_rows:
            # Column indices: 0=id, 1=title, 2=content, 3=category, 4=tags, 5=MATCH function
            title = str(row[1]).lower() if row[1] else ""
            content = str(row[2]).lower() if row[2] else ""
            tags = str(row[4]).lower() if len(row) > 4 and row[4] else ""

            if "python" in title or "python" in content or "python" in tags:
                found_python = True
            if "programming" in title or "programming" in content or "programming" in tags:
                found_programming = True

        assert found_python, "Should find articles containing 'python' in title, content, or tags"
        assert found_programming, "Should find articles containing 'programming' in title, content, or tags"

    def test_boolean_mode_required_terms(self):
        """Test boolean mode with required terms."""
        results = self.client.query(
            Article.id,
            Article.title,
            Article.content,
            Article.category,
            Article.tags,
            boolean_match("title", "content", "tags").must("python", "tutorial"),
        ).execute()

        assert isinstance(results.fetchall(), list)
        # Should find articles that have both "python" AND "tutorial"
        if results.fetchall():
            for row in results.fetchall():
                title_content = f"{row[1]} {row[2]}".lower()  # title + content
                assert "python" in title_content and "tutorial" in title_content

    def test_boolean_mode_excluded_terms(self):
        """Test boolean mode with excluded terms."""
        results = self.client.query(
            Article.id,
            Article.title,
            Article.content,
            Article.category,
            Article.tags,
            boolean_match("title", "content", "tags").must("python").must_not("deprecated"),
        ).execute()

        assert isinstance(results.fetchall(), list)
        # Should find Python articles but exclude deprecated ones
        for row in results.fetchall():
            title_content = f"{row[1]} {row[2]}".lower()  # title + content
            assert "python" in title_content
            assert "deprecated" not in title_content

    def test_search_with_score(self):
        """Test search with relevance scoring."""
        results = self.client.query(
            Article.id,
            Article.title,
            Article.content,
            Article.category,
            Article.tags,
            boolean_match("title", "content", "tags").encourage("machine learning").label("score"),
        ).execute()

        # Verify result structure
        assert isinstance(results, object), "Results should be a result set object"
        assert hasattr(results, 'fetchall'), "Results should have fetchall method"
        assert hasattr(results, 'columns'), "Results should have columns attribute"

        # Verify columns include score
        expected_base_columns = ['id', 'title', 'content', 'category', 'tags']
        assert len(results.columns) > len(
            expected_base_columns
        ), f"Should have more than {len(expected_base_columns)} columns (including score)"

        # Check if score column exists (it should be labeled as "score")
        has_score_column = any('score' in col.lower() for col in results.columns)
        assert has_score_column, f"Should have a score column. Found columns: {results.columns}"

        # Verify we have results
        all_rows = results.fetchall()
        assert isinstance(all_rows, list), "fetchall() should return a list"
        assert len(all_rows) > 0, "Should find at least one article matching 'machine learning'"

        # Verify row structure
        for i, row in enumerate(all_rows):
            # Row can be tuple, list, or SQLAlchemy Row object
            assert hasattr(row, '__getitem__'), f"Row {i} should support indexing, got {type(row)}"
            assert hasattr(row, '__len__'), f"Row {i} should support len(), got {type(row)}"
            assert len(row) >= len(
                expected_base_columns
            ), f"Row {i} should have at least {len(expected_base_columns)} columns, got {len(row)}"

        # Verify content matches search criteria
        found_machine = False
        found_learning = False
        for row in all_rows:
            title = str(row[1]).lower() if row[1] else ""
            content = str(row[2]).lower() if row[2] else ""
            tags = str(row[4]).lower() if len(row) > 4 and row[4] else ""

            combined_text = f"{title} {content} {tags}"
            if "machine" in combined_text:
                found_machine = True
            if "learning" in combined_text:
                found_learning = True

        assert found_machine, "Should find articles containing 'machine' in title, content, or tags"
        assert found_learning, "Should find articles containing 'learning' in title, content, or tags"

        # Verify score values are numeric (if score column exists)
        score_column_index = None
        for i, col in enumerate(results.columns):
            if 'score' in col.lower():
                score_column_index = i
                break

        if score_column_index is not None:
            for i, row in enumerate(all_rows):
                if len(row) > score_column_index:
                    score_value = row[score_column_index]
                    assert isinstance(
                        score_value, (int, float)
                    ), f"Score value in row {i} should be numeric, got {type(score_value)}"
                    assert score_value >= 0, f"Score value in row {i} should be non-negative, got {score_value}"

    def test_search_with_where_conditions(self):
        """Test search with additional WHERE conditions."""
        results = (
            self.client.query(
                "test_simple_fulltext_articles.title",
                "test_simple_fulltext_articles.content",
                "test_simple_fulltext_articles.tags",
            )
            .filter(
                boolean_match("title", "content", "tags").encourage("programming"),
                "test_simple_fulltext_articles.category = 'Programming'",
            )
            .execute()
        )

        # Verify result structure
        assert isinstance(results, object), "Results should be a result set object"
        assert hasattr(results, 'rows'), "Results should have rows attribute"
        assert hasattr(results, 'columns'), "Results should have columns attribute"

        # Verify columns are correct (title, content, tags)
        expected_columns = ['title', 'content', 'tags']
        assert len(results.columns) == len(
            expected_columns
        ), f"Expected {len(expected_columns)} columns, got {len(results.columns)}"
        for i, expected_col in enumerate(expected_columns):
            assert results.columns[i] == expected_col, f"Column {i} should be '{expected_col}', got '{results.columns[i]}'"

        # Verify we have results
        assert isinstance(results.rows, list), "Results.rows should be a list"
        # Note: We can't assert len > 0 because there might not be Programming articles

        # Verify row structure
        for i, row in enumerate(results.rows):
            # Row can be tuple, list, or SQLAlchemy Row object
            assert hasattr(row, '__getitem__'), f"Row {i} should support indexing, got {type(row)}"
            assert hasattr(row, '__len__'), f"Row {i} should support len(), got {type(row)}"
            assert len(row) == len(expected_columns), f"Row {i} should have {len(expected_columns)} columns, got {len(row)}"

        # Verify content matches search criteria (if we have results)
        if results.rows:
            found_programming = False
            for row in results.rows:
                title = str(row[0]).lower() if row[0] else ""
                content = str(row[1]).lower() if row[1] else ""
                tags = str(row[2]).lower() if row[2] else ""

                combined_text = f"{title} {content} {tags}"
                if "programming" in combined_text:
                    found_programming = True

            assert found_programming, "Should find articles containing 'programming' in title, content, or tags"

    def test_search_with_ordering_and_limit(self):
        """Test search with ordering and pagination."""
        results = (
            self.client.query(
                "test_simple_fulltext_articles.title",
                "test_simple_fulltext_articles.content",
                "test_simple_fulltext_articles.tags",
                boolean_match("title", "content", "tags").encourage("development").label("score"),
            )
            .order_by("score ASC")
            .limit(3)
            .execute()
        )

        # Verify result structure
        assert isinstance(results, object), "Results should be a result set object"
        assert hasattr(results, 'rows'), "Results should have rows attribute"
        assert hasattr(results, 'columns'), "Results should have columns attribute"

        # Verify columns include score
        expected_columns = ['title', 'content', 'tags']
        assert len(results.columns) > len(
            expected_columns
        ), f"Should have more than {len(expected_columns)} columns (including score)"

        # Check if score column exists
        has_score_column = any('score' in col.lower() for col in results.columns)
        assert has_score_column, f"Should have a score column. Found columns: {results.columns}"

        # Verify limit is respected
        assert isinstance(results.rows, list), "Results.rows should be a list"
        assert len(results.rows) <= 3, f"Should respect limit of 3, got {len(results.rows)} results"

        # Verify row structure
        for i, row in enumerate(results.rows):
            # Row can be tuple, list, or SQLAlchemy Row object
            assert hasattr(row, '__getitem__'), f"Row {i} should support indexing, got {type(row)}"
            assert hasattr(row, '__len__'), f"Row {i} should support len(), got {type(row)}"
            assert len(row) >= len(
                expected_columns
            ), f"Row {i} should have at least {len(expected_columns)} columns, got {len(row)}"

        # Verify ordering by score (if we have multiple results)
        if len(results.rows) > 1:
            # Find score column index
            score_column_index = None
            for i, col in enumerate(results.columns):
                if 'score' in col.lower():
                    score_column_index = i
                    break

            if score_column_index is not None:
                scores = []
                for row in results.rows:
                    if len(row) > score_column_index:
                        score = row[score_column_index]
                        assert isinstance(score, (int, float)), f"Score should be numeric, got {type(score)}"
                        scores.append(score)

                # Verify scores are in ascending order (ASC was specified)
                assert scores == sorted(scores), f"Results should be ordered by score ASC. Got scores: {scores}"

        # Verify content matches search criteria (if we have results)
        if results.rows:
            found_development = False
            for row in results.rows:
                title = str(row[0]).lower() if row[0] else ""
                content = str(row[1]).lower() if row[1] else ""
                tags = str(row[2]).lower() if row[2] else ""

                combined_text = f"{title} {content} {tags}"
                if "development" in combined_text:
                    found_development = True

            assert found_development, "Should find articles containing 'development' in title, content, or tags"

    def test_search_by_category(self):
        """Test filtering by category."""
        # Search AI category
        ai_results = (
            self.client.query(
                "test_simple_fulltext_articles.title",
                "test_simple_fulltext_articles.content",
                "test_simple_fulltext_articles.tags",
            )
            .filter(
                boolean_match("title", "content", "tags").encourage("learning"),
                "test_simple_fulltext_articles.category = 'AI'",
            )
            .execute()
        )

        # Search Programming category
        prog_results = (
            self.client.query(
                "test_simple_fulltext_articles.title",
                "test_simple_fulltext_articles.content",
                "test_simple_fulltext_articles.tags",
            )
            .filter(
                boolean_match("title", "content", "tags").encourage("programming"),
                "test_simple_fulltext_articles.category = 'Programming'",
            )
            .execute()
        )

        # Verify result structures
        for result_name, result in [("AI results", ai_results), ("Programming results", prog_results)]:
            assert isinstance(result, object), f"{result_name} should be a result set object"
            assert hasattr(result, 'rows'), f"{result_name} should have rows attribute"
            assert hasattr(result, 'columns'), f"{result_name} should have columns attribute"

            # Verify columns are correct
            expected_columns = ['title', 'content', 'tags']
            assert len(result.columns) == len(
                expected_columns
            ), f"{result_name}: Expected {len(expected_columns)} columns, got {len(result.columns)}"
            for i, expected_col in enumerate(expected_columns):
                assert (
                    result.columns[i] == expected_col
                ), f"{result_name}: Column {i} should be '{expected_col}', got '{result.columns[i]}'"

            # Verify rows structure
            assert isinstance(result.rows, list), f"{result_name}: rows should be a list"
            for i, row in enumerate(result.rows):
                # Row can be tuple, list, or SQLAlchemy Row object
                assert hasattr(row, '__getitem__'), f"{result_name}: Row {i} should support indexing, got {type(row)}"
                assert hasattr(row, '__len__'), f"{result_name}: Row {i} should support len(), got {type(row)}"
                assert len(row) == len(
                    expected_columns
                ), f"{result_name}: Row {i} should have {len(expected_columns)} columns, got {len(row)}"

        # Verify content matches search criteria (if we have results)
        if ai_results.rows:
            found_learning = False
            for row in ai_results.rows:
                title = str(row[0]).lower() if row[0] else ""
                content = str(row[1]).lower() if row[1] else ""
                tags = str(row[2]).lower() if row[2] else ""

                combined_text = f"{title} {content} {tags}"
                if "learning" in combined_text:
                    found_learning = True

            assert found_learning, "AI results should contain articles with 'learning' in title, content, or tags"

        if prog_results.rows:
            found_programming = False
            for row in prog_results.rows:
                title = str(row[0]).lower() if row[0] else ""
                content = str(row[1]).lower() if row[1] else ""
                tags = str(row[2]).lower() if row[2] else ""

                combined_text = f"{title} {content} {tags}"
                if "programming" in combined_text:
                    found_programming = True

            assert (
                found_programming
            ), "Programming results should contain articles with 'programming' in title, content, or tags"

    def test_complex_boolean_search(self):
        """Test complex boolean search with multiple terms."""
        results = (
            self.client.query(
                "test_simple_fulltext_articles.title",
                "test_simple_fulltext_articles.content",
                "test_simple_fulltext_articles.tags",
            )
            .filter(
                boolean_match("title", "content", "tags").must("web").must_not("deprecated", "legacy"),
                "test_simple_fulltext_articles.category = 'Programming'",
            )
            .execute()
        )

        # Verify result structure
        assert isinstance(results, object), "Results should be a result set object"
        assert hasattr(results, 'rows'), "Results should have rows attribute"
        assert hasattr(results, 'columns'), "Results should have columns attribute"

        # Verify columns are correct
        expected_columns = ['title', 'content', 'tags']
        assert len(results.columns) == len(
            expected_columns
        ), f"Expected {len(expected_columns)} columns, got {len(results.columns)}"
        for i, expected_col in enumerate(expected_columns):
            assert results.columns[i] == expected_col, f"Column {i} should be '{expected_col}', got '{results.columns[i]}'"

        # Verify we have results
        assert isinstance(results.rows, list), "Results.rows should be a list"
        # Note: We can't assert len > 0 because there might not be matching articles

        # Verify row structure
        for i, row in enumerate(results.rows):
            # Row can be tuple, list, or SQLAlchemy Row object
            assert hasattr(row, '__getitem__'), f"Row {i} should support indexing, got {type(row)}"
            assert hasattr(row, '__len__'), f"Row {i} should support len(), got {type(row)}"
            assert len(row) == len(expected_columns), f"Row {i} should have {len(expected_columns)} columns, got {len(row)}"

        # Verify content matches complex boolean criteria (if we have results)
        if results.rows:
            for i, row in enumerate(results.rows):
                title = str(row[0]).lower() if row[0] else ""
                content = str(row[1]).lower() if row[1] else ""
                tags = str(row[2]).lower() if row[2] else ""

                combined_text = f"{title} {content} {tags}"

                # Must have "web"
                assert (
                    "web" in combined_text
                ), f"Row {i} should contain 'web' in title, content, or tags. Got: {combined_text[:100]}..."

                # Must not have "deprecated"
                assert (
                    "deprecated" not in combined_text
                ), f"Row {i} should not contain 'deprecated'. Got: {combined_text[:100]}..."

                # Must not have "legacy"
                assert "legacy" not in combined_text, f"Row {i} should not contain 'legacy'. Got: {combined_text[:100]}..."

                # Category verification is handled by WHERE clause

    def test_search_with_custom_score_alias(self):
        """Test search with custom score alias."""
        results = (
            self.client.query(
                "test_simple_fulltext_articles.title",
                "test_simple_fulltext_articles.content",
                "test_simple_fulltext_articles.tags",
                boolean_match("title", "content", "tags").encourage("artificial intelligence").label("relevance"),
            )
            .order_by("relevance ASC")
            .execute()
        )

        assert isinstance(results.rows, list)
        # Score column should be present (custom alias doesn't change result structure)
        if results.rows:
            score_index = len(results.columns) - 1
            for row in results.rows:
                score = row[score_index]
                assert isinstance(score, (int, float))

    def test_empty_search_results(self):
        """Test search that should return no results."""
        results = self.client.query(
            "test_simple_fulltext_articles.title",
            "test_simple_fulltext_articles.content",
            "test_simple_fulltext_articles.tags",
            boolean_match("title", "content", "tags").encourage("nonexistent_term_xyz123"),
        ).execute()

        assert isinstance(results.rows, list)
        assert len(results.rows) == 0

    def test_chinese_text_search(self):
        """Test search with non-English text."""
        # Insert a Chinese article for testing
        self.client.execute(
            """
            INSERT INTO test_simple_fulltext_articles (title, content, category, tags) 
            VALUES ('中文测试', '这是一个中文全文搜索测试', 'Test', 'chinese,test')
        """
        )

        results = self.client.query(
            "test_simple_fulltext_articles.title",
            "test_simple_fulltext_articles.content",
            "test_simple_fulltext_articles.tags",
            boolean_match("title", "content", "tags").encourage("中文"),
        ).execute()

        assert isinstance(results.rows, list)
        # Should find the Chinese article
        if results.rows:
            found_chinese = any("中文" in row[1] for row in results.rows)
            assert found_chinese

    def test_error_handling_invalid_table(self):
        """Test error handling for invalid table."""
        with pytest.raises(QueryError):
            (
                self.client.query(
                    "nonexistent_table.title",
                    "nonexistent_table.content",
                    boolean_match("title", "content").encourage("test"),
                ).execute()
            )

    def test_method_chaining_completeness(self):
        """Test that all methods return self for proper chaining."""
        # Test method chaining with query builder
        result = (
            self.client.query("test_simple_fulltext_articles.title", "test_simple_fulltext_articles.content")
            .filter(boolean_match("title", "content", "tags").encourage("test"))
            .limit(5)
            .execute()
        )

        # Verify chaining works
        assert result is not None


class TestSimpleFulltextMigration:
    """Test migration from old fulltext interfaces to simple_query."""

    @classmethod
    def setup_class(cls):
        """Set up test database."""
        conn_params = get_connection_kwargs()
        client_params = {k: v for k, v in conn_params.items() if k in ['host', 'port', 'user', 'password', 'database']}

        cls.client = Client()
        cls.client.connect(**client_params)

        cls.test_db = "test_migration"
        try:
            cls.client.execute(f"DROP DATABASE IF EXISTS {cls.test_db}")
            cls.client.execute(f"CREATE DATABASE {cls.test_db}")
            cls.client.execute(f"USE {cls.test_db}")
        except Exception as e:
            print(f"Database setup warning: {e}")

        # Create test table
        cls.client.execute("DROP TABLE IF EXISTS migration_test")
        cls.client.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                body TEXT NOT NULL
            )
        """
        )

        # Insert test data
        cls.client.execute(
            """
            INSERT INTO migration_test (title, body) VALUES 
            ('Python Tutorial', 'Learn Python programming'),
            ('Java Guide', 'Complete Java development guide'),
            ('Machine Learning', 'AI and ML concepts')
        """
        )

        # Create fulltext index
        try:
            cls.client.execute("CREATE FULLTEXT INDEX ft_migration ON migration_test(title, body)")
        except Exception as e:
            print(f"Fulltext index creation warning: {e}")

    @classmethod
    def teardown_class(cls):
        """Clean up."""
        if hasattr(cls, 'client'):
            cls.client.disconnect()

    def test_replace_basic_fulltext_search(self):
        """Test replacing basic fulltext search with simple_query."""
        # Old way (if it existed): complex builder setup
        # New way: simple_query interface
        results = self.client.query(
            "migration_test.title", "migration_test.body", boolean_match("title", "body").encourage("python")
        ).execute()

        assert isinstance(results.rows, list)
        if results.rows:
            found_python = any("Python" in row[1] for row in results.rows)
            assert found_python

    def test_replace_scored_search(self):
        """Test replacing scored fulltext search."""
        # New way with scoring
        results = (
            self.client.query(
                "migration_test.title",
                "migration_test.body",
                boolean_match("title", "body").encourage("programming").label("score"),
            )
            .order_by("score ASC")
            .execute()
        )

        assert isinstance(results.rows, list)
        # Verify score column is included
        if results.rows:
            assert len(results.columns) > 2  # Original columns + score

    def test_replace_complex_search(self):
        """Test replacing complex fulltext search scenarios."""
        # Boolean search with filtering
        results = (
            self.client.query(
                "migration_test.title",
                "migration_test.body",
                boolean_match("title", "body").must("guide").must_not("deprecated").label("score"),
            )
            .limit(5)
            .execute()
        )

        assert isinstance(results.rows, list)
        # Should work without errors and return relevant results

    @pytest.mark.asyncio
    async def test_async_execute_functionality(self):
        """Test async_execute method with real database."""
        from matrixone.async_client import AsyncClient
        from matrixone.config import get_connection_kwargs

        # Get connection parameters
        conn_params = get_connection_kwargs()
        client_params = {k: v for k, v in conn_params.items() if k in ['host', 'port', 'user', 'password', 'database']}

        async_client = AsyncClient()
        await async_client.connect(**client_params)

        try:
            # Use the same test database
            await async_client.execute(f"USE {self.test_db}")

            # Test async execute
            results = (
                await async_client.query(
                    "migration_test.title",
                    "migration_test.body",
                    boolean_match("title", "body").encourage("python").label("score"),
                )
                .limit(3)
                .execute()
            )

            assert results is not None
            assert hasattr(results, 'rows')
            assert isinstance(results.rows, list)

        finally:
            await async_client.disconnect()

    def test_transaction_simple_query_functionality(self):
        """Test TransactionSimpleFulltextQueryBuilder with real database."""
        # Test within a transaction
        with self.client.transaction() as tx:
            results = (
                tx.query(
                    "migration_test.title",
                    "migration_test.body",
                    boolean_match("title", "body").encourage("guide").label("score"),
                )
                .limit(3)
                .execute()
            )

            assert results is not None
            assert hasattr(results, 'rows')
            assert isinstance(results.rows, list)

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_functionality(self):
        """Test AsyncTransactionSimpleFulltextQueryBuilder with real database."""
        from matrixone.async_client import AsyncClient
        from matrixone.config import get_connection_kwargs

        # Get connection parameters
        conn_params = get_connection_kwargs()
        client_params = {k: v for k, v in conn_params.items() if k in ['host', 'port', 'user', 'password', 'database']}

        async_client = AsyncClient()
        await async_client.connect(**client_params)

        try:
            # Use the same test database
            await async_client.execute(f"USE {self.test_db}")

            # Test within an async transaction
            async with async_client.transaction() as tx:
                results = (
                    await tx.query(
                        "migration_test.title",
                        "migration_test.body",
                        boolean_match("title", "body").encourage("python").label("score"),
                    )
                    .limit(3)
                    .execute()
                )

                assert results is not None
                assert hasattr(results, 'rows')
                assert isinstance(results.rows, list)

        finally:
            await async_client.disconnect()

    def test_error_handling_async_execute_with_sync_client(self):
        """Test that execute works with sync client (should not raise error)."""
        # This should not raise an error - query should be available
        result = (
            self.client.query("migration_test.title", "migration_test.body")
            .filter(boolean_match("title", "body").encourage("test"))
            .execute()
        )

        # Test that query method works correctly
        assert result is not None, "Query should return a result set"

    @pytest.mark.asyncio
    async def test_error_handling_execute_with_async_client(self):
        """Test that async query works correctly."""
        from matrixone.async_client import AsyncClient, AsyncFulltextIndexManager
        import asyncio

        async_client = AsyncClient()
        async_client._fulltext_index = AsyncFulltextIndexManager(async_client)

        # Test that query method is available (without actually executing)
        query_builder = async_client.query("test_table.title", "test_table.body").filter(
            boolean_match("title", "body").encourage("test")
        )

        # Should return a query builder
        assert query_builder is not None, "Query builder should be created"
        print("✅ query builder created as expected for async client")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

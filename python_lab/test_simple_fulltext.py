#!/usr/bin/env python3
"""
Test script for the new simplified fulltext search interface.

Tests the SimpleFulltextQueryBuilder class and simple_query method.
"""

import sys
import os

# Add the parent directory to the path so we can import matrixone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'clients', 'python'))

from matrixone.client import Client, SimpleFulltextQueryBuilder


def test_simple_fulltext_sql_generation():
    """Test SQL generation without database connection."""
    print("🧪 Testing SimpleFulltextQueryBuilder SQL Generation")
    print("=" * 60)
    
    # Create a mock client (we won't execute queries, just generate SQL)
    client = Client()
    
    # Test 1: Basic natural language search
    print("\n1. Natural Language Search:")
    builder = SimpleFulltextQueryBuilder(client, "articles")
    builder.columns("title", "content").search("machine learning")
    sql = builder.explain()
    print(f"   SQL: {sql}")
    expected = "SELECT * FROM articles WHERE MATCH(title, content) AGAINST('machine learning')"
    assert expected in sql, f"Expected: {expected}, Got: {sql}"
    print("   ✅ PASS")
    
    # Test 2: Boolean mode with required terms
    print("\n2. Boolean Mode - Required Terms:")
    builder = SimpleFulltextQueryBuilder(client, "articles")
    builder.columns("title", "content").must_have("python", "tutorial")
    sql = builder.explain()
    print(f"   SQL: {sql}")
    expected = "MATCH(title, content) AGAINST('+python +tutorial' IN BOOLEAN MODE)"
    assert expected in sql, f"Expected: {expected}, Got: {sql}"
    print("   ✅ PASS")
    
    # Test 3: Boolean mode with excluded terms
    print("\n3. Boolean Mode - Required + Excluded Terms:")
    builder = SimpleFulltextQueryBuilder(client, "articles")
    builder.columns("title", "content").must_have("python").must_not_have("deprecated")
    sql = builder.explain()
    print(f"   SQL: {sql}")
    expected = "MATCH(title, content) AGAINST('+python -deprecated' IN BOOLEAN MODE)"
    assert expected in sql, f"Expected: {expected}, Got: {sql}"
    print("   ✅ PASS")
    
    # Test 4: Search with score
    print("\n4. Search with Relevance Score:")
    builder = SimpleFulltextQueryBuilder(client, "articles")
    builder.columns("title", "content").search("data science").with_score()
    sql = builder.explain()
    print(f"   SQL: {sql}")
    expected_select = "SELECT *, MATCH(title, content) AGAINST('data science') AS score"
    assert expected_select in sql, f"Expected: {expected_select}, Got: {sql}"
    print("   ✅ PASS")
    
    # Test 5: Complete query with ordering and limits
    print("\n5. Complete Query with Ordering and Limits:")
    builder = SimpleFulltextQueryBuilder(client, "articles")
    builder.columns("title", "content") \
           .search("artificial intelligence") \
           .with_score("relevance") \
           .where("category = 'Technology'") \
           .order_by_score() \
           .limit(10, 5)
    sql = builder.explain()
    print(f"   SQL: {sql}")
    
    # Check various parts
    assert "SELECT *, MATCH(title, content) AGAINST('artificial intelligence') AS relevance" in sql
    assert "WHERE MATCH(title, content) AGAINST('artificial intelligence') AND category = 'Technology'" in sql
    assert "ORDER BY relevance DESC" in sql
    assert "LIMIT 10 OFFSET 5" in sql
    print("   ✅ PASS")
    
    # Test 6: Error handling
    print("\n6. Error Handling:")
    try:
        builder = SimpleFulltextQueryBuilder(client, "articles")
        builder.explain()  # Should fail - no columns or query
        assert False, "Should have raised ValueError"
    except ValueError as e:
        print(f"   Expected error: {e}")
        print("   ✅ PASS")
    
    print("\n✅ All SQL generation tests passed!")


def test_simple_query_interface():
    """Test the simple_query interface on FulltextIndexManager."""
    print("\n🧪 Testing simple_query Interface")
    print("=" * 60)
    
    # Create a mock client and initialize fulltext_index
    client = Client()
    from matrixone.client import FulltextIndexManager
    client._fulltext_index = FulltextIndexManager(client)
    
    # Test 1: Table name interface
    print("\n1. Table Name Interface:")
    builder = client.fulltext_index.simple_query("articles")
    builder.columns("title", "content").search("python programming")
    sql = builder.explain()
    print(f"   SQL: {sql}")
    expected = "SELECT * FROM articles WHERE MATCH(title, content) AGAINST('python programming')"
    assert expected in sql
    print("   ✅ PASS")
    
    # Test 2: Chaining interface  
    print("\n2. Method Chaining:")
    sql = client.fulltext_index.simple_query("documents") \
            .columns("title", "body") \
            .must_have("machine", "learning") \
            .must_not_have("deprecated") \
            .with_score() \
            .order_by_score() \
            .limit(5) \
            .explain()
    print(f"   SQL: {sql}")
    
    # Verify components
    assert "MATCH(title, body) AGAINST('+machine +learning -deprecated' IN BOOLEAN MODE)" in sql
    assert "AS score" in sql
    assert "ORDER BY score DESC" in sql
    assert "LIMIT 5" in sql
    print("   ✅ PASS")
    
    print("\n✅ All interface tests passed!")


def test_matrixone_compatibility():
    """Test compatibility with MatrixOne syntax from test cases."""
    print("\n🧪 Testing MatrixOne Compatibility")
    print("=" * 60)
    
    client = Client()
    from matrixone.client import FulltextIndexManager
    client._fulltext_index = FulltextIndexManager(client)
    
    # Test 1: Natural language mode (from fulltext.result line 35)
    print("\n1. Natural Language Mode (matching MatrixOne test case):")
    sql = client.fulltext_index.simple_query("src") \
            .columns("body", "title") \
            .search("is red") \
            .with_score() \
            .explain()
    print(f"   Generated: {sql}")
    # Should match: select *, match(body, title) against('is red' in natural language mode) as score from src;
    expected_select = "SELECT *, MATCH(body, title) AGAINST('is red') AS score"
    expected_where = "WHERE MATCH(body, title) AGAINST('is red')"
    assert expected_select in sql and expected_where in sql
    print("   ✅ PASS - Matches MatrixOne syntax")
    
    # Test 2: Boolean mode (from fulltext.result lines with +/- operators)
    print("\n2. Boolean Mode (matching MatrixOne test case):")
    sql = client.fulltext_index.simple_query("src") \
            .columns("body", "title") \
            .must_have("red") \
            .must_not_have("blue") \
            .explain()
    print(f"   Generated: {sql}")
    # Should match: select * from src where match(body, title) against('+red -blue' in boolean mode);
    expected = "MATCH(body, title) AGAINST('+red -blue' IN BOOLEAN MODE)"
    assert expected in sql
    print("   ✅ PASS - Matches MatrixOne syntax")
    
    # Test 3: Simple search (from fulltext.result line 31)
    print("\n3. Simple Search (matching MatrixOne test case):")
    sql = client.fulltext_index.simple_query("src") \
            .columns("body", "title") \
            .search("red") \
            .explain()
    print(f"   Generated: {sql}")
    # Should match: select * from src where match(body, title) against('red');
    expected = "SELECT * FROM src WHERE MATCH(body, title) AGAINST('red')"
    assert expected in sql
    print("   ✅ PASS - Matches MatrixOne syntax")
    
    print("\n✅ All MatrixOne compatibility tests passed!")


def main():
    """Run all tests."""
    print("🚀 Testing Simplified Fulltext Search Interface")
    print("=" * 80)
    
    try:
        test_simple_fulltext_sql_generation()
        test_simple_query_interface()
        test_matrixone_compatibility()
        
        print("\n" + "=" * 80)
        print("🎉 ALL TESTS PASSED!")
        print("\n📋 Summary:")
        print("   ✅ SimpleFulltextQueryBuilder SQL generation works correctly")
        print("   ✅ simple_query() interface provides easy access")
        print("   ✅ Generated SQL matches MatrixOne syntax requirements")
        print("   ✅ Supports both natural language and boolean modes")
        print("   ✅ Includes scoring, ordering, and pagination features")
        print("\n🔧 Ready for integration!")
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    return 0


if __name__ == "__main__":
    sys.exit(main())

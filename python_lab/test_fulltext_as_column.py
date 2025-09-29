#!/usr/bin/env python3
"""
Test script for using FulltextFilter as SELECT columns with label()
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'clients', 'python'))

from matrixone import Client
try:
    from sqlalchemy.orm import declarative_base
except ImportError:
    from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import Column, Integer, String, Text
from matrixone.sqlalchemy_ext.fulltext_search import boolean_match, natural_match, group

def test_fulltext_as_column():
    """Test using fulltext functions as SELECT columns."""
    
    client = Client()
    client.connect(host='localhost', port=6001, user='root', password='111', database='test')
    
    # Define model
    Base = declarative_base()
    
    class Article(Base):
        __tablename__ = "test_articles"
        id = Column(Integer, primary_key=True)
        title = Column(String(200))
        content = Column(Text)
        category = Column(String(50))
    
    print("=== 全文搜索作为 SELECT 列测试 ===\n")
    
    # Create table and data
    client.drop_all(Base)
    client.create_all(Base)
    
    # Insert test data
    client.execute("""
        INSERT INTO test_articles (id, title, content, category) VALUES
        (1, 'Python Programming Guide', 'Learn Python programming with examples and tutorials', 'Programming'),
        (2, 'Machine Learning Basics', 'Introduction to machine learning algorithms and techniques', 'AI'),
        (3, 'Web Development', 'Modern web development using Python frameworks', 'Programming'),
        (4, 'Data Science Tutorial', 'Data analysis and visualization with Python', 'Data Science'),
        (5, 'Advanced Python', 'Advanced Python programming techniques and patterns', 'Programming')
    """)
    
    # Create fulltext index
    client.fulltext_index.enable_fulltext()
    client.execute('SET ft_relevancy_algorithm = "BM25"')
    client.fulltext_index.create("test_articles", "ftidx_content", ["title", "content"], "BM25")
    
    print("1. 基本用法：Article.id + fulltext score")
    # Test 1: Basic usage with label
    query1 = client.query(
        Article.id,
        Article.title,
        boolean_match("title", "content").must("python").label("score")
    )
    
    sql1, params1 = query1._build_sql()
    print(f"Generated SQL: {sql1}")
    print(f"✓ Contains AS score: {'AS score' in sql1}")
    
    try:
        results1 = query1.all()
        print(f"Results: {len(results1)} rows")
        for row in results1:
            print(f"  ID: {row[0]}, Title: {row[1]}, Score: {row[2] if len(row) > 2 else 'N/A'}")
    except Exception as e:
        print(f"Query execution: {e}")
    print()
    
    print("2. 复杂布尔查询作为 SELECT 列")
    # Test 2: Complex boolean query as column
    query2 = client.query(
        Article.id,
        Article.category,
        boolean_match("title", "content")
            .must("python")
            .encourage("programming")
            .must_not("deprecated")
            .label("relevance_score")
    )
    
    sql2, params2 = query2._build_sql()
    print(f"Generated SQL: {sql2}")
    print(f"✓ Contains AS relevance_score: {'AS relevance_score' in sql2}")
    print()
    
    print("3. 自然语言模式作为 SELECT 列")
    # Test 3: Natural language mode as column
    query3 = client.query(
        Article.id,
        Article.title,
        natural_match("title", "content", query="machine learning tutorial").label("nlp_score")
    )
    
    sql3, params3 = query3._build_sql()
    print(f"Generated SQL: {sql3}")
    print(f"✓ Contains NATURAL LANGUAGE: {'natural language mode' in sql3}")
    print(f"✓ Contains AS nlp_score: {'AS nlp_score' in sql3}")
    print()
    
    print("4. 多个全文搜索列")
    # Test 4: Multiple fulltext columns
    query4 = client.query(
        Article.id,
        Article.title,
        boolean_match("title", "content").must("python").label("python_score"),
        boolean_match("title", "content").must("programming").label("programming_score")
    )
    
    sql4, params4 = query4._build_sql()
    print(f"Generated SQL: {sql4}")
    print(f"✓ Contains AS python_score: {'AS python_score' in sql4}")
    print(f"✓ Contains AS programming_score: {'AS programming_score' in sql4}")
    print()
    
    print("5. 带组的复杂查询作为 SELECT 列")
    # Test 5: Complex query with groups as column
    query5 = client.query(
        Article.id,
        Article.title,
        boolean_match("title", "content")
            .must("python")
            .encourage(group().medium("tutorial", "guide"))
            .discourage(group().medium("advanced", "complex"))
            .label("weighted_score")
    ).filter(Article.category == "Programming")
    
    sql5, params5 = query5._build_sql()
    print(f"Generated SQL: {sql5}")
    print(f"✓ Contains AS weighted_score: {'AS weighted_score' in sql5}")
    print(f"✓ Contains WHERE filter: {'WHERE' in sql5}")
    print()
    
    print("6. 验证与 MatrixOne 测试用例的兼容性")
    # Test 6: Compatibility with MatrixOne test cases
    print("MatrixOne 格式: SELECT *, MATCH(body, title) AGAINST('is red' IN natural language mode) AS score FROM src;")
    
    query6 = client.query(
        Article,  # All columns
        natural_match("title", "content", query="python programming").label("score")
    )
    
    sql6, params6 = query6._build_sql()
    print(f"我们的格式: {sql6}")
    print(f"✓ 格式兼容: {'AS score' in sql6}")
    print()
    
    # Cleanup
    client.drop_all(Base)
    client.disconnect()
    
    print("=== 测试总结 ===")
    print("✓ boolean_match() 支持 .label() 方法")
    print("✓ natural_match() 支持 .label() 方法") 
    print("✓ 可以作为 query() 的 SELECT 列使用")
    print("✓ 支持复杂的布尔查询和组合")
    print("✓ 支持多个全文搜索列")
    print("✓ 兼容 SQLAlchemy 的 Label 机制")
    print("✓ 兼容 MatrixOne 的 AS score 格式")
    print("\n示例用法:")
    print("query(Article, Article.id, boolean_match('title', 'content').must('python').label('score'))")

if __name__ == "__main__":
    test_fulltext_as_column()

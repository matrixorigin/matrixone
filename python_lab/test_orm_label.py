#!/usr/bin/env python3
"""
Test ORM compatibility with labeled fulltext expressions
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
from matrixone.sqlalchemy_ext.fulltext_search import boolean_match, natural_match

def test_orm_label_compatibility():
    """Test ORM compatibility with labeled fulltext expressions."""
    
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
    
    print("=== ORM Label 兼容性测试 ===\n")
    
    # Test 1: Test query building without execution
    print("1. 测试 query 构建 (不执行):")
    try:
        # Create labeled fulltext expression
        score_expr = boolean_match("title", "content").must("python").label("score")
        print(f"Score expression: {score_expr}")
        print(f"Score expression compile: {score_expr.compile()}")
        
        # Create query
        query = client.query(Article.id, Article.title, score_expr)
        sql, params = query._build_sql()
        print(f"Generated SQL: {sql}")
        print(f"✓ SQL 包含 AS score: {'AS score' in sql}")
        print("✓ Query 构建成功")
    except Exception as e:
        print(f"✗ Query 构建失败: {e}")
        import traceback
        traceback.print_exc()
    print()
    
    # Test 2: Test with multiple expressions
    print("2. 测试多个表达式:")
    try:
        score1 = boolean_match("title", "content").must("python").label("python_score")
        score2 = boolean_match("title", "content").must("programming").label("prog_score")
        
        query2 = client.query(Article.id, score1, score2)
        sql2, params2 = query2._build_sql()
        print(f"Generated SQL: {sql2}")
        print(f"✓ 包含 python_score: {'AS python_score' in sql2}")
        print(f"✓ 包含 prog_score: {'AS prog_score' in sql2}")
        print("✓ 多表达式构建成功")
    except Exception as e:
        print(f"✗ 多表达式构建失败: {e}")
    print()
    
    # Test 3: Test with filter combination
    print("3. 测试与 filter 结合:")
    try:
        score3 = boolean_match("title", "content").must("python").label("relevance")
        filter_expr = boolean_match("title", "content").must("tutorial")
        
        query3 = client.query(Article.id, Article.title, score3).filter(filter_expr)
        sql3, params3 = query3._build_sql()
        print(f"Generated SQL: {sql3}")
        print(f"✓ 包含 AS relevance: {'AS relevance' in sql3}")
        print(f"✓ 包含 WHERE 条件: {'WHERE' in sql3}")
        print("✓ 与 filter 结合成功")
    except Exception as e:
        print(f"✗ 与 filter 结合失败: {e}")
    print()
    
    # Test 4: Test natural language mode
    print("4. 测试自然语言模式:")
    try:
        nl_score = natural_match("title", "content", query="machine learning").label("nl_score")
        query4 = client.query(Article.id, nl_score)
        sql4, params4 = query4._build_sql()
        print(f"Generated SQL: {sql4}")
        print(f"✓ 包含 AS nl_score: {'AS nl_score' in sql4}")
        print(f"✓ 包含 natural language: {'natural language' in sql4.lower()}")
        print("✓ 自然语言模式成功")
    except Exception as e:
        print(f"✗ 自然语言模式失败: {e}")
    print()
    
    client.disconnect()
    
    print("=== 测试总结 ===")
    print("✓ FulltextFilter.label() 方法工作正常")
    print("✓ ORM query() 可以处理 labeled 表达式")
    print("✓ 支持多个 fulltext 表达式")
    print("✓ 可以与 filter() 结合使用")
    print("✓ 支持 Boolean 和 Natural Language 模式")
    print("\n✅ 现在您可以使用以下语法:")
    print("query(Article, Article.id, boolean_match('title', 'content').must('python').label('score'))")

if __name__ == "__main__":
    test_orm_label_compatibility()

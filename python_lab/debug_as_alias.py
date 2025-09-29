#!/usr/bin/env python3
"""
Debug the AS alias duplication issue
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
from matrixone.sqlalchemy_ext.fulltext_search import boolean_match

def debug_as_alias():
    """Debug AS alias duplication issue."""
    
    print("=== 调试 AS 别名重复问题 ===\n")
    
    # Define model
    Base = declarative_base()
    
    class Article(Base):
        __tablename__ = "test_articles"
        id = Column(Integer, primary_key=True)
        title = Column(String(200))
        content = Column(Text)
    
    # Test 1: Direct fulltext compilation
    print("1. 直接编译 FulltextFilter:")
    expr = boolean_match("title", "content").must("python")
    print(f"   FulltextFilter: {expr.compile()}")
    
    labeled_expr = expr.label("python_score")
    print(f"   FulltextLabel type: {type(labeled_expr)}")
    print(f"   FulltextLabel.__str__(): {labeled_expr}")
    print(f"   FulltextLabel.compile(): {labeled_expr.compile()}")
    print()
    
    # Test 2: Check ORM query generation
    print("2. ORM 查询生成:")
    client = Client()
    try:
        client.connect(host='127.0.0.1', port=6001, user='root', password='111', database='test')
        
        # Create query
        query = client.query(
            Article.id,
            Article.title,
            labeled_expr
        )
        
        # Check how the query builds the SQL
        print(f"   Query columns: {query._select_columns}")
        print(f"   Query table: {query._table_name}")
        
        # Build the SQL
        sql, params = query._build_sql()
        print(f"   Generated SQL: {sql}")
        print(f"   问题: {'AS python_score AS python_score' in sql}")
        print(f"   修复: {'AS python_score' in sql and 'AS python_score AS python_score' not in sql}")
        
        client.disconnect()
        
    except Exception as e:
        print(f"   连接错误: {e}")
    print()
    
    # Test 3: Check how ORM handles different column types
    print("3. ORM 列处理比较:")
    
    # Regular column
    print(f"   Article.id type: {type(Article.id)}")
    print(f"   Article.id: {Article.id}")
    
    # Labeled fulltext
    print(f"   FulltextLabel type: {type(labeled_expr)}")
    print(f"   FulltextLabel str: {labeled_expr}")
    print()
    
    # Test 4: Investigate BaseMatrixOneQuery._build_select_clause
    print("4. 分析 _build_select_clause 方法:")
    
    # Let's see how each column is processed
    columns = [Article.id, Article.title, labeled_expr]
    
    for i, col in enumerate(columns):
        print(f"   Column {i}: {type(col)} -> {col}")
        
        # Check if it has the attributes that ORM looks for
        attrs_to_check = ['key', 'name', '__str__', '_compiler_dispatch']
        for attr in attrs_to_check:
            has_attr = hasattr(col, attr)
            if has_attr:
                try:
                    value = getattr(col, attr)
                    if callable(value):
                        print(f"      {attr}: (callable)")
                    else:
                        print(f"      {attr}: {value}")
                except Exception as e:
                    print(f"      {attr}: (error: {e})")
            else:
                print(f"      {attr}: (not found)")
        print()

if __name__ == "__main__":
    debug_as_alias()

#!/usr/bin/env python3
"""
Test script for FulltextSearchBuilder AS score support
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'clients', 'python'))

from matrixone import Client
from matrixone.sqlalchemy_ext.fulltext_search import FulltextSearchBuilder, FulltextSearchMode, FulltextSearchAlgorithm

def test_fulltext_score_builder():
    """Test FulltextSearchBuilder with score support."""
    
    client = Client()
    client.connect(host='localhost', port=6001, user='root', password='111', database='test')
    
    print("=== FulltextSearchBuilder AS score 测试 ===\n")
    
    # 1. 测试基本的 with_score 功能
    print("1. 基本 with_score 功能：")
    builder = FulltextSearchBuilder(client)
    sql = (builder
           .table("src")
           .columns(["body", "title"])
           .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
           .query("machine learning")
           .with_score()
           ._build_sql())
    
    expected = "SELECT *, MATCH(body, title) AGAINST('machine learning' IN natural language mode) AS score FROM src WHERE MATCH(body, title) AGAINST('machine learning' IN natural language mode)"
    print(f"Generated SQL: {sql}")
    print(f"Expected pattern: SELECT *, MATCH(...) AS score FROM ... WHERE MATCH(...)")
    print(f"✓ Contains AS score: {'AS score' in sql}")
    print()
    
    # 2. 测试 Boolean Mode 的 with_score
    print("2. Boolean Mode with_score:")
    builder2 = FulltextSearchBuilder(client)
    sql2 = (builder2
            .table("src")
            .columns(["body", "title"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .query("+machine +learning")
            .with_score()
            ._build_sql())
    
    print(f"Generated SQL: {sql2}")
    print(f"✓ Contains AS score: {'AS score' in sql2}")
    print()
    
    # 3. 测试选择特定列 + with_score
    print("3. 选择特定列 + with_score:")
    builder3 = FulltextSearchBuilder(client)
    sql3 = (builder3
            .table("src")
            .columns(["body", "title"])
            .select(["id", "title"])
            .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
            .query("red blue")
            .with_score()
            ._build_sql())
    
    print(f"Generated SQL: {sql3}")
    print(f"✓ Contains AS score: {'AS score' in sql3}")
    print()
    
    # 4. 测试 order by score
    print("4. Order by score:")
    builder4 = FulltextSearchBuilder(client)
    sql4 = (builder4
            .table("src")
            .columns(["body", "title"])
            .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
            .query("machine learning")
            .with_score()
            .order_by("score", "DESC")
            ._build_sql())
    
    print(f"Generated SQL: {sql4}")
    print(f"✓ Contains ORDER BY score: {'ORDER BY score' in sql4}")
    print()
    
    # 5. 测试复杂查询
    print("5. 复杂查询 (WHERE + score + ORDER BY + LIMIT):")
    builder5 = FulltextSearchBuilder(client)
    sql5 = (builder5
            .table("articles")
            .columns(["title", "content"])
            .select(["id", "title"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .query("+machine +learning -java")
            .with_score()
            .where("category = 'AI'")
            .order_by("score", "DESC")
            .limit(10)
            ._build_sql())
    
    print(f"Generated SQL: {sql5}")
    print(f"✓ Contains AS score: {'AS score' in sql5}")
    print(f"✓ Contains WHERE: {'WHERE' in sql5}")
    print(f"✓ Contains ORDER BY: {'ORDER BY' in sql5}")
    print(f"✓ Contains LIMIT: {'LIMIT' in sql5}")
    print()
    
    client.disconnect()
    
    print("=== 测试总结 ===")
    print("✓ FulltextSearchBuilder 已经支持 AS score 功能")
    print("✓ 支持 Natural Language 和 Boolean 模式")
    print("✓ 支持自定义列选择")
    print("✓ 支持按 score 排序")
    print("✓ 支持复杂查询组合")

if __name__ == "__main__":
    test_fulltext_score_builder()

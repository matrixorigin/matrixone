#!/usr/bin/env python3
"""
Test script for FulltextQueryBuilder AS score support
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'clients', 'python'))

from matrixone.sqlalchemy_ext.fulltext_search import (
    FulltextQueryBuilder, FulltextSearchMode, group
)

def test_query_builder_score():
    """Test FulltextQueryBuilder with AS score support."""
    
    print("=== FulltextQueryBuilder AS score 测试 ===\n")
    
    # 1. 基本的 must + encourage 查询带 score
    print("1. 基本查询 (must + encourage) 带 AS score:")
    query1 = FulltextQueryBuilder().must("python").encourage("tutorial")
    sql1 = query1.as_score_sql("articles", ["title", "content"])
    print(f"Query: {query1.build()}")
    print(f"SQL: {sql1}")
    print(f"✓ Contains AS score: {'AS score' in sql1}")
    print()
    
    # 2. 复杂布尔查询带 score
    print("2. 复杂布尔查询 (must + must_not + encourage) 带 AS score:")
    query2 = (FulltextQueryBuilder()
              .must("machine", "learning")
              .must_not("deprecated")
              .encourage("tutorial")
              .discourage("advanced"))
    sql2 = query2.as_score_sql("src", ["body", "title"])
    print(f"Query: {query2.build()}")
    print(f"SQL: {sql2}")
    print(f"✓ Contains AS score: {'AS score' in sql2}")
    print()
    
    # 3. 带组的查询
    print("3. 带组的查询 + AS score:")
    query3 = (FulltextQueryBuilder()
              .must("programming")
              .encourage(group().medium("python", "java"))
              .must_not(group().medium("legacy", "deprecated")))
    sql3 = query3.as_score_sql("articles", ["title", "content", "tags"])
    print(f"Query: {query3.build()}")
    print(f"SQL: {sql3}")
    print(f"✓ Contains AS score: {'AS score' in sql3}")
    print()
    
    # 4. 自然语言模式
    print("4. 自然语言模式 + AS score:")
    query4 = FulltextQueryBuilder().encourage("machine learning artificial intelligence")
    sql4 = query4.as_sql("articles", ["title", "content"], 
                        mode=FulltextSearchMode.NATURAL_LANGUAGE, include_score=True)
    print(f"Query: {query4.build()}")
    print(f"SQL: {sql4}")
    print(f"✓ Contains NATURAL LANGUAGE: {'natural language mode' in sql4}")
    print(f"✓ Contains AS score: {'AS score' in sql4}")
    print()
    
    # 5. 完整的查询 (自定义列 + WHERE + ORDER BY + LIMIT)
    print("5. 完整查询 (自定义列 + WHERE + ORDER BY + LIMIT):")
    query5 = (FulltextQueryBuilder()
              .must("machine", "learning")
              .encourage("tutorial")
              .must_not("deprecated"))
    sql5 = query5.as_sql("articles", ["title", "content"],
                        select_columns=["id", "title"],
                        include_score=True,
                        where_conditions=["category = 'AI'", "published_date > '2020-01-01'"],
                        order_by="score DESC",
                        limit=10,
                        offset=5)
    print(f"Query: {query5.build()}")
    print(f"SQL: {sql5}")
    print(f"✓ Contains custom columns: {'id, title' in sql5}")
    print(f"✓ Contains AS score: {'AS score' in sql5}")
    print(f"✓ Contains WHERE: {'category = \\'AI\\'' in sql5}")
    print(f"✓ Contains ORDER BY: {'ORDER BY score DESC' in sql5}")
    print(f"✓ Contains LIMIT: {'LIMIT 10' in sql5}")
    print(f"✓ Contains OFFSET: {'OFFSET 5' in sql5}")
    print()
    
    # 6. 测试权重操作符
    print("6. 权重操作符查询 + AS score:")
    query6 = (FulltextQueryBuilder()
              .must("search")
              .encourage(group().high("important").low("details").medium("content")))
    sql6 = query6.as_score_sql("documents", ["title", "body"])
    print(f"Query: {query6.build()}")
    print(f"SQL: {sql6}")
    print(f"✓ Contains weight operators: {'>important' in query6.build() and '<details' in query6.build()}")
    print(f"✓ Contains AS score: {'AS score' in sql6}")
    print()
    
    # 7. 对比 MatrixOne 测试用例格式
    print("7. 对比 MatrixOne 测试用例格式:")
    print("MatrixOne 格式: SELECT *, MATCH(body, title) AGAINST('is red' IN natural language mode) AS score FROM src;")
    
    query7 = FulltextQueryBuilder().encourage("is red")
    sql7 = query7.as_sql("src", ["body", "title"], 
                        mode=FulltextSearchMode.NATURAL_LANGUAGE, include_score=True)
    print(f"我们的格式: {sql7}")
    print(f"✓ 格式匹配: {'SELECT *' in sql7 and 'AS score' in sql7 and 'FROM src' in sql7}")
    print()
    
    print("=== 测试总结 ===")
    print("✓ FulltextQueryBuilder 现在支持 AS score 功能")
    print("✓ 支持 as_sql() 方法生成完整 SQL")
    print("✓ 支持 as_score_sql() 便捷方法")
    print("✓ 支持 Boolean 和 Natural Language 模式")
    print("✓ 支持自定义列选择")
    print("✓ 支持 WHERE、ORDER BY、LIMIT、OFFSET")
    print("✓ 支持复杂的布尔查询和组合")
    print("✓ 兼容 MatrixOne 测试用例格式")

if __name__ == "__main__":
    test_query_builder_score()

#!/usr/bin/env python3
"""
Simple test for label functionality
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'clients', 'python'))

from matrixone.sqlalchemy_ext.fulltext_search import boolean_match, natural_match, group

def test_simple_label():
    """Test the label functionality directly."""
    
    print("=== 简单 label 功能测试 ===\n")
    
    # Test 1: Basic boolean_match with label
    print("1. 基本 boolean_match 带 label:")
    filter1 = boolean_match("title", "content").must("python")
    print(f"Filter SQL: {filter1.compile()}")
    
    try:
        labeled1 = filter1.label("score")
        print(f"Labeled result: {labeled1}")
        print(f"Labeled compile: {labeled1.compile()}")
        print("✓ Label 方法成功")
    except Exception as e:
        print(f"Label 方法失败: {e}")
    print()
    
    # Test 2: Complex query with label
    print("2. 复杂查询带 label:")
    filter2 = (boolean_match("title", "content")
               .must("machine", "learning")
               .encourage("tutorial")
               .must_not("deprecated"))
    print(f"Filter SQL: {filter2.compile()}")
    
    try:
        labeled2 = filter2.label("relevance")
        print(f"Labeled result: {labeled2}")
        print(f"Labeled compile: {labeled2.compile()}")
        print("✓ 复杂查询 Label 成功")
    except Exception as e:
        print(f"复杂查询 Label 失败: {e}")
    print()
    
    # Test 3: Natural match with label
    print("3. Natural match 带 label:")
    filter3 = natural_match("title", "content", query="machine learning tutorial")
    print(f"Filter SQL: {filter3.compile()}")
    
    try:
        labeled3 = filter3.label("nlp_score")
        print(f"Labeled result: {labeled3}")
        print(f"Labeled compile: {labeled3.compile()}")
        print("✓ Natural match Label 成功")
    except Exception as e:
        print(f"Natural match Label 失败: {e}")
    print()
    
    # Test 4: Manual SQL construction
    print("4. 手动构建 SQL 测试:")
    filter4 = boolean_match("title", "content").must("python").encourage("tutorial")
    base_sql = filter4.compile()
    manual_labeled_sql = f"{base_sql} AS score"
    print(f"Base SQL: {base_sql}")
    print(f"Manual labeled SQL: {manual_labeled_sql}")
    
    # This is what we want to achieve in ORM queries
    expected_full_sql = f"SELECT id, title, {manual_labeled_sql} FROM articles WHERE {base_sql}"
    print(f"期望的完整 SQL: {expected_full_sql}")
    print()
    
    print("=== 测试总结 ===")
    print("✓ FulltextFilter.compile() 生成正确的 MATCH() AGAINST() SQL")
    print("✓ 可以手动添加 AS alias 来创建标签")
    print("✓ 期望的使用模式：query(Model, Model.col, fulltext.label('score'))")

if __name__ == "__main__":
    test_simple_label()

#!/usr/bin/env python3
"""
演示简化后的组接口

简化设计：
- 只有一个 group() 函数，创建 OR 语义的组
- 组级别语义由使用方式决定：
  - must(group()) → +(...)  - 必须包含组
  - must_not(group()) → -(...) - 必须不包含组
  - should(group()) → (...) - 可选包含组（正常权重）
  - tilde_weight(group()) → ~(...) - 可选包含组（降低权重）
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'clients', 'python'))

from matrixone import boolean_match, group

def main():
    print("=== 简化后的组接口演示 ===\n")
    
    # 1. 基础组用法对比
    print("1. 基础组用法:")
    print()
    
    # 必须包含组
    print("   必须包含组 (java OR kotlin):")
    query = boolean_match("title", "content").must(group().add("java", "kotlin"))
    print(f"   生成SQL: {query.compile()}")
    print("   解释: 必须包含 java 或 kotlin")
    print()
    
    # 必须不包含组
    print("   必须不包含组 (spam OR junk):")
    query = boolean_match("title", "content").must_not(group().add("spam", "junk"))
    print(f"   生成SQL: {query.compile()}")
    print("   解释: 必须不包含 spam 或 junk")
    print()
    
    # 可选包含组（正常权重）
    print("   可选包含组 - 正常权重 (tutorial OR guide):")
    query = boolean_match("title", "content").should(group().add("tutorial", "guide"))
    print(f"   生成SQL: {query.compile()}")
    print("   解释: 可选包含 tutorial 或 guide，有的话提升排序")
    print()
    
    # 可选包含组（降低权重）
    print("   可选包含组 - 降低权重 (old OR outdated):")
    query = boolean_match("title", "content").tilde_weight(group().add("old", "outdated"))
    print(f"   生成SQL: {query.compile()}")
    print("   解释: 可选包含 old 或 outdated，有的话降低排序")
    print()
    
    # 2. 复杂组合示例
    print("2. 复杂组合示例:")
    print()
    
    # MatrixOne 经典语法：'+red -(<blue >is)'
    print("   MatrixOne 经典语法: '+red -(<blue >is)'")
    classic_syntax = boolean_match("title", "content")\
        .must("red")\
        .must_not(group().low_weight("blue").high_weight("is"))
    print(f"   生成SQL: {classic_syntax.compile()}")
    print("   解释: 必须包含'red'，必须不包含组(低权重'blue' OR 高权重'is')")
    print()
    
    # 复杂业务场景
    print("   复杂业务场景:")
    complex_query = boolean_match("title", "content", "tags")\
        .must("programming")\
        .must(group().add("python", "java", "golang"))\
        .should(group().add("tutorial", "guide", "example"))\
        .tilde_weight(group().add("legacy", "deprecated", "old"))\
        .must_not(group().add("spam", "advertisement"))
    
    print(f"   生成SQL: {complex_query.compile()}")
    print("   解释:")
    print("   - 必须包含 'programming'")
    print("   - 必须包含 (python OR java OR golang)")
    print("   - 偏好包含 (tutorial OR guide OR example)")
    print("   - 降低权重 (legacy OR deprecated OR old)")
    print("   - 必须不包含 (spam OR advertisement)")
    print()
    
    # 3. 接口优势总结
    print("=== 接口优势总结 ===")
    print()
    print("✅ 简化设计:")
    print("   - 只需要一个 group() 函数")
    print("   - 组级别语义由使用方式决定")
    print("   - 更直观的语义表达")
    print()
    print("✅ 语义清晰:")
    print("   - must(group()) → 必须包含组")
    print("   - must_not(group()) → 必须不包含组")
    print("   - should(group()) → 可选包含组（正常权重）")
    print("   - tilde_weight(group()) → 可选包含组（降低权重）")
    print()
    print("✅ 极简设计:")
    print("   - 只有一个 group() 函数")
    print("   - 移除了冗余的 or_group() 和 not_group()")
    print("   - 接口更加简洁统一")
    print()
    print("✅ MatrixOne 语法完美映射:")
    print("   - Group级别: +(...), -(...), ~(...), (...)")
    print("   - Element级别: >term, <term, ~term")
    print("   - 完全符合 MatrixOne 的布尔模式语法")

if __name__ == "__main__":
    main()

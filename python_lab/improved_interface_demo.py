#!/usr/bin/env python3
"""
演示改进后的全文搜索接口

改进要点：
1. tilde_weight → should_not (更直观的语义)
2. add → medium (明确表示中等权重)
3. low_weight → low (简化命名)
4. high_weight → high (简化命名)
5. 新增 discourage 方法 (组内元素级别)
6. 所有方法都支持多个字符串参数
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'clients', 'python'))

from matrixone import boolean_match, group

def main():
    print("=== 改进后的全文搜索接口演示 ===\n")
    
    # 1. 新的语义化方法名
    print("1. 新的语义化方法名:")
    print()
    
    print("   Group级别方法 (应用于整个组):")
    print("   - must(): 必须包含")
    print("   - must_not(): 必须不包含") 
    print("   - should(): 应该包含 (正常权重)")
    print("   - should_not(): 应该不包含 (降低权重)")
    print()
    
    print("   Element级别方法 (组内元素权重):")
    print("   - medium(): 中等权重 (无操作符)")
    print("   - high(): 高权重 (>)")
    print("   - low(): 低权重 (<)")
    print()
    
    # 2. 基础用法演示
    print("2. 基础用法演示:")
    print()
    
    # Group级别操作符
    print("   Group级别操作符:")
    
    query1 = boolean_match("title", "content").must("python")
    print(f"   must('python'): {query1.compile()}")
    
    query2 = boolean_match("title", "content").should("tutorial")
    print(f"   should('tutorial'): {query2.compile()}")
    
    query3 = boolean_match("title", "content").should_not("legacy")
    print(f"   should_not('legacy'): {query3.compile()}")
    
    query4 = boolean_match("title", "content").must_not("spam")
    print(f"   must_not('spam'): {query4.compile()}")
    print()
    
    # 3. 多参数支持
    print("3. 多参数支持:")
    print()
    
    query5 = boolean_match("title", "content").should_not("legacy", "deprecated", "old")
    print(f"   should_not('legacy', 'deprecated', 'old'): {query5.compile()}")
    print("   解释: 同时抑制多个词的权重")
    print()
    
    # 4. 组内元素权重控制
    print("4. 组内元素权重控制:")
    print()
    
    print("   Element级别权重 (组内使用):")
    
    # 中等权重
    query6 = boolean_match("title", "content").must(
        group().medium("java", "kotlin", "scala")
    )
    print(f"   medium('java', 'kotlin', 'scala'): {query6.compile()}")
    print("   解释: 必须包含 java 或 kotlin 或 scala (中等权重)")
    print()
    
    # 高权重
    query7 = boolean_match("title", "content").must(
        group().high("important", "critical")
    )
    print(f"   high('important', 'critical'): {query7.compile()}")
    print("   解释: 必须包含 important 或 critical (高权重)")
    print()
    
    # 低权重
    query8 = boolean_match("title", "content").must(
        group().low("minor", "optional")
    )
    print(f"   low('minor', 'optional'): {query8.compile()}")
    print("   解释: 必须包含 minor 或 optional (低权重)")
    print()
    
    # 抑制权重
    query9 = boolean_match("title", "content").must_not(
        group().discourage("spam", "junk", "advertisement")
    )
    print(f"   discourage('spam', 'junk', 'advertisement'): {query9.compile()}")
    print("   解释: 必须不包含组，组内词汇被抑制")
    print()
    
    # 5. 复杂组合示例
    print("5. 复杂组合示例:")
    print()
    
    # MatrixOne 经典语法
    print("   MatrixOne 经典语法: '+red -(<blue >is)'")
    classic = boolean_match("title", "content")\
        .must("red")\
        .must_not(group().low("blue").high("is"))
    print(f"   生成SQL: {classic.compile()}")
    print("   解释: 必须'red'，不能包含组(低权重'blue' OR 高权重'is')")
    print()
    
    # 现代业务场景
    print("   现代业务场景:")
    modern = boolean_match("title", "content", "tags")\
        .must("programming")\
        .must(group().medium("python", "java", "golang"))\
        .should(group().high("tutorial", "guide").medium("example"))\
        .should_not(group().discourage("legacy", "deprecated").medium("old"))\
        .must_not(group().medium("spam", "advertisement"))
    
    print(f"   生成SQL: {modern.compile()}")
    print("   解释:")
    print("   - 必须包含 'programming'")
    print("   - 必须包含 (python OR java OR golang)")
    print("   - 偏好包含 (高权重tutorial/guide OR 中权重example)")
    print("   - 抑制包含 (抑制legacy/deprecated OR 中权重old)")
    print("   - 必须不包含 (spam OR advertisement)")
    print()
    
    # 6. 接口优势总结
    print("=== 接口优势总结 ===")
    print()
    print("✅ 语义化命名:")
    print("   - should_not() 比 tilde_weight() 更直观")
    print("   - high/low/medium 比 high_weight/low_weight/add 更简洁")
    print("   - discourage() 明确表达抑制语义")
    print()
    print("✅ 多参数支持:")
    print("   - 所有方法都支持多个字符串参数")
    print("   - 减少重复调用，代码更简洁")
    print("   - 批量操作更高效")
    print()
    print("✅ 权重层次清晰:")
    print("   - Group级别: must, must_not, should, should_not")
    print("   - Element级别: high, medium, low, discourage")
    print("   - 两个层次职责分明")
    print()
    print("✅ 向后兼容:")
    print("   - 保留所有旧方法作为别名")
    print("   - 现有代码无需修改")
    print("   - 渐进式升级")
    print()
    print("✅ MatrixOne 语法完美映射:")
    print("   - Group级别: +(...), -(...), ~(...), (...)")
    print("   - Element级别: >term, <term, ~term, term")
    print("   - 完全符合 MatrixOne 的布尔模式语法")

if __name__ == "__main__":
    main()

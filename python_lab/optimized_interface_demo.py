#!/usr/bin/env python3
"""
演示优化后的全文搜索接口

新接口逻辑：
- Group级别操作符: must(+), must_not(-), should(no prefix), tilde_weight(~)
- Element级别操作符: high_weight(>), low_weight(<) 
- 参数可以是 str (element) 或 FulltextGroup (group)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'clients', 'python'))

from matrixone import Client, boolean_match, group
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer, String, Text

Base = declarative_base()

class Article(Base):
    __tablename__ = "optimized_test_articles"
    
    id = Column(Integer, primary_key=True)
    title = Column(String(200))
    content = Column(Text)
    tags = Column(String(500))
    category = Column(String(100))

def main():
    # 连接数据库
    client = Client(
        host="127.0.0.1",
        port=6001,
        user="root",
        password="111",
        database="test"
    )
    
    try:
        # 创建表和数据
        print("创建测试表和数据...")
        client.execute("DROP TABLE IF EXISTS optimized_test_articles")
        client.execute("""
            CREATE TABLE optimized_test_articles (
                id INT PRIMARY KEY AUTO_INCREMENT,
                title VARCHAR(200),
                content TEXT,
                tags VARCHAR(500),
                category VARCHAR(100)
            )
        """)
        
        client.execute("CREATE FULLTEXT INDEX ft_idx ON optimized_test_articles (title, content, tags)")
        
        test_data = [
            ("Python Machine Learning Tutorial", "Learn machine learning with Python programming", "python,ml,tutorial", "Programming"),
            ("Java Spring Framework Guide", "Complete guide to Java Spring development", "java,spring,guide", "Programming"),
            ("Python Legacy Code Examples", "Examples of Python 2.7 legacy code", "python,legacy,deprecated", "Programming"),
            ("Modern Python Best Practices", "Best practices for Python 3.11", "python,modern,best-practices", "Programming"),
            ("Outdated JavaScript Techniques", "Old JavaScript techniques from 2010", "javascript,old,outdated", "Programming")
        ]
        
        for i, data in enumerate(test_data, 1):
            try:
                client.execute(f"""
                    INSERT INTO optimized_test_articles (id, title, content, tags, category) 
                    VALUES ({i}, '{data[0]}', '{data[1]}', '{data[2]}', '{data[3]}')
                """)
            except Exception as e:
                if "Duplicate entry" not in str(e):
                    print(f"插入数据失败: {e}")
        
        print("✓ 数据准备完成")
        print("\n=== 优化后的全文搜索接口演示 ===\n")
        
        # 1. 基础用法 - 字符串参数
        print("1. 基础用法 - 字符串参数:")
        
        print("   must('python') - 必须包含:")
        filter_obj = boolean_match("title", "content", "tags").must("python")
        print(f"   SQL: {filter_obj.compile()}")
        
        print("   should('tutorial') - 可选包含 (正常权重):")
        filter_obj = boolean_match("title", "content", "tags").should("tutorial")
        print(f"   SQL: {filter_obj.compile()}")
        
        print("   tilde_weight('legacy') - 可选包含 (降低权重):")
        filter_obj = boolean_match("title", "content", "tags").tilde_weight("legacy")
        print(f"   SQL: {filter_obj.compile()}")
        
        print("   must_not('deprecated') - 必须不包含:")
        filter_obj = boolean_match("title", "content", "tags").must_not("deprecated")
        print(f"   SQL: {filter_obj.compile()}")
        print()
        
        # 2. 高级用法 - Group 对象参数
        print("2. 高级用法 - Group 对象参数:")
        
        print("   must(group(...)) - 必须包含组:")
        filter_obj = boolean_match("title", "content", "tags").must(
            group().add("java", "kotlin")
        )
        print(f"   SQL: {filter_obj.compile()}")
        print("   解释: 必须包含 java 或 kotlin")
        
        print("   must_not(group(...)) - 必须不包含组:")
        filter_obj = boolean_match("title", "content", "tags").must_not(
            group().add("spam", "junk")
        )
        print(f"   SQL: {filter_obj.compile()}")
        print("   解释: 不能包含 spam 或 junk")
        
        print("   should(group(...)) - 可选包含组 (正常权重):")
        filter_obj = boolean_match("title", "content", "tags").should(
            group().add("tutorial", "guide")
        )
        print(f"   SQL: {filter_obj.compile()}")
        print("   解释: 可选包含 tutorial 或 guide，有的话加分")
        
        print("   tilde_weight(group(...)) - 可选包含组 (降低权重):")
        filter_obj = boolean_match("title", "content", "tags").tilde_weight(
            group().add("old", "outdated")
        )
        print(f"   SQL: {filter_obj.compile()}")
        print("   解释: 可选包含 old 或 outdated，有的话减分")
        print()
        
        # 3. Element 级别权重操作符
        print("3. Element 级别权重操作符 (在 group 内部使用):")
        
        print("   组内使用 high_weight(>):")
        filter_obj = boolean_match("title", "content", "tags").must_not(
            group().low_weight("blue").high_weight("is")
        )
        print(f"   SQL: {filter_obj.compile()}")
        print("   解释: 类似 MatrixOne 的 '+red -(<blue >is)' 语法")
        print()
        
        # 4. 复杂组合示例
        print("4. 复杂组合示例:")
        
        print("   多操作符组合:")
        filter_obj = boolean_match("title", "content", "tags")\
            .must("python")\
            .should("tutorial")\
            .tilde_weight("legacy")\
            .must_not(group().add("spam", "junk"))
        
        print(f"   SQL: {filter_obj.compile()}")
        print("   解释:")
        print("   - 必须包含 'python'")
        print("   - 可选 'tutorial' (加分)")
        print("   - 可选 'legacy' (减分)")
        print("   - 不能包含 'spam' 或 'junk'")
        print()
        
        # 5. 实际查询测试
        print("5. 实际查询测试:")
        
        print("   查询1: 必须Python + 偏好教程 + 降低遗留代码权重")
        results = client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .must("python")
            .should("tutorial")
            .tilde_weight("legacy")
        ).all()
        
        for i, article in enumerate(results, 1):
            print(f"   {i}. {article.title}")
        print()
        
        print("   查询2: 必须编程 + 必须包含(Java或Python) + 避免过时内容")
        results = client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .must("programming")
            .must(group().add("java", "python"))
            .tilde_weight(group().add("old", "outdated", "legacy"))
        ).all()
        
        for i, article in enumerate(results, 1):
            print(f"   {i}. {article.title}")
        print()
        
        # 6. 接口优势总结
        print("=== 接口优势总结 ===")
        print()
        print("✅ 简化的API:")
        print("   - 只有4个核心方法: must, must_not, should, tilde_weight")
        print("   - 统一的参数类型: str 或 FulltextGroup")
        print("   - 不再需要 add_groups 方法")
        print()
        print("✅ 语义清晰:")
        print("   - Group级别: +, -, ~, (无前缀)")
        print("   - Element级别: >, < (在组内部)")
        print("   - 符合 MatrixOne 的语法逻辑")
        print()
        print("✅ 灵活性:")
        print("   - 字符串参数 → 简单元素")
        print("   - Group对象参数 → 复杂组合")
        print("   - 可以随意组合各种操作符")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 清理
        try:
            client.execute("DROP TABLE IF EXISTS optimized_test_articles")
            print("\n✓ 清理完成")
        except:
            pass

if __name__ == "__main__":
    main()

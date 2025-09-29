#!/usr/bin/env python3
"""
演示 MatrixOne 全文搜索的权重操作符

展示 group-level (+ -) 和 element-level (> < ~) 操作符的用法
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'clients', 'python'))

from matrixone import Client, boolean_match, group
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer, String, Text, create_engine

Base = declarative_base()

class Article(Base):
    __tablename__ = "weight_test_articles"
    
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
        
        # 创建表
        print("创建测试表...")
        client.execute("DROP TABLE IF EXISTS weight_test_articles")
        client.execute("""
            CREATE TABLE weight_test_articles (
                id INT PRIMARY KEY AUTO_INCREMENT,
                title VARCHAR(200),
                content TEXT,
                tags VARCHAR(500),
                category VARCHAR(100)
            )
        """)
        
        # 创建全文索引
        print("创建全文索引...")
        client.execute("CREATE FULLTEXT INDEX ft_weight_idx ON weight_test_articles (title, content, tags)")
        
        # 插入测试数据
        print("插入测试数据...")
        test_data = [
            (1, "Python Machine Learning", "This is a comprehensive guide to machine learning with Python programming", "python,machine,learning,ai", "Programming"),
            (2, "Advanced Python Programming", "Learn advanced Python concepts and machine learning algorithms", "python,advanced,programming,algorithms", "Programming"), 
            (3, "Java Programming Basics", "Introduction to Java programming language fundamentals", "java,programming,basics,fundamentals", "Programming"),
            (4, "Machine Learning with R", "Statistical machine learning using R programming language", "r,machine,learning,statistics", "Data Science"),
            (5, "Deep Learning Neural Networks", "Understanding deep learning and neural network architectures", "deep,learning,neural,networks,ai", "AI"),
            (6, "Blue Ocean Strategy", "Business strategy for creating uncontested market space", "business,strategy,blue,ocean", "Business"),
            (7, "Red Hat Enterprise Linux", "System administration with Red Hat Linux distributions", "redhat,linux,system,administration", "System"),
            (8, "Blue is the Warmest Color", "A coming-of-age story about love and relationships", "blue,love,relationships,story", "Literature")
        ]
        
        for data in test_data:
            try:
                client.execute(f"""
                    INSERT INTO weight_test_articles (id, title, content, tags, category) 
                    VALUES ({data[0]}, '{data[1]}', '{data[2]}', '{data[3]}', '{data[4]}')
                """)
            except Exception as e:
                if "Duplicate entry" not in str(e):
                    print(f"插入数据失败: {e}")
        
        print("✓ 数据准备完成")
        print("\n=== MatrixOne 权重操作符演示 ===\n")
        
        # 1. 基础权重操作符演示
        print("1. Element-level 权重操作符:")
        print("   高权重 (>term) - 提高 'machine' 的相关性")
        try:
            query = client.query(Article).filter(
                boolean_match("title", "content", "tags")
                .should("python")
                .high_weight("machine")  # >machine
            )
            sql, _ = query._build_sql()
            print(f"   生成的SQL: {sql}")
            
            results = query.all()
            for article in results:
                print(f"   - {article.title}")
        except Exception as e:
            print(f"   高权重搜索失败: {e}")
        print()
        
        print("2. 低权重 (<term) - 降低 'basic' 的相关性")
        try:
            query = client.query(Article).filter(
                boolean_match("title", "content", "tags")
                .should("programming")
                .low_weight("basic")  # <basic
            )
            sql, _ = query._build_sql()
            print(f"   生成的SQL: {sql}")
            
            results = query.all()
            for article in results:
                print(f"   - {article.title}")
        except Exception as e:
            print(f"   低权重搜索失败: {e}")
        print()
        
        print("3. 波浪号权重 (~term) - 降低 'java' 的相关性")
        try:
            query = client.query(Article).filter(
                boolean_match("title", "content", "tags")
                .should("programming")
                .tilde_weight("java")  # ~java
            )
            sql, _ = query._build_sql()
            print(f"   生成的SQL: {sql}")
            
            results = query.all()
            for article in results:
                print(f"   - {article.title}")
        except Exception as e:
            print(f"   波浪号权重搜索失败: {e}")
        print()
        
        # 4. 组合权重操作符
        print("4. 组合权重操作符:")
        print("   '+python >machine <basic' - 必须有python，machine高权重，basic低权重")
        try:
            query = client.query(Article).filter(
                boolean_match("title", "content", "tags")
                .must("python")
                .high_weight("machine")
                .low_weight("basic")
            )
            sql, _ = query._build_sql()
            print(f"   生成的SQL: {sql}")
            
            results = query.all()
            for article in results:
                print(f"   - {article.title}")
        except Exception as e:
            print(f"   组合权重搜索失败: {e}")
        print()
        
        # 5. 尝试复制 MatrixOne 测试用例的语法
        print("5. 尝试 MatrixOne 测试用例语法:")
        print("   模拟 '+red -(<blue >is)' 的构造")
        try:
            # 创建一个包含权重元素的 not_group
            # 注意：这个可能因为复杂嵌套而失败
            query = client.query(Article).filter(
                boolean_match("title", "content", "tags")
                .must("machine")  # +machine (相当于 +red)
                .add_groups(
                    not_group()  # -(group)
                    .low_weight("programming")   # <programming (相当于 <blue)
                    .high_weight("basic")        # >basic (相当于 >is)
                )
            )
            sql, _ = query._build_sql()
            print(f"   生成的SQL: {sql}")
            print(f"   期望的SQL: MATCH(...) AGAINST('+machine -(<programming >basic)' IN BOOLEAN MODE)")
            
            results = query.all()
            for article in results:
                print(f"   - {article.title}")
        except Exception as e:
            print(f"   复杂嵌套失败 (预期): {e}")
        print()
        
        # 6. 简化版本的权重组合
        print("6. 简化的权重组合 (推荐用法):")
        print("   '+learning >deep ~basic' - 简单的权重组合")
        try:
            query = client.query(Article).filter(
                boolean_match("title", "content", "tags")
                .must("learning")
                .high_weight("deep")
                .tilde_weight("basic")
            )
            sql, _ = query._build_sql()
            print(f"   生成的SQL: {sql}")
            
            results = query.all()
            for article in results:
                print(f"   - {article.title}")
        except Exception as e:
            print(f"   简化权重组合失败: {e}")
        print()
        
        print("=== 总结 ===")
        print("✅ 支持的权重操作符:")
        print("  - high_weight(term) -> >term (提高权重)")
        print("  - low_weight(term) -> <term (降低权重)")  
        print("  - tilde_weight(term) -> ~term (降低权重)")
        print()
        print("⚠️  MatrixOne 限制:")
        print("  - 复杂嵌套如 '+red -(<blue >is)' 可能不被支持")
        print("  - 推荐使用简单的权重组合")
        print("  - Group-level 的 +/- 操作符主要用于组的包含/排除")
        
    except Exception as e:
        print(f"错误: {e}")
    finally:
        if 'client' in locals():
            # 清理
            try:
                client.execute("DROP TABLE IF EXISTS weight_test_articles")
                print("\n✓ 清理完成")
            except:
                pass

if __name__ == "__main__":
    main()

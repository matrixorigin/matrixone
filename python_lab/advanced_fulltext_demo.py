#!/usr/bin/env python3
"""
演示新的高级 fulltext 过滤器构建器
"""

import os
import sys

# 添加项目根目录到 Python 路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(project_root, "clients", "python"))

from matrixone import Client
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer, String, Text, and_, or_
from matrixone.sqlalchemy_ext import (
    boolean_match,
    natural_match,
    fulltext_and,
    fulltext_or,
    or_group,
    not_group
)

# 创建 ORM 模型
Base = declarative_base()

class Article(Base):
    __tablename__ = "articles"
    
    id = Column(Integer, primary_key=True)
    title = Column(String(200))
    content = Column(Text)
    author = Column(String(100))
    category = Column(String(50))
    tags = Column(String(200))

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
        client.create_all(Base)
        
        # 插入测试数据
        test_articles = [
            {
                "title": "Introduction to Machine Learning",
                "content": "Machine learning is a subset of artificial intelligence that focuses on algorithms and statistical models. It enables computers to learn and improve from experience without being explicitly programmed.",
                "author": "John Doe",
                "category": "AI",
                "tags": "machine learning, AI, algorithms"
            },
            {
                "title": "Deep Learning with Neural Networks",
                "content": "Deep learning uses neural networks with multiple layers to learn complex patterns in data. It has revolutionized computer vision, natural language processing, and speech recognition.",
                "author": "Jane Smith",
                "category": "AI",
                "tags": "deep learning, neural networks, computer vision"
            },
            {
                "title": "Python Programming Basics",
                "content": "Python is a versatile programming language used for web development, data analysis, automation, and scientific computing. It's known for its simplicity and readability.",
                "author": "Bob Johnson",
                "category": "Programming",
                "tags": "python, programming, web development"
            },
            {
                "title": "Database Design Principles",
                "content": "Good database design involves normalization, indexing, and proper relationship modeling. It's crucial for performance and data integrity.",
                "author": "Alice Brown",
                "category": "Database",
                "tags": "database, design, normalization"
            },
            {
                "title": "Advanced Machine Learning Techniques",
                "content": "Advanced techniques include ensemble methods, support vector machines, and deep learning architectures. These methods can handle complex patterns and large datasets.",
                "author": "John Doe",
                "category": "AI",
                "tags": "advanced ML, ensemble methods, SVM"
            }
        ]
        
        for i, article_data in enumerate(test_articles):
            # 转义单引号
            title = article_data["title"].replace("'", "''")
            content = article_data["content"].replace("'", "''")
            author = article_data["author"].replace("'", "''")
            category = article_data["category"].replace("'", "''")
            tags = article_data["tags"].replace("'", "''")
            
            try:
                client.execute(f"""
                    INSERT INTO articles (id, title, content, author, category, tags) 
                    VALUES ({i+1}, '{title}', '{content}', '{author}', '{category}', '{tags}')
                """)
            except Exception as e:
                if "Duplicate entry" in str(e):
                    print(f"  记录 {i+1} 已存在，跳过插入")
                else:
                    raise e
        
        # 设置全文搜索参数
        client.execute("SET experimental_fulltext_index = 1")
        client.execute("SET ft_relevancy_algorithm = 'TF-IDF'")
        
        # 创建全文索引
        try:
            client.execute("""
                CREATE FULLTEXT INDEX ft_articles ON articles(title, content, tags)
            """)
            print("✓ 全文索引创建成功")
        except Exception as e:
            print(f"创建全文索引失败: {e}")
            print("继续演示 SQL 生成功能...")
        
        print("=== 高级 Fulltext 过滤器演示 ===\n")
        
        # 1. 基础布尔模式搜索
        print("1. 基础布尔模式搜索 (必须包含 'machine learning'):")
        print("   重要：MATCH字段必须与索引字段完全匹配！")
        try:
            results = client.query(Article).filter(
                boolean_match("title", "content", "tags").must("machine").must("learning")
            ).all()
            
            for article in results:
                print(f"  - {article.title} by {article.author}")
        except Exception as e:
            print(f"  查询失败: {e}")
        print()
        
        # 2. 复杂布尔查询
        print("2. 复杂布尔查询 (必须包含 'python' 且不包含 'java'):")
        try:
            results = client.query(Article).filter(
                boolean_match("title", "content", "tags")
                .must("python")
                .must_not("java")
                .should("programming")
            ).all()
            
            for article in results:
                print(f"  - {article.title} by {article.author}")
        except Exception as e:
            print(f"  查询失败: {e}")
        print()
        
        # 3. 短语搜索
        print("3. 短语搜索 (必须包含 'machine learning' 短语):")
        try:
            results = client.query(Article).filter(
                boolean_match("title", "content", "tags")
                .must_phrase("machine learning")
            ).all()
            
            for article in results:
                print(f"  - {article.title} by {article.author}")
        except Exception as e:
            print(f"  查询失败: {e}")
        print()
        
        # 4. 前缀搜索
        print("4. 前缀搜索 (包含 'neural*' 前缀):")
        print("   SQL: MATCH(title, content, tags) AGAINST('neural*' IN BOOLEAN MODE)")
        print()
        
        # 5. 权重搜索
        print("5. 权重搜索 (提升 'deep' 的权重):")
        results = client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .should("learning")
            .boost("deep", ">")
        ).all()
        
        for article in results:
            print(f"  - {article.title} by {article.author}")
        print()
        
        # 6. 组合条件 (使用兼容性 fulltext_and)
        print("6. 组合条件 (全文搜索 + 其他条件):")
        try:
            results = client.query(Article).filter(
                fulltext_and(
                    boolean_match("title", "content", "tags").must("learning"),
                    Article.author == "John Doe"
                )
            ).all()
            
            for article in results:
                print(f"  - {article.title} by {article.author}")
        except Exception as e:
            print(f"  fulltext_and() 组合失败: {e}")
        
        # 6b. 复杂嵌套表达式 - MatrixOne 限制测试
        print("\n6b. 复杂嵌套表达式 fulltext_or(fulltext_and(fulltext, condition)):")
        print("    注意：MatrixOne 限制 - 不支持同一查询中的多个 MATCH() 函数")
        try:
            results = client.query(Article).filter(
                fulltext_or(
                    fulltext_and(
                        boolean_match("title", "content", "tags").must("machine"),
                        Article.category == "AI"
                    ),
                    fulltext_and(
                        boolean_match("title", "content", "tags").must("python"),
                        Article.category == "Programming"
                    )
                )
            ).all()
            
            for article in results:
                print(f"  - {article.title} ({article.category})")
        except Exception as e:
            print(f"  复杂嵌套表达式失败 (预期): {e}")
        
        # 6c. 原生 SQLAlchemy and_() 测试
        print("\n6c. 原生 SQLAlchemy and_() 测试 (应该失败):")
        try:
            results = client.query(Article).filter(
                and_(
                    boolean_match("title", "content", "tags").must("learning"),
                    Article.author == "John Doe"
                )
            ).all()
            
            for article in results:
                print(f"  - {article.title} by {article.author}")
        except Exception as e:
            print(f"  原生 and_() 失败 (预期): {e}")
        
        # 6d. 链式 filter 调用 (备选方案)
        print("\n6c. 链式 filter 调用 (备选方案):")
        results = client.query(Article).filter(
            boolean_match("title", "content", "tags").must("learning")
        ).filter(
            Article.author == "John Doe"
        ).all()
        
        for article in results:
            print(f"  - {article.title} by {article.author}")
        print()
        
        # 7. 布尔操作符 (权重和组合)
        print("7. 布尔操作符 (权重和组合):")
        print("   注意：使用简单的必须/可选组合，避免复杂嵌套")
        try:
            # 使用简单的多词组合：必须包含 learning，应该包含 machine 或 deep
            results = client.query(Article).filter(
                boolean_match("title", "content", "tags")
                .must("learning")
                .should("machine")
                .should("deep")
            ).all()
            
            for article in results:
                print(f"  - {article.title} by {article.author}")
        except Exception as e:
            print(f"  布尔操作符失败: {e}")
        print()
        
        # 8. 自然语言模式
        print("8. 自然语言模式搜索:")
        results = client.query(Article).filter(
            natural_match("title", "content", "tags", query="artificial intelligence machine learning")
        ).all()
        
        for article in results:
            print(f"  - {article.title} by {article.author}")
        print()
        
        # 9. 查询扩展模式 (已移除)
        print("9. 查询扩展模式搜索:")
        print("    注意：查询扩展模式已被移除，因为 MatrixOne 支持有限")
        print()
        
        # 10. 链式调用
        print("10. 链式调用:")
        results = (client.query(Article)
                  .filter(boolean_match("title", "content", "tags").must("python"))
                  .filter(Article.category == "Programming")
                  .all())
        
        for article in results:
            print(f"  - {article.title} by {article.author}")
        print()
        
        # 11. 显示生成的 SQL
        print("11. 生成的 SQL 示例:")
        query = client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .must("python")
            .must_not("java")
            .should("programming")
        )
        sql, _ = query._build_sql()
        print(f"  SQL: {sql}")
        print()
        
        # 12. 复杂布尔查询 (避免复杂嵌套)
        print("12. 复杂布尔查询 (避免复杂嵌套):")
        print("    注意：使用简单的布尔操作符组合，避免 MatrixOne 不支持的复杂嵌套")
        try:
            results = client.query(Article).filter(
                boolean_match("title", "content", "tags")
                .must("learning")
                .should("machine")
                .should("deep")
                .must_not("basic")
            ).all()
            
            for article in results:
                print(f"  - {article.title} by {article.author}")
        except Exception as e:
            print(f"  复杂布尔查询失败: {e}")
        print()
        
        # 13. 测试所有 MatrixOne 支持的语法
        print("13. 测试所有 MatrixOne 支持的语法:")
        
        # +red blue (必须包含 red，可选包含 blue)
        print("  - +red blue:")
        results = client.query(Article).filter(
            boolean_match("title", "content", "tags").must("red").should("blue")
        ).all()
        for article in results:
            print(f"    {article.title}")
        
        # re* (前缀匹配)
        print("  - re* (前缀匹配):")
        results = client.query(Article).filter(
            boolean_match("title", "content", "tags").prefix("re")
        ).all()
        for article in results:
            print(f"    {article.title}")
        
        # +red -blue (必须包含 red，不能包含 blue)
        print("  - +red -blue:")
        results = client.query(Article).filter(
            boolean_match("title", "content", "tags").must("red").must_not("blue")
        ).all()
        for article in results:
            print(f"    {article.title}")
        
        # +red +blue (必须同时包含 red 和 blue)
        print("  - +red +blue:")
        results = client.query(Article).filter(
            boolean_match("title", "content", "tags").must("red", "blue")
        ).all()
        for article in results:
            print(f"    {article.title}")
        
        # +red ~blue (必须包含 red，blue 降低权重)
        print("  - +red ~blue:")
        results = client.query(Article).filter(
            boolean_match("title", "content", "tags").must("red").boost("blue", "~")
        ).all()
        for article in results:
            print(f"    {article.title}")
        
        # "is not red" (精确短语)
        print("  - \"is not red\" (精确短语):")
        results = client.query(Article).filter(
            boolean_match("title", "content", "tags").phrase("is not red")
        ).all()
        for article in results:
            print(f"    {article.title}")
        
        # 14. 更直观的组合方式
        print("14. 更直观的组合方式:")
        
        # 简单组合: +red -blue
        print("  - 简单组合: +red -blue")
        results = client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .must("red")
            .must_not("blue")
        ).all()
        for article in results:
            print(f"    {article.title}")
        
        # 15. 测试生成的 SQL
        print("15. 测试生成的 SQL:")
        
        # 16. 更直观的组合方式
        print("16. 更直观的组合方式:")
        
        # 简单组合: +red -blue
        print("  - 简单组合: +red -blue")
        results = client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .must("red")
            .must_not("blue")
        ).all()
        for article in results:
            print(f"    {article.title}")
        
        # 复杂组合: +red (blue OR term) - 正确语法
        print("  - 复杂组合: +red (blue OR term)")
        results = client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .must("red")
            .add_groups(or_group().add("blue", "term"))  # 正确：组内使用 add()
        ).all()
        for article in results:
            print(f"    {article.title}")
        
        # 复杂组合: +red -(blue AND term) - 正确语法
        print("  - 复杂组合: +red -(blue AND term)")
        results = client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .must("red")
            .add_groups(not_group().add("blue", "term"))  # 正确：组内使用 add()
        ).all()
        for article in results:
            print(f"    {article.title}")
        print()
        
        # 17. 测试生成的 SQL
        print("17. 测试生成的 SQL:")
        
        # 测试 OR 组合 - 正确语法
        print("  - OR 组合 +red (blue OR term) 生成的 SQL:")
        filter_obj = boolean_match("title", "content", "tags").must("red").add_groups(or_group().add("blue", "term"))
        sql = filter_obj.compile()
        print(f"    {sql}")
        
        # 测试否定组合 - 正确语法
        print("  - 否定组合 +red -(blue AND term) 生成的 SQL:")
        filter_obj = boolean_match("title", "content", "tags").must("red").add_groups(not_group().add("blue", "term"))
        sql = filter_obj.compile()
        print(f"    {sql}")
        
        # 测试复杂 OR 组合 - 正确语法
        print("  - 复杂 OR 组合生成的 SQL:")
        filter_obj = boolean_match("title", "content", "tags").must("red").add_groups(or_group().add("blue", "green", "yellow"))
        sql = filter_obj.compile()
        print(f"    {sql}")
        
        # 测试复杂嵌套组合 - 正确语法
        print("  - 复杂嵌套组合生成的 SQL:")
        filter_obj = boolean_match("title", "content", "tags").must("red").add_groups(
            or_group().add("blue", "green"),  # 不再使用 must/must_not
            or_group().add("yellow", "purple")
        )
        sql = filter_obj.compile()
        print(f"    {sql}")
        
        # 测试自然语言模式
        print("  - 自然语言模式生成的 SQL:")
        filter_obj = natural_match("title", "content", "tags", query="machine learning AI")
        sql = filter_obj.compile()
        print(f"    {sql}")
        
        # 测试 MatrixOne 风格的权重组合 '+red -(<blue >is)'
        print("  - MatrixOne 风格权重组合 '+red -(<blue >is)' 生成的 SQL:")
        filter_obj = boolean_match("title", "content", "tags").must("red").add_groups(
            not_group().low_weight("blue").high_weight("is")
        )
        sql = filter_obj.compile()
        print(f"    实际生成: {sql}")
        print(f"    期望语法: MATCH(...) AGAINST('+red -(<blue >is)' IN BOOLEAN MODE)")
        
        # 展示 MatrixOne 对应的实际语法
        print("  - 对应的 MatrixOne 语法示例:")
        print("    +red (blue term)      -> OR group: 包含任一术语")
        print("    +red -(blue term)     -> NOT group: 排除包含术语的组") 
        print("    +red (blue green yellow) -> 多术语 OR 组合")
        print("    +red -(<blue >is)     -> 权重组合：排除低权重blue和高权重is的组")
        print("    natural language      -> 自然语言模式搜索")
        print()
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 清理
        try:
            client.execute("DROP TABLE IF EXISTS articles")
        except:
            pass
        pass

if __name__ == "__main__":
    main()

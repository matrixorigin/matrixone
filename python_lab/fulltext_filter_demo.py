#!/usr/bin/env python3
"""
演示新的 fulltext 过滤器在 ORM filter 中的使用
"""

import os
import sys

# 添加项目根目录到 Python 路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(project_root, "clients", "python"))

from matrixone import Client, Column, Integer, String, Text, and_, or_
from matrixone.orm import declarative_base
from matrixone.sqlalchemy_ext import (
    FulltextNaturalFilter, 
    FulltextBooleanFilter, 
    FulltextQueryExpansionFilter
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
                "content": "Machine learning is a subset of artificial intelligence that focuses on algorithms and statistical models.",
                "author": "John Doe",
                "category": "AI"
            },
            {
                "title": "Deep Learning with Neural Networks",
                "content": "Deep learning uses neural networks with multiple layers to learn complex patterns in data.",
                "author": "Jane Smith",
                "category": "AI"
            },
            {
                "title": "Python Programming Basics",
                "content": "Python is a versatile programming language used for web development, data analysis, and automation.",
                "author": "Bob Johnson",
                "category": "Programming"
            },
            {
                "title": "Database Design Principles",
                "content": "Good database design involves normalization, indexing, and proper relationship modeling.",
                "author": "Alice Brown",
                "category": "Database"
            }
        ]
        
        for article_data in test_articles:
            client.execute("""
                INSERT INTO articles (title, content, author, category) 
                VALUES (%s, %s, %s, %s)
            """, (
                article_data["title"],
                article_data["content"], 
                article_data["author"],
                article_data["category"]
            ))
        
        # 创建全文索引
        client.execute("""
            CREATE FULLTEXT INDEX ft_articles ON articles(title, content)
        """)
        
        print("=== 新的 Fulltext 过滤器演示 ===\n")
        
        # 1. 自然语言模式
        print("1. 自然语言模式搜索:")
        results = client.query(Article).filter(
            FulltextNaturalFilter(["title", "content"], "machine learning")
        ).all()
        
        for article in results:
            print(f"  - {article.title} by {article.author}")
        print()
        
        # 2. 布尔模式
        print("2. 布尔模式搜索 (必须包含 'python' 且不包含 'java'):")
        results = client.query(Article).filter(
            FulltextBooleanFilter(["title", "content"], "+python -java")
        ).all()
        
        for article in results:
            print(f"  - {article.title} by {article.author}")
        print()
        
        # 3. 组合条件：全文搜索 + 其他条件
        print("3. 组合条件：全文搜索 + 作者过滤:")
        results = client.query(Article).filter(
            and_(
                FulltextNaturalFilter(["title", "content"], "learning"),
                Article.author == "John Doe"
            )
        ).all()
        
        for article in results:
            print(f"  - {article.title} by {article.author}")
        print()
        
        # 4. 多个全文搜索条件
        print("4. 多个全文搜索条件 (OR 关系):")
        results = client.query(Article).filter(
            or_(
                FulltextNaturalFilter(["title", "content"], "machine learning"),
                FulltextNaturalFilter(["title", "content"], "python programming")
            )
        ).all()
        
        for article in results:
            print(f"  - {article.title} by {article.author}")
        print()
        
        # 5. 复杂组合条件
        print("5. 复杂组合条件:")
        results = client.query(Article).filter(
            and_(
                FulltextBooleanFilter(["title", "content"], "+learning"),
                or_(
                    Article.category == "AI",
                    Article.category == "Programming"
                )
            )
        ).all()
        
        for article in results:
            print(f"  - {article.title} by {article.author} ({article.category})")
        print()
        
        # 6. 查询扩展模式
        print("6. 查询扩展模式搜索:")
        results = client.query(Article).filter(
            FulltextQueryExpansionFilter(["title", "content"], "AI")
        ).all()
        
        for article in results:
            print(f"  - {article.title} by {article.author}")
        print()
        
        # 7. 链式调用
        print("7. 链式调用:")
        results = (client.query(Article)
                  .filter(FulltextNaturalFilter(["title", "content"], "programming"))
                  .filter(Article.category == "Programming")
                  .all())
        
        for article in results:
            print(f"  - {article.title} by {article.author}")
        print()
        
        # 8. 显示生成的 SQL
        print("8. 生成的 SQL 示例:")
        query = client.query(Article).filter(
            and_(
                FulltextBooleanFilter(["title", "content"], "+python -java"),
                Article.category == "Programming"
            )
        )
        sql, _ = query._build_sql()
        print(f"  SQL: {sql}")
        
    except Exception as e:
        print(f"错误: {e}")
    finally:
        # 清理
        try:
            client.execute("DROP TABLE IF EXISTS articles")
        except:
            pass
        client.close()

if __name__ == "__main__":
    main()

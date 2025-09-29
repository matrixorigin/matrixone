#!/usr/bin/env python3
"""
演示全文搜索生成的 SQL 语句的含义
"""

from matrixone import Client
from matrixone.sqlalchemy_ext import FulltextSearchMode, FulltextSearchAlgorithm

def main():
    # 创建客户端
    client = Client(
        host="localhost",
        port=6001,
        user="root",
        password="111",
        database="test"
    )
    
    try:
        print("=== 全文搜索 SQL 语句详解 ===")
        
        # 创建测试表
        print("\n=== 创建测试表 ===")
        client.execute("DROP TABLE IF EXISTS articles")
        client.execute("""
            CREATE TABLE articles (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                title VARCHAR(200),
                content TEXT,
                category VARCHAR(50),
                author VARCHAR(100)
            )
        """)
        
        # 插入测试数据
        print("=== 插入测试数据 ===")
        client.execute("""
            INSERT INTO articles (title, content, category, author) VALUES
            ('Machine Learning Basics', 'Machine learning is a subset of artificial intelligence that focuses on algorithms and statistical models.', 'AI', 'John Doe'),
            ('Deep Learning with Python', 'Deep learning uses neural networks with multiple layers to learn complex patterns in data.', 'AI', 'Jane Smith'),
            ('Python Programming Guide', 'Python is a versatile programming language used in web development, data science, and automation.', 'Programming', 'Bob Johnson'),
            ('Data Science Fundamentals', 'Data science combines statistics, programming, and domain expertise to extract insights from data.', 'Data Science', 'Alice Brown'),
            ('Web Development with React', 'React is a JavaScript library for building user interfaces, particularly web applications.', 'Web Development', 'Charlie Wilson')
        """)
        
        # 启用全文索引
        print("=== 启用全文索引 ===")
        client.fulltext_index.enable_fulltext()
        
        # 创建全文索引
        print("=== 创建全文索引 ===")
        client.fulltext_index.create("articles", "ftidx_content", ["title", "content"], FulltextSearchAlgorithm.BM25)
        print("✓ 全文索引创建成功")
        
        # 1. 自然语言搜索 SQL
        print("\n=== 1. 自然语言搜索 SQL ===")
        sql1 = (client.fulltext_search.search()
            .table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
            .query("machine learning")
            .with_score()
            .select(["id", "title", "category"])
            .limit(3)
            .explain())
        
        print("生成的 SQL:")
        print(f"  {sql1}")
        print("\nSQL 解释:")
        print("  SELECT id, title, category, MATCH(title, content) AGAINST('machine learning' IN natural language mode) AS score")
        print("  FROM articles")
        print("  WHERE MATCH(title, content) AGAINST('machine learning' IN natural language mode)")
        print("  LIMIT 3")
        print("\n含义:")
        print("  - 在 title 和 content 列中搜索 'machine learning'")
        print("  - 使用自然语言模式，支持同义词和词形变化")
        print("  - 返回相关性分数 (score)")
        print("  - 限制返回 3 条结果")
        
        # 2. 布尔搜索 SQL
        print("\n=== 2. 布尔搜索 SQL ===")
        sql2 = (client.fulltext_search.search()
            .table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .add_term("python", required=True)
            .add_term("programming", required=True)
            .with_score()
            .select(["id", "title", "author"])
            .explain())
        
        print("生成的 SQL:")
        print(f"  {sql2}")
        print("\nSQL 解释:")
        print("  SELECT id, title, author, MATCH(title, content) AGAINST('+python +programming' IN boolean mode) AS score")
        print("  FROM articles")
        print("  WHERE MATCH(title, content) AGAINST('+python +programming' IN boolean mode)")
        print("\n含义:")
        print("  - +python: 必须包含 'python'")
        print("  - +programming: 必须包含 'programming'")
        print("  - 使用布尔模式，支持精确的搜索控制")
        print("  - 返回相关性分数")
        
        # 3. 排除词搜索 SQL
        print("\n=== 3. 排除词搜索 SQL ===")
        sql3 = (client.fulltext_search.search()
            .table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .add_term("learning", required=True)
            .add_term("python", excluded=True)
            .with_score()
            .select(["id", "title", "category"])
            .explain())
        
        print("生成的 SQL:")
        print(f"  {sql3}")
        print("\nSQL 解释:")
        print("  SELECT id, title, category, MATCH(title, content) AGAINST('+learning -python' IN boolean mode) AS score")
        print("  FROM articles")
        print("  WHERE MATCH(title, content) AGAINST('+learning -python' IN boolean mode)")
        print("\n含义:")
        print("  - +learning: 必须包含 'learning'")
        print("  - -python: 不能包含 'python'")
        print("  - 返回包含 'learning' 但不包含 'python' 的结果")
        
        # 4. 精确短语搜索 SQL
        print("\n=== 4. 精确短语搜索 SQL ===")
        sql4 = (client.fulltext_search.search()
            .table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .add_phrase("deep learning")
            .with_score()
            .select(["id", "title", "author"])
            .explain())
        
        print("生成的 SQL:")
        print(f"  {sql4}")
        print("\nSQL 解释:")
        print("  SELECT id, title, author, MATCH(title, content) AGAINST('\"deep learning\"' IN boolean mode) AS score")
        print("  FROM articles")
        print("  WHERE MATCH(title, content) AGAINST('\"deep learning\"' IN boolean mode)")
        print("\n含义:")
        print("  - \"deep learning\": 精确匹配短语 'deep learning'")
        print("  - 双引号表示精确短语匹配")
        print("  - 返回包含完整短语的结果")
        
        # 5. 通配符搜索 SQL
        print("\n=== 5. 通配符搜索 SQL ===")
        sql5 = (client.fulltext_search.search()
            .table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .add_wildcard("develop*")
            .with_score()
            .select(["id", "title", "category"])
            .explain())
        
        print("生成的 SQL:")
        print(f"  {sql5}")
        print("\nSQL 解释:")
        print("  SELECT id, title, category, MATCH(title, content) AGAINST('develop*' IN boolean mode) AS score")
        print("  FROM articles")
        print("  WHERE MATCH(title, content) AGAINST('develop*' IN boolean mode)")
        print("\n含义:")
        print("  - develop*: 匹配以 'develop' 开头的词")
        print("  - * 表示通配符，匹配任意字符")
        print("  - 返回包含 'develop', 'development', 'developer' 等词的结果")
        
        # 6. 复杂布尔搜索 SQL
        print("\n=== 6. 复杂布尔搜索 SQL ===")
        sql6 = (client.fulltext_search.search()
            .table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .add_term("data", required=True)
            .add_term("science", required=True)
            .add_term("python", excluded=True)
            .with_score()
            .select(["id", "title", "author"])
            .order_by("score", "DESC")
            .explain())
        
        print("生成的 SQL:")
        print(f"  {sql6}")
        print("\nSQL 解释:")
        print("  SELECT id, title, author, MATCH(title, content) AGAINST('+data +science -python' IN boolean mode) AS score")
        print("  FROM articles")
        print("  WHERE MATCH(title, content) AGAINST('+data +science -python' IN boolean mode)")
        print("  ORDER BY score DESC")
        print("\n含义:")
        print("  - +data: 必须包含 'data'")
        print("  - +science: 必须包含 'science'")
        print("  - -python: 不能包含 'python'")
        print("  - 按相关性分数降序排列")
        
        # 7. 带过滤条件的搜索 SQL
        print("\n=== 7. 带过滤条件的搜索 SQL ===")
        sql7 = (client.fulltext_search.search()
            .table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
            .query("learning")
            .where("category = 'AI'")
            .with_score()
            .select(["id", "title", "category", "author"])
            .order_by("score", "DESC")
            .explain())
        
        print("生成的 SQL:")
        print(f"  {sql7}")
        print("\nSQL 解释:")
        print("  SELECT id, title, category, author, MATCH(title, content) AGAINST('learning' IN natural language mode) AS score")
        print("  FROM articles")
        print("  WHERE MATCH(title, content) AGAINST('learning' IN natural language mode) AND category = 'AI'")
        print("  ORDER BY score DESC")
        print("\n含义:")
        print("  - 搜索包含 'learning' 的内容")
        print("  - 同时过滤 category = 'AI' 的记录")
        print("  - 按相关性分数降序排列")
        
        # 8. 分页搜索 SQL
        print("\n=== 8. 分页搜索 SQL ===")
        sql8 = (client.fulltext_search.search()
            .table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .add_term("machine", required=True)
            .add_term("learning", required=True)
            .with_score()
            .select(["id", "title"])
            .limit(5)
            .offset(2)
            .explain())
        
        print("生成的 SQL:")
        print(f"  {sql8}")
        print("\nSQL 解释:")
        print("  SELECT id, title, MATCH(title, content) AGAINST('+machine +learning' IN boolean mode) AS score")
        print("  FROM articles")
        print("  WHERE MATCH(title, content) AGAINST('+machine +learning' IN boolean mode)")
        print("  LIMIT 5 OFFSET 2")
        print("\n含义:")
        print("  - 搜索包含 'machine' 和 'learning' 的内容")
        print("  - 跳过前 2 条结果 (OFFSET 2)")
        print("  - 返回接下来的 5 条结果 (LIMIT 5)")
        print("  - 实现分页功能")
        
        print("\n=== SQL 语句总结 ===")
        print("1. MATCH() AGAINST() 是 MatrixOne 的全文搜索函数")
        print("2. 支持三种模式：natural language mode, boolean mode, query expansion mode")
        print("3. 布尔模式支持操作符：+ (必须), - (排除), * (通配符), \"\" (精确短语)")
        print("4. 相关性分数表示匹配程度，分数越高越相关")
        print("5. 可以结合 WHERE 条件进行过滤")
        print("6. 支持 ORDER BY, LIMIT, OFFSET 等标准 SQL 操作")
        
    except Exception as e:
        print(f"错误: {e}")
    finally:
        # 清理
        try:
            client.execute("DROP TABLE IF EXISTS articles")
        except:
            pass
        client.disconnect()

if __name__ == "__main__":
    main()

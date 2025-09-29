#!/usr/bin/env python3
"""
演示 query() 和 add_term() 的区别
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
        print("=== query() vs add_term() 区别演示 ===")
        
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
        
        # 1. query() 方法 - 简单查询字符串
        print("\n=== 1. query() 方法 - 简单查询字符串 ===")
        sql1 = (client.fulltext_search.search()
            .table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
            .query("machine learning")
            .explain())
        
        print("生成的 SQL:")
        print(f"  {sql1}")
        print("\n特点:")
        print("  - 接受一个完整的查询字符串")
        print("  - 适合自然语言搜索")
        print("  - 不支持布尔操作符")
        print("  - 每次调用会重置之前的查询")
        
        # 2. add_term() 方法 - 添加单个词
        print("\n=== 2. add_term() 方法 - 添加单个词 ===")
        sql2 = (client.fulltext_search.search()
            .table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .add_term("machine")
            .add_term("learning")
            .explain())
        
        print("生成的 SQL:")
        print(f"  {sql2}")
        print("\n特点:")
        print("  - 每次添加一个词")
        print("  - 支持布尔操作符 (required, excluded)")
        print("  - 可以链式调用多个词")
        print("  - 适合精确控制搜索")
        
        # 3. add_term() 带操作符
        print("\n=== 3. add_term() 带操作符 ===")
        sql3 = (client.fulltext_search.search()
            .table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .add_term("python", required=True)
            .add_term("programming", required=True)
            .add_term("java", excluded=True)
            .explain())
        
        print("生成的 SQL:")
        print(f"  {sql3}")
        print("\n特点:")
        print("  - required=True: 添加 + 前缀 (必须包含)")
        print("  - excluded=True: 添加 - 前缀 (不能包含)")
        print("  - 支持复杂的布尔逻辑")
        
        # 4. 对比查询结果
        print("\n=== 4. 对比查询结果 ===")
        
        # 使用 query() 搜索
        print("\n使用 query('machine learning'):")
        results1 = (client.fulltext_search.search()
            .table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
            .query("machine learning")
            .with_score()
            .select(["id", "title"])
            .execute())
        
        for row in results1:
            print(f"  ID: {row[0]}, Title: {row[1]}, Score: {row[2]}")
        
        # 使用 add_term() 搜索
        print("\n使用 add_term('machine') + add_term('learning'):")
        results2 = (client.fulltext_search.search()
            .table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .add_term("machine")
            .add_term("learning")
            .with_score()
            .select(["id", "title"])
            .execute())
        
        for row in results2:
            print(f"  ID: {row[0]}, Title: {row[1]}, Score: {row[2]}")
        
        # 5. 复杂查询对比
        print("\n=== 5. 复杂查询对比 ===")
        
        # query() 无法实现复杂查询
        print("\nquery() 的限制:")
        print("  - 无法指定哪些词是必须的")
        print("  - 无法排除某些词")
        print("  - 无法使用通配符")
        print("  - 无法精确控制搜索逻辑")
        
        # add_term() 可以实现复杂查询
        print("\nadd_term() 的优势:")
        print("  - 可以精确控制每个词")
        print("  - 支持布尔操作符")
        print("  - 可以组合多种搜索条件")
        print("  - 支持通配符和短语")
        
        # 6. 实际使用场景
        print("\n=== 6. 实际使用场景 ===")
        
        print("\n适合使用 query() 的场景:")
        print("  - 用户输入的自然语言查询")
        print("  - 简单的关键词搜索")
        print("  - 不需要精确控制的搜索")
        print("  - 快速原型开发")
        
        print("\n适合使用 add_term() 的场景:")
        print("  - 需要精确控制搜索逻辑")
        print("  - 复杂的布尔查询")
        print("  - 需要排除某些词")
        print("  - 高级搜索功能")
        
        # 7. 组合使用
        print("\n=== 7. 组合使用示例 ===")
        
        # 先设置基础查询，再添加条件
        sql4 = (client.fulltext_search.search()
            .table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .query("python")  # 基础查询
            .add_term("programming", required=True)  # 添加必须词
            .add_term("java", excluded=True)  # 添加排除词
            .explain())
        
        print("组合使用生成的 SQL:")
        print(f"  {sql4}")
        print("\n注意: query() 会重置之前的查询，所以最终只包含 'python' 和添加的词")
        
        print("\n=== 总结 ===")
        print("1. query(): 简单、快速，适合自然语言搜索")
        print("2. add_term(): 灵活、精确，适合复杂搜索")
        print("3. 两者可以组合使用，但 query() 会重置之前的查询")
        print("4. 根据具体需求选择合适的方法")
        
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

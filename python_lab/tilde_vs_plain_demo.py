#!/usr/bin/env python3
"""
演示 ~xxx 和 xxx 的区别

基于 MatrixOne 测试用例分析两者在布尔模式下的不同行为
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'clients', 'python'))

from matrixone import Client, boolean_match
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer, String, Text

Base = declarative_base()

class TestArticle(Base):
    __tablename__ = "tilde_test_articles"
    
    id = Column(Integer, primary_key=True)
    body = Column(Text)
    title = Column(String(200))

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
        client.execute("DROP TABLE IF EXISTS tilde_test_articles")
        client.execute("""
            CREATE TABLE tilde_test_articles (
                id INT PRIMARY KEY,
                body TEXT,
                title VARCHAR(200)
            )
        """)
        
        # 插入 MatrixOne 测试用例相同的数据
        client.execute("CREATE FULLTEXT INDEX ft_idx ON tilde_test_articles (body, title)")
        
        test_data = [
            (0, 'color is red', 't1'),
            (1, 'car is yellow', 'crazy car'), 
            (2, 'sky is blue', 'no limit'),
            (3, 'blue is not red', 'colorful')
        ]
        
        for data in test_data:
            try:
                client.execute(f"""
                    INSERT INTO tilde_test_articles (id, body, title) 
                    VALUES ({data[0]}, '{data[1]}', '{data[2]}')
                """)
            except Exception as e:
                if "Duplicate entry" not in str(e):
                    print(f"插入数据失败: {e}")
        
        print("✓ 数据准备完成")
        print("\n=== ~xxx vs xxx 区别演示 ===\n")
        
        # 1. +red blue (必须包含 red，可选包含 blue，blue 有正常权重)
        print("1. '+red blue' - 必须包含 red，可选包含 blue (正常权重):")
        results = client.query(TestArticle).filter(
            boolean_match("body", "title").must("red").should("blue")
        ).all()
        print("   生成的 SQL: MATCH(body, title) AGAINST('+red blue' IN BOOLEAN MODE)")
        for article in results:
            print(f"   id={article.id}: '{article.body}' - '{article.title}'")
        print()
        
        # 2. +red ~blue (必须包含 red，可选包含 blue，但 blue 权重降低)
        print("2. '+red ~blue' - 必须包含 red，可选包含 blue (降低权重):")
        results = client.query(TestArticle).filter(
            boolean_match("body", "title").must("red").tilde_weight("blue")
        ).all()
        print("   生成的 SQL: MATCH(body, title) AGAINST('+red ~blue' IN BOOLEAN MODE)")
        for article in results:
            print(f"   id={article.id}: '{article.body}' - '{article.title}'")
        print()
        
        # 3. 对比其他操作符
        print("3. 对比其他操作符:")
        
        # +red +blue (必须包含 red 且必须包含 blue)
        print("   '+red +blue' - 必须包含 red 且必须包含 blue:")
        results = client.query(TestArticle).filter(
            boolean_match("body", "title").must("red", "blue")
        ).all()
        for article in results:
            print(f"     id={article.id}: '{article.body}' - '{article.title}'")
        
        # +red -blue (必须包含 red 且必须不包含 blue)
        print("   '+red -blue' - 必须包含 red 且必须不包含 blue:")
        results = client.query(TestArticle).filter(
            boolean_match("body", "title").must("red").must_not("blue")
        ).all()
        for article in results:
            print(f"     id={article.id}: '{article.body}' - '{article.title}'")
        print()
        
        # 4. 详细分析排序差异
        print("4. 排序差异分析:")
        print("   注意观察返回结果的顺序差异：")
        
        print("   '+red blue' 结果顺序:")
        results = client.query(TestArticle).filter(
            boolean_match("body", "title").must("red").should("blue")
        ).all()
        for i, article in enumerate(results, 1):
            has_blue = "blue" in article.body.lower()
            print(f"     {i}. id={article.id} (包含blue: {has_blue}): '{article.body}'")
        
        print("   '+red ~blue' 结果顺序:")
        results = client.query(TestArticle).filter(
            boolean_match("body", "title").must("red").tilde_weight("blue")
        ).all()
        for i, article in enumerate(results, 1):
            has_blue = "blue" in article.body.lower()
            print(f"     {i}. id={article.id} (包含blue: {has_blue}): '{article.body}'")
        print()
        
        # 5. 总结区别
        print("=== 总结：~xxx vs xxx 的区别 ===")
        print()
        print("📊 匹配范围：")
        print("   • xxx (普通可选)   : 包含该词的文档会匹配，不包含的也可能匹配")
        print("   • ~xxx (降低权重)  : 包含该词的文档会匹配，不包含的也可能匹配")
        print("   ➤ 在匹配范围上，两者基本相同")
        print()
        print("🎯 权重差异：")
        print("   • xxx (普通可选)   : 包含该词的文档获得正常的相关性加分")
        print("   • ~xxx (降低权重)  : 包含该词的文档获得较低的相关性加分")
        print("   ➤ 主要区别在于权重计算和排序")
        print()
        print("📈 排序影响：")
        print("   • '+red blue'  : 有blue的文档通常排序较高 (正向加分)")
        print("   • '+red ~blue' : 有blue的文档排序可能较低 (降低加分)")
        print("   ➤ ~操作符主要用于微调排序，而不是过滤")
        print()
        print("💡 使用场景：")
        print("   • xxx  : 希望包含某词的文档排序更高")
        print("   • ~xxx : 希望降低某词的影响，但不完全排除")
        
    except Exception as e:
        print(f"错误: {e}")
    finally:
        # 清理
        try:
            client.execute("DROP TABLE IF EXISTS tilde_test_articles")
            print("\n✓ 清理完成")
        except:
            pass

if __name__ == "__main__":
    main()

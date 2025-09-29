#!/usr/bin/env python3
"""
演示 MatrixOne ORM 统一 filter 接口

展示优化后的 filter 接口，支持字符串、SQLAlchemy 表达式和关键字参数
"""

from matrixone import Client
from sqlalchemy import Column, Integer, String, DECIMAL, TIMESTAMP, and_, or_, not_, desc, asc
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# 定义模型
class User(Base):
    """用户模型"""
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    email = Column(String(100))
    age = Column(Integer)
    city = Column(String(50))
    created_at = Column(TIMESTAMP)

class Product(Base):
    """产品模型"""
    __tablename__ = "products"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    price = Column(DECIMAL(10, 2))
    category = Column(String(50))
    stock = Column(Integer)
    created_at = Column(TIMESTAMP)

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
        print("=== MatrixOne ORM 统一 Filter 接口演示 ===")
        
        # 创建测试数据库和表
        print("\n=== 创建测试数据 ===")
        client.execute("CREATE DATABASE IF NOT EXISTS unified_filter_demo")
        client.execute("USE unified_filter_demo")
        
        # 删除现有表
        client.execute("DROP TABLE IF EXISTS products")
        client.execute("DROP TABLE IF EXISTS users")
        
        # 创建表
        Base.metadata.create_all(client._engine)
        
        # 插入测试数据
        print("=== 插入测试数据 ===")
        
        # 用户数据
        users_data = [
            (1, "张三", "zhangsan@example.com", 25, "北京", "2024-01-01 10:00:00"),
            (2, "李四", "lisi@example.com", 30, "上海", "2024-01-02 11:00:00"),
            (3, "王五", "wangwu@example.com", 35, "广州", "2024-01-03 12:00:00"),
            (4, "赵六", "zhaoliu@example.com", 28, "深圳", "2024-01-04 13:00:00"),
            (5, "钱七", "qianqi@example.com", 32, "北京", "2024-01-05 14:00:00"),
        ]
        
        for user_data in users_data:
            client.execute(
                "INSERT INTO users (id, name, email, age, city, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                user_data
            )
        
        # 产品数据
        products_data = [
            (1, "笔记本电脑", 5999.99, "电子产品", 100, "2024-01-01 10:00:00"),
            (2, "智能手机", 3999.99, "电子产品", 200, "2024-01-01 10:00:00"),
            (3, "编程书籍", 89.99, "图书", 500, "2024-01-01 10:00:00"),
            (4, "咖啡杯", 29.99, "生活用品", 300, "2024-01-01 10:00:00"),
            (5, "耳机", 299.99, "电子产品", 150, "2024-01-01 10:00:00"),
        ]
        
        for product_data in products_data:
            client.execute(
                "INSERT INTO products (id, name, price, category, stock, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                product_data
            )
        
        print("✓ 测试数据插入完成")
        
        # 1. 字符串条件
        print("\n=== 1. 字符串条件 ===")
        
        # 简单字符串条件
        string_query = client.query(User).filter("age < 30")
        print(f"字符串条件SQL: {string_query._build_sql()[0]}")
        string_results = string_query.all()
        print("年龄<30的用户:")
        for user in string_results:
            print(f"  - {user.name} ({user.age}岁)")
        
        # 2. SQLAlchemy 表达式条件
        print("\n=== 2. SQLAlchemy 表达式条件 ===")
        
        # 使用 SQLAlchemy 列表达式
        expr_query = client.query(Product).filter(Product.category == "电子产品")
        print(f"SQLAlchemy表达式SQL: {expr_query._build_sql()[0]}")
        expr_results = expr_query.all()
        print("电子产品:")
        for product in expr_results:
            print(f"  - {product.name}: ¥{product.price}")
        
        # 3. OR 条件
        print("\n=== 3. OR 条件 ===")
        
        # 使用 SQLAlchemy OR
        or_query = client.query(User).filter(or_(User.age < 30, User.city == "北京"))
        print(f"OR条件SQL: {or_query._build_sql()[0]}")
        or_results = or_query.all()
        print("年龄<30 OR 城市=北京:")
        for user in or_results:
            print(f"  - {user.name} ({user.age}岁, {user.city})")
        
        # 4. AND 条件
        print("\n=== 4. AND 条件 ===")
        
        # 使用 SQLAlchemy AND
        and_query = client.query(Product).filter(and_(Product.category == "电子产品", Product.price > 1000))
        print(f"AND条件SQL: {and_query._build_sql()[0]}")
        and_results = and_query.all()
        print("类别=电子产品 AND 价格>1000:")
        for product in and_results:
            print(f"  - {product.name}: ¥{product.price}")
        
        # 5. 多个 filter 调用 (AND 关系)
        print("\n=== 5. 多个 filter 调用 (AND 关系) ===")
        
        # 多个 filter 调用，自动 AND 连接
        multi_filter_query = (client.query(Product)
                            .filter(Product.category == "电子产品")
                            .filter(Product.price > 1000)
                            .filter(Product.stock > 50))
        print(f"多个filter调用SQL: {multi_filter_query._build_sql()[0]}")
        multi_filter_results = multi_filter_query.all()
        print("电子产品 AND 价格>1000 AND 库存>50:")
        for product in multi_filter_results:
            print(f"  - {product.name}: ¥{product.price}, 库存: {product.stock}")
        
        # 6. 复杂嵌套条件
        print("\n=== 6. 复杂嵌套条件 ===")
        
        # 复杂 OR 条件
        complex_or = or_(
            and_(Product.category == "电子产品", Product.price > 2000),
            and_(Product.category == "图书", Product.stock > 400)
        )
        complex_or_query = client.query(Product).filter(complex_or)
        print(f"复杂OR条件SQL: {complex_or_query._build_sql()[0]}")
        complex_or_results = complex_or_query.all()
        print("(电子产品 AND 价格>2000) OR (图书 AND 库存>400):")
        for product in complex_or_results:
            print(f"  - {product.name}: ¥{product.price}, 类别: {product.category}, 库存: {product.stock}")
        
        # 7. NOT 条件
        print("\n=== 7. NOT 条件 ===")
        
        # 使用 NOT 条件
        not_query = client.query(Product).filter(not_(Product.category == "电子产品"))
        print(f"NOT条件SQL: {not_query._build_sql()[0]}")
        not_results = not_query.all()
        print("NOT 类别=电子产品:")
        for product in not_results:
            print(f"  - {product.name}: ¥{product.price}, 类别: {product.category}")
        
        # 8. 混合条件类型
        print("\n=== 8. 混合条件类型 ===")
        
        # 混合使用字符串和 SQLAlchemy 表达式
        mixed_query = (client.query(User)
                      .filter("age > 25")  # 字符串条件
                      .filter(User.city != "深圳")  # SQLAlchemy 表达式
                      .filter(or_(User.name.like("张%"), User.name.like("李%"))))  # 复杂表达式
        print(f"混合条件SQL: {mixed_query._build_sql()[0]}")
        mixed_results = mixed_query.all()
        print("年龄>25 AND 城市!=深圳 AND (姓名LIKE('张%') OR 姓名LIKE('李%')):")
        for user in mixed_results:
            print(f"  - {user.name} ({user.age}岁, {user.city})")
        
        # 9. 与排序和限制结合
        print("\n=== 9. 与排序和限制结合 ===")
        
        # 复杂条件 + 排序 + 限制
        complex_query = (client.query(Product)
                        .filter(or_(Product.price > 1000, Product.stock < 200))
                        .order_by(desc(Product.price))
                        .limit(3))
        print(f"复杂查询SQL: {complex_query._build_sql()[0]}")
        complex_results = complex_query.all()
        print("价格>1000 OR 库存<200, 按价格降序, 限制3条:")
        for product in complex_results:
            print(f"  - {product.name}: ¥{product.price}, 库存: {product.stock}")
        
        # 10. 动态条件构建
        print("\n=== 10. 动态条件构建 ===")
        
        def build_dynamic_filter(categories=None, min_price=None, max_price=None, min_stock=None):
            """动态构建 filter 条件"""
            conditions = []
            
            if categories:
                conditions.append(Product.category.in_(categories))
            
            if min_price is not None:
                conditions.append(Product.price >= min_price)
            
            if max_price is not None:
                conditions.append(Product.price <= max_price)
            
            if min_stock is not None:
                conditions.append(Product.stock >= min_stock)
            
            return conditions
        
        # 使用动态条件
        dynamic_conditions = build_dynamic_filter(
            categories=["电子产品", "图书"],
            min_price=100,
            max_price=5000,
            min_stock=50
        )
        
        if dynamic_conditions:
            # 使用 * 展开条件列表
            dynamic_query = client.query(Product).filter(*dynamic_conditions)
            print(f"动态条件SQL: {dynamic_query._build_sql()[0]}")
            dynamic_results = dynamic_query.all()
            print("动态条件 (电子产品/图书, 价格100-5000, 库存>=50):")
            for product in dynamic_results:
                print(f"  - {product.name}: ¥{product.price}, 类别: {product.category}, 库存: {product.stock}")
        
        # 11. filter_by 关键字参数
        print("\n=== 11. filter_by 关键字参数 ===")
        
        # 使用 filter_by 进行简单条件查询
        filter_by_query = client.query(User).filter_by(city="北京", age=25)
        print(f"filter_by查询SQL: {filter_by_query._build_sql()[0]}")
        filter_by_results = filter_by_query.all()
        print("城市=北京 AND 年龄=25:")
        for user in filter_by_results:
            print(f"  - {user.name} ({user.age}岁, {user.city})")
        
        # 12. 链式调用示例
        print("\n=== 12. 链式调用示例 ===")
        
        # 完整的链式调用
        chain_query = (client.query(User)
                      .filter(User.age >= 25)
                      .filter("city IN ('北京', '上海')")
                      .order_by(asc(User.age))
                      .limit(3))
        print(f"链式调用SQL: {chain_query._build_sql()[0]}")
        chain_results = chain_query.all()
        print("年龄>=25 AND 城市IN(北京,上海), 按年龄升序, 限制3条:")
        for user in chain_results:
            print(f"  - {user.name} ({user.age}岁, {user.city})")
        
        # 清理
        print("\n=== 清理测试数据 ===")
        client.execute("DROP DATABASE IF EXISTS unified_filter_demo")
        print("✓ 测试数据清理完成")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()

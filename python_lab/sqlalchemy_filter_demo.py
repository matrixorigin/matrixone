#!/usr/bin/env python3
"""
演示 MatrixOne ORM 支持 SQLAlchemy 原生 filter 对象

展示如何使用 OR(), AND(), 等 SQLAlchemy 原生条件对象
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
        print("=== MatrixOne ORM SQLAlchemy 原生 Filter 演示 ===")
        
        # 创建测试数据库和表
        print("\n=== 创建测试数据 ===")
        client.execute("CREATE DATABASE IF NOT EXISTS sqlalchemy_filter_demo")
        client.execute("USE sqlalchemy_filter_demo")
        
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
        
        # 1. 基础 OR 条件
        print("\n=== 1. 基础 OR 条件 ===")
        
        # 使用 SQLAlchemy 原生 OR
        or_condition = or_(User.age < 30, User.city == "北京")
        or_query = client.query(User).filter_sqlalchemy(or_condition)
        print(f"OR条件SQL: {or_query._build_sql()[0]}")
        or_results = or_query.all()
        print("年龄<30 OR 城市=北京:")
        for user in or_results:
            print(f"  - {user.name} ({user.age}岁, {user.city})")
        
        # 2. 基础 AND 条件
        print("\n=== 2. 基础 AND 条件 ===")
        
        # 使用 SQLAlchemy 原生 AND
        and_condition = and_(Product.category == "电子产品", Product.price > 1000)
        and_query = client.query(Product).filter_sqlalchemy(and_condition)
        print(f"AND条件SQL: {and_query._build_sql()[0]}")
        and_results = and_query.all()
        print("类别=电子产品 AND 价格>1000:")
        for product in and_results:
            print(f"  - {product.name}: ¥{product.price}")
        
        # 3. 复杂嵌套条件
        print("\n=== 3. 复杂嵌套条件 ===")
        
        # 复杂 OR 条件
        complex_or = or_(
            and_(Product.category == "电子产品", Product.price > 2000),
            and_(Product.category == "图书", Product.stock > 400)
        )
        complex_or_query = client.query(Product).filter_sqlalchemy(complex_or)
        print(f"复杂OR条件SQL: {complex_or_query._build_sql()[0]}")
        complex_or_results = complex_or_query.all()
        print("(电子产品 AND 价格>2000) OR (图书 AND 库存>400):")
        for product in complex_or_results:
            print(f"  - {product.name}: ¥{product.price}, 类别: {product.category}, 库存: {product.stock}")
        
        # 4. 多重条件组合
        print("\n=== 4. 多重条件组合 ===")
        
        # 多个 OR 条件
        multiple_or = or_(
            User.city == "北京",
            User.city == "上海", 
            User.age > 30
        )
        multiple_or_query = client.query(User).filter_sqlalchemy(multiple_or)
        print(f"多重OR条件SQL: {multiple_or_query._build_sql()[0]}")
        multiple_or_results = multiple_or_query.all()
        print("城市=北京 OR 城市=上海 OR 年龄>30:")
        for user in multiple_or_results:
            print(f"  - {user.name} ({user.age}岁, {user.city})")
        
        # 5. NOT 条件
        print("\n=== 5. NOT 条件 ===")
        
        # 使用 NOT 条件
        not_condition = not_(Product.category == "电子产品")
        not_query = client.query(Product).filter_sqlalchemy(not_condition)
        print(f"NOT条件SQL: {not_query._build_sql()[0]}")
        not_results = not_query.all()
        print("NOT 类别=电子产品:")
        for product in not_results:
            print(f"  - {product.name}: ¥{product.price}, 类别: {product.category}")
        
        # 6. 复杂嵌套 AND/OR 组合
        print("\n=== 6. 复杂嵌套 AND/OR 组合 ===")
        
        # 复杂的 AND/OR 组合
        complex_condition = and_(
            or_(User.age >= 25, User.city.in_(["北京", "上海"])),
            not_(User.email.like("%test%"))
        )
        complex_query = client.query(User).filter_sqlalchemy(complex_condition)
        print(f"复杂嵌套条件SQL: {complex_query._build_sql()[0]}")
        complex_results = complex_query.all()
        print("(年龄>=25 OR 城市IN(北京,上海)) AND NOT 邮箱LIKE('%test%'):")
        for user in complex_results:
            print(f"  - {user.name} ({user.age}岁, {user.city}, {user.email})")
        
        # 7. 与排序结合
        print("\n=== 7. 与排序结合 ===")
        
        # OR 条件 + 排序
        or_with_order = or_(Product.price > 1000, Product.stock < 200)
        or_order_query = client.query(Product).filter_sqlalchemy(or_with_order).order_by(desc(Product.price))
        print(f"OR条件+排序SQL: {or_order_query._build_sql()[0]}")
        or_order_results = or_order_query.all()
        print("价格>1000 OR 库存<200, 按价格降序:")
        for product in or_order_results:
            print(f"  - {product.name}: ¥{product.price}, 库存: {product.stock}")
        
        # 8. 与 LIMIT 结合
        print("\n=== 8. 与 LIMIT 结合 ===")
        
        # AND 条件 + LIMIT
        and_with_limit = and_(User.age > 25, User.city != "深圳")
        and_limit_query = client.query(User).filter_sqlalchemy(and_with_limit).limit(3)
        print(f"AND条件+LIMIT SQL: {and_limit_query._build_sql()[0]}")
        and_limit_results = and_limit_query.all()
        print("年龄>25 AND 城市!=深圳, 限制3条:")
        for user in and_limit_results:
            print(f"  - {user.name} ({user.age}岁, {user.city})")
        
        # 9. 动态条件构建
        print("\n=== 9. 动态条件构建 ===")
        
        def build_dynamic_sqlalchemy_filter(categories=None, min_price=None, max_price=None, min_stock=None):
            """动态构建 SQLAlchemy 条件"""
            conditions = []
            
            if categories:
                conditions.append(Product.category.in_(categories))
            
            if min_price is not None:
                conditions.append(Product.price >= min_price)
            
            if max_price is not None:
                conditions.append(Product.price <= max_price)
            
            if min_stock is not None:
                conditions.append(Product.stock >= min_stock)
            
            if len(conditions) == 1:
                return conditions[0]
            elif len(conditions) > 1:
                return and_(*conditions)
            else:
                return None
        
        # 使用动态条件
        dynamic_condition = build_dynamic_sqlalchemy_filter(
            categories=["电子产品", "图书"],
            min_price=100,
            max_price=5000,
            min_stock=50
        )
        
        if dynamic_condition is not None:
            dynamic_query = client.query(Product).filter_sqlalchemy(dynamic_condition)
            print(f"动态条件SQL: {dynamic_query._build_sql()[0]}")
            dynamic_results = dynamic_query.all()
            print("动态条件 (电子产品/图书, 价格100-5000, 库存>=50):")
            for product in dynamic_results:
                print(f"  - {product.name}: ¥{product.price}, 类别: {product.category}, 库存: {product.stock}")
        
        # 10. 混合使用 filter 和 filter_sqlalchemy
        print("\n=== 10. 混合使用 filter 和 filter_sqlalchemy ===")
        
        # 先使用 filter_sqlalchemy 添加复杂条件
        mixed_query = (client.query(Product)
                      .filter_sqlalchemy(or_(Product.category == "电子产品", Product.category == "图书"))
                      .filter("price > ?", 50)  # 再使用普通 filter
                      .order_by("price DESC"))
        
        print(f"混合条件SQL: {mixed_query._build_sql()[0]}")
        mixed_results = mixed_query.all()
        print("(电子产品 OR 图书) AND 价格>50, 按价格降序:")
        for product in mixed_results:
            print(f"  - {product.name}: ¥{product.price}, 类别: {product.category}")
        
        # 清理
        print("\n=== 清理测试数据 ===")
        client.execute("DROP DATABASE IF EXISTS sqlalchemy_filter_demo")
        print("✓ 测试数据清理完成")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()

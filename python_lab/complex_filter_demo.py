#!/usr/bin/env python3
"""
演示 MatrixOne ORM 复杂过滤器的使用

展示各种复杂的 WHERE 条件、JOIN、GROUP BY、HAVING 等高级查询功能
"""

from matrixone import Client
from sqlalchemy import Column, Integer, String, DECIMAL, TIMESTAMP, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

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

class Order(Base):
    """订单模型"""
    __tablename__ = "orders"
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    product_id = Column(Integer, ForeignKey('products.id'))
    quantity = Column(Integer)
    total_amount = Column(DECIMAL(10, 2))
    order_date = Column(TIMESTAMP)
    status = Column(String(20))

class OrderItem(Base):
    """订单项模型"""
    __tablename__ = "order_items"
    
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey('orders.id'))
    product_id = Column(Integer, ForeignKey('products.id'))
    quantity = Column(Integer)
    price = Column(DECIMAL(10, 2))

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
        print("=== MatrixOne ORM 复杂过滤器演示 ===")
        
        # 创建测试数据库和表
        print("\n=== 创建测试数据 ===")
        client.execute("CREATE DATABASE IF NOT EXISTS complex_filter_demo")
        client.execute("USE complex_filter_demo")
        
        # 删除现有表
        client.execute("DROP TABLE IF EXISTS order_items")
        client.execute("DROP TABLE IF EXISTS orders")
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
        
        # 订单数据
        orders_data = [
            (1, 1, 1, 1, 5999.99, "2024-01-10 10:00:00", "已完成"),
            (2, 1, 2, 2, 7999.98, "2024-01-11 11:00:00", "已完成"),
            (3, 2, 3, 5, 449.95, "2024-01-12 12:00:00", "已完成"),
            (4, 2, 4, 3, 89.97, "2024-01-13 13:00:00", "已完成"),
            (5, 3, 5, 1, 299.99, "2024-01-14 14:00:00", "已完成"),
            (6, 4, 1, 1, 5999.99, "2024-01-15 15:00:00", "已完成"),
            (7, 5, 2, 1, 3999.99, "2024-01-16 16:00:00", "已完成"),
        ]
        
        for order_data in orders_data:
            client.execute(
                "INSERT INTO orders (id, user_id, product_id, quantity, total_amount, order_date, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                order_data
            )
        
        print("✓ 测试数据插入完成")
        
        # 1. 基础过滤
        print("\n=== 1. 基础过滤 ===")
        
        # 简单条件
        young_users_query = client.query(User).filter("age < 30")
        print(f"生成的SQL: {young_users_query._build_sql()[0]}")
        print(f"参数: {young_users_query._build_sql()[1]}")
        young_users = young_users_query.all()
        print(f"年龄小于30的用户: {len(young_users)} 人")
        for user in young_users:
            print(f"  - {user.name} ({user.age}岁)")
        
        # 多条件组合
        beijing_users_query = client.query(User).filter("city = ?", "北京")
        print(f"生成的SQL: {beijing_users_query._build_sql()[0]}")
        print(f"参数: {beijing_users_query._build_sql()[1]}")
        beijing_users = beijing_users_query.all()
        print(f"北京用户: {len(beijing_users)} 人")
        
        # 2. 复杂条件组合
        print("\n=== 2. 复杂条件组合 ===")
        
        # AND 条件
        electronics_query = client.query(Product).filter("category = ? AND price > ?", "电子产品", 1000)
        print(f"AND条件SQL: {electronics_query._build_sql()[0]}")
        print(f"参数: {electronics_query._build_sql()[1]}")
        electronics = electronics_query.all()
        print(f"电子产品且价格>1000: {len(electronics)} 个")
        for product in electronics:
            print(f"  - {product.name}: ¥{product.price}")
        
        # OR 条件
        expensive_or_electronics_query = client.query(Product).filter("price > ? OR category = ?", 1000, "电子产品")
        print(f"OR条件SQL: {expensive_or_electronics_query._build_sql()[0]}")
        print(f"参数: {expensive_or_electronics_query._build_sql()[1]}")
        expensive_or_electronics = expensive_or_electronics_query.all()
        print(f"价格>1000或电子产品: {len(expensive_or_electronics)} 个")
        
        # 3. 范围查询
        print("\n=== 3. 范围查询 ===")
        
        # IN 查询
        target_cities = ["北京", "上海"]
        city_users = client.query(User).filter("city IN (?, ?)", *target_cities).all()
        print(f"北京或上海用户: {len(city_users)} 人")
        
        # BETWEEN 查询
        age_range_users = client.query(User).filter("age BETWEEN ? AND ?", 25, 35).all()
        print(f"25-35岁用户: {len(age_range_users)} 人")
        
        # 4. 模糊查询
        print("\n=== 4. 模糊查询 ===")
        
        # LIKE 查询
        email_users = client.query(User).filter("email LIKE ?", "%example.com%").all()
        print(f"example.com邮箱用户: {len(email_users)} 人")
        
        # 5. 聚合查询
        print("\n=== 5. 聚合查询 ===")
        
        # GROUP BY
        category_stats_query = client.query(Product).select("category, COUNT(*) as count, AVG(price) as avg_price").group_by("category")
        print(f"GROUP BY查询SQL: {category_stats_query._build_sql()[0]}")
        print(f"参数: {category_stats_query._build_sql()[1]}")
        category_stats = category_stats_query.all()
        print("产品分类统计:")
        for stat in category_stats:
            # 聚合查询返回的是RowData对象，使用索引访问
            print(f"  - {stat[0]}: {stat[1]} 个, 平均价格: ¥{stat[2]:.2f}")
        
        # 6. HAVING 条件
        print("\n=== 6. HAVING 条件 ===")
        
        # 使用ORM的HAVING功能
        category_stats_having_query = client.query(Product).select("category, COUNT(*) as count, AVG(price) as avg_price").group_by("category").having("COUNT(*) > 1")
        print(f"HAVING查询SQL: {category_stats_having_query._build_sql()[0]}")
        print(f"参数: {category_stats_having_query._build_sql()[1]}")
        category_stats_having = category_stats_having_query.all()
        print("产品数量>1的分类:")
        for stat in category_stats_having:
            print(f"  - {stat[0]}: {stat[1]} 个, 平均价格: ¥{stat[2]:.2f}")
        
        # 7. 子查询
        print("\n=== 7. 子查询 ===")
        
        # 使用ORM查询高消费用户
        high_spenders = client.query(User).filter("id IN (SELECT DISTINCT user_id FROM orders WHERE total_amount > 1000)").all()
        print("消费超过1000的用户:")
        for user in high_spenders:
            print(f"  - {user.name} ({user.email})")
        
        # 8. JOIN 查询
        print("\n=== 8. JOIN 查询 ===")
        
        # 使用ORM的JOIN功能
        order_details = client.query(Order).join("users", "orders.user_id = users.id").join("products", "orders.product_id = products.id").filter("orders.status = ?", "已完成").order_by("orders.order_date DESC").all()
        print("订单详情 (用户-产品):")
        for order in order_details:
            print(f"  - 订单ID: {order.id}, 金额: ¥{order.total_amount}, 时间: {order.order_date}")
        
        # 9. 复杂条件组合示例
        print("\n=== 9. 复杂条件组合示例 ===")
        
        # 使用ORM的复杂条件组合
        complex_orders = client.query(Order).join("users", "orders.user_id = users.id").join("products", "orders.product_id = products.id").filter("users.age BETWEEN ? AND ?", 25, 35).filter("products.category = ?", "电子产品").filter("orders.total_amount > ?", 1000).filter("orders.status = ?", "已完成").order_by("orders.total_amount DESC").all()
        print("25-35岁用户购买电子产品且金额>1000的订单:")
        for order in complex_orders:
            print(f"  - 订单ID: {order.id}, 金额: ¥{order.total_amount}")
        
        # 10. 窗口函数查询
        print("\n=== 10. 窗口函数查询 ===")
        
        # 使用ORM查询订单排名
        ranked_orders = client.query(Order).join("users", "orders.user_id = users.id").filter("orders.status = ?", "已完成").order_by("orders.total_amount DESC").all()
        print("订单金额排名:")
        for i, order in enumerate(ranked_orders, 1):
            print(f"  - 排名{i}: 订单ID {order.id}, 金额: ¥{order.total_amount}")
        
        # 11. 动态条件构建
        print("\n=== 11. 动态条件构建 ===")
        
        def build_dynamic_query(filters):
            """动态构建查询条件"""
            query = client.query(Product)
            
            if filters.get('category'):
                query = query.filter("category = ?", filters['category'])
            
            if filters.get('min_price'):
                query = query.filter("price >= ?", filters['min_price'])
            
            if filters.get('max_price'):
                query = query.filter("price <= ?", filters['max_price'])
            
            if filters.get('min_stock'):
                query = query.filter("stock >= ?", filters['min_stock'])
            
            return query
        
        # 使用动态查询
        filters = {
            'category': '电子产品',
            'min_price': 1000,
            'max_price': 5000,
            'min_stock': 50
        }
        
        filtered_products = build_dynamic_query(filters).all()
        print(f"动态过滤结果 (电子产品, 价格1000-5000, 库存>=50): {len(filtered_products)} 个")
        for product in filtered_products:
            print(f"  - {product.name}: ¥{product.price}, 库存: {product.stock}")
        
        # 12. 性能优化查询
        print("\n=== 12. 性能优化查询 ===")
        
        # 使用LIMIT和OFFSET进行分页
        page_size = 2
        page = 1
        offset = (page - 1) * page_size
        
        paged_orders = client.query(Order).filter("status = ?", "已完成").order_by("order_date DESC").limit(page_size).offset(offset).all()
        print(f"分页查询 (第{page}页, 每页{page_size}条):")
        for order in paged_orders:
            print(f"  - 订单ID: {order.id}, 金额: ¥{order.total_amount}, 时间: {order.order_date}")
        
        # 13. 复杂条件组合
        print("\n=== 13. 复杂条件组合 ===")
        
        # 多条件AND组合
        complex_products_query = client.query(Product).filter("category = ?", "电子产品").filter("price > ?", 1000).filter("stock > ?", 50).order_by("price DESC")
        print(f"多条件AND组合SQL: {complex_products_query._build_sql()[0]}")
        print(f"参数: {complex_products_query._build_sql()[1]}")
        complex_products = complex_products_query.all()
        print("电子产品且价格>1000且库存>50:")
        for product in complex_products:
            print(f"  - {product.name}: ¥{product.price}, 库存: {product.stock}")
        
        # 多条件OR组合
        print("\n=== 多条件OR组合 ===")
        or_products_query = client.query(Product).filter("category = ? OR price > ? OR stock < ?", "图书", 2000, 200)
        print(f"多条件OR组合SQL: {or_products_query._build_sql()[0]}")
        print(f"参数: {or_products_query._build_sql()[1]}")
        or_products = or_products_query.all()
        print("图书类别 OR 价格>2000 OR 库存<200:")
        for product in or_products:
            print(f"  - {product.name}: ¥{product.price}, 类别: {product.category}, 库存: {product.stock}")
        
        # 混合AND/OR条件
        print("\n=== 混合AND/OR条件 ===")
        mixed_query = client.query(Product).filter("(category = ? OR category = ?) AND price > ?", "电子产品", "图书", 50)
        print(f"混合AND/OR条件SQL: {mixed_query._build_sql()[0]}")
        print(f"参数: {mixed_query._build_sql()[1]}")
        mixed_products = mixed_query.all()
        print("(电子产品 OR 图书) AND 价格>50:")
        for product in mixed_products:
            print(f"  - {product.name}: ¥{product.price}, 类别: {product.category}")
        
        # 复杂嵌套条件
        print("\n=== 复杂嵌套条件 ===")
        nested_query = client.query(Product).filter("(category = ? AND price > ?) OR (category = ? AND stock > ?)", "电子产品", 1000, "图书", 100)
        print(f"复杂嵌套条件SQL: {nested_query._build_sql()[0]}")
        print(f"参数: {nested_query._build_sql()[1]}")
        nested_products = nested_query.all()
        print("(电子产品 AND 价格>1000) OR (图书 AND 库存>100):")
        for product in nested_products:
            print(f"  - {product.name}: ¥{product.price}, 类别: {product.category}, 库存: {product.stock}")
        
        # 14. 链式查询
        print("\n=== 14. 链式查询 ===")
        
        # 链式调用多个方法
        chain_query = (client.query(User)
                      .filter("age >= ?", 25)
                      .filter("city IN (?, ?)", "北京", "上海")
                      .order_by("age ASC")
                      .limit(3))
        
        print(f"链式查询SQL: {chain_query._build_sql()[0]}")
        print(f"参数: {chain_query._build_sql()[1]}")
        chain_results = chain_query.all()
        print("链式查询结果 (25岁以上, 北京或上海, 前3名):")
        for user in chain_results:
            print(f"  - {user.name} ({user.age}岁, {user.city})")
        
        # 15. 聚合查询
        print("\n=== 15. 聚合查询 ===")
        
        # 统计查询
        total_products = client.query(Product).count()
        
        avg_price_query = client.query(Product).select("AVG(price) as avg_price")
        print(f"平均价格查询SQL: {avg_price_query._build_sql()[0]}")
        avg_price = avg_price_query.first()
        
        max_price_query = client.query(Product).select("MAX(price) as max_price")
        print(f"最高价格查询SQL: {max_price_query._build_sql()[0]}")
        max_price = max_price_query.first()
        
        min_price_query = client.query(Product).select("MIN(price) as min_price")
        print(f"最低价格查询SQL: {min_price_query._build_sql()[0]}")
        min_price = min_price_query.first()
        
        print(f"产品统计:")
        print(f"  - 总数量: {total_products}")
        print(f"  - 平均价格: ¥{avg_price[0]:.2f}")
        print(f"  - 最高价格: ¥{max_price[0]}")
        print(f"  - 最低价格: ¥{min_price[0]}")
        
        # 16. 条件查询
        print("\n=== 16. 条件查询 ===")
        
        # 使用filter_by进行条件查询
        electronics_by_filter_query = client.query(Product).filter_by(category="电子产品")
        print(f"filter_by查询SQL: {electronics_by_filter_query._build_sql()[0]}")
        print(f"参数: {electronics_by_filter_query._build_sql()[1]}")
        electronics_by_filter = electronics_by_filter_query.all()
        print(f"使用filter_by查询电子产品: {len(electronics_by_filter)} 个")
        
        # 使用filter进行复杂条件查询
        expensive_electronics_query = client.query(Product).filter("category = ? AND price > ?", "电子产品", 2000)
        print(f"filter查询SQL: {expensive_electronics_query._build_sql()[0]}")
        print(f"参数: {expensive_electronics_query._build_sql()[1]}")
        expensive_electronics = expensive_electronics_query.all()
        print(f"使用filter查询高价电子产品: {len(expensive_electronics)} 个")
        
        # 17. 排序和限制
        print("\n=== 17. 排序和限制 ===")
        
        # 按价格排序
        price_sorted_query = client.query(Product).order_by("price DESC").limit(3)
        print(f"价格排序SQL: {price_sorted_query._build_sql()[0]}")
        price_sorted = price_sorted_query.all()
        print("价格最高的3个产品:")
        for product in price_sorted:
            print(f"  - {product.name}: ¥{product.price}")
        
        # 按库存排序
        stock_sorted_query = client.query(Product).order_by("stock ASC").limit(3)
        print(f"库存排序SQL: {stock_sorted_query._build_sql()[0]}")
        stock_sorted = stock_sorted_query.all()
        print("库存最少的3个产品:")
        for product in stock_sorted:
            print(f"  - {product.name}: 库存 {product.stock}")
        
        # 18. 多表关联查询
        print("\n=== 18. 多表关联查询 ===")
        
        # 查询有订单的用户
        users_with_orders_query = client.query(User).filter("id IN (SELECT DISTINCT user_id FROM orders)")
        print(f"有订单用户查询SQL: {users_with_orders_query._build_sql()[0]}")
        users_with_orders = users_with_orders_query.all()
        print(f"有订单的用户: {len(users_with_orders)} 人")
        for user in users_with_orders:
            print(f"  - {user.name} ({user.email})")
        
        # 查询被订购的产品
        ordered_products_query = client.query(Product).filter("id IN (SELECT DISTINCT product_id FROM orders)")
        print(f"被订购产品查询SQL: {ordered_products_query._build_sql()[0]}")
        ordered_products = ordered_products_query.all()
        print(f"被订购的产品: {len(ordered_products)} 个")
        for product in ordered_products:
            print(f"  - {product.name} (¥{product.price})")
        
        # 19. 复杂过滤条件
        print("\n=== 19. 复杂过滤条件 ===")
        
        # 使用BETWEEN
        age_range_query = client.query(User).filter("age BETWEEN ? AND ?", 25, 35)
        print(f"BETWEEN查询SQL: {age_range_query._build_sql()[0]}")
        print(f"参数: {age_range_query._build_sql()[1]}")
        age_range = age_range_query.all()
        print(f"25-35岁用户: {len(age_range)} 人")
        
        # 使用LIKE
        email_pattern_query = client.query(User).filter("email LIKE ?", "%example.com%")
        print(f"LIKE查询SQL: {email_pattern_query._build_sql()[0]}")
        print(f"参数: {email_pattern_query._build_sql()[1]}")
        email_pattern = email_pattern_query.all()
        print(f"example.com邮箱用户: {len(email_pattern)} 人")
        
        # 使用IN
        target_cities = ["北京", "上海", "广州"]
        city_users_query = client.query(User).filter("city IN (?, ?, ?)", *target_cities)
        print(f"IN查询SQL: {city_users_query._build_sql()[0]}")
        print(f"参数: {city_users_query._build_sql()[1]}")
        city_users = city_users_query.all()
        print(f"目标城市用户: {len(city_users)} 人")
        
        # 20. 查询优化
        print("\n=== 20. 查询优化 ===")
        
        # 只选择需要的字段
        user_names_query = client.query(User).select("name, email").filter("age > ?", 30)
        print(f"选择字段查询SQL: {user_names_query._build_sql()[0]}")
        print(f"参数: {user_names_query._build_sql()[1]}")
        user_names = user_names_query.all()
        print("30岁以上用户的姓名和邮箱:")
        for user in user_names:
            # 选择字段查询返回的是RowData对象，使用索引访问
            print(f"  - {user[0]}: {user[1]}")
        
        # 使用first()获取单条记录
        first_user_query = client.query(User)
        print(f"first()查询SQL: {first_user_query._build_sql()[0]}")
        first_user = first_user_query.first()
        if first_user:
            print(f"第一个用户: {first_user.name}")
        
        # 使用count()统计数量
        electronics_count = client.query(Product).filter("category = ?", "电子产品").count()
        print(f"电子产品数量: {electronics_count}")
        
        # 21. OR条件的详细演示
        print("\n=== 21. OR条件的详细演示 ===")
        
        # 单个OR条件
        single_or_query = client.query(Product).filter("category = ? OR price > ?", "图书", 3000)
        print(f"单个OR条件SQL: {single_or_query._build_sql()[0]}")
        print(f"参数: {single_or_query._build_sql()[1]}")
        single_or_results = single_or_query.all()
        print("图书类别 OR 价格>3000:")
        for product in single_or_results:
            print(f"  - {product.name}: ¥{product.price}, 类别: {product.category}")
        
        # 多个OR条件
        multiple_or_query = client.query(Product).filter("category = ? OR category = ? OR category = ?", "电子产品", "图书", "生活用品")
        print(f"多个OR条件SQL: {multiple_or_query._build_sql()[0]}")
        print(f"参数: {multiple_or_query._build_sql()[1]}")
        multiple_or_results = multiple_or_query.all()
        print("电子产品 OR 图书 OR 生活用品:")
        for product in multiple_or_results:
            print(f"  - {product.name}: ¥{product.price}, 类别: {product.category}")
        
        # 复杂OR条件组合
        complex_or_query = client.query(Product).filter("(category = ? AND price > ?) OR (category = ? AND stock < ?)", "电子产品", 2000, "图书", 400)
        print(f"复杂OR条件SQL: {complex_or_query._build_sql()[0]}")
        print(f"参数: {complex_or_query._build_sql()[1]}")
        complex_or_results = complex_or_query.all()
        print("(电子产品 AND 价格>2000) OR (图书 AND 库存<400):")
        for product in complex_or_results:
            print(f"  - {product.name}: ¥{product.price}, 类别: {product.category}, 库存: {product.stock}")
        
        # 22. 动态OR条件构建
        print("\n=== 22. 动态OR条件构建 ===")
        
        def build_or_query(categories, min_price=None, max_stock=None):
            """动态构建OR查询条件"""
            conditions = []
            params = []
            
            # 添加类别条件
            if categories:
                placeholders = ", ".join(["?" for _ in categories])
                conditions.append(f"category IN ({placeholders})")
                params.extend(categories)
            
            # 添加价格条件
            if min_price is not None:
                conditions.append("price > ?")
                params.append(min_price)
            
            # 添加库存条件
            if max_stock is not None:
                conditions.append("stock < ?")
                params.append(max_stock)
            
            if conditions:
                or_condition = " OR ".join(conditions)
                return client.query(Product).filter(or_condition, *params)
            else:
                return client.query(Product)
        
        # 使用动态OR查询
        dynamic_or_query = build_or_query(["电子产品", "图书"], min_price=100, max_stock=300)
        print(f"动态OR查询SQL: {dynamic_or_query._build_sql()[0]}")
        print(f"参数: {dynamic_or_query._build_sql()[1]}")
        dynamic_or_results = dynamic_or_query.all()
        print("动态OR查询结果 (电子产品/图书 OR 价格>100 OR 库存<300):")
        for product in dynamic_or_results:
            print(f"  - {product.name}: ¥{product.price}, 类别: {product.category}, 库存: {product.stock}")
        
        # 清理
        print("\n=== 清理测试数据 ===")
        client.execute("DROP DATABASE IF EXISTS complex_filter_demo")
        print("✓ 测试数据清理完成")
        
    except Exception as e:
        print(f"错误: {e}")
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()

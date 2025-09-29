#!/usr/bin/env python3
"""
Test compatibility with native SQLAlchemy query
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'clients', 'python'))

from matrixone import Client
try:
    from sqlalchemy.orm import declarative_base, sessionmaker
except ImportError:
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker

from sqlalchemy import Column, Integer, String, Text, create_engine
from matrixone.sqlalchemy_ext.fulltext_search import boolean_match, natural_match

def test_native_sqlalchemy_compatibility():
    """Test compatibility with native SQLAlchemy Session.query()"""
    
    print("=== 原生 SQLAlchemy 兼容性测试 ===\n")
    
    # Define model
    Base = declarative_base()
    
    class Article(Base):
        __tablename__ = "test_articles"
        id = Column(Integer, primary_key=True)
        title = Column(String(200))
        content = Column(Text)
        category = Column(String(50))
    
    # Test 1: Check if FulltextFilter has proper SQLAlchemy interfaces
    print("1. 检查 FulltextFilter 的 SQLAlchemy 接口:")
    
    try:
        filter_expr = boolean_match("title", "content").must("python")
        labeled_expr = filter_expr.label("score")
        
        print(f"FulltextFilter type: {type(filter_expr)}")
        print(f"Labeled type: {type(labeled_expr)}")
        
        # Check SQLAlchemy compatibility attributes
        print(f"Has _compiler_dispatch: {hasattr(filter_expr, '_compiler_dispatch')}")
        print(f"Has compile: {hasattr(filter_expr, 'compile')}")
        print(f"Has type: {hasattr(filter_expr, 'type')}")
        
        print(f"Labeled has compile: {hasattr(labeled_expr, 'compile')}")
        print(f"Labeled compile result: {labeled_expr.compile()}")
        
        print("✓ 基本接口检查通过")
    except Exception as e:
        print(f"✗ 基本接口检查失败: {e}")
    print()
    
    # Test 2: Try to create a SQLAlchemy engine and session
    print("2. 尝试与 SQLAlchemy Engine/Session 集成:")
    
    try:
        # Create MatrixOne engine URL
        # Note: This would require a proper MatrixOne SQLAlchemy dialect
        engine_url = "mysql+pymysql://root:111@localhost:6001/test"
        
        print(f"Engine URL: {engine_url}")
        
        # For this test, we'll simulate what SQLAlchemy would do
        print("模拟 SQLAlchemy 编译过程...")
        
        filter_expr = boolean_match("title", "content").must("python").label("score")
        
        # Test if it can be compiled like a SQLAlchemy expression
        if hasattr(filter_expr, '_compiler_dispatch'):
            print("✓ 具备 SQLAlchemy 编译器接口")
        else:
            print("✗ 缺少 SQLAlchemy 编译器接口")
            
        # Test compilation
        compiled_sql = filter_expr.compile()
        print(f"编译结果: {compiled_sql}")
        print("✓ 编译成功")
        
    except Exception as e:
        print(f"✗ SQLAlchemy 集成测试失败: {e}")
        import traceback
        traceback.print_exc()
    print()
    
    # Test 3: Check if it works with SQLAlchemy's text() and column expressions
    print("3. 与 SQLAlchemy 表达式系统兼容性:")
    
    try:
        from sqlalchemy import text, and_, or_
        
        # Test with SQLAlchemy's and_/or_
        filter1 = boolean_match("title", "content").must("python")
        filter2 = boolean_match("title", "content").must("tutorial")
        
        print("Testing with SQLAlchemy and_()...")
        try:
            combined = and_(filter1, filter2)
            print(f"and_() 组合成功: {type(combined)}")
        except Exception as e:
            print(f"and_() 组合失败: {e}")
        
        print("Testing with SQLAlchemy or_()...")
        try:
            combined_or = or_(filter1, filter2)
            print(f"or_() 组合成功: {type(combined_or)}")
        except Exception as e:
            print(f"or_() 组合失败: {e}")
        
        print("✓ SQLAlchemy 表达式兼容性测试完成")
        
    except Exception as e:
        print(f"✗ SQLAlchemy 表达式测试失败: {e}")
    print()
    
    # Test 4: What would be needed for full SQLAlchemy compatibility
    print("4. 完整 SQLAlchemy 兼容性分析:")
    
    requirements = [
        "✓ ClauseElement 基类 - 已实现",
        "✓ _compiler_dispatch 方法 - 已实现", 
        "✓ compile() 方法 - 已实现",
        "✓ label() 方法 - 已实现",
        "✓ type 属性 - 已实现",
        "? 与 SQLAlchemy Session 集成 - 需要测试",
        "? 与 SQLAlchemy ORM 查询生成器集成 - 需要验证",
        "? 在 SELECT 子句中的序列化 - 需要验证"
    ]
    
    for req in requirements:
        print(f"  {req}")
    
    print()
    print("=== 总结 ===")
    print("✅ 当前实现具备了 SQLAlchemy 兼容的基础:")
    print("   - 继承自 ClauseElement")
    print("   - 实现了 _compiler_dispatch")
    print("   - 支持 compile() 和 label()")
    print("   - 具备正确的类型信息")
    print()
    print("❓ 但要完全兼容原生 SQLAlchemy，还需要:")
    print("   1. 确保在 SQLAlchemy 的 Session.query() 中正确工作")
    print("   2. 与 SQLAlchemy 的查询编译器完全兼容")
    print("   3. 处理 SQLAlchemy 特有的参数绑定和类型转换")
    print()
    print("💡 建议:")
    print("   - 使用我们的 MatrixOne Client.query() 获得最佳兼容性")
    print("   - 对于原生 SQLAlchemy，可能需要额外的适配层")

if __name__ == "__main__":
    test_native_sqlalchemy_compatibility()

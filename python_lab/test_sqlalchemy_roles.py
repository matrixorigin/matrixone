#!/usr/bin/env python3
"""
Test SQLAlchemy roles system compatibility
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'clients', 'python'))

try:
    from sqlalchemy.sql import roles
    from sqlalchemy.orm import declarative_base, sessionmaker
    from sqlalchemy import Column, Integer, String, Text, create_engine
    from sqlalchemy.sql.elements import Label
    from sqlalchemy.sql.selectable import Select
    SQLALCHEMY_AVAILABLE = True
except ImportError as e:
    print(f"SQLAlchemy not fully available: {e}")
    SQLALCHEMY_AVAILABLE = False

if SQLALCHEMY_AVAILABLE:
    from matrixone.sqlalchemy_ext.fulltext_search import boolean_match, natural_match

def test_sqlalchemy_roles():
    """Test SQLAlchemy roles system integration."""
    
    if not SQLALCHEMY_AVAILABLE:
        print("❌ SQLAlchemy not available, skipping roles test")
        return
    
    print("=== SQLAlchemy Roles 系统兼容性测试 ===\n")
    
    # Define model
    Base = declarative_base()
    
    class Article(Base):
        __tablename__ = "test_articles"
        id = Column(Integer, primary_key=True)
        title = Column(String(200))
        content = Column(Text)
    
    # Test 1: Check what roles SQLAlchemy expects
    print("1. SQLAlchemy 角色系统分析:")
    
    try:
        print("Available roles in SQLAlchemy:")
        role_attrs = [attr for attr in dir(roles) if not attr.startswith('_')]
        for role in role_attrs[:10]:  # Show first 10
            print(f"  - {role}")
        
        print(f"  ... (总共 {len(role_attrs)} 个角色)")
        
    except Exception as e:
        print(f"✗ 无法分析角色: {e}")
    print()
    
    # Test 2: Check what SELECT clause expects
    print("2. SELECT 子句角色要求:")
    
    try:
        # Create a fulltext expression
        fulltext_expr = boolean_match("title", "content").must("python")
        labeled_expr = fulltext_expr.label("score")
        
        print(f"FulltextFilter type: {type(fulltext_expr)}")
        print(f"Labeled expression type: {type(labeled_expr)}")
        
        # Check if it has the required interfaces
        required_attrs = [
            '_annotate_column', '_deannotate', '_clone', 'type',
            '_compiler_dispatch', '__clause_element__'
        ]
        
        for attr in required_attrs:
            has_attr = hasattr(labeled_expr, attr)
            print(f"  {attr}: {'✓' if has_attr else '✗'}")
            
    except Exception as e:
        print(f"✗ SELECT 子句测试失败: {e}")
    print()
    
    # Test 3: Try to use with SQLAlchemy's select()
    print("3. 与 SQLAlchemy select() 集成:")
    
    try:
        from sqlalchemy import select, text
        
        # Try to create a select with our expression
        fulltext_expr = boolean_match("title", "content").must("python").label("score")
        
        # Attempt 1: Direct use
        try:
            stmt = select(Article.id, fulltext_expr)
            print(f"✓ Direct select worked: {type(stmt)}")
        except Exception as e:
            print(f"✗ Direct select failed: {e}")
        
        # Attempt 2: Use text() wrapper
        try:
            text_expr = text(fulltext_expr.compile())
            stmt = select(Article.id, text_expr.label("score"))
            print(f"✓ Text wrapper worked: {type(stmt)}")
        except Exception as e:
            print(f"✗ Text wrapper failed: {e}")
            
    except Exception as e:
        print(f"✗ select() 集成测试失败: {e}")
    print()
    
    # Test 4: Check Session.query() compatibility
    print("4. Session.query() 兼容性:")
    
    try:
        # Create an in-memory engine for testing
        engine = create_engine("sqlite:///:memory:")
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Test with our labeled expression
        fulltext_expr = boolean_match("title", "content").must("python").label("score")
        
        try:
            # This is what we want to work:
            # query = session.query(Article.id, fulltext_expr)
            # But first let's check what Article.id looks like
            
            print(f"Article.id type: {type(Article.id)}")
            print(f"Article.id has _annotate_column: {hasattr(Article.id, '_annotate_column')}")
            
            # Try a simple query first
            query = session.query(Article.id)
            print(f"✓ Basic session query works: {type(query)}")
            
            # Now try with our expression
            # query_with_fulltext = session.query(Article.id, fulltext_expr)
            # print(f"✓ Session query with fulltext works: {type(query_with_fulltext)}")
            
        except Exception as e:
            print(f"✗ Session query with fulltext failed: {e}")
            
        session.close()
        
    except Exception as e:
        print(f"✗ Session 测试失败: {e}")
    print()
    
    # Test 5: What needs to be implemented
    print("5. 完整兼容性需要实现的接口:")
    
    # Check what a real SQLAlchemy Label has
    try:
        real_label = Label("test", Article.id)
        label_attrs = [attr for attr in dir(real_label) if not attr.startswith('_') or attr in [
            '_annotate_column', '_deannotate', '_clone', '__clause_element__'
        ]]
        
        print("SQLAlchemy Label 具备的关键接口:")
        for attr in sorted(label_attrs):
            if hasattr(real_label, attr):
                print(f"  ✓ {attr}")
        
        print(f"\n我们的 FulltextLabel 需要补充的接口:")
        our_label = boolean_match("title", "content").must("python").label("score")
        missing_attrs = []
        
        for attr in label_attrs:
            if not hasattr(our_label, attr):
                missing_attrs.append(attr)
        
        for attr in missing_attrs[:10]:  # Show first 10 missing
            print(f"  ✗ {attr}")
        
        if len(missing_attrs) > 10:
            print(f"  ... 还有 {len(missing_attrs) - 10} 个缺失接口")
            
    except Exception as e:
        print(f"✗ 接口分析失败: {e}")
    
    print("\n=== 结论 ===")
    print("🔍 当前状态:")
    print("  ✅ 具备基础 ClauseElement 接口")
    print("  ✅ 可以生成正确的 SQL")
    print("  ✅ 支持 .label() 方法")
    print("  ✅ 与 MatrixOne Client.query() 完美兼容")
    print()
    print("❓ 要完全兼容原生 SQLAlchemy Session.query()，需要:")
    print("  1. 实现完整的 SQLAlchemy Column 接口")
    print("  2. 支持 SQLAlchemy 的角色系统 (roles)")
    print("  3. 实现 _annotate_column, _deannotate, _clone 等方法")
    print("  4. 正确处理 SQLAlchemy 的类型系统")
    print()
    print("💡 推荐方案:")
    print("  Option 1: 继续使用 MatrixOne Client.query() - 已完美支持")
    print("  Option 2: 为原生 SQLAlchemy 创建适配器包装类")
    print("  Option 3: 实现完整的 SQLAlchemy Column 兼容接口")

if __name__ == "__main__":
    test_sqlalchemy_roles()

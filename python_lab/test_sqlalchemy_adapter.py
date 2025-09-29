#!/usr/bin/env python3
"""
Create a SQLAlchemy adapter for FulltextFilter to work with native SQLAlchemy
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'clients', 'python'))

try:
    from sqlalchemy.sql.elements import Label
    from sqlalchemy.sql import text
    from sqlalchemy.orm import declarative_base, sessionmaker
    from sqlalchemy import Column, Integer, String, Text, create_engine, select
    from sqlalchemy.sql.type_api import TypeEngine
    SQLALCHEMY_AVAILABLE = True
except ImportError as e:
    print(f"SQLAlchemy not fully available: {e}")
    SQLALCHEMY_AVAILABLE = False

if SQLALCHEMY_AVAILABLE:
    from matrixone.sqlalchemy_ext.fulltext_search import boolean_match, natural_match

def create_sqlalchemy_adapter():
    """Create an adapter to make FulltextFilter work with native SQLAlchemy."""
    
    if not SQLALCHEMY_AVAILABLE:
        print("❌ SQLAlchemy not available")
        return
    
    print("=== 创建 SQLAlchemy 适配器 ===\n")
    
    # Create a simple adapter class
    class FulltextAdapter:
        """Adapter to make FulltextFilter compatible with native SQLAlchemy."""
        
        def __init__(self, fulltext_filter):
            self.fulltext_filter = fulltext_filter
            self._sql = fulltext_filter.compile()
        
        def as_text_clause(self):
            """Convert to SQLAlchemy text clause."""
            return text(self._sql)
        
        def as_labeled_text(self, name):
            """Convert to labeled text clause."""
            return text(self._sql).label(name)
        
        def for_select(self, alias=None):
            """Make it suitable for SELECT clause."""
            if alias:
                return text(self._sql).label(alias)
            else:
                return text(self._sql)
    
    # Test the adapter
    print("1. 创建适配器:")
    
    try:
        # Create a fulltext expression
        fulltext_expr = boolean_match("title", "content").must("python").encourage("tutorial")
        adapter = FulltextAdapter(fulltext_expr)
        
        print(f"Original SQL: {fulltext_expr.compile()}")
        print(f"Adapter SQL: {adapter._sql}")
        print("✓ 适配器创建成功")
        
    except Exception as e:
        print(f"✗ 适配器创建失败: {e}")
        return
    print()
    
    # Test with SQLAlchemy select()
    print("2. 与 SQLAlchemy select() 集成:")
    
    try:
        Base = declarative_base()
        
        class Article(Base):
            __tablename__ = "test_articles"
            id = Column(Integer, primary_key=True)
            title = Column(String(200))
            content = Column(Text)
        
        # Create adapted expression
        fulltext_expr = boolean_match("title", "content").must("python")
        adapter = FulltextAdapter(fulltext_expr)
        text_expr = adapter.as_labeled_text("score")
        
        # Try with select()
        stmt = select(Article.id, Article.title, text_expr)
        
        print(f"✓ select() 成功: {type(stmt)}")
        print(f"SQL: {stmt}")
        
    except Exception as e:
        print(f"✗ select() 失败: {e}")
    print()
    
    # Test with Session.query()
    print("3. 与 Session.query() 集成:")
    
    try:
        # Create in-memory engine
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Create adapted expression
        fulltext_expr = boolean_match("title", "content").must("machine", "learning")
        adapter = FulltextAdapter(fulltext_expr)
        score_expr = adapter.as_labeled_text("ml_score")
        
        # Try with session.query()
        query = session.query(Article.id, Article.title, score_expr)
        
        print(f"✓ Session.query() 成功: {type(query)}")
        print(f"Query SQL 预期包含: MATCH(title, content) AGAINST")
        
        session.close()
        
    except Exception as e:
        print(f"✗ Session.query() 失败: {e}")
        import traceback
        traceback.print_exc()
    print()
    
    # Create convenience functions
    print("4. 创建便利函数:")
    
    def sqlalchemy_boolean_match(*columns, **kwargs):
        """Create SQLAlchemy-compatible boolean match expression."""
        fulltext_expr = boolean_match(*columns, **kwargs)
        return FulltextAdapter(fulltext_expr)
    
    def sqlalchemy_natural_match(*columns, **kwargs):
        """Create SQLAlchemy-compatible natural match expression."""
        fulltext_expr = natural_match(*columns, **kwargs)
        return FulltextAdapter(fulltext_expr)
    
    try:
        # Test convenience functions
        expr1 = sqlalchemy_boolean_match("title", "content").fulltext_filter.must("python")
        adapter1 = FulltextAdapter(expr1)
        labeled1 = adapter1.as_labeled_text("python_score")
        
        expr2 = sqlalchemy_natural_match("title", "content", query="machine learning")
        adapter2 = FulltextAdapter(expr2)
        labeled2 = adapter2.as_labeled_text("ml_score")
        
        print("✓ 便利函数创建成功")
        print(f"Boolean match: {adapter1._sql}")
        print(f"Natural match: {adapter2._sql}")
        
    except Exception as e:
        print(f"✗ 便利函数失败: {e}")
    print()
    
    print("=== 使用指南 ===")
    print("🎯 对于 **原生 SQLAlchemy** 用户:")
    print()
    print("```python")
    print("# 1. 创建适配器")
    print("from matrixone.sqlalchemy_ext.fulltext_search import boolean_match")
    print("fulltext_expr = boolean_match('title', 'content').must('python')")
    print("adapter = FulltextAdapter(fulltext_expr)")
    print("score_expr = adapter.as_labeled_text('score')")
    print()
    print("# 2. 用于 SQLAlchemy select()")
    print("stmt = select(Article.id, Article.title, score_expr)")
    print()
    print("# 3. 用于 Session.query()")
    print("results = session.query(Article.id, Article.title, score_expr).all()")
    print("```")
    print()
    print("🚀 对于 **MatrixOne Client** 用户 (推荐):")
    print()
    print("```python")
    print("# 直接使用，无需适配器")
    print("results = client.query(")
    print("    Article, ")
    print("    Article.id, ")
    print("    boolean_match('title', 'content').must('python').label('score')")
    print(").all()")
    print("```")
    print()
    print("✅ **总结**:")
    print("  - MatrixOne Client.query(): 原生支持，推荐使用")
    print("  - 原生 SQLAlchemy: 通过 FulltextAdapter 适配器支持")
    print("  - 两种方式都能生成正确的 MATCH() AGAINST() SQL")

if __name__ == "__main__":
    create_sqlalchemy_adapter()

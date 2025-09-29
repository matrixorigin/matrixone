#!/usr/bin/env python3
"""
示例：如何使用新的简化全文搜索接口替换旧的复杂接口

演示从复杂的fulltext search builder迁移到简化的simple_query接口
"""

import sys
import os

# Add the parent directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'clients', 'python'))

from matrixone.client import Client


def demonstrate_simple_fulltext_interface():
    """演示新的简化全文搜索接口的使用方法"""
    
    print("🚀 MatrixOne 简化全文搜索接口演示")
    print("=" * 60)
    
    # 创建客户端（这里只演示SQL生成，不实际连接数据库）
    client = Client()
    from matrixone.client import FulltextIndexManager
    client._fulltext_index = FulltextIndexManager(client)
    
    print("\n📝 1. 基础自然语言搜索")
    print("-" * 30)
    
    # 简单搜索
    sql = (client.fulltext_index.simple_query("articles")
           .columns("title", "content") 
           .search("机器学习入门教程")
           .explain())
    print(f"生成的SQL: {sql}")
    print("用途: 基础的自然语言全文搜索，适合大多数搜索场景")
    
    print("\n📝 2. 布尔模式搜索")
    print("-" * 30)
    
    # 必须包含某些词，排除某些词
    sql = (client.fulltext_index.simple_query("articles")
           .columns("title", "content", "tags")
           .must_have("python", "教程")
           .must_not_have("过时", "弃用")
           .explain())
    print(f"生成的SQL: {sql}")
    print("用途: 精确控制搜索词，必须包含某些词，排除某些词")
    
    print("\n📝 3. 带相关性评分的搜索")
    print("-" * 30)
    
    # 包含相关性评分
    sql = (client.fulltext_index.simple_query("documents")
           .columns("title", "body", "tags")
           .search("人工智能深度学习")
           .with_score("相关性")
           .order_by_score()
           .limit(10)
           .explain())
    print(f"生成的SQL: {sql}")
    print("用途: 获取搜索结果的相关性评分，按相关性排序")
    
    print("\n📝 4. 复杂条件组合搜索")
    print("-" * 30)
    
    # 复杂查询：全文搜索 + 筛选条件 + 排序 + 分页
    sql = (client.fulltext_index.simple_query("articles")
           .columns("title", "content", "summary")
           .must_have("数据科学", "分析")
           .must_not_have("入门", "基础")
           .with_score("评分")
           .where("category = 'AI'")
           .where("status = 'published'")
           .order_by_score()
           .limit(20, 10)
           .explain())
    print(f"生成的SQL: {sql}")
    print("用途: 复杂的企业级搜索，结合全文搜索、筛选、排序和分页")
    
    print("\n📝 5. 实际使用示例")
    print("-" * 30)
    
    print("# 连接数据库并执行搜索")
    print("client = Client()")
    print("client.connect(host='127.0.0.1', port=6001, user='root', password='111', database='mydb')")
    print("")
    print("# 搜索包含'机器学习'的文章，按相关性排序")
    print("results = (client.fulltext_index.simple_query('articles')")
    print("           .columns('title', 'content')")
    print("           .search('机器学习')")
    print("           .with_score()")
    print("           .order_by_score()")
    print("           .limit(10)")
    print("           .execute())")
    print("")
    print("# 处理结果")
    print("for row in results.rows:")
    print("    title, content, score = row[1], row[2], row[-1]")
    print("    print(f'标题: {title}, 相关性: {score}')")


def compare_old_vs_new_interfaces():
    """对比旧接口和新接口的差异"""
    
    print("\n🔄 接口对比：旧 vs 新")
    print("=" * 60)
    
    client = Client()
    from matrixone.client import FulltextIndexManager
    client._fulltext_index = FulltextIndexManager(client)
    
    print("\n❌ 旧方式 (复杂, 需要深入了解全文搜索细节):")
    print("-" * 40)
    print("""
# 需要导入多个类和了解复杂概念
from matrixone.sqlalchemy_ext.fulltext_search import (
    FulltextSearchBuilder, 
    FulltextSearchMode,
    boolean_match,
    natural_match
)

# 复杂的builder设置
builder = FulltextSearchBuilder(client)
builder.table("articles") \\
       .columns(["title", "content"]) \\
       .with_mode(FulltextSearchMode.BOOLEAN) \\
       .add_term("python", required=True) \\
       .add_term("deprecated", excluded=True) \\
       .with_score() \\
       .order_by("score", "DESC") \\
       .limit(10)

# 或者使用ORM风格但需要理解复杂的匹配语法
filter_obj = boolean_match("title", "content").must("python").must_not("deprecated")
""")
    
    print("\n✅ 新方式 (简单, 直观):")
    print("-" * 40)
    print("""
# 只需要使用client，一行导入
from matrixone.client import Client

# 简洁直观的链式调用
results = client.fulltext_index.simple_query("articles") \\
    .columns("title", "content") \\
    .must_have("python") \\
    .must_not_have("deprecated") \\
    .with_score() \\
    .order_by_score() \\
    .limit(10) \\
    .execute()
""")
    
    print("\n📊 对比总结:")
    print("-" * 40)
    print("旧方式:")
    print("  ❌ 需要导入多个类和枚举")
    print("  ❌ 需要理解FulltextSearchMode等概念")
    print("  ❌ 方法名不够直观 (add_term vs must_have)")
    print("  ❌ 需要手动处理SQL构建")
    print("")
    print("新方式:")
    print("  ✅ 只需要一个simple_query方法")
    print("  ✅ 方法名语义化 (must_have, must_not_have)")
    print("  ✅ 自动生成符合MatrixOne语法的SQL")
    print("  ✅ 完全链式调用，代码更简洁")
    print("  ✅ 基于MatrixOne实际测试用例设计")


def migration_checklist():
    """迁移检查清单"""
    
    print("\n📋 迁移检查清单")
    print("=" * 60)
    
    print("\n1. ✅ 替换imports:")
    print("   旧: from matrixone.sqlalchemy_ext.fulltext_search import ...")
    print("   新: from matrixone.client import Client  # 已包含所有需要的功能")
    
    print("\n2. ✅ 替换搜索方法:")
    print("   旧: client.fulltext_index.search().table(...).columns(...)")
    print("   新: client.fulltext_index.simple_query(table).columns(...)")
    
    print("\n3. ✅ 替换查询语法:")
    print("   旧: .add_term('word', required=True)")
    print("   新: .must_have('word')")
    print("")
    print("   旧: .add_term('word', excluded=True)")
    print("   新: .must_not_have('word')")
    
    print("\n4. ✅ 替换评分:")
    print("   旧: .with_score().order_by('score', 'DESC')")
    print("   新: .with_score().order_by_score()")
    
    print("\n5. ✅ 验证SQL兼容性:")
    print("   使用 .explain() 方法查看生成的SQL")
    print("   确保生成的SQL符合MatrixOne语法要求")
    
    print("\n6. ✅ 测试功能:")
    print("   运行offline测试验证SQL正确性")
    print("   运行online测试验证结果正确性")


def main():
    """主函数"""
    try:
        demonstrate_simple_fulltext_interface()
        compare_old_vs_new_interfaces()
        migration_checklist()
        
        print("\n" + "=" * 60)
        print("🎉 简化全文搜索接口演示完成!")
        print("\n核心优势:")
        print("  🚀 易于使用 - 无需深入了解全文搜索内部机制")
        print("  🎯 专注MatrixOne - 基于真实测试用例设计")
        print("  🔗 链式调用 - 代码简洁直观")
        print("  ✅ 类型安全 - 完整的类型提示支持")
        print("  📊 即时验证 - 支持explain()查看生成的SQL")
        
    except Exception as e:
        print(f"\n❌ 演示过程中出错: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    return 0


if __name__ == "__main__":
    sys.exit(main())

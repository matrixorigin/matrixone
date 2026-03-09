"""
Example: Branch Statement Builders - SQLAlchemy Style API

This example demonstrates the new branch statement builders that work like
SQLAlchemy's select(), insert(), delete(), update() functions.

No client dependency - just build SQL statements and execute them.
"""

from matrixone.branch_builder import (
    create_table_branch,
    create_database_branch,
    delete_table_branch,
    delete_database_branch,
    diff_table_branch,
    merge_table_branch,
)


def example_create_table_branch():
    """Create table branch statements"""
    print("=" * 60)
    print("CREATE TABLE BRANCH Examples")
    print("=" * 60)

    # Basic table branch
    stmt = create_table_branch('users_dev').from_table('users')
    print(f"Basic: {stmt}")

    # With snapshot
    stmt = create_table_branch('users_backup').from_table('users', snapshot='daily_snap')
    print(f"With snapshot: {stmt}")

    # Cross-tenant (sys only)
    stmt = create_table_branch('users_dev').from_table('users').to_account('tenant1')
    print(f"Cross-tenant: {stmt}")

    # Full example
    stmt = create_table_branch('users_historical').from_table('users', snapshot='2024_01_01').to_account('analytics_team')
    print(f"Full: {stmt}\n")


def example_create_database_branch():
    """Create database branch statements"""
    print("=" * 60)
    print("CREATE DATABASE BRANCH Examples")
    print("=" * 60)

    # Basic database branch
    stmt = create_database_branch('dev_db').from_database('prod_db')
    print(f"Basic: {stmt}")

    # With snapshot
    stmt = create_database_branch('backup_db').from_database('prod_db', snapshot='weekly_snap')
    print(f"With snapshot: {stmt}")

    # Cross-tenant
    stmt = create_database_branch('dev_db').from_database('prod_db').to_account('dev_team')
    print(f"Cross-tenant: {stmt}\n")


def example_delete_branch():
    """Delete branch statements"""
    print("=" * 60)
    print("DELETE BRANCH Examples")
    print("=" * 60)

    # Delete table branch
    stmt = delete_table_branch('users_dev')
    print(f"Delete table: {stmt}")

    # Delete database branch
    stmt = delete_database_branch('dev_db')
    print(f"Delete database: {stmt}\n")


def example_diff_branch():
    """Diff branch statements"""
    print("=" * 60)
    print("DIFF BRANCH Examples")
    print("=" * 60)

    # Basic diff
    stmt = diff_table_branch('users_dev').against('users')
    print(f"Basic diff: {stmt}")

    # Diff with snapshots
    stmt = diff_table_branch('users').snapshot('snap_v1').against('users', snapshot='snap_v2')
    print(f"With snapshots: {stmt}")

    # Diff with output count
    stmt = diff_table_branch('users_dev').against('users').output_count()
    print(f"Output count: {stmt}")

    # Diff with output limit
    stmt = diff_table_branch('users_dev').against('users').output_limit(100)
    print(f"Output limit: {stmt}")

    # Diff with file export (local)
    stmt = diff_table_branch('users_dev').against('users').output_file('/tmp/diff.sql')
    print(f"Export to file: {stmt}")

    # Diff with file export (stage)
    stmt = diff_table_branch('users_dev').against('users').output_file('stage://backup_stage/')
    print(f"Export to stage: {stmt}")

    # Diff with output as table
    stmt = diff_table_branch('users_dev').against('users').output_as('diff_result')
    print(f"Output as table: {stmt}\n")


def example_merge_branch():
    """Merge branch statements"""
    print("=" * 60)
    print("MERGE BRANCH Examples")
    print("=" * 60)

    # Basic merge (default: skip conflicts)
    stmt = merge_table_branch('users_dev').into('users')
    print(f"Basic merge (skip): {stmt}")

    # Merge with accept strategy
    stmt = merge_table_branch('users_dev').into('users').when_conflict('accept')
    print(f"Merge (accept): {stmt}")

    # Merge with enum
    from matrixone.branch_builder import MergeConflictStrategy

    stmt = merge_table_branch('users_dev').into('users').when_conflict(MergeConflictStrategy.ACCEPT)
    print(f"Merge with enum: {stmt}\n")


def example_with_session():
    """Example of using statements with session"""
    print("=" * 60)
    print("Using with Session")
    print("=" * 60)

    # Build statements
    create_stmt = create_table_branch('users_dev').from_table('users')
    diff_stmt = diff_table_branch('users_dev').against('users').output_count()
    merge_stmt = merge_table_branch('users_dev').into('users')

    print("Statements ready to execute:")
    print(f"  1. {create_stmt}")
    print(f"  2. {diff_stmt}")
    print(f"  3. {merge_stmt}")

    print("\nUsage with session:")
    print("""
    from matrixone import Client
    
    client = Client()
    client.connect(database='test')
    
    # Execute statements
    with client.session() as session:
        # Create branch
        session.execute(create_table_branch('users_dev').from_table('users'))
        
        # Check differences
        result = session.execute(
            diff_table_branch('users_dev').against('users').output_count()
        )
        print(f"Differences: {result.fetchone()}")
        
        # Merge back
        session.execute(
            merge_table_branch('users_dev').into('users').when_conflict('accept')
        )
    """)


def example_complex_workflow():
    """Complex workflow example"""
    print("=" * 60)
    print("Complex Workflow Example")
    print("=" * 60)

    print("""
    # Development workflow with branches
    
    # 1. Create development branch from production
    create_dev = create_table_branch('orders_dev').from_table('orders')
    
    # 2. Create testing branch from development
    create_test = create_table_branch('orders_test').from_table('orders_dev')
    
    # 3. Compare test with production
    diff_test = (diff_table_branch('orders_test')
                 .against('orders')
                 .output_file('stage://reports/test_diff.sql'))
    
    # 4. Merge test back to production
    merge_prod = (merge_table_branch('orders_test')
                  .into('orders')
                  .when_conflict('accept'))
    
    # 5. Clean up development branch
    delete_dev = delete_table_branch('orders_dev')
    
    # Execute in transaction
    with client.session() as session:
        session.execute(create_dev)
        session.execute(create_test)
        
        # Verify differences
        result = session.execute(
            diff_table_branch('orders_test').against('orders').output_count()
        )
        
        if result.fetchone()[0] == 0:
            # No differences, safe to merge
            session.execute(merge_prod)
            session.execute(delete_dev)
    """)


if __name__ == '__main__':
    example_create_table_branch()
    example_create_database_branch()
    example_delete_branch()
    example_diff_branch()
    example_merge_branch()
    example_with_session()
    example_complex_workflow()

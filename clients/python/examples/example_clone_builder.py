"""
Example: Clone Statement Builders - SQLAlchemy Style API

Demonstrates the clone statement builders that produce SQL strings
independently of the client, similar to SQLAlchemy's select()/insert().
"""

from matrixone.clone_builder import clone_table, clone_database


def example_clone_table():
    print("=" * 60)
    print("CLONE TABLE Examples")
    print("=" * 60)

    stmt = clone_table('users_copy').from_table('users')
    print(f"Basic:            {stmt}")

    stmt = clone_table('users_copy').if_not_exists().from_table('users')
    print(f"If not exists:    {stmt}")

    stmt = clone_table('users_copy').from_table('users', snapshot='snap1')
    print(f"With snapshot:    {stmt}")

    stmt = clone_table('db2.users').from_table('db1.users')
    print(f"Cross-database:   {stmt}")

    stmt = clone_table('users_copy').if_not_exists().from_table('users', snapshot='snap1').to_account('tenant1')
    print(f"Cross-tenant:     {stmt}")
    print()


def example_clone_database():
    print("=" * 60)
    print("CLONE DATABASE Examples")
    print("=" * 60)

    stmt = clone_database('dev_db').from_database('prod_db')
    print(f"Basic:            {stmt}")

    stmt = clone_database('dev_db').if_not_exists().from_database('prod_db')
    print(f"If not exists:    {stmt}")

    stmt = clone_database('dev_db').from_database('prod_db', snapshot='daily_snap')
    print(f"With snapshot:    {stmt}")

    stmt = clone_database('dev_db').from_database('prod_db', snapshot='snap1').to_account('acc1')
    print(f"Cross-tenant:     {stmt}")
    print()


if __name__ == '__main__':
    example_clone_table()
    example_clone_database()

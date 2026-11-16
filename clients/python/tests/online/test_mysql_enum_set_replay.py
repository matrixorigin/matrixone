# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Standard MySQL clients preserve ENUM/SET values through schema replay."""

import uuid
import warnings

import pytest
import sqlalchemy as sa


@pytest.fixture
def mysql_engine(connection_params):
    host, port, user, password, database = connection_params
    engine = sa.create_engine(
        sa.URL.create('mysql+pymysql', username=user, password=password, host=host, port=port, database=database)
    )
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.mark.online
@pytest.mark.parametrize(
    'sql_mode,member_sql',
    [
        pytest.param('', ["'UP'", "'O''Brien'", r"'Back\\slash'", r"'Back\\''quote'"], id='default'),
        pytest.param(
            'NO_BACKSLASH_ESCAPES',
            ["'UP'", "'O''Brien'", r"'Back\slash'", r"'Back\''quote'"],
            id='no_backslash_escapes',
        ),
    ],
)
def test_enum_set_show_create_replay(mysql_engine, connection_params, sql_mode, member_sql):
    # Verify server replay directly: SQLAlchemy does not decode reflected
    # backslashes in ENUM domains, and MO does not advertise the NBE bind flag.
    engine = mysql_engine
    database = connection_params[4]
    source = 'enum_replay_' + uuid.uuid4().hex
    clone = source + '_clone'
    members = ['UP', "O'Brien", r'Back\slash', r"Back\'quote"]
    with engine.connect() as conn:
        original_mode = conn.exec_driver_sql('SELECT @@SESSION.sql_mode').scalar_one()
        conn.commit()
        try:
            with conn.begin():
                conn.exec_driver_sql('SET SESSION sql_mode=%s', (sql_mode,))
                domain = ','.join(member_sql)
                conn.exec_driver_sql(f'CREATE TABLE `{source}` (id INT PRIMARY KEY, e ENUM({domain}), s SET({domain}))')
                ddl = conn.exec_driver_sql(f'SHOW CREATE TABLE `{source}`').one()[1]
                conn.exec_driver_sql(ddl.replace(f'CREATE TABLE `{source}`', f'CREATE TABLE `{clone}`', 1))
                for name in (source, clone):
                    domains = conn.exec_driver_sql(
                        'SELECT attname,attr_enum FROM mo_catalog.mo_columns '
                        'WHERE att_database=%s AND att_relname=%s AND attname IN (%s,%s) ORDER BY attname',
                        (database, name, 'e', 's'),
                    ).all()
                    assert domains == [('e', ','.join(members)), ('s', ','.join(members))]
                    for index, (member, literal) in enumerate(zip(members, member_sql)):
                        if sql_mode:
                            conn.exec_driver_sql(f'INSERT INTO `{name}` VALUES ({index},{literal},{literal})')
                        else:
                            # Retain the existing default-mode parameter-binding oracle.
                            conn.exec_driver_sql(f'INSERT INTO `{name}` VALUES (%s,%s,%s)', (index, member, member))
                    assert conn.exec_driver_sql(f'SELECT e,s FROM `{name}` ORDER BY id').all() == [
                        (member, member) for member in members
                    ]
        finally:
            conn.rollback()
            try:
                with conn.begin():
                    for name in (clone, source):
                        conn.exec_driver_sql(f'DROP TABLE IF EXISTS `{name}`')
            finally:
                conn.exec_driver_sql('SET SESSION sql_mode=%s', (original_mode,))
                assert conn.exec_driver_sql('SELECT @@SESSION.sql_mode').scalar_one() == original_mode
                conn.commit()
            with conn.begin():
                assert (
                    conn.exec_driver_sql(
                        'SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME IN (%s,%s)',
                        (database, source, clone),
                    ).scalar_one()
                    == 0
                )


@pytest.mark.online
def test_empty_enum_metadata_replay(mysql_engine, connection_params):
    engine = mysql_engine
    database = connection_params[4]
    source = 'empty_enum_' + uuid.uuid4().hex
    clone, view = source + '_clone', source + '_view'
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(f"CREATE TABLE `{source}` (id INT PRIMARY KEY, e ENUM(''))")
            conn.exec_driver_sql(f'CREATE VIEW `{view}` AS SELECT * FROM `{source}`')
            ddl = conn.exec_driver_sql(f'SHOW CREATE TABLE `{source}`').one()[1]
            assert "`e` enum('')" in ddl
            conn.exec_driver_sql(ddl.replace(f'CREATE TABLE `{source}`', f'CREATE TABLE `{clone}`', 1))
            for name in (source, clone, view):
                for statement in ('SHOW COLUMNS FROM', 'SHOW FULL COLUMNS FROM', 'DESCRIBE'):
                    columns = conn.exec_driver_sql(f'{statement} `{name}`').all()
                    assert next(row[1] for row in columns if row[0] == 'e') == "enum('')"
                metadata = conn.exec_driver_sql(
                    'SELECT COLUMN_TYPE FROM information_schema.COLUMNS '
                    "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME='e'",
                    (database, name),
                ).scalar_one()
                assert metadata == "enum('')"
                native = conn.exec_driver_sql(
                    'SELECT mo_show_visible_bin(atttyp,2),attr_enum FROM mo_catalog.mo_columns '
                    "WHERE att_database=%s AND att_relname=%s AND attname='e'",
                    (database, name),
                ).one()
                assert native == ('ENUM', '')
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always', sa.exc.SAWarning)
            # The same view is checked through SQL above; standard client
            # view reflection is tracked independently in #32746.
            for name in (source, clone):
                table = sa.Table(name, sa.MetaData(), autoload_with=engine)
                assert table.c.e.type.enums == ['']
        assert not [str(w.message) for w in caught if issubclass(w.category, sa.exc.SAWarning)]
    finally:
        with engine.begin() as conn:
            conn.exec_driver_sql(f'DROP VIEW IF EXISTS `{view}`')
            for name in (clone, source):
                conn.exec_driver_sql(f'DROP TABLE IF EXISTS `{name}`')

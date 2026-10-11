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

"""Standard MySQL dialect reflection must preserve native MO boolean columns."""

import uuid
import warnings
from decimal import Decimal

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
def test_boolean_metadata_reflection(mysql_engine, connection_params):
    engine = mysql_engine
    database = connection_params[4]
    suffix = uuid.uuid4().hex
    source, view, clone_name = [f'bool_reflection_{kind}_{suffix}' for kind in ('source', 'view', 'clone')]
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(
                f'CREATE TABLE `{source}` (id INT PRIMARY KEY, flag BOOL DEFAULT NULL, '
                'enabled BOOLEAN NOT NULL DEFAULT TRUE, disabled BOOL DEFAULT FALSE, '
                "empty_flag BOOLEAN DEFAULT NULL, n BIGINT UNSIGNED, amount DECIMAL(10,2), "
                "integer_flag TINYINT(1), "
                "literal_flag ENUM('UP','O''Brien','Down word'), "
                "literal_set SET('UP','O''Brien','Down word'))"
            )
            conn.exec_driver_sql(
                f'INSERT INTO `{source}` (id,flag,n,amount,integer_flag) '
                'VALUES (1,TRUE,18446744073709551615,1.25,2),(2,FALSE,8,2.50,0),(3,NULL,9,3.75,NULL)'
            )
            conn.exec_driver_sql(
                f"CREATE VIEW `{view}` AS SELECT *, CAST('2026-01-02' AS DATE) AS day, "
                f"CAST('Up' AS VARCHAR(20)) AS label FROM `{source}`"
            )

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always', sa.exc.SAWarning)
            table = sa.Table(source, sa.MetaData(), autoload_with=engine)
            reflected_view = sa.Table(view, sa.MetaData(), autoload_with=engine)
        assert not [str(w.message) for w in caught if issubclass(w.category, sa.exc.SAWarning)]
        for name in ('flag', 'enabled', 'disabled', 'empty_flag'):
            assert isinstance(table.c[name].type, sa.Boolean)
        assert isinstance(reflected_view.c.flag.type, sa.Boolean)
        assert isinstance(reflected_view.c.id.type, sa.Integer)
        assert isinstance(reflected_view.c.integer_flag.type, sa.Integer)
        assert reflected_view.c.n.type.unsigned
        members = ['UP', "O'Brien", 'Down word']
        assert table.c.literal_flag.type.enums == members
        assert reflected_view.c.literal_flag.type.enums == members
        assert table.c.literal_set.type.values == tuple(members)
        assert reflected_view.c.literal_set.type.values == tuple(members)
        assert (reflected_view.c.amount.type.precision, reflected_view.c.amount.type.scale) == (10, 2)
        assert isinstance(reflected_view.c.day.type, sa.Date)
        assert reflected_view.c.label.type.length == 20
        assert not any(isinstance(column.type, sa.types.NullType) for column in reflected_view.columns)
        sa.schema.CreateTable(reflected_view.to_metadata(sa.MetaData(), name=view + '_clone')).compile(
            dialect=engine.dialect
        )
        assert table.c.flag.nullable and not table.c.enabled.nullable
        assert table.c.n.type.unsigned
        assert (table.c.amount.type.precision, table.c.amount.type.scale) == (10, 2)
        assert isinstance(table.c.integer_flag.type, sa.Integer)

        clone = table.to_metadata(sa.MetaData(), name=clone_name)
        # Execute the compiled definition: accepting the text alone cannot prove
        # that replay retains T_bool rather than silently constructing an integer.
        ddl = str(sa.schema.CreateTable(clone).compile(dialect=engine.dialect))
        with engine.begin() as conn:
            conn.exec_driver_sql(ddl)
            conn.execute(
                clone.insert(),
                [
                    {'id': 1, 'flag': True, 'n': 18446744073709551615, 'amount': Decimal('1.25'), 'integer_flag': 2},
                    {'id': 2, 'flag': False, 'n': 8, 'amount': Decimal('2.50'), 'integer_flag': 0},
                    {'id': 3, 'flag': None, 'n': 9, 'amount': Decimal('3.75'), 'integer_flag': None},
                ],
            )
            for current in (table, clone):
                rows = conn.execute(sa.select(current).order_by(current.c.id)).all()
                assert rows == [
                    (1, True, True, False, None, 18446744073709551615, Decimal('1.25'), 2, None, None),
                    (2, False, True, False, None, 8, Decimal('2.50'), 0, None, None),
                    (3, None, True, False, None, 9, Decimal('3.75'), None, None, None),
                ]
                assert rows[0].flag is True and rows[1].flag is False and rows[2].flag is None
                metadata = conn.exec_driver_sql(
                    'SELECT DATA_TYPE,COLUMN_TYPE FROM information_schema.COLUMNS '
                    "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME='flag'",
                    (database, current.name),
                ).one()
                assert metadata[0] == 'tinyint' and metadata[1].upper() == 'BOOLEAN'
                native_type = conn.exec_driver_sql(
                    'SELECT mo_show_visible_bin(atttyp,2) FROM mo_catalog.mo_columns '
                    "WHERE att_database=%s AND att_relname=%s AND attname='flag'",
                    (database, current.name),
                ).one()
                assert native_type[0] == 'BOOL'

        reflected_clone = sa.Table(clone_name, sa.MetaData(), autoload_with=engine)
        assert reflected_clone.c.literal_flag.type.enums == members
        assert reflected_clone.c.literal_set.type.values == tuple(members)

        with pytest.raises(sa.exc.DBAPIError):
            with engine.begin() as conn:
                conn.execute(clone.insert(), {'id': 4, 'enabled': None})
        with engine.connect() as conn:
            assert conn.execute(sa.select(sa.func.count()).select_from(clone)).scalar_one() == 3
    finally:
        with engine.begin() as conn:
            conn.exec_driver_sql(f'DROP VIEW IF EXISTS `{view}`')
            for name in (clone_name, source):
                conn.exec_driver_sql(f'DROP TABLE IF EXISTS `{name}`')


@pytest.mark.online
def test_enum_set_show_create_replay(mysql_engine, connection_params):
    # SQLAlchemy 2.0.44 does not unescape backslashes in reflected enum domains.
    # Verify the server's replay contract directly, without that client parser.
    engine = mysql_engine
    database = connection_params[4]
    source = 'enum_replay_' + uuid.uuid4().hex
    clone = source + '_clone'
    members = ['UP', "O'Brien", r'Back\slash', r"Back\'quote"]
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(
                f'CREATE TABLE `{source}` (id INT PRIMARY KEY, '
                r"e ENUM('UP','O''Brien','Back\\slash','Back\\''quote'), "
                r"s SET('UP','O''Brien','Back\\slash','Back\\''quote'))"
            )
            ddl = conn.exec_driver_sql(f'SHOW CREATE TABLE `{source}`').one()[1]
            conn.exec_driver_sql(ddl.replace(f'CREATE TABLE `{source}`', f'CREATE TABLE `{clone}`', 1))
            for name in (source, clone):
                domains = conn.exec_driver_sql(
                    'SELECT attname,attr_enum FROM mo_catalog.mo_columns '
                    'WHERE att_database=%s AND att_relname=%s AND attname IN (%s,%s) ORDER BY attname',
                    (database, name, 'e', 's'),
                ).all()
                assert domains == [('e', ','.join(members)), ('s', ','.join(members))]
                for index, member in enumerate(members):
                    conn.exec_driver_sql(f'INSERT INTO `{name}` VALUES (%s,%s,%s)', (index, member, member))
                assert conn.exec_driver_sql(f'SELECT e,s FROM `{name}` ORDER BY id').all() == [
                    (member, member) for member in members
                ]
    finally:
        with engine.begin() as conn:
            for name in (clone, source):
                conn.exec_driver_sql(f'DROP TABLE IF EXISTS `{name}`')


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
            for name in (source, clone, view):
                table = sa.Table(name, sa.MetaData(), autoload_with=engine)
                assert table.c.e.type.enums == ['']
        assert not [str(w.message) for w in caught if issubclass(w.category, sa.exc.SAWarning)]
    finally:
        with engine.begin() as conn:
            conn.exec_driver_sql(f'DROP VIEW IF EXISTS `{view}`')
            for name in (clone, source):
                conn.exec_driver_sql(f'DROP TABLE IF EXISTS `{name}`')

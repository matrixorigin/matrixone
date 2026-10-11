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


@pytest.mark.online
def test_boolean_metadata_reflection(connection_params):
    host, port, user, password, database = connection_params
    engine = sa.create_engine(
        sa.URL.create('mysql+pymysql', username=user, password=password, host=host, port=port, database=database)
    )
    suffix = uuid.uuid4().hex
    source, view, clone_name = [f'bool_reflection_{kind}_{suffix}' for kind in ('source', 'view', 'clone')]
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(
                f'CREATE TABLE `{source}` (id INT PRIMARY KEY, flag BOOL DEFAULT NULL, '
                'enabled BOOLEAN NOT NULL DEFAULT TRUE, disabled BOOL DEFAULT FALSE, '
                'empty_flag BOOLEAN DEFAULT NULL, n INT UNSIGNED, amount DECIMAL(10,2), integer_flag TINYINT(1))'
            )
            conn.exec_driver_sql(
                f'INSERT INTO `{source}` (id,flag,n,amount,integer_flag) '
                'VALUES (1,TRUE,7,1.25,2),(2,FALSE,8,2.50,0),(3,NULL,9,3.75,NULL)'
            )
            conn.exec_driver_sql(f'CREATE VIEW `{view}` AS SELECT flag FROM `{source}`')

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always', sa.exc.SAWarning)
            table = sa.Table(source, sa.MetaData(), autoload_with=engine)
            reflected_view = sa.Table(view, sa.MetaData(), autoload_with=engine)
        assert not [str(w.message) for w in caught if issubclass(w.category, sa.exc.SAWarning)]
        for name in ('flag', 'enabled', 'disabled', 'empty_flag'):
            assert isinstance(table.c[name].type, sa.Boolean)
        assert isinstance(reflected_view.c.flag.type, sa.Boolean)
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
                    {'id': 1, 'flag': True, 'n': 7, 'amount': Decimal('1.25'), 'integer_flag': 2},
                    {'id': 2, 'flag': False, 'n': 8, 'amount': Decimal('2.50'), 'integer_flag': 0},
                    {'id': 3, 'flag': None, 'n': 9, 'amount': Decimal('3.75'), 'integer_flag': None},
                ],
            )
            for current in (table, clone):
                rows = conn.execute(sa.select(current).order_by(current.c.id)).all()
                assert rows == [
                    (1, True, True, False, None, 7, Decimal('1.25'), 2),
                    (2, False, True, False, None, 8, Decimal('2.50'), 0),
                    (3, None, True, False, None, 9, Decimal('3.75'), None),
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

        with pytest.raises(sa.exc.DBAPIError):
            with engine.begin() as conn:
                conn.execute(clone.insert(), {'id': 4, 'enabled': None})
        with engine.connect() as conn:
            assert conn.execute(sa.select(sa.func.count()).select_from(clone)).scalar_one() == 3
    finally:
        try:
            with engine.begin() as conn:
                conn.exec_driver_sql(f'DROP VIEW IF EXISTS `{view}`')
                for name in (clone_name, source):
                    conn.exec_driver_sql(f'DROP TABLE IF EXISTS `{name}`')
        finally:
            engine.dispose()

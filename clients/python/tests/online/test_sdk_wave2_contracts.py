"""Exact result and atomicity contracts for the second SDK issue set."""

import asyncio
import os
from decimal import Decimal
from pathlib import Path

import pytest
import sqlalchemy as sa
from pymysql.err import OperationalError as DriverOperationalError
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import declarative_base

from matrixone import Client, AsyncClient
from matrixone.exceptions import QueryError, PitrError
from matrixone.sqlalchemy_ext import AdvancedFulltextSearchBuilder, FulltextSearchBuilder
from .test_sdk_quality_contracts import server, client, async_client  # noqa: F401
from .test_sdk_manager_contracts import name, resolve

pytestmark = pytest.mark.skipif(os.environ.get('MO_SDK_ONLINE') != '1', reason='explicit isolated target required')


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_vector_exact_values_identity_and_atomic_upsert(client, async_client, kind):
    c = client if kind == 'sync' else async_client
    table = name('atomic_vec_')
    client.execute(f'CREATE TABLE {table}(doc_key INT PRIMARY KEY,v VECF32(2),note VARCHAR(20))')
    client.execute(f"INSERT INTO {table} VALUES(1,'[3,4]','old')")
    handle = c.get_pinecone_index(table, 'v')
    query = handle.query if kind == 'sync' else handle.query_async
    upsert = handle.upsert if kind == 'sync' else handle.upsert_async
    try:
        for values in (False, True):
            for metadata in (False, True):
                match = (await resolve(query([0, 0], include_values=values, include_metadata=metadata))).matches[0]
                assert match.id == '1'
                assert match.values == ([3.0, 4.0] if values else None)
                assert match.metadata == ({'note': 'old'} if metadata else {})
        with pytest.raises(ValueError):
            await resolve(upsert([{'doc_key': 2, 'v': [1, 1]}, {'doc_key': 3}]))
        assert client.execute(f'SELECT doc_key,note FROM {table}').rows == [(1, 'old')]
        # An oversized vector is locally rejected before the initial update.
        with pytest.raises(ValueError):
            await resolve(upsert([{'doc_key': 1, 'v': [0, 0], 'note': 'changed'}, {'doc_key': 2, 'v': [1, 2, 3]}]))
        assert client.execute(f'SELECT note FROM {table}').rows == [('old',)]
        # NULL violates a server-only NOT NULL constraint after the first DML.
        client.execute(f'ALTER TABLE {table} ADD COLUMN required INT NOT NULL DEFAULT 7')
        with pytest.raises(QueryError):
            await resolve(
                upsert([{'doc_key': 1, 'v': [0, 0], 'note': 'changed'}, {'doc_key': 2, 'v': [1, 2], 'required': None}])
            )
        assert client.execute(f'SELECT doc_key,note,v FROM {table}').rows == [(1, 'old', '[3, 4]')]
        result = await resolve(
            upsert([{'doc_key': 1, 'v': [0, 0], 'note': "O'Reilly"}, {'doc_key': 2, 'v': [0, 2], 'note': None}])
        )
        assert result['upserted_count'] == 2
        assert client.execute(f'SELECT doc_key,note FROM {table} ORDER BY doc_key').rows == [(1, "O'Reilly"), (2, None)]
        for metric, expected in [('l2', [0, 2]), ('l2_sq', [0, 4])]:
            rows = await resolve(c.vector_ops.similarity_search(table, 'v', [0, 0], distance_type=metric))
            assert [r['distance'] for r in rows] == expected
        with pytest.raises(QueryError, match='Unsupported distance_type'):
            await resolve(c.vector_ops.similarity_search(table, 'v', [0, 0], distance_type='typo'))
        await resolve(c.execute('SET experimental_ivf_index=1'))
        client.execute('SET experimental_ivf_index=1')
        client.execute(f"CREATE INDEX idx_v USING ivfflat ON {table}(v) LISTS 1 op_type 'vector_l2_ops'")
        ranked = await resolve(c.vector_ops.search_with_rank(table, 'v', [0, 0], distance_type='l2_sq', limit=2))
        assert [r['distance'] for r in ranked] == [0, 4]
        client.execute(f"DROP INDEX idx_v ON {table}")
        client.execute(f"INSERT INTO {table}(doc_key,v) VALUES(3,NULL)")
        with pytest.raises(QueryError, match='Invalid vector response'):
            await resolve(query([0, 0], include_values=False))
    finally:
        client.execute('DROP TABLE ' + table)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize('key', ['none', 'composite'])
async def test_nonunique_vector_identity_rejected(client, async_client, kind, key):
    c = client if kind == 'sync' else async_client
    table = name('identity_')
    suffix = ', PRIMARY KEY(id,part)' if key == 'composite' else ''
    client.execute(f'CREATE TABLE {table}(id INT,part INT,v VECF32(2){suffix})')
    client.execute(f"INSERT INTO {table} VALUES(1,1,'[1,0]'),(1,2,'[0,1]')")
    try:
        h = c.get_pinecone_index(table, 'v')
        operations = [
            lambda: h.query([1, 0]),
            lambda: h.delete([1]),
            lambda: h.upsert([{'id': 1, 'part': 1, 'v': [1, 0]}]),
            lambda: h.batch_insert([{'id': 2, 'part': 1, 'v': [1, 0]}]),
        ]
        if kind == 'async':
            operations = [
                lambda: h.query_async([1, 0]),
                lambda: h.delete_async([1]),
                lambda: h.upsert_async([{'id': 1, 'part': 1, 'v': [1, 0]}]),
                lambda: h.batch_insert_async([{'id': 2, 'part': 1, 'v': [1, 0]}]),
            ]
        for operation in operations:
            with pytest.raises(ValueError, match='single primary key'):
                await resolve(operation())
        assert client.execute(f'SELECT id,part FROM {table} ORDER BY part').rows == [(1, 1), (1, 2)]
    finally:
        client.execute('DROP TABLE ' + table)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_vector_distance_collision_is_explicit(client, async_client, kind):
    c = client if kind == 'sync' else async_client
    table = name('distance_')
    client.execute(f'CREATE TABLE {table}(id INT PRIMARY KEY,v VECF32(2),distance DECIMAL(10,2))')
    client.execute(f"INSERT INTO {table} VALUES(1,'[3,4]',99.25)")
    try:
        for operation in [
            lambda: c.vector_ops.similarity_search(table, 'v', [0, 0]),
            lambda: c.vector_ops.range_search(table, 'v', [0, 0], 10),
        ]:
            with pytest.raises(QueryError, match='(?i)ambiguous'):
                await resolve(operation())
        rows = await resolve(
            c.vector_ops.similarity_search(
                table, 'v', [0, 0], select_columns=['id', 'distance AS stored_distance'], distance_type='l2_sq'
            )
        )
        assert rows == [{'id': 1, 'stored_distance': Decimal('99.25'), 'distance': 25.0}]
        client.execute('DELETE FROM ' + table)
        with pytest.raises(QueryError, match='(?i)ambiguous'):
            await resolve(c.vector_ops.similarity_search(table, 'v', [0, 0]))
    finally:
        client.execute('DROP TABLE ' + table)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_join_fk_and_binary_subquery(client, async_client, kind):
    c = client if kind == 'sync' else async_client
    parent, child = name('parent_'), name('child_')
    base = declarative_base()
    Parent = type(
        'Parent',
        (base,),
        dict(
            __tablename__=parent,
            id=sa.Column(sa.Integer, primary_key=True),
            amount=sa.Column(sa.DECIMAL(10, 2)),
            payload=sa.Column(sa.VARBINARY(20)),
        ),
    )
    Child = type(
        'Child',
        (base,),
        dict(
            __tablename__=child,
            id=sa.Column(sa.Integer, primary_key=True),
            parent_id=sa.Column(sa.Integer, sa.ForeignKey(parent + '.id')),
        ),
    )
    client.execute(f'CREATE TABLE {parent}(id INT PRIMARY KEY,amount DECIMAL(10,2),payload VARBINARY(20))')
    client.execute(f'CREATE TABLE {child}(id INT PRIMARY KEY,parent_id INT)')
    try:
        client.execute(f'INSERT INTO {parent} VALUES(1,1.20,?),(2,2.30,?)', (b'\0\xff', b'abc'))
        client.execute(f'INSERT INTO {child} VALUES(10,1),(20,2),(30,2)')
        for alias_first in (False, True):
            q = c.query(Parent).select(Parent.id)
            if alias_first:
                q.alias('p')
            q.join(Child)
            if not alias_first:
                q.alias('p')
            rows = await resolve(q.order_by(Parent.id).all())
            assert [r.id for r in rows] == [1, 2, 2]
        assert [r.id for r in await resolve(c.query(Parent).filter(Parent.payload == b'\0\xff').all())] == [1]
        inner = c.query(Parent).select('id').logical_in('amount', [Decimal('1.20')]).filter(Parent.payload == b'\0\xff')
        sub = inner.subquery('s')
        assert inner.subquery('s') == sub
        rows = await resolve(c.query(parent + ' AS p').select('p.id', 'p.amount').join(sub, 'p.id=s.id').all())
        assert [(r.id, r.amount) for r in rows] == [(1, Decimal('1.20'))]
    finally:
        client.execute('DROP TABLE ' + child)
        client.execute('DROP TABLE ' + parent)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_stage_and_pitr_observed_state(client, async_client, kind, server):
    c = client if kind == 'sync' else async_client
    stage, pitr = name('MixedStage'), name('MixedPitr')
    try:
        await resolve(c.stage.create(stage, 'file:///tmp/first', comment='first'))
        actual = await resolve(c.stage.create(stage, 'file:///tmp/second', comment='second', if_not_exists=True))
        assert (actual.url, actual.comment) == ('file:///tmp/first', 'first')
        assert actual.name == stage.lower()
        assert (await resolve(c.stage.get(stage))).name == stage.lower()
        assert await resolve(c.stage.alter(stage + 'absent', url='file:///tmp/none', if_exists=True)) is None
        actual = await resolve(c.stage.alter(stage, comment='changed'))
        assert actual.comment == 'changed'
        created = await resolve(c.pitr.create_database_pitr(pitr, server['database'], range_value=3))
        assert created.name == pitr.lower()
        assert (await resolve(c.pitr.get(pitr))).name == created.name
        changed = await resolve(c.pitr.alter(pitr, 2, 'd'))
        assert changed.range_value == 2
        with pytest.raises(PitrError):
            await resolve(c.pitr.alter(pitr, 0, 'd'))
        assert (await resolve(c.pitr.get(pitr))).range_value == 2
    finally:
        client.execute('DROP PITR IF EXISTS ' + pitr)
        client.stage.drop(stage, if_exists=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_local_file_requires_opt_in_and_loads_exact_bytes(client, async_client, server, tmp_path, kind):
    table = name('local_')
    path = tmp_path / 'rows.csv'
    path.write_text('1,alpha\n2,beta\n')
    client.execute(f'CREATE TABLE {table}(id INT PRIMARY KEY,label VARCHAR(20))')
    disabled = client if kind == 'sync' else async_client
    enabled = Client(sql_log_mode='off') if kind == 'sync' else AsyncClient(sql_log_mode='off')
    try:
        with pytest.raises(ValueError, match='local_infile=True'):
            await resolve(disabled.load_data.from_local_file(str(path), table))
        await resolve(enabled.connect(**server, local_infile=True))
        await resolve(enabled.load_data.from_local_file(str(path), table))
        assert client.execute(f'SELECT * FROM {table} ORDER BY id').rows == [(1, 'alpha'), (2, 'beta')]
        with pytest.raises(QueryError):
            await resolve(enabled.load_data.from_local_file(str(path) + 'missing', table))
        assert (await resolve(enabled.execute('SELECT 42'))).scalar() == 42
        assert enabled._engine.pool.checkedout() == 0
    finally:
        await resolve(enabled.disconnect())
        client.execute('DROP TABLE ' + table)


@pytest.mark.asyncio
async def test_cancelled_upsert_rolls_back_first_write(client, async_client, monkeypatch):
    from matrixone.session import AsyncSession

    table = name('cancel_batch_')
    client.execute(f'CREATE TABLE {table}(id INT PRIMARY KEY,v VECF32(2))')
    reached = asyncio.Event()
    original = AsyncSession.execute

    async def ordered(self, statement, *args, **kwargs):
        result = await original(self, statement, *args, **kwargs)
        if isinstance(statement, str) and statement.startswith('INSERT INTO ' + table):
            reached.set()
            await asyncio.Event().wait()
        return result

    monkeypatch.setattr(AsyncSession, 'execute', ordered)
    task = None
    try:
        task = asyncio.create_task(
            async_client.get_pinecone_index(table, 'v').upsert_async([{'id': 1, 'v': [1, 0]}, {'id': 2, 'v': [0, 1]}])
        )
        await asyncio.wait_for(reached.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert client.execute('SELECT COUNT(*) FROM ' + table).scalar() == 0
        assert async_client._engine.pool.checkedout() == 0
        assert (await async_client.execute('SELECT 42')).scalar() == 42
    finally:
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        client.execute('DROP TABLE ' + table)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_fulltext_terms_match_native_in_both_modes(client, async_client, kind, record_property):
    from matrixone._sql_literals import fulltext_term_token
    from matrixone.sqlalchemy_ext.fulltext_search import natural_match

    c = client if kind == 'sync' else async_client
    table = name('term_')
    client.execute('SET experimental_fulltext_index=1')
    client.execute(f'CREATE TABLE {table}(id INT PRIMARY KEY,content TEXT)')
    client.execute(
        f'INSERT INTO {table} VALUES(1,?),(2,?),(3,?)', ("O'Reilly handbook", r'C:\path guide', 'dollar $sdk$ 测试')
    )
    client.execute(f'CREATE FULLTEXT INDEX idx ON {table}(content)')

    async def compare_composed(native_sql, operation, feature):
        try:
            expected_rows = (await resolve(c.execute(native_sql))).rows
        except QueryError as error:
            if error.errno != 20105 or 'full table scan with fulltext search is not supported' not in str(error):
                raise
            with pytest.raises(QueryError) as raised:
                await resolve(operation())
            assert raised.value.errno == 20105
            record_property('matrixone.feature.' + feature, 'native-limitation:20105')
        else:
            result = await resolve(operation())
            actual = result.rows if hasattr(result, 'rows') else [tuple(row) for row in result]
            assert actual == expected_rows
            record_property('matrixone.feature.' + feature, 'supported')

    previous = (await resolve(c.execute('SELECT @@sql_mode'))).scalar()
    try:
        for mode in ('', 'NO_BACKSLASH_ESCAPES'):
            await resolve(c.execute('SET sql_mode=?', (mode,)))
            for term, token, expected in [
                ("O'Reilly", "'O''Reilly'", [(1,)]),
                (r'C:\path', r'$sdk$C:\path$sdk$', [(2,)]),
                ('$sdk$', "'$sdk$'", None),
                ('测试', "'测试'", None),
            ]:
                native = (
                    await resolve(
                        c.execute(f'SELECT id FROM {table} WHERE MATCH(content) AGAINST({token}) ORDER BY id')
                    )
                ).rows
                if expected is not None:
                    assert native == expected
                if term == r'C:\path':
                    from matrixone.orm import logical_in
                    from matrixone.export import _query_to_sql

                    generated = c.query(table).select('id').filter(natural_match('content', query=term))
                    nested = c.query(table).select('id').filter(logical_in('id', generated))
                    native_nested = f'SELECT id FROM {table} WHERE id IN (SELECT id FROM {table} WHERE MATCH(content) AGAINST({token}))'
                    await compare_composed(native_nested, nested.execute, 'nested_fulltext_backslash')
                    assert (await resolve(c.execute(_query_to_sql(generated)))).rows == native
                basic = FulltextSearchBuilder(table, 'content').search(term)
                rows = (await resolve(c.execute(basic.build_sql()))).rows
                assert sorted(row[0] for row in rows) == [row[0] for row in native]
                advanced = (
                    AdvancedFulltextSearchBuilder(c)
                    .table(table)
                    .columns(['content'])
                    .query(term)
                    .select(['id'])
                    .order_by('id', 'ASC')
                )
                assert (await resolve(advanced.execute())).rows == native
                sub = c.query(table).select('id').filter(natural_match('content', query=term)).subquery('s')
                derived = c.query(table + ' AS p').select('p.id').join(sub, 'p.id=s.id').order_by('p.id')
                native_derived = f'SELECT p.id FROM {table} AS p JOIN (SELECT id FROM {table} WHERE MATCH(content) AGAINST({token})) AS s ON p.id=s.id ORDER BY p.id'
                await compare_composed(native_derived, derived.all, 'derived_fulltext')
            for term in ['bad\0term', r'bad\$term']:
                with pytest.raises(ValueError):
                    FulltextSearchBuilder(table, 'content').search(term).build_sql()
                with pytest.raises(ValueError):
                    await resolve(
                        AdvancedFulltextSearchBuilder(c).table(table).columns(['content']).query(term).execute()
                    )
    finally:
        await resolve(c.execute('SET sql_mode=?', (previous,)))
        client.execute('DROP TABLE ' + table)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_metadata_object_sizes_and_column_zonemaps(client, async_client, server, kind):
    from matrixone.metadata import MetadataColumn

    c = client if kind == 'sync' else async_client
    table = name('meta_')
    db = server['database']
    client.execute(f'CREATE TABLE {table}(id INT PRIMARY KEY,v INT)')
    client.execute(f'INSERT INTO {table} VALUES(1,10),(2,20)')
    try:
        client.moctl.increment_checkpoint()
        native = client.execute(
            f"SELECT object_name,rows_cnt,origin_size,compress_size FROM metadata_scan('{db}.{table}','*') g"
        ).rows
        unique = {r[0]: r for r in native}
        assert unique
        details = (await resolve(c.metadata.get_table_detail_stats(db, table)))[table]
        brief = (await resolve(c.metadata.get_table_brief_stats(db, table)))[table]
        assert len(details) == len(unique) == brief['total_objects']
        assert brief['original_size'] == c.metadata._format_size(sum(r[2] for r in unique.values()))
        assert brief['compress_size'] == c.metadata._format_size(sum(r[3] for r in unique.values()))
        expected = client.execute(f"SELECT min,max,sum FROM metadata_scan('{db}.{table}','v') g").rows
        for projection in [
            ['col_name', 'min', 'max', 'sum'],
            [MetadataColumn.COL_NAME, MetadataColumn.MIN, MetadataColumn.MAX, MetadataColumn.SUM],
        ]:
            rows = await resolve(c.metadata.scan(db, table, columns=projection))
            actual = [(r['min'], r['max'], r['sum']) for r in rows if r['col_name'] == 'v']
            assert actual == expected
            assert [int.from_bytes(value, 'little', signed=True) for value in actual[0]] == [10, 20, 30]
        one = await resolve(c.metadata.scan(db, table, columns=['min', 'max', 'sum'], source_column='v'))
        assert [(r['min'], r['max'], r['sum']) for r in one] == expected
        fast = await resolve(c.metadata.scan(db, table, columns=['min'], source_column='*'))
        assert all(r['min'] is None for r in fast)
    finally:
        client.execute('DROP TABLE ' + table)


def test_csv_drift_blocks_sql_query_and_import(client, server):
    local = Path(os.environ['MO_SDK_EXPORT_LOCAL_DIR']) / (name('drift_') + '.csv')
    remote = os.environ['MO_SDK_EXPORT_DIR'].rstrip('/') + '/' + local.name
    table = name('drift_target_')
    client.execute(f'CREATE TABLE {table}(id BIGINT)')
    local.write_text('id\n1\n2\n')
    relation = None
    try:
        relation = client.analytics.read_csv(str(local), server_paths=[remote])
        local.write_text('id\n3.5\n4.5\n')
        for operation in [lambda: relation.sql, lambda: relation.query(), lambda: relation.import_into(table)]:
            with pytest.raises(ValueError, match='Source schema changed'):
                operation()
        assert client.execute('SELECT COUNT(*) FROM ' + table).scalar() == 0
        local.write_text('id\n3\n4\n')
        assert client.execute('SELECT * FROM ' + relation.sql + ' AS q ORDER BY id').rows == [(3,), (4,)]
    finally:
        if relation is not None:
            relation.close()
        local.unlink(missing_ok=True)
        client.execute('DROP TABLE ' + table)


def test_explicit_csv_cast_keeps_observed_schema_boundary(client):
    import pyarrow as pa

    local = Path(os.environ['MO_SDK_EXPORT_LOCAL_DIR']) / (name('cast_') + '.csv')
    remote = os.environ['MO_SDK_EXPORT_DIR'].rstrip('/') + '/' + local.name
    local.write_text('id\n1\n2\n')
    relation = None
    try:
        relation = client.analytics.read_csv(
            str(local), server_paths=[remote], schema=pa.schema([('id', pa.float64())])
        )
        assert client.execute('SELECT * FROM ' + relation.sql + ' AS q ORDER BY id').rows == [(1.0,), (2.0,)]
        local.write_text('id\n1.5\n2.5\n')
        with pytest.raises(ValueError, match='Source schema changed'):
            _ = relation.sql
    finally:
        if relation is not None:
            relation.close()
        local.unlink(missing_ok=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_metadata_after_add_column_and_clone(client, async_client, server, kind):
    c = client if kind == 'sync' else async_client
    table, clone = name('meta_add_'), name('meta_clone_')
    db = server['database']
    client.execute(f'CREATE TABLE {table}(id INT PRIMARY KEY,v INT)')
    try:
        assert await resolve(c.metadata.scan(db, table, columns=['min', 'max', 'sum'])) == []
        client.execute(f'INSERT INTO {table} VALUES(1,10),(2,NULL)')
        client.moctl.increment_checkpoint()
        client.execute(f'ALTER TABLE {table} ADD COLUMN extra INT')
        client.clone.clone_table(clone, table)
        client.moctl.increment_checkpoint()
        for target in (table, clone):
            native = client.execute(
                f"SELECT object_name,rows_cnt,origin_size,compress_size FROM metadata_scan('{db}.{target}','*') g"
            ).rows
            unique = {r[0]: r for r in native}
            assert unique
            details = (await resolve(c.metadata.get_table_detail_stats(db, target)))[target]
            brief = (await resolve(c.metadata.get_table_brief_stats(db, target)))[target]
            assert len(details) == len(unique) == brief['total_objects']
            assert brief['original_size'] == c.metadata._format_size(sum(r[2] for r in unique.values()))
            assert brief['compress_size'] == c.metadata._format_size(sum(r[3] for r in unique.values()))
    finally:
        client.execute('DROP TABLE IF EXISTS ' + clone)
        client.execute('DROP TABLE ' + table)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_fulltext_group_has_exact_or_membership(client, async_client, kind):
    from matrixone.sqlalchemy_ext.fulltext_search import boolean_match

    c = client if kind == 'sync' else async_client
    table = name('group_or_')
    client.execute('SET experimental_fulltext_index=1')
    client.execute(f'CREATE TABLE {table}(id INT PRIMARY KEY,body TEXT)')
    client.execute(
        f"INSERT INTO {table} VALUES(1,'alpha match'),(2,'beta match'),(3,'alpha beta match'),"
        "(4,'other match'),(5,NULL),(6,'alpha outside')"
    )
    client.execute(f'CREATE FULLTEXT INDEX ft ON {table}(body)')
    try:
        for method in ['must', 'encourage']:
            for with_parent in [False, True]:
                q = boolean_match('body')
                if with_parent:
                    q.must('match')
                q.group(getattr(boolean_match('body'), method)('alpha'), getattr(boolean_match('body'), method)('beta'))
                expected = [(1,), (2,), (3,)] if with_parent else [(1,), (2,), (3,), (6,)]
                token = '+match +(alpha beta)' if with_parent else '+(alpha beta)'
                native = await resolve(
                    c.execute(
                        f"SELECT id FROM {table} WHERE MATCH(body) AGAINST(? IN BOOLEAN MODE) ORDER BY id", (token,)
                    )
                )
                direct = await resolve(c.execute(f'SELECT id FROM {table} WHERE {q.compile()} ORDER BY id'))
                orm = await resolve(c.query(table).select('id').filter(q).order_by('id').all())
                assert native.rows == direct.rows == expected
                assert [row.id for row in orm] == [row[0] for row in expected]
        assert (await resolve(c.execute('SELECT 42'))).scalar() == 42
    finally:
        client.execute('DROP TABLE ' + table)


@pytest.mark.parametrize('mapping', ['local', 'file-uri', 'stage'])
def test_mapped_file_presence_and_complete_import(single_file_client, tmp_path, mapping):
    client = single_file_client
    import shutil
    from urllib.parse import quote
    import pyarrow as pa
    import pyarrow.parquet as pq

    suffix = name('mapped_')
    root = Path(os.environ['MO_SDK_EXPORT_LOCAL_DIR'])
    remote = os.environ['MO_SDK_EXPORT_DIR'].rstrip('/')
    paths = [tmp_path / 'a.parquet', tmp_path / 'b.parquet']
    staged = [root / (suffix + f"_{i}'" + ('%' if mapping != 'stage' else '') + '.parquet') for i in range(2)]
    target = suffix + '_target'
    stage = suffix + '_stage'
    for i, (local, shared) in enumerate(zip(paths, staged), 1):
        pq.write_table(pa.table({'id': [i], 'label': [str(i)]}), local)
        shutil.copyfile(local, shared)
    if mapping == 'stage':
        client.stage.create(stage, 'file://' + remote + '/')

    def server_path(shared):
        if mapping == 'local':
            return remote + '/' + shared.name
        if mapping == 'file-uri':
            return 'file://' + quote(remote + '/' + shared.name, safe='/')
        return 'stage://' + stage + '/' + quote(shared.name, safe='/')

    mapped = [server_path(path) for path in staged]
    client.execute(f'CREATE TABLE {target}(id BIGINT PRIMARY KEY,label TEXT)')
    try:
        with client.analytics.read_parquet(paths, server_paths=mapped) as rel:
            with rel.query() as stream:
                assert sorted(stream.to_arrow().to_pylist(), key=lambda row: row['id']) == [
                    {'id': 1, 'label': '1'},
                    {'id': 2, 'label': '2'},
                ]
            assert rel.import_into(target) == 2
        assert client.execute(f'SELECT * FROM {target} ORDER BY id').rows == [(1, '1'), (2, '2')]
        client.execute('DELETE FROM ' + target)
        for missing in [0, 1]:
            absent = list(mapped)
            absent[missing] = server_path(root / (suffix + '_absent.parquet'))
            with pytest.raises((DBAPIError, DriverOperationalError)) as caught:
                client.analytics.read_parquet(paths, server_paths=absent)
            assert 'not found' in str(caught.value).lower() or 'not exist' in str(caught.value).lower()
            assert client.execute('SELECT COUNT(*) FROM ' + target).scalar() == 0
        with client.analytics.read_parquet(paths, server_paths=mapped) as rel:
            staged[1].unlink()
            for action in [lambda: rel.sql, rel.query, lambda: rel.import_into(target)]:
                with pytest.raises((DBAPIError, DriverOperationalError)) as caught:
                    action()
                assert 'not found' in str(caught.value).lower() or 'not exist' in str(caught.value).lower()
            assert client.execute('SELECT COUNT(*) FROM ' + target).scalar() == 0
        assert client._engine.pool.checkedout() == 0
        assert client.execute('SELECT 42').scalar() == 42
        assert not [r[0] for r in client.execute('SHOW TABLES').rows if r[0].startswith('mo_py_files_')]
    finally:
        client.execute('DROP TABLE ' + target)
        if mapping == 'stage':
            client.stage.drop(stage, if_exists=True)
        for shared in staged:
            shared.unlink(missing_ok=True)


def test_file_disappears_at_stream_eof_and_before_commit(single_file_client, tmp_path, monkeypatch):
    client = single_file_client
    import shutil
    import pyarrow as pa
    import pyarrow.parquet as pq
    from sqlalchemy.engine import Connection

    suffix = name('late_file_')
    source = tmp_path / 'source.parquet'
    shared = Path(os.environ['MO_SDK_EXPORT_LOCAL_DIR']) / (suffix + '.parquet')
    mapped = os.environ['MO_SDK_EXPORT_DIR'].rstrip('/') + '/' + shared.name
    pq.write_table(pa.table({'id': [1, 2]}), source)
    shutil.copyfile(source, shared)
    client.execute(f'CREATE TABLE {suffix}(id BIGINT PRIMARY KEY)')
    try:
        with client.analytics.read_parquet(source, server_paths=[mapped]) as rel:
            with rel.query(batch_size=1) as stream:
                assert next(stream).column(0).to_pylist() == [1]
                shared.unlink()
                with pytest.raises((DBAPIError, DriverOperationalError), match='not found|not exist'):
                    stream.to_arrow()
            assert client._engine.pool.checkedout() == 0
            shutil.copyfile(source, shared)
            original = Connection.exec_driver_sql

            def execute(conn, sql, *args, **kwargs):
                result = original(conn, sql, *args, **kwargs)
                if sql.startswith('INSERT INTO `' + suffix + '`'):
                    shared.unlink()
                return result

            with monkeypatch.context() as patch:
                patch.setattr(Connection, 'exec_driver_sql', execute)
                with pytest.raises((DBAPIError, DriverOperationalError), match='not found|not exist'):
                    rel.import_into(suffix)
            assert client.execute('SELECT COUNT(*) FROM ' + suffix).scalar() == 0
            assert client._engine.pool.checkedout() == 0
            shutil.copyfile(source, shared)
            assert rel.import_into(suffix) == 2
            assert client.execute('SELECT * FROM ' + suffix + ' ORDER BY id').rows == [(1,), (2,)]
    finally:
        client.execute('DROP TABLE ' + suffix)
        shared.unlink(missing_ok=True)


@pytest.fixture
def single_file_client(server):
    c = Client(pool_size=1, max_overflow=0, pool_timeout=1, sql_log_mode='off')
    c.connect(**server)
    try:
        yield c
        assert c._engine.pool.checkedout() == 0
    finally:
        c.disconnect()


def test_empty_mapped_json_file_is_not_missing(single_file_client, tmp_path):
    import pyarrow as pa

    client = single_file_client
    source = tmp_path / 'source.jsonl'
    source.write_text('{"id":1}\n')
    shared = Path(os.environ['MO_SDK_EXPORT_LOCAL_DIR']) / (name('empty_') + '.jsonl')
    shared.write_bytes(b'')
    mapped = os.environ['MO_SDK_EXPORT_DIR'].rstrip('/') + '/' + shared.name
    try:
        with client.analytics.read_json(source, server_paths=[mapped], schema=pa.schema([('id', pa.int64())])) as rel:
            with rel.query() as stream:
                assert stream.to_arrow().to_pylist() == []
        assert client.execute('SELECT 42').scalar() == 42
    finally:
        shared.unlink(missing_ok=True)


@pytest.mark.parametrize('mapping', ['local-hash', 'stage-percent'])
def test_unrepresentable_native_filename_rejects_without_ddl(single_file_client, tmp_path, mapping):
    from urllib.parse import quote
    import pyarrow as pa
    import pyarrow.parquet as pq

    client = single_file_client
    source = tmp_path / 'source.parquet'
    pq.write_table(pa.table({'id': [1]}), source)
    suffix = name('reserved_')
    shared = Path(os.environ['MO_SDK_EXPORT_LOCAL_DIR']) / (
        suffix + ('#' if mapping == 'local-hash' else '%') + '.parquet'
    )
    shared.write_bytes(source.read_bytes())
    stage = suffix + '_stage'
    remote = os.environ['MO_SDK_EXPORT_DIR'].rstrip('/')
    if mapping == 'stage-percent':
        client.stage.create(stage, 'file://' + remote + '/')
    mapped = (
        remote + '/' + shared.name
        if mapping == 'local-hash'
        else 'stage://' + stage + '/' + quote(shared.name, safe='/')
    )
    try:
        with pytest.raises((DBAPIError, DriverOperationalError)):
            client.analytics.read_parquet(source, server_paths=[mapped])
        assert not [r[0] for r in client.execute('SHOW TABLES').rows if r[0].startswith('mo_py_files_')]
        assert client.execute('SELECT 42').scalar() == 42
        assert client._engine.pool.checkedout() == 0
    finally:
        if mapping == 'stage-percent':
            client.stage.drop(stage, if_exists=True)
        shared.unlink(missing_ok=True)

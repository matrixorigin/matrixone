"""Catalog text must not interpret identifiers as index definitions."""

from matrixone.search_vector_index import PineconeCompatibleIndex


def test_quoted_index_text_is_not_an_index():
    handle = PineconeCompatibleIndex(None, 't', 'v')
    sql = """CREATE TABLE `t` (
        `id` INT PRIMARY KEY,
        `v` VECF32(2),
        `KEY fake USING ivfflat (v) op_type 'vector_cosine_ops'` INT,
        `other` VARCHAR(100) DEFAULT 'KEY fake USING hnsw (v)',
        `misleading, v vecf32(99)` INT
    )"""
    assert handle._parse_index_info(sql) == dict(algorithm=None, metric='l2', dimensions=2, parameters={})
    sql = sql.rsplit(')', 1)[0] + ", KEY `real` USING ivfflat (`v`) LISTS 1 op_type 'vector_l2_ops')"
    info = handle._parse_index_info(sql)
    assert (info['algorithm'], info['metric'], info['dimensions']) == ('ivfflat', 'l2', 2)


def test_distinct_vector_columns_have_distinct_algorithms():
    sql = """CREATE TABLE `t` (`a` vecf32(2), `b` vecf32(3),
      KEY `ivf` USING ivfflat (`a`) LISTS 1 op_type 'vector_cosine_ops',
      KEY `hnsw` USING hnsw (`b`) m = 16 op_type 'vector_l2_ops')"""
    a = PineconeCompatibleIndex(None, 't', 'a')._parse_index_info(sql)
    b = PineconeCompatibleIndex(None, 't', 'b')._parse_index_info(sql)
    assert (a['algorithm'], a['metric'], a['dimensions']) == ('ivfflat', 'cosine', 2)
    assert (b['algorithm'], b['metric'], b['dimensions']) == ('hnsw', 'l2', 3)

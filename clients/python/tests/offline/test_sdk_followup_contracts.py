"""Accepted OR branches and exact bounded file identities fail closed."""

import pytest
from matrixone.analytics import _server_file_source
from matrixone.sqlalchemy_ext.fulltext_search import FulltextQueryBuilder, boolean_match, natural_match, group


@pytest.mark.parametrize('method', ['must', 'encourage'])
def test_or_group_is_required_and_copies_children(method):
    a = getattr(boolean_match('body'), method)('alpha')
    b = getattr(boolean_match('body'), method)('beta')
    parent = boolean_match('body').must('match').group(a, b)
    before = parent.compile()
    assert "'+match +(alpha beta)'" in before
    assert parent.compile() == before
    a.must('later')
    assert parent.compile() == before
    assert b.query_builder.build() == ('+beta' if method == 'must' else 'beta')


def test_optional_or_branch_and_prefix_preserve_atoms():
    q = FulltextQueryBuilder().group(
        FulltextQueryBuilder().encourage('alpha', 'beta'), FulltextQueryBuilder().prefix('gam')
    )
    assert q.build() == '+(alpha beta gam*)'


@pytest.mark.parametrize(
    'child',
    [
        lambda: boolean_match('body').must('alpha', 'beta'),
        lambda: boolean_match('body').must('alpha').encourage('beta'),
        lambda: boolean_match('body').must_not('alpha'),
        lambda: boolean_match('body').discourage('alpha'),
        lambda: boolean_match('body').phrase('alpha beta'),
        lambda: boolean_match('body').encourage(group().medium('alpha', 'beta')),
        lambda: boolean_match('body').must('alpha beta'),
        lambda: boolean_match('body').must('alpha+beta'),
        lambda: boolean_match('body'),
        lambda: boolean_match('other').must('alpha'),
        lambda: natural_match('body', query='alpha'),
    ],
)
def test_unrepresentable_or_leaves_parent_unchanged(child):
    q = boolean_match('body').must('parent')
    before = q.compile()
    with pytest.raises(ValueError):
        q.group(boolean_match('body').must('valid'), child())
    assert q.compile() == before


def test_empty_and_invalid_groups_reject_atomically():
    q = boolean_match('body').must('parent')
    with pytest.raises(ValueError):
        q.group()
    with pytest.raises(TypeError):
        q.group(object())
    with pytest.raises(ValueError):
        natural_match('body', query='alpha').group(boolean_match('body').must('alpha'))
    assert "'+parent'" in q.compile()


@pytest.mark.parametrize(
    'path,scan,uri',
    [
        ('/data/a%#x.parquet', '/data/a%#x.parquet', 'file:///data/a%25%23x.parquet'),
        ('file:///data/a%25%23x.parquet', '/data/a%#x.parquet', 'file:///data/a%25%23x.parquet'),
        ('stage://example/a%25%23x.parquet', 'stage://example/a%25%23x.parquet', 'stage://example/a%25%23x.parquet'),
    ],
)
def test_probe_and_scan_share_literal_identity(path, scan, uri):
    assert _server_file_source(path) == (scan, uri + '?offset=0&size=1')


@pytest.mark.parametrize(
    'path',
    [
        'relative/file',
        'https://host/file',
        'file://remote/path',
        'stage://user@host/file',
        'stage://x/f?size=0',
        'stage://x/f#fragment',
        'file:///data/f?',
        '/data/f*',
        '/data/f?',
        '/data/f[x]',
        '/data/f\\x',
        'stage://x/f%3F',
        'file:///data/f%2A',
        'stage://x/f%00',
        '',
    ],
)
def test_ambiguous_server_mapping_rejects_before_io(path):
    with pytest.raises(ValueError):
        _server_file_source(path)

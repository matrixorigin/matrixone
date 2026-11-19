# `offline.log.txt`

Artifact: {"name":"offline.log.txt","bytes":3519,"sha256":"d833854c7df9316647af7a3da233478d2cdacd25d07573297e64db7fa4576cc0","encoding":"utf-8","stored_bytes":3519}

````text
============================= test session starts ==============================
platform darwin -- Python 3.12.8, pytest-9.1.1, pluggy-1.7.0 -- /Users/xupeng/.codex/issue-to-pr/matrixone/30994/venv/bin/python
cachedir: .pytest_cache
rootdir: /private/tmp/mo-pr30994/clients/python
configfile: pytest.ini (WARNING: ignoring pytest config in pyproject.toml!)
plugins: cov-7.1.0, asyncio-1.4.0, Faker-40.43.0
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collecting ... collected 26 items

tests/offline/test_metadata_offline.py::TestMetadataManager::test_format_size PASSED [  3%]
tests/offline/test_metadata_offline.py::TestMetadataManager::test_get_table_brief_stats PASSED [  7%]
tests/offline/test_metadata_offline.py::TestMetadataManager::test_get_table_brief_stats_with_indexes PASSED [ 11%]
tests/offline/test_metadata_offline.py::TestMetadataManager::test_get_table_brief_stats_with_tombstone PASSED [ 15%]
tests/offline/test_metadata_offline.py::TestMetadataManager::test_get_table_detail_stats PASSED [ 19%]
tests/offline/test_metadata_offline.py::TestMetadataManager::test_init PASSED [ 23%]
tests/offline/test_metadata_offline.py::TestMetadataManager::test_scan_basic PASSED [ 26%]
tests/offline/test_metadata_offline.py::TestMetadataManager::test_scan_with_index PASSED [ 30%]
tests/offline/test_metadata_offline.py::TestMetadataManager::test_scan_with_index_and_tombstone PASSED [ 34%]
tests/offline/test_metadata_offline.py::TestMetadataManager::test_scan_with_tombstone PASSED [ 38%]
tests/offline/test_metadata_offline.py::TestSessionMetadataManager::test_get_table_brief_stats_uses_session PASSED [ 42%]
tests/offline/test_metadata_offline.py::TestSessionMetadataManager::test_get_table_detail_stats_uses_session PASSED [ 46%]
tests/offline/test_metadata_offline.py::TestSessionMetadataManager::test_init PASSED [ 50%]
tests/offline/test_metadata_offline.py::TestSessionMetadataManager::test_scan_uses_session PASSED [ 53%]
tests/offline/test_metadata_offline.py::TestAsyncMetadataManager::test_get_table_brief_stats PASSED [ 57%]
tests/offline/test_metadata_offline.py::TestAsyncMetadataManager::test_get_table_detail_stats PASSED [ 61%]
tests/offline/test_metadata_offline.py::TestAsyncMetadataManager::test_init PASSED [ 65%]
tests/offline/test_metadata_offline.py::TestAsyncMetadataManager::test_scan_basic PASSED [ 69%]
tests/offline/test_metadata_offline.py::TestAsyncSessionMetadataManager::test_get_table_brief_stats_uses_session PASSED [ 73%]
tests/offline/test_metadata_offline.py::TestAsyncSessionMetadataManager::test_get_table_detail_stats_uses_session PASSED [ 76%]
tests/offline/test_metadata_offline.py::TestAsyncSessionMetadataManager::test_init PASSED [ 80%]
tests/offline/test_metadata_offline.py::TestAsyncSessionMetadataManager::test_scan_uses_session PASSED [ 84%]
tests/offline/test_metadata_offline.py::TestMetadataSQLConsistency::test_scan_sql_consistency PASSED [ 88%]
tests/offline/test_metadata_offline.py::TestMetadataSQLConsistency::test_scan_with_distinct_sql_consistency PASSED [ 92%]
tests/offline/test_metadata_offline.py::TestMetadataSQLConsistency::test_scan_with_index_sql_consistency PASSED [ 96%]
tests/offline/test_metadata_offline.py::TestMetadataSQLConsistency::test_scan_with_tombstone_sql_consistency PASSED [100%]

- generated xml file: /Users/xupeng/.codex/issue-to-pr/matrixone/30994/offline.xml -
============================== 26 passed in 1.65s ==============================

````

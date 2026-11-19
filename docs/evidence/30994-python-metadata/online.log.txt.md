# `online.log.txt`

Artifact: {"name":"online.log.txt","bytes":3938,"sha256":"4420f03929762f4f9b627806784827b914d331ce7846dcdb99ebd84fdbc913a9","encoding":"utf-8","stored_bytes":3938}

````text
============================= test session starts ==============================
platform darwin -- Python 3.12.8, pytest-9.1.1, pluggy-1.7.0 -- /Users/xupeng/.codex/issue-to-pr/matrixone/30994/venv/bin/python
cachedir: .pytest_cache
rootdir: /private/tmp/mo-pr30994/clients/python
configfile: pytest.ini (WARNING: ignoring pytest config in pyproject.toml!)
plugins: cov-7.1.0, asyncio-1.4.0, Faker-40.43.0
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collecting ... collected 31 items

tests/online/test_metadata_online.py::TestMetadataOnline::test_basic_metadata_scanning PASSED [  3%]
tests/online/test_metadata_online.py::TestMetadataOnline::test_column_selection_enum PASSED [  6%]
tests/online/test_metadata_online.py::TestMetadataOnline::test_column_selection_strings PASSED [  9%]
tests/online/test_metadata_online.py::TestMetadataOnline::test_distinct_object_name PASSED [ 12%]
tests/online/test_metadata_online.py::TestMetadataOnline::test_error_handling PASSED [ 16%]
tests/online/test_metadata_online.py::TestMetadataOnline::test_index_metadata_scanning PASSED [ 19%]
tests/online/test_metadata_online.py::TestMetadataOnline::test_metadata_column_enum_completeness PASSED [ 22%]
tests/online/test_metadata_online.py::TestMetadataOnline::test_metadata_row_from_sqlalchemy PASSED [ 25%]
tests/online/test_metadata_online.py::TestMetadataOnline::test_multiple_tables_metadata PASSED [ 29%]
tests/online/test_metadata_online.py::TestMetadataOnline::test_structured_metadata PASSED [ 32%]
tests/online/test_metadata_online.py::TestMetadataOnline::test_table_brief_stats PASSED [ 35%]
tests/online/test_metadata_online.py::TestMetadataOnline::test_table_detail_stats PASSED [ 38%]
tests/online/test_metadata_online.py::TestMetadataOnline::test_transaction_metadata_operations PASSED [ 41%]
tests/online/test_metadata_online.py::TestAsyncMetadataOnline::test_async_basic_metadata_scanning PASSED [ 45%]
tests/online/test_metadata_online.py::TestAsyncMetadataOnline::test_async_column_selection PASSED [ 48%]
tests/online/test_metadata_online.py::TestAsyncMetadataOnline::test_async_distinct_object_name PASSED [ 51%]
tests/online/test_metadata_online.py::TestAsyncMetadataOnline::test_async_structured_metadata PASSED [ 54%]
tests/online/test_metadata_online.py::TestAsyncMetadataOnline::test_async_table_brief_stats PASSED [ 58%]
tests/online/test_metadata_online.py::TestAsyncMetadataOnline::test_async_table_detail_stats PASSED [ 61%]
tests/online/test_metadata_online.py::TestMetadataSession::test_session_brief_stats PASSED [ 64%]
tests/online/test_metadata_online.py::TestMetadataSession::test_session_detail_stats PASSED [ 67%]
tests/online/test_metadata_online.py::TestMetadataSession::test_session_scan PASSED [ 70%]
tests/online/test_metadata_online.py::TestMetadataSession::test_session_scan_with_columns PASSED [ 74%]
tests/online/test_metadata_online.py::TestMetadataSession::test_session_with_distinct PASSED [ 77%]
tests/online/test_metadata_online.py::TestMetadataSession::test_session_with_tombstone PASSED [ 80%]
tests/online/test_metadata_online.py::TestAsyncMetadataSession::test_async_session_brief_stats PASSED [ 83%]
tests/online/test_metadata_online.py::TestAsyncMetadataSession::test_async_session_detail_stats PASSED [ 87%]
tests/online/test_metadata_online.py::TestAsyncMetadataSession::test_async_session_scan PASSED [ 90%]
tests/online/test_metadata_online.py::TestAsyncMetadataSession::test_async_session_scan_with_columns PASSED [ 93%]
tests/online/test_metadata_online.py::TestAsyncMetadataSession::test_async_session_with_distinct PASSED [ 96%]
tests/online/test_metadata_online.py::TestAsyncMetadataSession::test_async_session_with_tombstone PASSED [100%]

- generated xml file: /Users/xupeng/.codex/issue-to-pr/matrixone/30994/online.xml -
============================== 31 passed in 3.35s ==============================

````

# `async-challenge.log.txt`

Artifact: {"name":"async-challenge.log.txt","bytes":3675,"sha256":"967a1f38267c871b67f2b7f2c05a24cecf49a0900122414fe6e03a738b363b1e","encoding":"utf-8","stored_bytes":3675}

````text
2026-10-24 09:35:45,234|matrixone|INFO|[async_client.py:714]: ✓ Connected to MatrixOne | host=127.0.0.1 | port=56411 | user=root | database=test
2026-10-24 09:35:45,236|matrixone|INFO|[async_client.py:1434]: ✓ | 0.002s | 5 rows | SELECT * FROM metadata_scan('test_metadata_db.test_users', '*') g
2026-10-24 09:35:45,243|matrixone|INFO|[session.py:1265]: ✓ | 0.008s | SELECT * FROM metadata_scan('test_metadata_db.test_users', '*') g
2026-10-24 09:35:45,243|matrixone|INFO|[session.py:1265]: ✓ | 0.008s | SELECT * FROM metadata_scan('test_metadata_db.test_users', '*') g
2026-10-24 09:35:45,243|matrixone|INFO|[async_client.py:1434]: ✓ | 0.009s | 5 rows | SELECT * FROM metadata_scan('test_metadata_db.test_users', '*') g
2026-10-24 09:35:45,244|matrixone|INFO|[session.py:1265]: ✓ | 0.009s | SELECT * FROM metadata_scan('test_metadata_db.test_users', '*') g
2026-10-24 09:35:45,244|matrixone|INFO|[async_client.py:1434]: ✓ | 0.009s | 5 rows | SELECT * FROM metadata_scan('test_metadata_db.test_users', '*') g
2026-10-24 09:35:45,245|matrixone|INFO|[session.py:1265]: ✓ | 0.009s | SELECT * FROM metadata_scan('test_metadata_db.test_users', '*') g
2026-10-24 09:35:45,245|matrixone|INFO|[async_client.py:1434]: ✓ | 0.010s | 5 rows | SELECT * FROM metadata_scan('test_metadata_db.test_users', '*') g
2026-10-24 09:35:45,248|matrixone|ERROR|[async_client.py:1448]: ✗ | 0.002s | [ERROR] SELECT * FROM metadata_scan('test_metadata_db.missing30994', '*') g
2026-10-24 09:35:45,248|matrixone|ERROR|[async_client.py:1449]: Error occurred: ProgrammingError | error_type=ProgrammingError | error_message=(pymysql.err.ProgrammingError) (1146, 'no such table test_metadata_db.missing30994')
[SQL: SELECT * FROM metadata_scan('test_metadata_db.missing30994', '*') g]
(Background on this error at: https://sqlalche.me/e/21/f405) | context=Async query execution
2026-10-24 09:35:45,249|matrixone|ERROR|[session.py:1279]: ✗ | 0.001s | [ERROR] SELECT * FROM metadata_scan('test_metadata_db.missing30994', '*') g
2026-10-24 09:35:45,249|matrixone|ERROR|[session.py:1285]: Error occurred: ProgrammingError | error_type=ProgrammingError | error_message=(pymysql.err.ProgrammingError) (1146, 'no such table test_metadata_db.missing30994')
[SQL: SELECT * FROM metadata_scan('test_metadata_db.missing30994', '*') g]
(Background on this error at: https://sqlalche.me/e/21/f405) | context=Async session query execution
2026-10-24 09:35:45,251|matrixone|INFO|[async_client.py:1434]: ✓ | 0.001s | 5 rows | SELECT * FROM metadata_scan('test_metadata_db.test_users', '*') g
2026-10-24 09:35:45,262|matrixone|INFO|[async_client.py:948]: ✓ Disconnected from MatrixOne
ASYNC_CHALLENGE_PASS {"overlapping_calls": [{"call": 0, "context": "client", "row_cnt": 50}, {"call": 1, "context": "independent-session", "row_cnt": 50}, {"call": 2, "context": "client", "row_cnt": 50}, {"call": 3, "context": "independent-session", "row_cnt": 50}, {"call": 4, "context": "client", "row_cnt": 50}, {"call": 5, "context": "independent-session", "row_cnt": 50}, {"call": 6, "context": "client", "row_cnt": 50}, {"call": 7, "context": "independent-session", "row_cnt": 50}], "propagated_errors": [{"context": "client", "type": "QueryError", "message": "Table or database 'test_metadata_db.missing30994' does not exist. Create it first using client.create_table() or CREATE TABLE/DATABASE statement."}, {"context": "session", "type": "QueryError", "message": "Table or database 'test_metadata_db.missing30994' does not exist. Create it first using client.create_table() or CREATE TABLE/DATABASE statement."}], "checked_out_connections_after_calls": 0, "post_error_row_cnt": 50, "disconnected": true}

````

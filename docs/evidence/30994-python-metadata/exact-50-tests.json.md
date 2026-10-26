# `exact-50-tests.json`

Artifact: {"name":"exact-50-tests.json","bytes":688,"sha256":"32077fe04ea81ab46559caca729adf42ccf3fc03c263ae8bc5bef05500b9afd0","encoding":"utf-8","stored_bytes":688}

````json
{
  "online_cases": 31,
  "skipped": 0,
  "exact_row_assertions": [
    {
      "class": "TestMetadataOnline",
      "test": "test_table_brief_stats",
      "result": "passed",
      "expected_row_count": 50
    },
    {
      "class": "TestAsyncMetadataOnline",
      "test": "test_async_table_brief_stats",
      "result": "passed",
      "expected_row_count": 50
    },
    {
      "class": "TestMetadataSession",
      "test": "test_session_brief_stats",
      "result": "passed",
      "expected_row_count": 50
    },
    {
      "class": "TestAsyncMetadataSession",
      "test": "test_async_session_brief_stats",
      "result": "passed",
      "expected_row_count": 50
    }
  ]
}
````

# `async-challenge-results.json`

Artifact: {"name":"async-challenge-results.json","bytes":1291,"sha256":"67dc3e4d27c07ab98cccfffbc28fe321a072279d27f92ba749682d4df42f1cb2","encoding":"utf-8","stored_bytes":1291}

````json
{
  "overlapping_calls": [
    {
      "call": 0,
      "context": "client",
      "row_cnt": 50
    },
    {
      "call": 1,
      "context": "independent-session",
      "row_cnt": 50
    },
    {
      "call": 2,
      "context": "client",
      "row_cnt": 50
    },
    {
      "call": 3,
      "context": "independent-session",
      "row_cnt": 50
    },
    {
      "call": 4,
      "context": "client",
      "row_cnt": 50
    },
    {
      "call": 5,
      "context": "independent-session",
      "row_cnt": 50
    },
    {
      "call": 6,
      "context": "client",
      "row_cnt": 50
    },
    {
      "call": 7,
      "context": "independent-session",
      "row_cnt": 50
    }
  ],
  "propagated_errors": [
    {
      "context": "client",
      "type": "QueryError",
      "message": "Table or database 'test_metadata_db.missing30994' does not exist. Create it first using client.create_table() or CREATE TABLE/DATABASE statement."
    },
    {
      "context": "session",
      "type": "QueryError",
      "message": "Table or database 'test_metadata_db.missing30994' does not exist. Create it first using client.create_table() or CREATE TABLE/DATABASE statement."
    }
  ],
  "checked_out_connections_after_calls": 0,
  "post_error_row_cnt": 50,
  "disconnected": true
}
````

ElasticSearch ES|QL TVF
=========================

This is to design and implement a elastic search foreign data search.  This TVF
is mainly used for query time integration, not for data loading -- while data loading 
CAN use this TVF.   Query time means that the schema of ESQL is decided at runtime
during MO query parsing and planning. 

```
select * from esql_tvf(esql, schema, conn) t

Where
    esql: is a elastic search ES|QL query.  This tvf will send the esql to elastic search.
    schema: Reuse same schema specification from parse_jsonl_data TVF.  If not sepcified, or if null, 
            the result has one column, which should be a json array type, each element of the array is string.
    conn: A connection handle, from the esql_tvf_connect function.   If not specified, or null, use default.  See esql_tvf_connect.
```

Use the CSV format for ESQL query result.  Parse csv according to the column spec in schema, and error if the format
errors.   Reuse as much CSV parsing as possible, preferrably totally reuse as in csv external table, treating response
from ESQL as IO stream.

Connection Management: two functions.

```
esql_tvf_connect(config): --
    config: A json string that can be parsed as elasticsearch.Config,  use this config to establish a connection.
            Cache this connection in the session object, so that next time if the config has been availabe in this
            session, reuse the connection.  If config is null, try to use session variable @esql_tvf_config.
    return: A handle. Error if cannot establish connection.

esql_tvf_disconnect(handle): --
    handle: returned from esql_connect.   Disconnect and remove handle from session cache.  Later esql requeset using 
            this handle, or using elastic client obtained from this handle, should error.
```

When session closes, should automatically close all connections and clean up client cache.  Check if our session object
has a finalizer.   If it has, also hook up this cache cleanup in finalizer.  If session object does not have finalizer 
yet, add one.

This function essentially is not parallelizable so make sure we do not run any single instance of the function in
parallel but different instances of esql tvf in same query should run in parallel.   Query processing later after 
the esql such as join, aggregate, etc, should be parallel as needed as decided by optimizer.

SQL TVF
================== 
Same as `esql_tvf`, implement `sql_tvf`
```
select * from sql_tvf(sql, schema, conn) t;
```

And the following two functions,
```
sql_tvf_connect(config): --
    config is a json that contains enough information to establish a sql connection.   Currently mysql and
    postgresql are supported; oracle and ms sqlserver are planned follow-ups behind the same driver
    registry (adding one is a blank import plus an alias entry).   Example config
    {
        "driver": "mysql",
        "dsn": "connection-string-to-my-database"
    }
    Use @sql_tvf_config as default.

sql_tvf_disconnect(handle)
```

Same connection/disconnection/session management.

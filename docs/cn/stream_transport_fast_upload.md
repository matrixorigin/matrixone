Streaming Transport Fast Upload
===============================

Create a new type of external table datastream, using the following syntax
```
CREATE EXTERNAL TABLE t (
        col1 int,
        col2 timestamp,
        col3 varchar(100),
        col4 text
        -- all column definitions
) 
ENGINE = datastream
WITH (
        'server' = 'grpc_server_ip',
        'port' = 'grpc_port',
        'table' = 'the_table_to_read_from',
        'recheck' = true/false,
);
```

After creation, user can issue sqlqueries 
```
SELECT * FROM T
WHERE col2 > '2020-11-11 00:00:00'
```

To execute this query, an external table scan should open connection to a grpc server
running at the `server:port`.  Send a streaming rpc request to the server, to read the
data.   The request should contain parameters to read data from `the_table_to_read_from`
and the request should push down predicates `col2 > '2020-11-11 00:00:00'`.   
Note that this pushed down predicates is a hint to grpc server.   Grpc server should 
apply the filter as early as possible, but, if the grpc server does not has the ablity
to evaluate this filter, it is OK for gprc server to return all records.   The external
table should apply the filter condition again during query execution.   There is an 
option `recheck`, default to true.   If `recheck` is false, matrixone query execution
can skip this recheck. 

The grpc call should be streaming.  For each chunk, it should contain data -- the data 
should be encoded in a correct CSV format.  The data should be a series of complete 
record encoded in CSV, that is, no record should span two chunks of the stream. 

The external table then should output these records.   The code should try to reuse as 
much code in the csv external table as possible.

Define necessary protobuf files for this grpc service.   Note the response may be an
error (such as table not found in the service, or error during execution) and the 
protobuf should define error messages as well.

Java GRPC Server
================

The first external table candidate should be a bridge java ecosystem.   Create a java 
project at @xtool/jstfu, implement the grpc service.   When start, this service should
read a configuration file.  When client connect and ask to read from a table, the server
lookup a datasource with name matching the table, and read data, encode records as csv
and streaming csv data chunck by chunk.   Make chucksize reasonable, for example around
1MB. 

We will implement 2 types of data source, jdbc and file. 

The jdbc data source should open jdbc connection to a database, lookup the sql query, and 
IF the SQL statement has a place holder `${FILTER}`, replace the ${FILTER} with the filter 
condition of the request.  Replace `${FILTER}` with `1=1` if the gprc request has no filter 
condition.  

The file data source should just read a file from the file system.  At this moment we 
assume the file is a valid csv file, otherwise rpc server should replied error when failed
to open the file, or failed to parse during data streaming.   Note that it is the grpc 
server's resposibility to make sure the records does not span chunks of grpc data stream.
File data source does not handle filter condition of the request.   Treat that as a noop.

```
{
    "port": 4444,
    "datasource": [
        { "name": "the_table_to_read_from",
          "type": "jdbc",
          "connectionstring": "jdbc:mysql://...",
          "user": "dump",
          "password": "123",
          "sql": "select col1, col2, col3, col4 from table_or_view_or_subquery_from_jdbc where ${FILTER}"
        },
        { "name": "the_table_to_read_from",
          "type": "file",
          "path": "/path/to/file"
        }
    ]
}
```


Deployment
================

Build the jar file for the grpc server.   Include necessary dependencies jar file.   And deploy all
the jar files, in both product release artifacts and docker images for tests.

Add all these to Makefile as a build target.

Test
================

Add bvt tests.  Test both file and jdbc.    Jdbc can use mysql jdbc and connect back to matrixone server
as matrixone server should be compatible with mysql jdbc driver.   Test error conditions, including jdbc
sql execution errors, or jdbc connection broken, etc.  

Test use this as etl tool.   That is, create a destination table and external table.   Read data from external 
table and insert into destination table.   Test parallel load by issuing several sql at the same time but with
different FILTER conditions for each load.






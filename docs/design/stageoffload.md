StageOffload
=============================

Allow to offload a column, or part of column to a stage file.   Store the datalink 
in the column or part of the column and load from stage AS LATE AS possible in the 
query processing.

SQL Syntax
----------------------------
Suppose we already have created a stage S, we create table with an option
```
CREATE TABLE T (id int not null primary key, b blob, c text, d blob, j json, jp json)
    WITH STAGEOFFLOAD = '{
       "stage": "S", 
       "directory_nest": 2,                         -- path is stage://S/XX/YY/ZZZZZZZZZ
       [                                            -- a list of offloaded columns
       {"column": "b", "compress": "no"},           -- offload column b, do not compress (jpg files already compressed)
       {"column": "c", "compress": "zstd"},         -- offload column c, compress with zstd
                                                    -- column d, even it is blob, not offloaded
       {"column": "j", "compress": "zstd"},         -- offload column j, compress with zstd, whole json column
       {"column": "j", "json": {                    -- json column, we will only offset part, using a strategy
                "tags": [                           -- json strategy is based on tags
                    {"tag": "image", "compress": "no"},     -- image binaryblob, just store
                    {"tag": "tdf", "compress": "lz4"}       -- dataframe, compress using lz4.
                ]}}]
    }'
;
```

We assume stage S already exists.  We will actually allow multiple table share a stage.  We do not allow one table
to offload to 2 or more different stages. 

User can still alter table, but the alter statement cannot change any of the offloaded columns and cannot change
the offset option.

Implementation Note
-----------------------------
At the INSERT node, the node should look at 

                

                    
           


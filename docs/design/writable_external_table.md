User can create an external table.  At this moment the external table is read only.   
We need to make external table writable.  Suppose user created an external table T, user
can write to the table with

```
INSERT INTO T SELECT * FROM ...
```

Insert into external table should be done in the same way as insert into a matrixone table.
The query should be planned and optimized, and when insert rows, instead of inserting into
matrixone table, it should just call a API and add rows into the table.  `LOAD` should load 
data into the external table using same API.  The API should be invoked using batches, to 
try to load multiple rows in one batch.

When `INSERT` or `LOAD` a large amount of data, it should be able to run on multi CN in parallel.
Just call the external insert API in parallel and we will assume the writer will be able to write
external table without causing race condition. 

At this moment, we do not support `UPDATE` and `DELETE`, we will add this feature later.

As implementation, we will only support INSERT to csv files and jsonline files.   For external table,
it must have an additional config option `WRITE_FILE_PATTERN=strftime_string`, such that newly inserted
data is written to a new file, (or many new files if there are parallel writers, but each of the pipeline
should only create one file).   The `strftime_string` can contain `%` formatting characters as strftime.
We will extend strftime with the following.   
   1. `%nN` be replaced by n random digit numbers.  
   2. `%U` be replaced by a generated UUID

For CSV and jsonline file, the `strftime_string` should point to a valid, writable stage, `stage://...` 






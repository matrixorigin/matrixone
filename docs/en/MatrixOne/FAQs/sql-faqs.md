# **SQL FAQs**

* **Are functions and other keywords case sensitive？**

  No, they are not case sensitive. Only in one case case is sensitive in MatrixOne, if user creates table and attributes with \`\`, the name in\`\` is case sensitive. To find this table name or attribute name in your query, it needs to be in \`\` as well.

* **How do I export data from MatrixOne to a file?**

  You can use `SELECT INTO OUTFILE` command to export data from MatrixOne to a **csv** file (only to the server host, not to the remote client).  
  For this command, you can see [SELECT Reference](../Reference/SQL-Reference/Data-Manipulation-Statements/select.md).  

* **What is the size limit of a transaction in MatrixOne?**

  The transaction size is limited to the memory size you have for hardware environment.

* **What kind of character set does MatrixOne support?**

  MatrixOne supports the UTF-8 character set by default and currently only supports UTF-8.

* **What is the `sql_mode` in MatrixOne?**

  MatrixOne doesn't support modifying the `sql_mode` for now, the default `sql_mode` is as the `only_full_group_by` in MySQL.

* **How do I bulk load data into MatrixOne?**

  MatrixOne provides two methods of bulk load data: 1. Using `source filename` command from shell, user can load the SQL file with all DDL and insert data statements. 2. Using `load data infile...into table...` command from shell, user can load an existing .csv file to MatrixOne.

* **How do I know how my query is executed?**

  To see how MatrixOne executes for a given query, you can use the [`EXPLAIN`](https://docs.matrixorigin.io/0.5.0/MatrixOne/Reference/SQL-Reference/Explain/explain/) statement, which will print out the query plan.

  ```
  EXPLAIN SELECT col1 FROM tbl1;
  ```

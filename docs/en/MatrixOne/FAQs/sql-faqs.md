## **SQL FAQs**

* **Whether functions and other keywords are case sensitive？**

   No, they are not case sensitive.

* **How do I export data from MatrixOne to a file?**

  You can use `SELECT INTO OUTFILE` command to export data from MatrixOne to a **csv** file (only to the server host, not to the remote client).  
  For this command, you can see [SELECT Reference](../Reference/SQL-Reference/Data-Manipulation-Statements/select.md).  

* **What is the size limit of a transaction in MatrixOne?**

  The transaction size is limited to the memory size you have for hardware environment. 


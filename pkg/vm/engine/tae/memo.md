## Transaction
1. Support transactional DDL. DDL and DML statements can be in a transaction together
2. If some statement has failed, this statement is rolled back but the transaction is not rolled back. If you run COMMIT, TAE commits all non rolled back statements of the current transaction: that is what happened. if you run ROLLBACK, all statements are rolled back whether they failed or succeeded.

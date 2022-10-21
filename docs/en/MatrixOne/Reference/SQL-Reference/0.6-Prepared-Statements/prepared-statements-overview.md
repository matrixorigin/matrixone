# Prepared Statements Overview

MatrixOne provides support for server-side prepared statements. This support takes advantage of the efficient client/server binary protocol. Using prepared statements with placeholders for parameter values has the following benefits:

Less overhead for parsing the statement each time it is executed. Typically, database applications process large volumes of almost-identical statements, with only changes to literal or variable values in clauses such as WHERE for queries and deletes, SET for updates, and VALUES for inserts.

Protection against SQL injection attacks. The parameter values can contain unescaped SQL quote and delimiter characters.

## PREPARE, EXECUTE, and DEALLOCATE PREPARE Statements

SQL syntax for prepared statements is based on three SQL statements:

- [PREPARE](prepare.md) prepares a statement for execution.

- [EXECUTE](execute.md) executes a prepared statement.

- [DEALLOCATE PREPARE](deallocate.md) releases a prepared statement.

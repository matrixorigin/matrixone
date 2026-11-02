# Least-privilege application accounts

MatrixOne grants object privileges to **roles**, then grants those roles to
users. Its explicit `ACCOUNT`, `DATABASE` and `TABLE` scopes are not MySQL's
implicit `GRANT ... ON db.* TO user` syntax.

For an application that owns migrations and CRUD in a pre-created database,
an administrator can provision a non-administrator role as follows:

```sql
CREATE DATABASE app_db;
CREATE ROLE app_role;
CREATE USER app_user IDENTIFIED BY 'replace-with-a-secret';
GRANT app_role TO app_user;

GRANT CONNECT ON ACCOUNT * TO app_role;
GRANT ALL PRIVILEGES ON DATABASE app_db TO app_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE app_db.* TO app_role;
```

Log in as `sys#app_user#app_role` (or `account#app_user#app_role` for another
account), using `app_db` as the connection database. `USE app_db` is also
available after the independent CONNECT grant. No moadmin/accountadmin grant
is needed. Keep the password out of command-line arguments and logs.

The three grants serve different purposes:

| Grant | Purpose |
| --- | --- |
| CONNECT ON ACCOUNT * | Allows `USE`; it does not authorize reading or writing arbitrary databases. |
| ALL ON DATABASE app_db | Database-scoped operations such as creating, altering and dropping tables/views; not blanket DML on tables owned by other roles. |
| SELECT, INSERT, UPDATE, DELETE ON TABLE app_db.* | CRUD across existing and future tables within this database, including administrator-created tables. |

`ON TABLE app_db.*` is database-wide **table** scope, not one grant per
table. For narrower access, use `ON TABLE app_db.table_name`. Table ownership
already authorizes the owning role's DML on tables it creates; explicit DML
grants also work regardless of which role created the table. Database DDL
privileges alone do not authorize DML on another role's tables.

Applications that do not run migrations should receive only CONNECT and the
required table privileges, not ALL ON DATABASE. Restrict `GRANT OPTION` to
administrators unless delegation is explicitly required. In particular,
`GRANT ALL ON TABLE *.*` is not an appropriate substitute for a single
application database: it grants account-wide table access.

## Common compatibility errors

- `GRANT ALL ON app_db.* TO app_user` omits MatrixOne's object type and uses a
  user as a privilege grantee. Use the role-based statements above instead.
- `GRANT UPDATE, DELETE ON DATABASE app_db TO app_role` mixes table privileges
  with a database object key. Use `ON TABLE app_db.*`.
- A connection that selects its database in the handshake is not proof that
  the role has CONNECT; `USE` checks CONNECT independently.

The real-client regression `TestLeastPrivilegeApplicationRole` covers owned
and administrator-created tables, committed UPDATE/DELETE effects, the same
session before and after grants, and denial of cross-database writes and
unauthorized delegation, with the privilege cache both enabled and disabled.

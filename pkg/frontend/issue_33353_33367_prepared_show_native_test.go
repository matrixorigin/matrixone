//go:build integration

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Maintained native regression for issues #33353 and #33367: server-side SQL
// prepared SHOW statements must keep validating the target their EXECUTE
// depends on across committed cross-session DDL and named-snapshot lifecycle
// changes, while the same original prepared handle stays in use.
//
// Coverage beyond the issue reproductions, required by the dependency capture:
//   - origin: unqualified forms prepared in the FIRST database stay bound to
//     that database's object after the session USEs a SECOND database with a
//     same-name distinguishable table, agreeing with the pre-existing prepared
//     SHOW COLUMNS surface at the same boundary;
//   - authorization: a restricted role receives exactly its direct controls'
//     outcomes through its prepared handle at invalidation boundaries,
//     including an inaccessible target and a mid-test revoked grant, so the
//     invalidation rebuild can never out-authorize the direct route;
//   - metadata: SHOW CREATE TABLE over a view (the delegation route) refreshes
//     like the direct SHOW CREATE VIEW spelling after a committed view
//     redefinition.
//
// Every fixture is bounded (<=3 rows); ordering is explicit statements on
// separate sessions, no sleeps; cleanup runs on all exits via t.Cleanup.

func issue3335xExec(t *testing.T, ctx context.Context, db *sql.DB, query string) {
	t.Helper()
	_, err := db.ExecContext(ctx, query)
	require.NoError(t, err, "exec: %s", query)
}

// issue3335xBestEffort issues cleanup from deferred contexts. It never fails
// the test: an earlier failed step may already have removed the object.
func issue3335xBestEffort(ctx context.Context, db *sql.DB, query string) {
	_, _ = db.ExecContext(ctx, query)
}

// issue3335xRows renders every cell as text; every assertion here only compares
// strings, so equal results are equal text. "<nil>" keeps SQL NULL distinct
// from the empty string and SQL 'NULL' markers.
func issue3335xRows(t *testing.T, ctx context.Context, db *sql.DB, query string) [][]string {
	t.Helper()
	rows, err := db.QueryContext(ctx, query)
	require.NoError(t, err, "query: %s", query)
	defer func() { require.NoError(t, rows.Close()) }()
	cols, err := rows.Columns()
	require.NoError(t, err)
	var out [][]string
	for rows.Next() {
		raw := make([]any, len(cols))
		for i := range raw {
			raw[i] = new(any)
		}
		require.NoError(t, rows.Scan(raw...))
		row := make([]string, len(cols))
		for i, cellRaw := range raw {
			cell := cellRaw.(*any)
			switch value := (*cell).(type) {
			case nil:
				row[i] = "<nil>"
			case []byte:
				row[i] = string(value)
			default:
				row[i] = fmt.Sprintf("%v", value)
			}
		}
		out = append(out, row)
	}
	require.NoError(t, rows.Err(), "query: %s", query)
	return out
}

// issue3335xQueryErr runs a result-set statement and returns its error, so
// failure-path assertions compare the exact wire error a client observes while
// staying inside the subtest's bounded context.
func issue3335xQueryErr(ctx context.Context, db *sql.DB, query string) error {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	return rows.Err()
}

// issue3335xShowIndexKeys reduces SHOW INDEX rows to the Key_name column so
// assertions do not depend on how auxiliary metadata columns render.
func issue3335xShowIndexKeys(rows [][]string) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row[2])
	}
	return out
}

func issue3335xText(rows [][]string) string {
	out := ""
	for i, row := range rows {
		if i > 0 {
			out += "\n"
		}
		out += fmt.Sprintf("%v", row)
	}
	return out
}

// issue3335xMysqlErrNo returns the wire error number behind a failed
// statement, pinning error-class comparisons to the protocol code.
func issue3335xMysqlErrNo(t *testing.T, err error) int {
	t.Helper()
	require.Error(t, err, "expected a failed statement")
	var mysqlErr *mysql.MySQLError
	require.True(t, errors.As(err, &mysqlErr),
		"expected a MySQL protocol error, got: %v", err)
	return int(mysqlErr.Number)
}

// issue3335xSession opens one independent mysql-protocol session. MaxOpenConns(1)
// keeps each SQL-level PREPARE/EXECUTE pair on one session, because SQL prepared
// statements are session state.
func issue3335xSession(t *testing.T, ctx context.Context, cn embed.ServiceOperator, credentials, database string) *sql.DB {
	t.Helper()
	db := openNativeViewSchemaSQL(t, ctx, cn, credentials, database)
	db.SetMaxOpenConns(1)
	return db
}

// TestIssue33353PreparedShowInvalidationNative drives SQL-prepared SHOW
// statements through committed cross-session catalog changes on a real CN.
func TestIssue33353PreparedShowInvalidationNative(t *testing.T) {
	runNativeViewSchemaCluster(t, func(cn embed.ServiceOperator, svc cnservice.Service) {
		t.Run("copy_alter_refreshes_same_original_handle", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			admin := issue3335xSession(t, ctx, cn, "dump:111", "")
			ddl := issue3335xSession(t, ctx, cn, "dump:111", "")
			defer func() { issue3335xBestEffort(ctx, admin, "drop database if exists e3335x_copy") }()

			issue3335xExec(t, ctx, admin, "create database e3335x_copy")
			issue3335xExec(t, ctx, admin, "create table e3335x_copy.t(a int, b int)")

			// The prepared-statement session adopts its database the same way
			// the issue reproduction does: an explicit USE after creation.
			prep := issue3335xSession(t, ctx, cn, "dump:111", "")
			issue3335xExec(t, ctx, prep, "use e3335x_copy")

			issue3335xExec(t, ctx, prep, "prepare p_create from 'SHOW CREATE TABLE t'")
			defer func() { issue3335xBestEffort(ctx, prep, "deallocate prepare p_create") }()
			issue3335xExec(t, ctx, prep, "prepare p_columns from 'SHOW COLUMNS FROM t'")
			defer func() { issue3335xBestEffort(ctx, prep, "deallocate prepare p_columns") }()
			warmCreate := issue3335xRows(t, ctx, prep, "execute p_create")
			require.Contains(t, issue3335xText(warmCreate), "`b` int")
			require.NotContains(t, issue3335xText(warmCreate), "`c` int",
				"fixture must not contain c before the ALTER")
			issue3335xExec(t, ctx, prep, "execute p_columns")

			issue3335xExec(t, ctx, ddl,
				"alter table e3335x_copy.t algorithm=copy, add column c int default 7")

			post := issue3335xRows(t, ctx, prep, "execute p_create")
			direct := issue3335xRows(t, ctx, prep, "show create table t")
			issue3335xExec(t, ctx, prep, "prepare f_create from 'SHOW CREATE TABLE t'")
			defer func() { issue3335xBestEffort(ctx, prep, "deallocate prepare f_create") }()
			fresh := issue3335xRows(t, ctx, prep, "execute f_create")
			columns := issue3335xRows(t, ctx, prep, "execute p_columns")
			require.Contains(t, issue3335xText(post), "`c` int DEFAULT 7",
				"the same original handle must show the committed COPY ADD definition")
			require.Equal(t, direct, post, "prepared SHOW CREATE must equal direct control after COPY ADD")
			require.Equal(t, fresh, post, "prepared SHOW CREATE must equal the fresh control after COPY ADD")
			require.Contains(t, issue3335xText(columns), "c", "prepared SHOW COLUMNS keeps the positive-control")

			issue3335xExec(t, ctx, ddl,
				"alter table e3335x_copy.t algorithm=copy, rename column c to c_renamed")

			postRename := issue3335xRows(t, ctx, prep, "execute p_create")
			directRename := issue3335xRows(t, ctx, prep, "show create table t")
			require.Equal(t, directRename, postRename,
				"prepared SHOW CREATE must equal direct control after COPY RENAME")
			require.Contains(t, issue3335xText(postRename), "`c_renamed` int DEFAULT 7")
			require.NotContains(t, issue3335xText(postRename), "`c` int",
				"renamed column spelling must leave the handle")
		})

		t.Run("unqualified_origin_survives_use_of_second_db", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			admin := issue3335xSession(t, ctx, cn, "dump:111", "")
			ddl := issue3335xSession(t, ctx, cn, "dump:111", "")
			defer func() {
				issue3335xBestEffort(ctx, admin, "drop database if exists e3335x_o1")
				issue3335xBestEffort(ctx, admin, "drop database if exists e3335x_o2")
			}()

			issue3335xExec(t, ctx, admin, "create database e3335x_o1")
			issue3335xExec(t, ctx, admin,
				"create table e3335x_o1.t(a int, v int, key idx_o1(v))")
			issue3335xExec(t, ctx, admin, "create database e3335x_o2")
			issue3335xExec(t, ctx, admin,
				"create table e3335x_o2.t(a int, w int, key idx_o2(w))")

			// The unqualified forms are prepared in the FIRST database, adopted
			// through an explicit USE after the fixture exists.
			prep := issue3335xSession(t, ctx, cn, "dump:111", "")
			issue3335xExec(t, ctx, prep, "use e3335x_o1")

			// Unqualified forms are prepared in the FIRST database: one handle
			// per affected surface, plus the pre-existing SHOW COLUMNS control.
			issue3335xExec(t, ctx, prep, "prepare p_create from 'SHOW CREATE TABLE t'")
			defer func() { issue3335xBestEffort(ctx, prep, "deallocate prepare p_create") }()
			issue3335xExec(t, ctx, prep, "prepare p_idx from 'SHOW INDEX FROM t'")
			defer func() { issue3335xBestEffort(ctx, prep, "deallocate prepare p_idx") }()
			issue3335xExec(t, ctx, prep, "prepare p_cols from 'SHOW COLUMNS FROM t'")
			defer func() { issue3335xBestEffort(ctx, prep, "deallocate prepare p_cols") }()
			warmCreate := issue3335xRows(t, ctx, prep, "execute p_create")
			require.Contains(t, issue3335xText(warmCreate), "`v` int",
				"the unqualified SHOW CREATE handle must warm on the FIRST database's definition")
			require.NotContains(t, issue3335xText(warmCreate), "`w` int")
			warmIdx := issue3335xRows(t, ctx, prep, "execute p_idx")
			require.Equal(t, []string{"idx_o1"}, issue3335xShowIndexKeys(warmIdx))
			warmCols := issue3335xRows(t, ctx, prep, "execute p_cols")

			// The ORIGINAL session now defaults to the SECOND database, whose
			// same-name table is distinguishable by both its index and columns.
			issue3335xExec(t, ctx, prep, "use e3335x_o2")

			// Cross-session, committed, decisive first-table change while the
			// second same-name table exists: the original is dropped and
			// recreated with a different primary key and a new index, so the
			// captured dependency still tracks e3335x_o1.t after the USE and
			// every surface changes observably across the replan.
			issue3335xExec(t, ctx, ddl, "drop table e3335x_o1.t")
			issue3335xExec(t, ctx, ddl,
				"create table e3335x_o1.t(id int primary key, v int, key idx_o1b(v))")

			postCreate := issue3335xRows(t, ctx, prep, "execute p_create")
			postIdx := issue3335xRows(t, ctx, prep, "execute p_idx")
			postCols := issue3335xRows(t, ctx, prep, "execute p_cols")

			// The current qualified definitions of both same-name candidates
			// in the same session state after the USE.
			firstCreate := issue3335xRows(t, ctx, prep, "show create table e3335x_o1.t")
			secondCreate := issue3335xRows(t, ctx, prep, "show create table t")
			firstIdx := issue3335xRows(t, ctx, prep, "show index from e3335x_o1.t")
			secondIdx := issue3335xRows(t, ctx, prep, "show index from t")
			firstCols := issue3335xRows(t, ctx, prep, "show columns from e3335x_o1.t")
			secondCols := issue3335xRows(t, ctx, prep, "show columns from t")
			t.Logf("origin boundary: warm create=%v warm cols=%v post create=%v post idx=%v post cols=%v first create=%v second create=%v first idx=%v second idx=%v",
				warmCreate, warmCols, postCreate, postIdx, postCols, firstCreate, secondCreate, firstIdx, secondIdx)

			// The invalidation must fire through the FIRST database's binding:
			// the warmed generation cannot survive the committed recreate.
			require.NotEqual(t, warmCreate, postCreate,
				"the FIRST database's committed recreate must replan the unqualified SHOW CREATE handle")

			// Exact origin: after USE of the second database, each unqualified
			// handle prepared in the FIRST database returns the FIRST
			// database's current qualified result, byte for byte.
			require.Equal(t, firstCreate, postCreate,
				"prepared SHOW CREATE must equal the FIRST database's current qualified result after USE")
			require.Equal(t, firstIdx, postIdx,
				"prepared SHOW INDEX must equal the FIRST database's current qualified result after USE")
			require.Equal(t, firstCols, postCols,
				"prepared SHOW COLUMNS must equal the FIRST database's current qualified result after USE")

			// Exact second-DB controls: the same-name candidate stays
			// distinguishable on every surface, so a rebind could not pass
			// these equalities either.
			require.Equal(t, []string{"idx_o2"}, issue3335xShowIndexKeys(secondIdx),
				"the second database's control must expose its own index exactly")
			require.NotEqual(t, secondCreate, postCreate,
				"the second database's definition must differ from the captured origin result")
			require.NotEqual(t, issue3335xShowIndexKeys(secondIdx), issue3335xShowIndexKeys(postIdx),
				"the second database's keys must differ from the captured origin keys")
			require.NotEqual(t, secondCols, postCols,
				"the second database's columns must differ from the captured origin columns")

			// The index invalidation is observable: the recreated first table
			// carries PRIMARY and idx_o1b, keys the warm generation never saw
			// (it held only idx_o1) and the second table does not own.
			require.Equal(t, []string{"PRIMARY", "idx_o1b"}, issue3335xShowIndexKeys(postIdx),
				"the original handle must show exactly the recreated first-table keys")
			require.NotEqual(t, issue3335xShowIndexKeys(warmIdx), issue3335xShowIndexKeys(postIdx),
				"a stale cached index generation must no longer match")
			require.Contains(t, issue3335xText(postCreate), "idx_o1b",
				"the recreated index must appear in the captured SHOW CREATE result")
		})

		t.Run("named_snapshot_drop_invalidates_old_handle", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			admin := issue3335xSession(t, ctx, cn, "dump:111", "")
			prep := issue3335xSession(t, ctx, cn, "dump:111", "")
			ddl := issue3335xSession(t, ctx, cn, "dump:111", "")
			defer func() { issue3335xBestEffort(ctx, admin, "drop database if exists e3335x_snap") }()

			issue3335xExec(t, ctx, admin, "create database e3335x_snap")
			issue3335xExec(t, ctx, admin, "create table e3335x_snap.t2(a int, b int)")
			issue3335xExec(t, ctx, ddl, "create snapshot e3335x_sp for database e3335x_snap")
			defer func() { issue3335xBestEffort(ctx, ddl, "drop snapshot if exists e3335x_sp") }()

			issue3335xExec(t, ctx, prep,
				"prepare p_snap from 'SHOW CREATE TABLE e3335x_snap.t2 {snapshot = ''e3335x_sp''}'")
			defer func() { issue3335xBestEffort(ctx, prep, "deallocate prepare p_snap") }()
			issue3335xExec(t, ctx, prep,
				"prepare p_sel from 'SELECT * FROM e3335x_snap.t2 {snapshot = ''e3335x_sp''}'")
			defer func() { issue3335xBestEffort(ctx, prep, "deallocate prepare p_sel") }()

			warmSnap := issue3335xRows(t, ctx, prep, "execute p_snap")
			require.NotContains(t, issue3335xText(warmSnap), "`d` int")
			require.NoError(t, issue3335xQueryErr(ctx, prep, "execute p_sel"))

			issue3335xExec(t, ctx, ddl,
				"alter table e3335x_snap.t2 algorithm=copy, add column d int default 9")
			historyExecuted := issue3335xRows(t, ctx, prep, "execute p_snap")
			historyDirect := issue3335xRows(t, ctx, prep,
				"show create table e3335x_snap.t2 {snapshot = 'e3335x_sp'}")
			liveDirect := issue3335xRows(t, ctx, prep, "show create table e3335x_snap.t2")
			require.Equal(t, historyDirect, historyExecuted,
				"prepared historical SHOW CREATE must equal the direct control while the snapshot exists")
			require.NotContains(t, issue3335xText(historyExecuted), "`d` int",
				"the snapshot definition must exclude later live DDL")
			require.Contains(t, issue3335xText(liveDirect), "`d` int",
				"the live definition must include the live DDL")

			issue3335xExec(t, ctx, ddl, "drop snapshot e3335x_sp")

			execErr := issue3335xQueryErr(ctx, prep, "execute p_snap")
			reuseErr := issue3335xQueryErr(ctx, prep, "execute p_snap")
			directErr := issue3335xQueryErr(ctx, prep,
				"show create table e3335x_snap.t2 {snapshot = 'e3335x_sp'}")
			selectErr := issue3335xQueryErr(ctx, prep, "execute p_sel")
			_, freshPrepareErr := prep.ExecContext(ctx,
				"prepare f_snap from 'SHOW CREATE TABLE e3335x_snap.t2 {snapshot = ''e3335x_sp''}'")
			require.Error(t, execErr, "the old prepared SHOW CREATE must fail after the snapshot is dropped")
			require.Error(t, reuseErr, "post-failure reuse must keep failing identically")
			require.Error(t, directErr, "the direct control must fail after the snapshot is dropped")
			require.Error(t, selectErr, "the old prepared SELECT must fail after the snapshot is dropped")
			execNo := issue3335xMysqlErrNo(t, execErr)
			reuseNo := issue3335xMysqlErrNo(t, reuseErr)
			directNo := issue3335xMysqlErrNo(t, directErr)
			selectNo := issue3335xMysqlErrNo(t, selectErr)
			require.Equal(t, 20101, execNo,
				"the missing-snapshot error class must be exactly 20101 on the old prepared SHOW CREATE handle")
			require.Equal(t, execNo, reuseNo, "handle reuse must keep the same error class")
			require.Equal(t, 20101, directNo,
				"the direct missing-snapshot control must also report exactly 20101")
			require.Equal(t, execNo, selectNo,
				"missing-snapshot error class must agree with the prepared SELECT control")
			t.Logf("missing-snapshot error class=%d fresh-prepare-err=%v", execNo, freshPrepareErr)
		})

		t.Run("show_index_drop_recreate_qualified_target", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			admin := issue3335xSession(t, ctx, cn, "dump:111", "")
			ddl := issue3335xSession(t, ctx, cn, "dump:111", "")
			defer func() {
				issue3335xBestEffort(ctx, admin, "drop database if exists e3335x_home")
				issue3335xBestEffort(ctx, admin, "drop database if exists e3335x_show")
			}()

			issue3335xExec(t, ctx, admin, "create database e3335x_home")
			issue3335xExec(t, ctx, admin, "create database e3335x_show")
			issue3335xExec(t, ctx, admin,
				"create table e3335x_show.t(id int primary key, v int, key idx_v(v))")

			// The prepare session keeps a different default database from the
			// fully qualified target, as in issue #33367.
			prep := issue3335xSession(t, ctx, cn, "dump:111", "")
			issue3335xExec(t, ctx, prep, "use e3335x_home")

			issue3335xExec(t, ctx, prep, "prepare p_idx from 'SHOW INDEX FROM e3335x_show.t'")
			defer func() { issue3335xBestEffort(ctx, prep, "deallocate prepare p_idx") }()
			warm := issue3335xRows(t, ctx, prep, "execute p_idx")
			require.Equal(t, []string{"PRIMARY", "idx_v"}, issue3335xShowIndexKeys(warm),
				"the original handle must warm on PRIMARY and idx_v")

			issue3335xExec(t, ctx, ddl, "drop table e3335x_show.t")

			execErr := issue3335xQueryErr(ctx, prep, "execute p_idx")
			reuseErr := issue3335xQueryErr(ctx, prep, "execute p_idx")
			directErr := issue3335xQueryErr(ctx, prep, "show index from e3335x_show.t")
			_, freshErr := prep.ExecContext(ctx, "prepare p_idx_new from 'SHOW INDEX FROM e3335x_show.t'")
			require.Error(t, execErr, "the old handle must not succeed after a committed cross-session DROP")
			require.Error(t, reuseErr, "post-failure handle reuse must keep failing")
			require.Error(t, directErr, "the direct control must fail after the DROP")
			require.Error(t, freshErr, "the fresh PREPARE control must fail after the DROP")
			require.Equal(t, 1146, issue3335xMysqlErrNo(t, execErr),
				"the old handle must report exactly MySQL 1146")
			require.Equal(t, 1146, issue3335xMysqlErrNo(t, reuseErr),
				"post-failure reuse must keep reporting exactly MySQL 1146")
			require.Equal(t, 1146, issue3335xMysqlErrNo(t, directErr))
			require.Equal(t, 1146, issue3335xMysqlErrNo(t, freshErr))

			issue3335xExec(t, ctx, ddl,
				"create table e3335x_show.t(id int primary key, v int, key idx_new(v))")
			post := issue3335xRows(t, ctx, prep, "execute p_idx")
			direct := issue3335xRows(t, ctx, prep, "show index from e3335x_show.t")
			require.Equal(t, []string{"PRIMARY", "idx_new"}, issue3335xShowIndexKeys(post),
				"the same original handle must show exactly the recreated PRIMARY and idx_new")
			require.Equal(t, []string{"PRIMARY", "idx_new"}, issue3335xShowIndexKeys(direct))
			require.Equal(t, direct, post, "prepared and direct SHOW INDEX must agree after recreate")
		})

		t.Run("restricted_role_matches_direct_at_boundaries", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			admin := issue3335xSession(t, ctx, cn, "dump:111", "")
			defer func() {
				issue3335xBestEffort(ctx, admin, "drop user if exists e3335x_reader")
				issue3335xBestEffort(ctx, admin, "drop role if exists e3335x_r")
				issue3335xBestEffort(ctx, admin, "drop database if exists e3335x_show")
				issue3335xBestEffort(ctx, admin, "drop database if exists e3335x_sec")
				issue3335xBestEffort(ctx, admin, "drop account if exists e3335x_acct")
			}()

			issue3335xExec(t, ctx, admin, "create database e3335x_sec")
			issue3335xExec(t, ctx, admin, "create database e3335x_show")
			issue3335xExec(t, ctx, admin,
				"create table e3335x_show.allowed_t(id int primary key, v int, key idx_v(v))")
			issue3335xExec(t, ctx, admin,
				"create table e3335x_sec.denied_t(a int, b int, key idx_d1(b))")
			issue3335xExec(t, ctx, admin, "create user e3335x_reader identified by 'e3335xpw'")
			issue3335xExec(t, ctx, admin, "create role e3335x_r")
			issue3335xExec(t, ctx, admin, "grant e3335x_r to e3335x_reader")
			issue3335xExec(t, ctx, admin, "grant select on table e3335x_show.allowed_t to e3335x_r")

			// The restricted session connects only after its user, role and
			// grant exist.
			const creds = "sys#e3335x_reader#e3335x_r:e3335xpw"
			reader := issue3335xSession(t, ctx, cn, creds, "")

			// Allowed target: prepared and direct must warm identically.
			issue3335xExec(t, ctx, reader, "prepare p_show from 'SHOW INDEX FROM e3335x_show.allowed_t'")
			defer func() { issue3335xBestEffort(ctx, reader, "deallocate prepare p_show") }()
			warm := issue3335xRows(t, ctx, reader, "execute p_show")
			require.Equal(t, []string{"PRIMARY", "idx_v"}, issue3335xShowIndexKeys(warm))
			require.Equal(t, issue3335xRows(t, ctx, reader, "show index from e3335x_show.allowed_t"), warm,
				"the allowed target must serve identical prepared and direct output")

			// Un-granted target within the same account: current policy serves
			// metadata SHOW statements without table-level SELECT grants. The
			// exact restricted-role outcome to preserve is therefore that
			// prepared and direct agree — the prepared route never exposes more
			// than the direct route, including across an invalidation boundary.
			issue3335xExec(t, ctx, reader, "prepare p_deny from 'SHOW INDEX FROM e3335x_sec.denied_t'")
			defer func() { issue3335xBestEffort(ctx, reader, "deallocate prepare p_deny") }()
			denyWarm := issue3335xRows(t, ctx, reader, "execute p_deny")
			require.Equal(t, []string{"idx_d1"}, issue3335xShowIndexKeys(denyWarm),
				"the un-granted target must warm with its own index rows")
			require.Equal(t, issue3335xRows(t, ctx, reader, "show index from e3335x_sec.denied_t"), denyWarm,
				"un-granted same-account target: prepared and direct outputs must agree")
			issue3335xExec(t, ctx, admin, "drop table e3335x_sec.denied_t")
			issue3335xExec(t, ctx, admin,
				"create table e3335x_sec.denied_t(a int, b int, key idx_d2(b))")
			denyPost := issue3335xRows(t, ctx, reader, "execute p_deny")
			require.Equal(t, []string{"idx_d2"}, issue3335xShowIndexKeys(denyPost),
				"the un-granted target's recreated index rows must replace the stale generation")
			require.Equal(t, issue3335xRows(t, ctx, reader, "show index from e3335x_sec.denied_t"), denyPost,
				"un-granted same-account target: prepared and direct must still agree across the invalidation boundary")
			require.NotEqual(t, issue3335xShowIndexKeys(denyWarm), issue3335xShowIndexKeys(denyPost),
				"the un-granted boundary must change observable index rows")

			// Truly inaccessible control: a table in ANOTHER account is
			// invisible to this account. Prepared and direct must fail with the
			// same error class.
			issue3335xExec(t, ctx, admin,
				"create account e3335x_acct admin_name 'root' identified by 'e3335xpw'")
			account := issue3335xSession(t, ctx, cn, "e3335x_acct#root#accountadmin:e3335xpw", "")
			issue3335xExec(t, ctx, account, "create database e3335x_acctdb")
			issue3335xExec(t, ctx, account,
				"create table e3335x_acctdb.t(id int primary key, k int, key idx_k(k))")
			_, crossPrepareErr := reader.ExecContext(ctx, "prepare p_cross from 'SHOW INDEX FROM e3335x_acctdb.t'")
			require.Error(t, crossPrepareErr, "a cross-account target must be inaccessible at fresh PREPARE")
			crossDirectErr := issue3335xQueryErr(ctx, reader, "show index from e3335x_acctdb.t")
			require.Error(t, crossDirectErr, "a cross-account target must be inaccessible directly")
			require.Equal(t,
				issue3335xMysqlErrNo(t, crossPrepareErr),
				issue3335xMysqlErrNo(t, crossDirectErr),
				"cross-account denial classes must agree between prepared and direct")

			// Invalidation boundary on the allowed target: the table is dropped
			// first, so the restricted handle must revalidate identity and
			// report the direct route's exact missing-table error while the
			// target is gone (a stale cached generation served that window as
			// an empty success — exactly what this boundary discriminates).
			issue3335xExec(t, ctx, admin, "drop table e3335x_show.allowed_t")
			droppedExecErr := issue3335xQueryErr(ctx, reader, "execute p_show")
			droppedDirectErr := issue3335xQueryErr(ctx, reader, "show index from e3335x_show.allowed_t")
			require.Error(t, droppedDirectErr,
				"the direct route must fail while the allowed target is missing")
			require.Error(t, droppedExecErr,
				"the restricted prepared handle must fail while the allowed target is missing")
			require.Equal(t, 1146, issue3335xMysqlErrNo(t, droppedExecErr),
				"the restricted prepared handle must report the direct route's exact missing-table error")
			require.Equal(t, 1146, issue3335xMysqlErrNo(t, droppedDirectErr),
				"the direct control must report exactly 1146")
			issue3335xExec(t, ctx, admin,
				"create table e3335x_show.allowed_t(id int primary key, v int, key idx_v2(v))")
			// Restore the grant precondition deterministically: the recreate
			// replaced the granted object identity.
			issue3335xExec(t, ctx, admin, "grant select on table e3335x_show.allowed_t to e3335x_r")
			post := issue3335xRows(t, ctx, reader, "execute p_show")
			require.Equal(t, []string{"PRIMARY", "idx_v2"}, issue3335xShowIndexKeys(post),
				"the restricted role's refreshed prepared handle shows exactly the recreated allowed-target keys")
			require.Equal(t, issue3335xRows(t, ctx, reader, "show index from e3335x_show.allowed_t"), post,
				"prepared and direct outputs must agree at the invalidation boundary")
			require.NotEqual(t, issue3335xShowIndexKeys(warm), issue3335xShowIndexKeys(post),
				"the allowed-target boundary must change observable index rows")

			// Revoked grant plus another forced replan: whatever the direct
			// route now reports, the prepared handle must report exactly that —
			// the prepared route can never carry a different authorization
			// outcome than the direct route at the same boundary. The recreate
			// makes this boundary's index rows decisive again.
			issue3335xExec(t, ctx, admin, "revoke select on table e3335x_show.allowed_t from e3335x_r")
			issue3335xExec(t, ctx, admin, "drop table e3335x_show.allowed_t")
			issue3335xExec(t, ctx, admin,
				"create table e3335x_show.allowed_t(id int primary key, v int, key idx_v3(v))")
			revokedExecErr := issue3335xQueryErr(ctx, reader, "execute p_show")
			revokedDirectErr := issue3335xQueryErr(ctx, reader, "show index from e3335x_show.allowed_t")
			require.Equal(t, revokedDirectErr == nil, revokedExecErr == nil,
				"after the revoke, prepared and direct must agree on whether the target is reachable")
			if revokedExecErr != nil {
				require.Equal(t,
					issue3335xMysqlErrNo(t, revokedExecErr),
					issue3335xMysqlErrNo(t, revokedDirectErr),
					"the prepared handle must carry exactly the direct route's denial class after the revoke")
				t.Logf("revoked-boundary denial class=%d", issue3335xMysqlErrNo(t, revokedExecErr))
			} else {
				revokedRows := issue3335xRows(t, ctx, reader, "execute p_show")
				require.Equal(t, []string{"PRIMARY", "idx_v3"}, issue3335xShowIndexKeys(revokedRows),
					"current policy keeps metadata SHOW reachable: the rebuilt handle serves exactly the recreated revoked-boundary rows")
				require.Equal(t, issue3335xRows(t, ctx, reader, "show index from e3335x_show.allowed_t"), revokedRows,
					"current policy keeps metadata SHOW reachable: prepared and direct outputs must stay equal after the revoke")
			}
		})

		t.Run("view_delegation_refreshes_like_direct_create_view", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			admin := issue3335xSession(t, ctx, cn, "dump:111", "")
			defer func() { issue3335xBestEffort(ctx, admin, "drop database if exists e3335x_meta") }()

			issue3335xExec(t, ctx, admin, "create database e3335x_meta")
			issue3335xExec(t, ctx, admin, "create table e3335x_meta.base(a int, b int)")
			issue3335xExec(t, ctx, admin,
				"create view e3335x_meta.v as select a from e3335x_meta.base")

			prep := issue3335xSession(t, ctx, cn, "dump:111", "")
			issue3335xExec(t, ctx, prep, "use e3335x_meta")

			// SHOW CREATE TABLE over a view delegates to the view builder; both
			// spellings must render the same stored definition.
			issue3335xExec(t, ctx, prep, "prepare pv from 'SHOW CREATE TABLE e3335x_meta.v'")
			defer func() { issue3335xBestEffort(ctx, prep, "deallocate prepare pv") }()
			warm := issue3335xRows(t, ctx, prep, "execute pv")
			require.Contains(t, issue3335xText(warm), "create view")
			require.Contains(t, issue3335xText(warm), "select a from")
			require.Equal(t, issue3335xRows(t, ctx, prep, "show create table e3335x_meta.v"), warm,
				"the delegation spelling must render what its direct control renders")
			require.Equal(t, issue3335xRows(t, ctx, prep, "show create view e3335x_meta.v"), warm,
				"the direct SHOW CREATE VIEW spelling must render the same definition")

			issue3335xExec(t, ctx, admin,
				"create or replace view e3335x_meta.v as select a, b from e3335x_meta.base")
			post := issue3335xRows(t, ctx, prep, "execute pv")
			direct := issue3335xRows(t, ctx, prep, "show create view e3335x_meta.v")
			require.Equal(t, direct, post,
				"the delegated handle must refresh exactly like the direct SHOW CREATE VIEW spelling")
			require.NotEqual(t, warm, post, "the committed view redefinition must invalidate the delegated handle")
			require.Contains(t, issue3335xText(post), "select a, b from")
		})
	})
}

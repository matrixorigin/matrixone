// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// issue31367PhysicalTempName matches the physical catalog name of a session
// temporary table: __mo_tmp_<32 hex session id>_<db>_<alias>.  Ordinary tables
// whose names merely look internal never match, so the assertions below hide
// only real session-owned temporary rows and never a legitimate table.
var issue31367PhysicalTempName = regexp.MustCompile(`^__mo_tmp_[0-9a-f]{32}_`)

const (
	issue31367DB   = "issue_31367_db"
	issue31367Keep = "__mo_tmp_keep_persist"
)

// issue31367Session pins one dedicated MySQL connection for a logical session.
// database/sql pools may otherwise migrate statements across backend
// connections, which would silently move the temporary table out of (or into)
// the session being tested and break the ownership this test relies on.
func issue31367Session(t *testing.T, ctx context.Context, db *sql.DB) *sql.Conn {
	t.Helper()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// issue31367FirstColumn returns the first column of every row produced by
// query on the pinned connection.
func issue31367FirstColumn(t *testing.T, ctx context.Context, conn *sql.Conn, query string) []string {
	t.Helper()
	rows, err := conn.QueryContext(ctx, query)
	require.NoError(t, err)
	defer rows.Close()
	cols, err := rows.Columns()
	require.NoError(t, err)
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	var out []string
	for rows.Next() {
		require.NoError(t, rows.Scan(ptrs...))
		switch v := vals[0].(type) {
		case nil:
			out = append(out, "")
		case []byte:
			out = append(out, string(v))
		case string:
			out = append(out, v)
		default:
			out = append(out, fmt.Sprintf("%v", v))
		}
	}
	require.NoError(t, rows.Err())
	return out
}

// issue31367RequireNoPhysicalTemp asserts that a listing contains no real
// session physical temp name: no name matches the physical shape and, a
// fortiori, the concrete realPhysical name captured via the owning session's
// direct catalog scan is absent.
func issue31367RequireNoPhysicalTemp(t *testing.T, names []string, what, realPhysical string) {
	t.Helper()
	for _, name := range names {
		require.Falsef(t, issue31367PhysicalTempName.MatchString(name),
			"%s must not list session temporary physical relation %q", what, name)
	}
	require.NotContainsf(t, names, realPhysical,
		"%s must not list the real physical temporary name %q", what, realPhysical)
}

// issue31367RequireVisible asserts the ordinary objects stay listed: the
// persistent control table and the internal-looking ordinary table that the
// temporary predicate's relkind guard must never hide.
func issue31367RequireVisible(t *testing.T, names []string, what string) {
	t.Helper()
	require.Containsf(t, names, "tp1", "%s must keep normal persistent table tp1 visible", what)
	require.Containsf(t, names, issue31367Keep,
		"%s must keep the legitimate internal-looking ordinary table visible", what)
}

// issue31367RequireInvariant checks the parity invariant from both logical
// sessions across the plain listings and the LIKE/WHERE rewrite forms.
func issue31367RequireInvariant(t *testing.T, ctx context.Context, owner *sql.Conn, other *sql.Conn, realPhysical string) {
	t.Helper()
	for _, tc := range []struct {
		name  string
		query string
	}{
		{"show table status", "show table status from " + issue31367DB},
		{"show tables", "show tables from " + issue31367DB},
		{"information_schema.tables", "select table_name from information_schema.tables where table_schema = '" + issue31367DB + "'"},
	} {
		for _, con := range []struct {
			name string
			conn *sql.Conn
		}{
			{"owning session", owner},
			{"other session", other},
		} {
			what := tc.name + " [" + con.name + "]"
			t.Run(tc.name+" hides session physical temp rows from "+con.name, func(t *testing.T) {
				names := issue31367FirstColumn(t, ctx, con.conn, tc.query)
				issue31367RequireNoPhysicalTemp(t, names, what, realPhysical)
			})
			t.Run(tc.name+" keeps ordinary and persistent tables visible from "+con.name, func(t *testing.T) {
				names := issue31367FirstColumn(t, ctx, con.conn, tc.query)
				issue31367RequireVisible(t, names, what)
			})
		}
	}

	// LIKE and WHERE rewrite paths must carry the same predicate as the
	// plain statement.
	t.Run("show table status LIKE tp% hides temp and returns tp1", func(t *testing.T) {
		names := issue31367FirstColumn(t, ctx, owner, "show table status from "+issue31367DB+" like 'tp%'")
		issue31367RequireNoPhysicalTemp(t, names, "show table status LIKE tp%", realPhysical)
		require.Contains(t, names, "tp1")
	})
	t.Run("show table status LIKE __mo_% keeps legit object, hides real temp", func(t *testing.T) {
		names := issue31367FirstColumn(t, ctx, owner, "show table status from "+issue31367DB+" like '\\_\\_mo\\_%'")
		issue31367RequireNoPhysicalTemp(t, names, "show table status LIKE __mo_%", realPhysical)
		require.Contains(t, names, issue31367Keep)
	})
	t.Run("show table status WHERE form hides temp", func(t *testing.T) {
		names := issue31367FirstColumn(t, ctx, owner, "show table status from "+issue31367DB+" where Name='tp1'")
		issue31367RequireNoPhysicalTemp(t, names, "show table status WHERE", realPhysical)
		require.Contains(t, names, "tp1")
	})
}

// TestIssue31367ShowTableStatusHidesSessionTemporaryRows guards the invariant
// that SHOW TABLE STATUS applies the same canonical temporary-object predicate
// as SHOW TABLES and information_schema.tables: a session temporary table's
// physical catalog row (`__mo_tmp_<session>_<db>_<alias>`) is never listed in
// any form, from the owning session or any other, while ordinary tables
// (including ones with internal-looking names) stay visible, and the real
// physical object stays addressable nowhere outside its owning session.
func TestIssue31367ShowTableStatusHidesSessionTemporaryRows(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port)
		ownerPool, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer ownerPool.Close()
		otherPool, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer otherPool.Close()
		owner := issue31367Session(t, ctx, ownerPool)
		other := issue31367Session(t, ctx, otherPool)
		_, err = owner.ExecContext(ctx, "set role moadmin")
		require.NoError(t, err)
		_, err = other.ExecContext(ctx, "set role moadmin")
		require.NoError(t, err)
		_, err = owner.ExecContext(ctx, "drop database if exists `"+issue31367DB+"`")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cleanupCancel()
			_, _ = owner.ExecContext(cleanupCtx, "drop database if exists `"+issue31367DB+"`")
		}()

		exec := func(sqlText string) {
			_, err := owner.ExecContext(ctx, sqlText)
			require.NoError(t, err)
		}
		exec("create database `" + issue31367DB + "`")
		exec("create table `" + issue31367DB + "`.`tp1`(id int primary key)")
		exec("insert into `" + issue31367DB + "`.`tp1` values (1),(2)")
		// Anti-overhide control: an ordinary table carrying an internal-looking
		// name. The fix must hide only real temporary objects, never it.
		exec("create table `" + issue31367DB + "`.`" + issue31367Keep + "`(id int primary key)")
		// The object under test, created and owned only by the pinned owning
		// connection's session.
		exec("create temporary table `" + issue31367DB + "`.`tt1`(id int primary key)")
		exec("insert into `" + issue31367DB + "`.`tt1` values (7)")

		// Real-object witness: the owning session's direct catalog scan sees the
		// real physical temp row. This is the concrete name every hiding
		// assertion is checked against; a fabricated name cannot stand in.
		scanNames := issue31367FirstColumn(t, ctx, owner,
			"select relname from mo_catalog.mo_tables where reldatabase = '"+issue31367DB+"' and relname like '\\_\\_mo\\_%'")
		var physical []string
		for _, n := range scanNames {
			if issue31367PhysicalTempName.MatchString(n) {
				physical = append(physical, n)
			}
		}
		require.Lenf(t, physical, 1,
			"owning session must own exactly one real temporary physical row, got %v (scan %v)", physical, scanNames)
		require.Contains(t, scanNames, issue31367Keep)
		realPhysical := physical[0]
		// Vacuity: the real temporary object is alive and readable through its
		// alias in the owning session.
		require.Equal(t, []string{"1"},
			issue31367FirstColumn(t, ctx, owner, "select count(*) from `"+issue31367DB+"`.`tt1`"))

		issue31367RequireInvariant(t, ctx, owner, other, realPhysical)

		t.Run("real physical temp name is not addressable in another session", func(t *testing.T) {
			// The real, correctly quoted object: outside the owning session it
			// must not resolve at all - the behavior behind the issue's ghost row.
			_, err := other.ExecContext(ctx, "show create table `"+issue31367DB+"`.`"+realPhysical+"`")
			require.Errorf(t, err, "the real physical temporary name `%s` must not resolve in another session", realPhysical)
		})
		t.Run("fabricated ghost physical name is rejected as an address", func(t *testing.T) {
			ghost := "__mo_tmp_0123456789abcdef0123456789abcdef_" + issue31367DB + "_tt1"
			_, err := owner.ExecContext(ctx, "show create table `"+issue31367DB+"`.`"+ghost+"`")
			require.Error(t, err, "a fabricated physical name must not resolve")
		})
		t.Run("nonexistent database keeps its error path", func(t *testing.T) {
			_, err := owner.ExecContext(ctx, "show table status from issue_31367_no_such_database")
			require.Error(t, err, "SHOW TABLE STATUS on a missing database must error")
		})
		t.Run("session temporary table stays alive in its owning session", func(t *testing.T) {
			require.Equal(t, []string{"1"},
				issue31367FirstColumn(t, ctx, owner, "select count(*) from `"+issue31367DB+"`.`tt1`"))
		})
	})
}

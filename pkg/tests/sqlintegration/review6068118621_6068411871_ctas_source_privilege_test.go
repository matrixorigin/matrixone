// Copyright 2026 MatrixOrigin
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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Reviews 6068118621/6068411871, gate 2: the merged child-only CTAS
// normalization must not broaden the source-privilege contract. A role that
// may create tables in the destination but may not read the source is
// rejected by the real CTAS authorization on both the ordinary and the public
// prepared route; only granting the source read restores those exact
// statements. Saved-result and lock checks keep their fail-closed order.
func TestReview6068CTASSourcePrivilegeDiscrimination(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		adminDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer adminDB.Close()

		const (
			sourceDB = "review6068_priv_source"
			destDB   = "review6068_priv_dest"
			role     = "review6068_priv_role"
			user     = "review6068_priv_user"
			password = "111"
		)
		defer cleanupSQLIntegration(t, cn,
			"drop user if exists "+user,
			"drop role if exists "+role,
			"drop database if exists "+sourceDB,
			"drop database if exists "+destDB,
		)

		adminConn, err := adminDB.Conn(ctx)
		require.NoError(t, err)
		defer adminConn.Close()
		execAdmin := func(statement string) {
			t.Helper()
			_, err := adminConn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		for _, statement := range []string{
			"set role moadmin",
			"drop user if exists " + user,
			"drop role if exists " + role,
			"drop database if exists " + sourceDB,
			"drop database if exists " + destDB,
			"create database `" + sourceDB + "`",
			"create database `" + destDB + "`",
			"create table `" + sourceDB + "`.src (id int primary key, note varchar(16))",
			"insert into `" + sourceDB + "`.src values (42, 'answer')",
			"create role " + role,
			"create user " + user + " identified by '" + password + "' default role " + role,
			"grant connect on account * to " + role,
			"grant create table,drop table on database `" + destDB + "` to " + role,
		} {
			execAdmin(statement)
		}

		userDB, err := sql.Open("mysql", fmt.Sprintf(
			"sys#%s#%s:%s@tcp(127.0.0.1:%d)/",
			user, role, password, port))
		require.NoError(t, err)
		defer userDB.Close()
		userConn, err := userDB.Conn(ctx)
		require.NoError(t, err)
		defer userConn.Close()

		// Denied source privilege: the role may create in the destination but
		// cannot read the source, so the real authorization must reject the
		// copy attempt on both public routes.
		_, err = userConn.ExecContext(ctx,
			"CREATE TABLE `"+destDB+"`.ctas_denied AS (SELECT id FROM `"+sourceDB+"`.src) LIMIT 1")
		require.Error(t, err, "a create-only role cannot copy an unread source through CTAS")
		require.Contains(t, err.Error(), "privilege", "the ordinary-route rejection names the missing source read privilege")

		_, err = userConn.ExecContext(ctx,
			"PREPARE denied_p FROM 'CREATE TABLE `"+destDB+"`.ctas_pdenied AS (SELECT id FROM `"+sourceDB+"`.src) LIMIT 1'")
		require.Error(t, err, "the prepared route performs real planning and denies the unread source at PREPARE")
		require.Contains(t, err.Error(), "privilege", "the prepared-route rejection names the missing source read privilege")

		// The same decision for the redundant-outer wrapper of review
		// 6090897398's gap: the privilege re-plan shares the same
		// effective source, so the unread source is denied before the
		// grant and the granted source prepares the exact redundant-count
		// statement.
		_, err = userConn.ExecContext(ctx,
			"PREPARE redundant_denied_p FROM 'CREATE TABLE `"+destDB+"`.ctas_pdenied_r2 AS (SELECT id FROM `"+sourceDB+"`.src LIMIT 1) LIMIT 2'")
		require.Error(t, err, "the prepared redundant-outer route performs real planning and denies the unread source at PREPARE")
		require.Contains(t, err.Error(), "privilege", "the redundant-outer rejection names the missing source read privilege")

		// Authorized recovery through the exact same statements: granting the
		// source read restores the ordinary route exactly.
		execAdmin("grant select on table `" + sourceDB + "`.src to " + role)
		_, err = userConn.ExecContext(ctx,
			"CREATE TABLE `"+destDB+"`.ctas_allowed AS (SELECT id FROM `"+sourceDB+"`.src) LIMIT 1")
		require.NoError(t, err, "the granted role can create the CTAS copy through the ordinary route")
		rows, err := userConn.QueryContext(ctx, "SELECT id FROM `"+destDB+"`.ctas_allowed")
		require.NoError(t, err)
		var copied int
		func() {
			defer rows.Close()
			require.True(t, rows.Next(), "the authorized CTAS copied exactly one row")
			require.NoError(t, rows.Scan(&copied))
			require.Equal(t, 42, copied, "the authorized CTAS reads the real source value 42")
			require.False(t, rows.Next(), "the authorized CTAS copied exactly one row")
			require.NoError(t, rows.Err(), "the row iteration completed without an iteration error")
		}()

		// The prepared route agrees once the source read privilege exists.
		_, err = userConn.ExecContext(ctx,
			"PREPARE ok_p FROM 'CREATE TABLE `"+destDB+"`.ctas_prep_ok AS (SELECT id FROM `"+sourceDB+"`.src) LIMIT 1'")
		require.NoError(t, err, "the granted role can prepare the same CTAS through the public prepared route")
		_, err = userConn.ExecContext(ctx, "EXECUTE ok_p")
		require.NoError(t, err, "the prepared CTAS executes for the granted role")
		_, err = userConn.ExecContext(ctx, "DEALLOCATE PREPARE ok_p")
		require.NoError(t, err)

		// The granted source agrees for the redundant-outer statement: the
		// granted privilege re-plan composes the exact prepared statement.
		_, err = userConn.ExecContext(ctx,
			"PREPARE redundant_ok_p FROM 'CREATE TABLE `"+destDB+"`.ctas_prep_ok_r2 AS (SELECT id FROM `"+sourceDB+"`.src LIMIT 1) LIMIT 2'")
		require.NoError(t, err, "the granted role prepares the redundant-outer CTAS through the public prepared route")
		_, err = userConn.ExecContext(ctx, "EXECUTE redundant_ok_p")
		require.NoError(t, err, "the redundant-outer prepared CTAS executes for the granted role")
		_, err = userConn.ExecContext(ctx, "DEALLOCATE PREPARE redundant_ok_p")
		require.NoError(t, err)
		r2Rows, err := userConn.QueryContext(ctx, "SELECT count(*) FROM `"+destDB+"`.ctas_prep_ok_r2")
		require.NoError(t, err)
		var r2Count int
		func() {
			defer r2Rows.Close()
			require.True(t, r2Rows.Next(), "the redundant-outer count readback returns its row")
			require.NoError(t, r2Rows.Scan(&r2Count))
			require.Equalf(t, 1, r2Count, "the child's own LIMIT caps the copied destination at its row")
			require.NoError(t, r2Rows.Err())
		}()
		prepRows, err := userConn.QueryContext(ctx, "SELECT count(*) FROM `"+destDB+"`.ctas_prep_ok")
		require.NoError(t, err)
		var prepared int
		func() {
			defer prepRows.Close()
			require.True(t, prepRows.Next(), "the prepared CTAS wrote its rows")
			require.NoError(t, prepRows.Scan(&prepared))
			require.Equal(t, 1, prepared, "the prepared CTAS copied exactly one row")
			require.False(t, prepRows.Next(), "the prepared CTAS count returned a single row")
			require.NoError(t, prepRows.Err(), "the prepared-count iteration completed cleanly")
		}()
	})
}

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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/stretchr/testify/require"
)

// Golden files check error text, not the error number and SQLSTATE on the wire.
func TestPreparedSyntaxErrorOverMySQLProtocol(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/mo_catalog", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		checkError := func(t *testing.T, err error, diagnostic string) {
			t.Helper()
			var serverErr *mysql.MySQLError
			require.ErrorAs(t, err, &serverErr)
			require.Equal(t, uint16(1064), serverErr.Number)
			require.Equal(t, "42000", string(serverErr.SQLState[:]))
			require.Contains(t, serverErr.Message, "SQL parser error: ")
			require.Contains(t, serverErr.Message, diagnostic)
		}

		for _, tc := range []struct {
			name, statement, diagnostic string
		}{
			{"syntax", "select from", "syntax error"},
			{"type parameter overflow", "create table rejected (a varchar(4294967297))", "value is out of range int32"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				// Direct execution establishes the same public error contract.
				_, err := conn.ExecContext(ctx, tc.statement)
				checkError(t, err, tc.diagnostic)
				for _, source := range []string{sqlquote.String(tc.statement), "@bad_prepare_sql"} {
					_, err = conn.ExecContext(ctx, "set @bad_prepare_sql = "+sqlquote.String(tc.statement))
					require.NoError(t, err)
					_, err = conn.ExecContext(ctx, "prepare syntax_stmt from "+source)
					checkError(t, err, tc.diagnostic)
					// The failed PREPARE must leave the same connection usable.
					_, err = conn.ExecContext(ctx, "prepare syntax_stmt from 'select 77'")
					require.NoError(t, err)
					var value int
					require.NoError(t, conn.QueryRowContext(ctx, "execute syntax_stmt").Scan(&value))
					require.Equal(t, 77, value)
					_, err = conn.ExecContext(ctx, "deallocate prepare syntax_stmt")
					require.NoError(t, err)
				}
				// PrepareContext sends COM_STMT_PREPARE, not SQL PREPARE text.
				stmt, err := conn.PrepareContext(ctx, tc.statement)
				if stmt != nil {
					defer stmt.Close()
				}
				checkError(t, err, tc.diagnostic)
				var value int
				require.NoError(t, conn.QueryRowContext(ctx, "select 77").Scan(&value))
				require.Equal(t, 77, value)
			})
		}
		t.Run("active transaction admission", func(t *testing.T) {
			for _, source := range []string{"'select from'", "@bad_prepare_sql"} {
				t.Run(source, func(t *testing.T) {
					_, err := conn.ExecContext(ctx, "set @bad_prepare_sql = 'select from'")
					require.NoError(t, err)
					_, err = conn.ExecContext(ctx, "prepare syntax_stmt from 'select 77'")
					require.NoError(t, err)
					_, err = conn.ExecContext(ctx, "begin")
					require.NoError(t, err)
					defer func() {
						_, rollbackErr := conn.ExecContext(ctx, "rollback")
						require.NoError(t, rollbackErr)
					}()
					_, err = conn.ExecContext(ctx, "prepare syntax_stmt from "+source)
					checkError(t, err, "syntax error")
					_, err = conn.ExecContext(ctx, "execute syntax_stmt")
					require.ErrorContains(t, err, "prepared statement 'syntax_stmt' does not exist")
				})
			}
		})
	})
}

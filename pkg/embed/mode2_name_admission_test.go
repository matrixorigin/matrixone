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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// Use the existing embedded fixture and public MySQL endpoint. The two
// creators start together, and catalog/results/cleanup provide the oracle.
func TestMode2NameAdmissionPublicSQL(t *testing.T) {
	defer func() { require.NoError(t, CloseSingleCNBaseClusterTests()) }()
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port)
		control, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer control.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		var previous int
		require.NoError(t, control.QueryRowContext(ctx, "select @@global.lower_case_table_names").Scan(&previous))
		_, err = control.ExecContext(ctx, "set global lower_case_table_names=2")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cleanupCancel()
			_, err := control.ExecContext(cleanupCtx, fmt.Sprintf("set global lower_case_table_names=%d", previous))
			require.NoError(t, err)
		}()
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(3)
		const database = "MoMode2Admission"
		_, err = db.ExecContext(ctx, "create database "+database)
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cleanupCancel()
			_, err := db.ExecContext(cleanupCtx, "drop database if exists "+database)
			require.NoError(t, err)
			var residue int
			require.NoError(t, db.QueryRowContext(cleanupCtx, "select count(*) from mo_catalog.mo_database where datname='"+database+"'").Scan(&residue))
			require.Zero(t, residue)
		}()
		exec := func(statement string) { _, err := db.ExecContext(ctx, statement); require.NoError(t, err, statement) }
		exec("create table momode2admission.MixTable(id int)")
		exec("insert into MOMODE2ADMISSION.mixtable values (7)")
		var value int
		require.NoError(t, db.QueryRowContext(ctx, "select id from momode2admission.MIXTABLE").Scan(&value))
		require.Equal(t, 7, value)
		exec("create table momode2admission.LikeCopy like MOMODE2ADMISSION.mixtable")
		exec("insert into momode2admission.likecopy values (9)")
		require.NoError(t, db.QueryRowContext(ctx, "select id from MOMODE2ADMISSION.LIKECOPY").Scan(&value))
		require.Equal(t, 9, value)

		connections := make([]*sql.Conn, 2)
		for i := range connections {
			connections[i], err = db.Conn(ctx)
			require.NoError(t, err)
			defer connections[i].Close()
		}
		start := make(chan struct{})
		results := make(chan error, 2)
		for i, name := range []string{"RaceName", "racename"} {
			go func(conn *sql.Conn, name string) {
				<-start
				_, err := conn.ExecContext(ctx, "create table momode2admission."+name+"(id int)")
				results <- err
			}(connections[i], name)
		}
		close(start)
		winners := 0
		for range connections {
			select {
			case err := <-results:
				if err == nil {
					winners++
				} else {
					var duplicate *mysql.MySQLError
					require.ErrorAs(t, err, &duplicate)
					require.EqualValues(t, 1050, duplicate.Number)
				}
			case <-ctx.Done():
				t.Fatal("competing creators did not terminate", ctx.Err())
			}
		}
		require.Equal(t, 1, winners)
		var count int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='"+database+"' and lower(relname)='racename'").Scan(&count))
		require.Equal(t, 1, count)
		exec("truncate table momode2admission.MIXTABLE")
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from momode2admission.mixtable").Scan(&count))
		require.Zero(t, count)
	})
}

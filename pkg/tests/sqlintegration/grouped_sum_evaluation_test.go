// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestGroupedSumPreservesSkippedFactPredicate(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists grouped_sum_evaluation")
		var originalHints string
		require.NoError(t, conn.QueryRowContext(ctx, "select @@optimizer_hints").Scan(&originalHints))
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			_, err := conn.ExecContext(cleanup, fmt.Sprintf("set optimizer_hints=%q", originalHints))
			require.NoError(t, err)
		}()
		exec := func(statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		exec("create database grouped_sum_evaluation")
		exec("use grouped_sum_evaluation")
		exec("create table f(k int, v decimal(7,2), s varchar(20))")
		exec("create table d(id int primary key, label varchar(64), flag int)")
		// The cardinalities exercise the costed hash-build transition; the
		// data-error domain itself has only two fact keys and one bad value.
		exec("insert into f select 1+mod(result,2),1.00,'broken' from generate_series(1,50000) g")
		exec("insert into d select result,concat('label-',result),mod(result,2) from generate_series(1,5000) g")
		for _, table := range []string{"f", "d"} {
			exec("select mo_ctl('dn','flush','grouped_sum_evaluation." + table + "')")
		}
		exec("analyze table f(k,v,s) fullscan")
		exec("analyze table d(id,label,flag) fullscan")
		exec("update d set flag=0") // Keep ordinary stale pre-update estimates.
		const query = `select d.label, sum(f.v) from f join d on f.k=d.id
where d.flag=1 and cast(f.s as bigint)>0 group by d.label`
		for _, hints := range []string{"aggPushDown=1", ""} {
			exec("set optimizer_hints='" + hints + "'")
			func() {
				rows, err := conn.QueryContext(ctx, query)
				require.NoError(t, err, hints)
				defer rows.Close()
				require.False(t, rows.Next(), "empty dimension must retain the empty result")
				require.NoError(t, rows.Err(), "the skipped CAST must not become observable")
				t.Logf("empty result preserved with optimizer_hints=%q", hints)
			}()
		}
		exec("update d set flag=1 where id=1")
		for _, hints := range []string{"aggPushDown=1", ""} {
			exec("set optimizer_hints='" + hints + "'")
			func() {
				rows, err := conn.QueryContext(ctx, query)
				if rows != nil {
					defer rows.Close()
					for rows.Next() {
					}
					if err == nil {
						err = rows.Err()
					}
				}
				var castError *mysql.MySQLError
				require.ErrorAs(t, err, &castError, "a demanded CAST must still fail")
				require.Equal(t, moerr.ErrInvalidArg, castError.Number)
			}()
		}
	})
}

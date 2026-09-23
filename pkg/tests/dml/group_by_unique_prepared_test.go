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

package dml

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestPreparedGroupByUniqueConstraintInvalidation(t *testing.T) {
	// Reuse the package's cluster; this specifically exercises COM_STMT_EXECUTE
	// revalidation after DDL, beyond SQL PREPARE and typed planner checks.
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		db := openRetestSQLDB(t, c)
		defer db.Close()
		name := strings.ToLower(testutils.GetDatabaseName(t))
		defer cleanupTestDatabases(t, db, name)
		execSQLDB(t, ctx, db, "create database "+name)
		execSQLDB(t, ctx, db, "use "+name)
		execSQLDB(t, ctx, db, "set sql_mode='ONLY_FULL_GROUP_BY'")
		execSQLDB(t, ctx, db, "create table t(k int not null, payload varchar(20), amount int, unique key uk(k))")
		execSQLDB(t, ctx, db, "insert into t values(1,'alpha',10),(2,'beta',20)")
		stmt, err := db.PrepareContext(ctx, "select payload,sum(amount) from t where k=? group by k")
		require.NoError(t, err)
		defer stmt.Close()
		for _, tc := range []struct {
			key   int
			value string
			total int
		}{{1, "alpha", 10}, {2, "beta", 20}} {
			var value string
			var total int
			require.NoError(t, stmt.QueryRowContext(ctx, tc.key).Scan(&value, &total))
			require.Equal(t, tc.value, value)
			require.Equal(t, tc.total, total)
		}
		execSQLDB(t, ctx, db, "alter table t drop index uk")
		execSQLDB(t, ctx, db, "insert into t values(1,'other',30)")
		var value string
		var total int
		err = stmt.QueryRowContext(ctx, 1).Scan(&value, &total)
		require.ErrorContains(t, err, "must appear in the GROUP BY")
		execSQLDB(t, ctx, db, "delete from t where payload='other'")
		execSQLDB(t, ctx, db, "alter table t add unique key uk(k)")
		require.NoError(t, stmt.QueryRowContext(ctx, 1).Scan(&value, &total))
		require.Equal(t, "alpha", value)
		require.Equal(t, 10, total)
	})
}

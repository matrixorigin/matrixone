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

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Run the actual protocol, frontend generation/transaction boundaries, nested
// SET execution and prepared EXECUTE against the production admission owner.
// Synthetic pressure/queue ordering is covered by the owning-package tests.
func TestStatementMemoryAdmissionSQLLifecycle(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1))
	if cluster != nil {
		t.Cleanup(func() {
			require.NoError(t, cluster.Close())
			require.Eventually(t, func() bool {
				s := system.DefaultMemoryAdmissionBudget().Snapshot()
				return s.StatementActive == 0 && s.StatementQueued == 0
			}, time.Second, 10*time.Millisecond, "request/cluster teardown must not leak admissions")
		})
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	before := system.DefaultMemoryAdmissionBudget().Snapshot().StatementAdmitted
	_, err = conn.ExecContext(ctx, "create database statement_memory_lifecycle; use statement_memory_lifecycle; create table t(id int); insert into t values (1)")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "begin; insert into t values (2); rollback; set @n=(select count(*) from t)")
	require.NoError(t, err)
	var count int
	require.NoError(t, conn.QueryRowContext(ctx, "select @n").Scan(&count))
	require.Equal(t, 1, count)
	stmt, err := conn.PrepareContext(ctx, "select count(*) from t where id > ?")
	require.NoError(t, err)
	defer stmt.Close()
	require.NoError(t, stmt.QueryRowContext(ctx, 0).Scan(&count))
	require.Equal(t, 1, count)
	_, err = conn.ExecContext(ctx, "select missing_column from t")
	require.Error(t, err)
	_, err = conn.ExecContext(ctx, "begin; insert into t values (3); commit")
	require.NoError(t, err)
	require.NoError(t, stmt.QueryRowContext(ctx, 0).Scan(&count))
	require.Equal(t, 2, count)
	_, err = conn.ExecContext(ctx, "drop database statement_memory_lifecycle")
	require.NoError(t, err)
	require.Greater(t, system.DefaultMemoryAdmissionBudget().Snapshot().StatementAdmitted, before)
}

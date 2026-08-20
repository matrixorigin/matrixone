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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue27091ConcurrentFunctionCreationUnderOptimisticSI(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		const creators = 8
		db.SetMaxOpenConns(creators + 1)

		const (
			database = "issue_27091_udf_lock"
			function = "f_exact_signature"
		)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+database)
		}()
		execSQLRequire(t, ctx, db, "drop database if exists "+database)
		execSQLRequire(t, ctx, db, "create database "+database)
		testutils.WaitDatabaseCreated(t, database, cn)
		// Exercise the supported optimistic/SI service configuration. The exact
		// signature is enforced by the catalog unique key, not by a mode-dependent
		// SELECT ... FOR UPDATE lock.
		rt := moruntime.ServiceRuntime(cn.ServiceID())
		oldMode, hadMode := rt.GetGlobalVariables(moruntime.TxnMode)
		oldIsolation, hadIsolation := rt.GetGlobalVariables(moruntime.TxnIsolation)
		rt.SetGlobalVariables(moruntime.TxnMode, pbtxn.TxnMode_Optimistic)
		rt.SetGlobalVariables(moruntime.TxnIsolation, pbtxn.TxnIsolation_SI)
		defer func() {
			if hadMode {
				rt.SetGlobalVariables(moruntime.TxnMode, oldMode)
			} else {
				rt.SetGlobalVariables(moruntime.TxnMode, pbtxn.TxnMode_Pessimistic)
			}
			if hadIsolation {
				rt.SetGlobalVariables(moruntime.TxnIsolation, oldIsolation)
			} else {
				rt.SetGlobalVariables(moruntime.TxnIsolation, pbtxn.TxnIsolation_RC)
			}
		}()

		createSQL := fmt.Sprintf(
			"create function %s.%s(a int) returns int language sql as '$1'", database, function,
		)
		start := make(chan struct{})
		ready := make(chan struct{}, creators)
		created := make(chan error, creators)
		for range creators {
			go func() {
				ready <- struct{}{}
				creator, createErr := db.Conn(ctx)
				if createErr != nil {
					created <- createErr
					return
				}
				defer creator.Close()
				<-start
				created <- execOnConn(ctx, creator, createSQL)
			}()
		}
		for range creators {
			<-ready
		}
		close(start)
		successes := 0
		failures := 0
		for range creators {
			createErr := <-created
			if createErr == nil {
				successes++
			} else {
				failures++
			}
		}
		require.Equal(t, 1, successes)
		require.Equal(t, creators-1, failures)

		var count int
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_user_defined_function where db = ? and name = ?", database, function,
		).Scan(&count))
		require.Equal(t, 1, count)
		var argTypes string
		require.NoError(t, db.QueryRowContext(ctx,
			"select arg_types from mo_catalog.mo_user_defined_function where db = ? and name = ?", database, function,
		).Scan(&argTypes))
		require.Equal(t, `["int"]`, argTypes)
	})
}

func TestIssue27091KeepsWideOverloadIdentities(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()

		const (
			database = "issue_27091_wide_signature"
			function = "f_wide"
		)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+database)
		}()
		execSQLRequire(t, ctx, db, "drop database if exists "+database)
		execSQLRequire(t, ctx, db, "create database "+database)
		testutils.WaitDatabaseCreated(t, database, cn)

		firstArgs := issue27091WideFunctionArguments("decimal")
		secondArgs := issue27091WideFunctionArguments("bigint")
		execSQLRequire(t, ctx, db, fmt.Sprintf(
			"create function %s.%s(%s) returns int language sql as '1'", database, function, firstArgs,
		))
		execSQLRequire(t, ctx, db, fmt.Sprintf(
			"create function %s.%s(%s) returns int language sql as '1'", database, function, secondArgs,
		))

		rows, err := db.QueryContext(ctx,
			"select arg_types from mo_catalog.mo_user_defined_function where db = ? and name = ? order by function_id",
			database, function,
		)
		require.NoError(t, err)
		defer rows.Close()
		var signatures []string
		for rows.Next() {
			var signature string
			require.NoError(t, rows.Scan(&signature))
			signatures = append(signatures, signature)
		}
		require.NoError(t, rows.Err())
		require.Len(t, signatures, 2)
		require.Greater(t, len(signatures[0]), 1024)
		require.Greater(t, len(signatures[1]), 1024)
		require.NotEqual(t, signatures[0], signatures[1])
		require.Equal(t, signatures[0][:1024], signatures[1][:1024])
	})
}

func issue27091WideFunctionArguments(lastType string) string {
	const argumentCount = 105
	arguments := make([]string, argumentCount)
	for i := range arguments {
		argumentType := "decimal"
		if i == argumentCount-1 {
			argumentType = lastType
		}
		arguments[i] = fmt.Sprintf("arg%d %s", i, argumentType)
	}
	return strings.Join(arguments, ", ")
}

func execOnConn(ctx context.Context, conn *sql.Conn, statement string) error {
	_, err := conn.ExecContext(ctx, statement)
	return err
}

// Copyright 2024 Matrix Origin
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

package txnexecutor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func Test_TxnExecutorExec(t *testing.T) {
	c, err := embed.NewCluster(embed.WithCNCount(1))
	require.NoError(t, err)
	require.NoError(t, c.Start())

	svc, err := c.GetCNService(0)
	require.NoError(t, err)

	exec := testutils.GetSQLExecutor(svc)
	require.NotNil(t, exec)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	err = exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		_, err = txn.Exec("select count(*) from mo_catalog.mo_tables", executor.StatementOption{}.WithAccountID(1).WithUserID(2).WithRoleID(2))
		require.NoError(t, err)
		return nil
	}, executor.Options{}.WithWaitCommittedLogApplied())
	require.NoError(t, err)
}

func TestPreparedParams(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			exec := testutils.GetSQLExecutor(cn)
			require.NotNil(t, exec)

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			exec.ExecTxn(
				ctx,
				func(txn executor.TxnExecutor) error {
					txn.Use("mo_catalog")

					res, err := txn.Exec(
						"select count(*) from mo_catalog.mo_tables where relname = ?",
						executor.StatementOption{}.WithParams("mo_version"),
					)
					require.NoError(t, err)
					res.Close()
					return nil
				},
				executor.Options{},
			)
		},
	)
}

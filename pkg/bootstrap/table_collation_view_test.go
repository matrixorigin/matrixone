// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bootstrap

import (
	"context"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	mock_executor "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/stretchr/testify/require"
)

func TestTableCollationDerivedViewPublication(t *testing.T) {
	for _, failAt := range []int{0, 1, 3} {
		t.Run(map[int]string{0: "success", 1: "lookup_error", 3: "replace_error"}[failAt], func(t *testing.T) {
			var statements []string
			rejected := moerr.NewNotSupportedNoCtx("protocol version 118")
			op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			op.EXPECT().TxnOptions().Return(txnpb.TxnOptions{}).AnyTimes()
			exec := mock_executor.NewMockSQLExecutor(gomock.NewController(t))
			exec.EXPECT().ExecTxn(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, body func(executor.TxnExecutor) error, opts executor.Options) error {
				require.True(t, opts.HasTxnMode())
				require.Equal(t, txnpb.TxnMode_Pessimistic, opts.TxnMode())
				require.True(t, opts.HasTxnIsolation())
				require.Equal(t, txnpb.TxnIsolation_RC, opts.TxnIsolation())
				require.False(t, opts.HasExistsTxn(), "bootstrap must not borrow a user operator")
				require.Nil(t, opts.Txn(), "the executor will create and own the transaction")
				return body(executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
					statements = append(statements, sql)
					if len(statements) == failAt {
						return executor.Result{}, rejected
					}
					if strings.HasPrefix(sql, "SELECT tbl.rel_createsql") || sql == versions.InformationSchemaTablesUpgradeEntry().UpgSql {
						return executor.Result{}, nil
					}
					return newBootstrapStringResult(catalog.SystemViewRel), nil
				}, op))
			}).Times(1)
			err := InitSystemViews(t.Context(), exec)
			require.NotEmpty(t, statements)
			require.Contains(t, statements[0], "SELECT tbl.rel_createsql")
			if failAt > 0 {
				require.ErrorIs(t, err, rejected)
				require.Len(t, statements, failAt, "stop the transaction body before later derived views")
			} else {
				require.NoError(t, err)
				require.Greater(t, len(statements), 2)
			}
			if len(statements) > 2 {
				require.Equal(t, versions.InformationSchemaTablesUpgradeEntry().UpgSql, statements[2])
			}
		})
	}
}

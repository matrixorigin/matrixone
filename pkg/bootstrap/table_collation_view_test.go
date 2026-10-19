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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	mock_executor "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestTableCollationDerivedViewPublication(t *testing.T) {
	for _, failAt := range []int{0, 1, 2} {
		t.Run(map[int]string{0: "success", 1: "drop_error", 2: "create_error"}[failAt], func(t *testing.T) {
			var statements []string
			rejected := moerr.NewNotSupportedNoCtx("protocol version 118")
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
					if len(statements) <= 2 {
						return executor.Result{}, nil
					}
					return newBootstrapStringResult(catalog.SystemViewRel), nil
				}, nil))
			}).Times(1)
			err := InitSystemViews(t.Context(), exec)
			require.NotEmpty(t, statements)
			require.Equal(t, "DROP VIEW IF EXISTS information_schema.TABLES", statements[0])
			if failAt > 0 {
				require.ErrorIs(t, err, rejected)
				require.Len(t, statements, failAt, "stop the transaction body before later derived views")
			} else {
				require.NoError(t, err)
				require.Greater(t, len(statements), 2)
			}
			if len(statements) > 1 {
				require.Equal(t, sysview.InformationSchemaTablesV118DDL, statements[1])
			}
		})
	}
}

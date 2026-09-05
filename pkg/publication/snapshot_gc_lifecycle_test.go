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

package publication

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func TestDeleteSnapshotWithLifecycleGateIsAtomicAndOrdered(t *testing.T) {
	for _, tc := range []struct {
		name         string
		gateErr      error
		deleteErr    error
		commitErr    error
		wantSQLs     int
		wantRollback bool
		txnMode      pbtxn.TxnMode
	}{
		{name: "success", wantSQLs: 2},
		{name: "pessimistic success", wantSQLs: 2, txnMode: pbtxn.TxnMode_Pessimistic},
		{name: "gate failure", gateErr: errors.New("gate failed"), wantSQLs: 1, wantRollback: true},
		{name: "delete failure", deleteErr: errors.New("delete failed"), wantSQLs: 2, wantRollback: true},
		{name: "commit failure", commitErr: errors.New("commit failed"), wantSQLs: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			txn := mock_frontend.NewMockTxnOperator(ctrl)
			txn.EXPECT().Txn().Return(pbtxn.TxnMeta{Mode: tc.txnMode}).Times(1)
			var events []string
			if tc.wantRollback {
				txn.EXPECT().Rollback(gomock.Any()).DoAndReturn(func(context.Context) error {
					events = append(events, "rollback")
					return nil
				})
			} else {
				txn.EXPECT().Commit(gomock.Any()).DoAndReturn(func(context.Context) error {
					events = append(events, "commit")
					return tc.commitErr
				})
			}

			var sqls []string
			exec := func(
				_ context.Context, sql, _ string, _ client.TxnOperator,
			) (executor.Result, error) {
				sqls = append(sqls, sql)
				events = append(events, sql)
				if len(sqls) == 1 {
					return executor.Result{}, tc.gateErr
				}
				return executor.Result{}, tc.deleteErr
			}

			err := deleteSnapshotWithLifecycleGate(
				context.Background(), txn, "cn", "ccpr_'quoted", exec,
			)
			wantErr := errors.Join(tc.gateErr, tc.deleteErr, tc.commitErr)
			if wantErr == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Len(t, sqls, tc.wantSQLs)
			wantGateSQL := databranchutils.LineageOwnerLifecycleLockSQL()
			if tc.txnMode == pbtxn.TxnMode_Pessimistic {
				wantGateSQL = databranchutils.LineageOwnerLifecyclePessimisticLockSQL()
			}
			require.Equal(t, wantGateSQL, sqls[0])
			if tc.wantSQLs == 2 {
				require.Equal(t,
					"delete from mo_catalog.mo_snapshots where sname = 'ccpr_''quoted'",
					sqls[1],
				)
			}
			if tc.wantRollback {
				require.Equal(t, "rollback", events[len(events)-1])
			} else {
				require.Equal(t, "commit", events[len(events)-1])
			}
		})
	}
}

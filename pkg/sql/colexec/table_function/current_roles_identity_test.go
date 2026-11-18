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

package table_function

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mockfrontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	executortest "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type roleQuery struct {
	account  uint32
	contains string
	rows     []int64
	err      error
}

// A plan made while version 12 is enabled must still fail closed if the
// execution service has since been downgraded to version 11.
func TestCurrentRolesExecutionProtocolChanges(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, existed := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if existed {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion12)
		}
	})
	tf := &TableFunction{}
	s := &currentRolesState{}
	defer s.free(tf, proc, false, nil)
	for _, version := range []int64{defines.MORPCVersion12, defines.MORPCVersion11, defines.MORPCVersion12} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		err := s.start(tf, proc, 0, nil)
		if version == defines.MORPCVersion11 {
			require.ErrorContains(t, err, "requires protocol version 12")
		} else {
			require.ErrorContains(t, err, "current role context is unset or invalid")
		}
	}
}

func TestCurrentRoleIdentityQueries(t *testing.T) {
	failure := errors.New("catalog failure")
	base := process.CurrentRoleContext{Mode: 2, AccountID: 7, AccountName: "tenant", UserID: 8, UserName: "alice", PrimaryRoleID: 9}
	account := roleQuery{0, "mo_account", []int64{7}, nil}
	user := roleQuery{7, "mo_user WHERE", []int64{8}, nil}
	grant := roleQuery{7, "role_id = 9", []int64{9}, nil}
	all := roleQuery{7, "SELECT DISTINCT", []int64{9, 10}, nil}
	for _, tc := range []struct {
		name    string
		id      process.CurrentRoleContext
		queries []roleQuery
		roots   []int64
		wantErr bool
	}{
		{"all", base, []roleQuery{account, user, grant, all}, []int64{9, 9, 10}, false},
		{"missing account", base, []roleQuery{{0, "mo_account", nil, nil}}, nil, true},
		{"missing user", base, []roleQuery{account, {7, "mo_user WHERE", nil, nil}}, nil, true},
		{"revoked primary", base, []roleQuery{account, user, {7, "role_id = 9", nil, nil}}, nil, true},
		{"query error", base, []roleQuery{account, {7, "mo_user WHERE", []int64{8}, failure}}, nil, true},
		{"trusted public", process.CurrentRoleContext{Mode: 1, AccountID: 7, AccountName: "tenant", UserID: 8, UserName: "alice", PrimaryRoleID: 1}, []roleQuery{account, user}, []int64{1}, false},
		{"non-system role zero is not trusted", process.CurrentRoleContext{Mode: 1, AccountID: 7, AccountName: "tenant", UserID: 8, UserName: "alice", PrimaryRoleID: 0}, []roleQuery{account, user, {7, "role_id = 0", nil, nil}}, nil, true},
		{"system role zero", process.CurrentRoleContext{Mode: 1, AccountName: "sys", UserID: 8, UserName: "alice"}, []roleQuery{{0, "mo_user WHERE", []int64{8}, nil}}, []int64{0}, false},
		{"system name case", process.CurrentRoleContext{Mode: 1, AccountName: "SYS", UserID: 8, UserName: "alice"}, []roleQuery{{0, "mo_user WHERE", []int64{8}, nil}}, []int64{0}, false},
		{"invalid system name", process.CurrentRoleContext{Mode: 1, AccountName: "other"}, nil, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			proc := testutil.NewProcess(t)
			proc.Ctx = defines.AttachAccountId(context.Background(), tc.id.AccountID)
			proc.Base.SessionInfo.CurrentRoleContext = tc.id
			txn := mockfrontend.NewMockTxnOperator(ctrl)
			proc.Base.TxnOperator = txn
			rt := moruntime.ServiceRuntime(proc.GetService())
			previous, existed := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
			sqlExec := executortest.NewMockSQLExecutor(ctrl)
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, sqlExec)
			t.Cleanup(func() {
				if existed {
					rt.SetGlobalVariables(moruntime.InternalSQLExecutor, previous)
				} else {
					rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, sqlExec)
				}
			})
			index := 0
			sqlExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
				require.Less(t, index, len(tc.queries))
				q := tc.queries[index]
				index++
				require.Same(t, proc.Ctx, ctx)
				require.Contains(t, sql, q.contains)
				require.True(t, opts.HasAccountID())
				require.Equal(t, q.account, opts.AccountID())
				require.Same(t, txn, opts.Txn())
				require.True(t, opts.KeepTxnAlive())
				require.True(t, opts.DisableIncrStatement())
				result := executor.NewMemResult([]types.Type{types.T_int64.ToType()}, proc.Mp())
				result.NewBatchWithRowCount(len(q.rows))
				require.NoError(t, executor.AppendFixedRows(result, 0, q.rows))
				return result.GetResult(), q.err
			}).AnyTimes()
			originalCtx := proc.Ctx
			before := proc.Mp().CurrNB()
			roots, err := loadCurrentRoleRoots(proc)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.roots, roots)
			}
			require.Equal(t, len(tc.queries), index)
			require.Equal(t, before, proc.Mp().CurrNB(), "query results must be released, including on error")
			require.Same(t, originalCtx, proc.Ctx)
			require.Equal(t, tc.id, proc.GetSessionInfo().CurrentRoleContext)
			require.Same(t, txn, proc.GetTxnOperator())
		})
	}
}

// Copyright 2022 Matrix Origin
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

package table_function

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	arg := TableFunction{FuncName: "unnest"}
	arg.String(bytes.NewBuffer(nil))
}

func TestPrepare(t *testing.T) {
	arg := TableFunction{FuncName: "unnest",
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}
	err := arg.Prepare(testutil.NewProc(t))
	require.Error(t, err)
	arg.FuncName = "generate_series"
	err = arg.Prepare(testutil.NewProc(t))
	require.NoError(t, err)
	arg.FuncName = "metadata_scan"
	err = arg.Prepare(testutil.NewProc(t))
	require.NoError(t, err)
	arg.FuncName = "not_exist"
	err = arg.Prepare(testutil.NewProc(t))
	require.Error(t, err)
}

func TestParseTablePathWithAccount(t *testing.T) {
	tests := []struct {
		name              string
		path              string
		currentDatabase   string
		currentAccountId  uint32
		expectedDb        string
		expectedTable     string
		expectedAccountId uint32
		expectError       bool
		errorContains     string
	}{
		{
			name:              "single part - table only",
			path:              "mytable",
			currentDatabase:   "mydb",
			currentAccountId:  1,
			expectedDb:        "mydb",
			expectedTable:     "mytable",
			expectedAccountId: 1,
			expectError:       false,
		},
		{
			name:             "single part - no database selected",
			path:             "mytable",
			currentDatabase:  "",
			currentAccountId: 1,
			expectError:      true,
			errorContains:    "no database selected",
		},
		{
			name:              "two parts - db.table",
			path:              "mydb.mytable",
			currentDatabase:   "otherdb",
			currentAccountId:  1,
			expectedDb:        "mydb",
			expectedTable:     "mytable",
			expectedAccountId: 1,
			expectError:       false,
		},
		{
			name:              "three parts - db.table.account_id (same account)",
			path:              "mydb.mytable.1",
			currentDatabase:   "otherdb",
			currentAccountId:  1,
			expectedDb:        "mydb",
			expectedTable:     "mytable",
			expectedAccountId: 1,
			expectError:       false,
		},
		{
			name:              "three parts - db.table.account_id (sys account queries other)",
			path:              "mydb.mytable.5",
			currentDatabase:   "otherdb",
			currentAccountId:  0, // sys account
			expectedDb:        "mydb",
			expectedTable:     "mytable",
			expectedAccountId: 5,
			expectError:       false,
		},
		{
			name:             "three parts - non-sys account queries other (should fail)",
			path:             "mydb.mytable.2",
			currentDatabase:  "otherdb",
			currentAccountId: 1, // non-sys account
			expectError:      true,
			errorContains:    "only sys account can query stats for other accounts",
		},
		{
			name:             "three parts - invalid account_id format",
			path:             "mydb.mytable.abc",
			currentDatabase:  "otherdb",
			currentAccountId: 1,
			expectError:      true,
			errorContains:    "invalid account_id",
		},
		{
			name:             "four parts - too many",
			path:             "mydb.mytable.1.extra",
			currentDatabase:  "otherdb",
			currentAccountId: 1,
			expectError:      true,
			errorContains:    "invalid table path",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			proc.GetSessionInfo().Database = tt.currentDatabase
			proc.Ctx = defines.AttachAccountId(proc.Ctx, tt.currentAccountId)

			db, table, accountId, err := parseTablePathWithAccount(tt.path, proc)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					require.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedDb, db)
				require.Equal(t, tt.expectedTable, table)
				require.Equal(t, tt.expectedAccountId, accountId)
			}
		})
	}
}

func TestResetAndFreeWithPartiallyInitializedExecutors(t *testing.T) {
	proc := testutil.NewProc(t)
	executor := &stubExpressionExecutor{}
	arg := &TableFunction{}
	arg.ctr.executorsForArgs = []colexec.ExpressionExecutor{executor}

	require.NotPanics(t, func() {
		arg.Reset(proc, false, nil)
	})
	require.Equal(t, 1, executor.resetCount)

	require.NotPanics(t, func() {
		arg.Free(proc, false, nil)
	})
	require.Equal(t, 1, executor.freeCount)
	require.Nil(t, arg.ctr.argVecs)
	require.Nil(t, arg.ctr.executorsForArgs)
}

func TestGenerateSeriesPrepareFaultLeavesPartialInitSafeToFree(t *testing.T) {
	proc := testutil.NewProc(t)
	fault.Enable()
	defer fault.Disable()

	require.NoError(t, fault.AddFaultPoint(proc.Ctx, "fj/cn/generate_series_partial_prepare", ":::", "echo", 0, "apply teardown repro", false))
	defer func() {
		_, err := fault.RemoveFaultPoint(proc.Ctx, "fj/cn/generate_series_partial_prepare")
		require.NoError(t, err)
	}()

	arg := &TableFunction{
		Args: []*plan.Expr{
			plan2.MakePlan2Int64ConstExprWithType(1),
			plan2.MakePlan2Int64ConstExprWithType(3),
		},
	}

	state, err := generateSeriesPrepare(proc, arg)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState))
	require.NotNil(t, state)
	require.NotNil(t, arg.ctr.executorsForArgs)
	require.Nil(t, arg.ctr.argVecs)

	require.NotPanics(t, func() {
		arg.Free(proc, false, nil)
	})
}

type stubExpressionExecutor struct {
	resetCount int
	freeCount  int
}

func (s *stubExpressionExecutor) Eval(*process.Process, []*batch.Batch, []bool) (*vector.Vector, error) {
	return nil, nil
}

func (s *stubExpressionExecutor) EvalWithoutResultReusing(*process.Process, []*batch.Batch, []bool) (*vector.Vector, error) {
	return nil, nil
}

func (s *stubExpressionExecutor) ResetForNextQuery() {
	s.resetCount++
}

func (s *stubExpressionExecutor) Free() {
	s.freeCount++
}

func (s *stubExpressionExecutor) IsColumnExpr() bool {
	return false
}

func (s *stubExpressionExecutor) TypeName() string {
	return "stub"
}

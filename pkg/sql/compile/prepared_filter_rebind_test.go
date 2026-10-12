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

package compile

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

func TestPreparedBlockPredicatesSurviveRebinding(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctrl := gomock.NewController(t)
	tx := mock_frontend.NewMockTxnOperator(ctrl)
	tx.EXPECT().GetWorkspace().Return(&disttae.Transaction{}).AnyTimes()
	tx.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	tx.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	proc.Base.TxnOperator = tx
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("7"), false, proc.Mp()))
	proc.SetPrepareParams(params)
	t.Cleanup(func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) })
	param := &plan.Expr{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
	target := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_T{T: &plan.TargetType{}}}
	cast, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "cast", []*plan.Expr{param, target})
	require.NoError(t, err)
	col := &plan.Expr{Typ: target.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	pred, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "=", []*plan.Expr{col, cast})
	require.NoError(t, err)
	table := &plan.TableDef{Name: "t", Cols: []*plan.ColDef{{Name: "k", Typ: target.Typ}}}
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().Reset(tx).Return(nil).AnyTimes()
	rel.EXPECT().GetTableDef(gomock.Any()).Return(table).AnyTimes()
	node := &plan.Node{NodeType: plan.Node_TABLE_SCAN, TableDef: table, ObjRef: &plan.ObjectRef{SchemaName: "db"}, BlockFilterList: []*plan.Expr{pred}}
	scope := &Scope{DataSource: &Source{node: node, Rel: rel}}
	c := &Compile{proc: proc}
	t.Cleanup(func() {
		for _, ex := range c.filterExprExes {
			ex.Free()
		}
	})
	for _, tc := range []struct {
		value string
		free  bool
	}{{"7", true}, {"2147483648", false}, {"8", true}} {
		require.NoError(t, scope.resetForReuse(c))
		require.Nil(t, scope.DataSource.remoteBlockFilters)
		require.NoError(t, vector.SetStringAt(params, 0, tc.value, proc.Mp()))
		free, err := plan2.ProbeStatementParameterDiagnosticFree(proc, pred)
		require.NoError(t, err)
		require.Equal(t, tc.free, free)
		c.preparedJoinDiagnosticFree = free
		require.NoError(t, c.compileTableScanDataSource(scope))
		require.Same(t, node, scope.DataSource.node)
		require.Len(t, node.BlockFilterList, 1, "execution admission must not overwrite the reusable template")
		require.Len(t, copyBlockFiltersForRemoteRun(scope).DataSource.BlockFilterList, len(scope.DataSource.BlockFilterList))
		if free {
			require.Len(t, scope.DataSource.BlockFilterList, 1)
		} else {
			require.Empty(t, scope.DataSource.BlockFilterList)
		}
	}
}

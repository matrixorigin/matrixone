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

package preinsertunique

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestOrderedForeignKeyCurrentRow(t *testing.T) {
	for _, generated := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit", true: "generated"}[generated], func(t *testing.T) {
			proc := testutil.NewProc(t)
			t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
			ctrl := gomock.NewController(t)
			eng := mock_frontend.NewMockEngine(ctrl)
			txn := mock_frontend.NewMockTxnOperator(ctrl)
			ws := mock_frontend.NewMockWorkspace(ctrl)
			proc.Base.SessionInfo.StorageEngine = eng
			proc.Base.TxnOperator = txn
			txn.EXPECT().GetWorkspace().Return(ws).AnyTimes()
			txn.EXPECT().Txn().Return(txnpb.TxnMeta{}).AnyTimes()
			typ := types.T_int32.ToType().PlanType()
			predicate, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "=", []*plan.Expr{
				{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
				{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0}}},
			})
			require.NoError(t, err)
			fk := &plan.OrderedForeignKey{TableDef: &plan.TableDef{TblId: 99, Cols: []*plan.ColDef{{Name: "id", Typ: typ}}}, ChildColumns: []*plan.ColRef{{ColPos: 1}}, ParentColumns: []int32{0}, SelfParentColumns: []*plan.ColRef{{ColPos: 0}}, Predicate: predicate}
			arg := &PreInsertUnique{PreInsertCtx: &plan.PreInsertUkCtx{AutoIncrementColumn: 0, OrderedAutoIncrement: &plan.OrderedAutoIncrement{ForeignKeys: []*plan.OrderedForeignKey{fk}}}}
			t.Cleanup(func() { arg.freeAcceptedState(proc) })
			input := batch.NewWithSize(2)
			input.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
			input.Vecs[1] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
			if generated {
				input.Vecs[0].SetNull(0)
			}
			input.SetRowCount(1)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			ok, err := arg.checkOrderedForeignKeys(proc, input, 0, generated, 1)
			require.NoError(t, err)
			require.True(t, ok, "当前候选自身提供完整父键，不应查询空表")
			require.Nil(t, arg.ctr.ordered.foreignKeyResults[0].parents, "校验不能提前发布父键")
		})
	}
}

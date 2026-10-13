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
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestOrderedForeignKeyProbe(t *testing.T) {
	for _, mode := range []string{"match", "absent", "self", "explicit", "null", "cancelled", "ranges_error", "readers_error", "read_error"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProc(t)
			t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
			ctx, cancel := context.WithCancel(proc.Ctx)
			defer cancel()
			proc.Ctx = ctx
			ctrl := gomock.NewController(t)
			eng := mock_frontend.NewMockEngine(ctrl)
			rel := mock_frontend.NewMockRelation(ctrl)
			txn := mock_frontend.NewMockTxnOperator(ctrl)
			ws := mock_frontend.NewMockWorkspace(ctrl)
			proc.Base.SessionInfo.StorageEngine = eng
			proc.Base.TxnOperator = txn
			txn.EXPECT().GetWorkspace().Return(ws).AnyTimes()
			txn.EXPECT().Txn().Return(txnpb.TxnMeta{}).AnyTimes()
			ws.EXPECT().GetSnapshotWriteOffset().Return(7).AnyTimes()
			typ := types.T_int32.ToType().PlanType()
			input := batch.NewWithSize(2)
			input.Vecs[0] = testutil.MakeInt32Vector([]int32{9}, nil, proc.Mp())
			input.Vecs[1] = testutil.MakeInt32Vector([]int32{9}, nil, proc.Mp())
			input.SetRowCount(1)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			if mode == "null" {
				input.Vecs[0].SetNull(0)
			}
			if mode == "cancelled" {
				cancel()
			}
			predicate, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "=", []*plan.Expr{
				{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 2, Name: "id"}}},
				{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0}}},
			})
			require.NoError(t, err)
			check := &plan.OrderedForeignKey{TableDef: &plan.TableDef{TblId: 99, Cols: []*plan.ColDef{{Name: "other", Typ: typ}, {Name: "unused", Typ: typ}, {Name: "id", Typ: typ}}}, ChildColumns: []*plan.ColRef{{ColPos: 0}}, ParentColumns: []int32{2}, Predicate: predicate}
			if mode == "self" {
				check.SelfParentColumns = []*plan.ColRef{{ColPos: 1}}
			}
			arg := &PreInsertUnique{PreInsertCtx: &plan.PreInsertUkCtx{AutoIncrementColumn: 0, OrderedAutoIncrement: &plan.OrderedAutoIncrement{ForeignKeys: []*plan.OrderedForeignKey{check}}}}
			t.Cleanup(func() { arg.freeAcceptedState(proc) })
			injected := moerr.NewInternalErrorNoCtx("probe failed")
			if mode != "null" && mode != "cancelled" {
				eng.EXPECT().GetRelationById(gomock.Any(), txn, uint64(99)).Return("db", "parent", rel, nil)
				var rangeErr error
				if mode == "ranges_error" {
					rangeErr = injected
				}
				rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, params engine.RangesParam) (engine.RelData, error) {
					require.Equal(t, 7, params.TxnOffset)
					return nil, rangeErr
				})
				if rangeErr == nil {
					var readerErr error
					if mode == "readers_error" {
						readerErr = injected
					}
					reader := mock_frontend.NewMockReader(ctrl)
					rel.EXPECT().BuildReaders(gomock.Any(), proc, gomock.Any(), nil, 1, 7, false, engine.TombstoneApplyPolicy(engine.Policy_CheckAll), engine.FilterHint{}).Return([]engine.Reader{reader}, readerErr)
					if readerErr == nil {
						reader.EXPECT().Close().Return(nil)
						reader.EXPECT().Read(gomock.Any(), []string{"id"}, nil, proc.Mp(), gomock.Any()).DoAndReturn(func(_ context.Context, _ []string, _ *plan.Expr, mp *mpool.MPool, out *batch.Batch) (bool, error) {
							require.Equal(t, []string{"id"}, out.Attrs)
							if mode == "read_error" {
								return false, injected
							}
							parent := int32(5)
							if mode == "absent" {
								parent = 6
							}
							if mode == "explicit" {
								parent = 9
							}
							require.NoError(t, vector.AppendFixed(out.Vecs[0], parent, false, mp))
							out.SetRowCount(1)
							return true, nil
						})
					}
				}
			}
			ok, err := arg.checkOrderedForeignKeys(proc, input, 0, mode != "explicit" && mode != "null", 5)
			switch mode {
			case "cancelled":
				require.ErrorIs(t, err, context.Canceled)
			case "ranges_error", "readers_error", "read_error":
				require.ErrorIs(t, err, injected)
			case "self":
				require.NoError(t, err)
				require.True(t, ok)
				require.NoError(t, arg.recordOrderedForeignKeyParents(proc, input, 0, true, 6))
				require.NoError(t, vector.SetFixedAtNoTypeCheck(input.Vecs[1], 0, int32(10)))
				next, err := arg.checkOrderedForeignKeys(proc, input, 0, true, 9)
				require.NoError(t, err)
				require.True(t, next, "later rows see an accepted parent even when their own parent key differs")
			default:
				require.NoError(t, err)
				require.Equal(t, mode != "absent", ok)
				if mode == "match" || mode == "absent" {
					again, err := arg.checkOrderedForeignKeys(proc, input, 0, true, 5)
					require.NoError(t, err)
					require.Equal(t, ok, again, "same candidate reuses the point lookup")
				}
			}
		})
	}
}

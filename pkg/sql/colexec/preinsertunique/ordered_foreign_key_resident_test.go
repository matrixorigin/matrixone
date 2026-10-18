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

package preinsertunique

import (
	"context"
	"fmt"
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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestOrderedForeignKeyResidentWorkAndCleanup(t *testing.T) {
	for _, tc := range []struct {
		parents, candidates, constraints int
		scenario                         string
	}{
		{64, 1, 1, ""}, {64, 8, 1, ""}, {64, 32, 1, ""}, {1024, 32, 1, ""},
		{1024, 32, 2, ""}, {colexec.DefaultBatchSize + 3, 2, 1, ""}, {0, 8, 1, ""},
		{64, 32, 1, "distinct"}, {64, 1, 1, "read"}, {64, 1, 1, "close"}, {64, 1, 1, "budget"},
	} {
		t.Run(fmt.Sprintf("P%d/N%d/F%d/%s", tc.parents, tc.candidates, tc.constraints, tc.scenario), func(t *testing.T) {
			proc := testutil.NewProc(t)
			t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
			ctrl := gomock.NewController(t)
			txn := mock_frontend.NewMockTxnOperator(ctrl)
			ws := mock_frontend.NewMockWorkspace(ctrl)
			eng := mock_frontend.NewMockEngine(ctrl)
			proc.Base.TxnOperator, proc.Base.SessionInfo.StorageEngine = txn, eng
			txn.EXPECT().GetWorkspace().Return(ws).AnyTimes()
			txn.EXPECT().Txn().Return(txnpb.TxnMeta{}).AnyTimes()
			// No GetSnapshotWriteOffset expectation: a late read of the mutable
			// workspace offset would violate this statement's fixed view.
			typ := types.T_int32.ToType().PlanType()
			predicate, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "=", []*plan.Expr{
				{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}},
				{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0}}},
			})
			require.NoError(t, err)
			table := &plan.TableDef{TblId: 99, Cols: []*plan.ColDef{{Name: "id", Typ: typ}, {Name: "parent_value", Typ: typ}}}
			checks := make([]*plan.OrderedForeignKey, tc.constraints)
			for i := range checks {
				checks[i] = &plan.OrderedForeignKey{TableDef: table, ParentColumns: []int32{1}, ChildColumns: []*plan.ColRef{{ColPos: 0}}, Predicate: predicate}
				addOrderedFKTestAccess(t, checks[i], typ)
				// Nonunique referenced value: preserve and lock a real base-PK witness.
				table.Pkey = &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}
				table.Name2ColIndex = map[string]int32{"id": 0, "parent_value": 1}
				checks[i].ParentAccess.Kind = plan.OrderedForeignKeyAccess_RESIDENT
				checks[i].ParentAccess.LookupFilter = nil
				checks[i].ParentAccess.WitnessLockKey = &plan.Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			}
			arg := &PreInsertUnique{TxnOffset: 7, PreInsertCtx: &plan.PreInsertUkCtx{AutoIncrementColumn: 0, OrderedAutoIncrement: &plan.OrderedAutoIncrement{ForeignKeys: checks}}}
			var account *mpool.AllocationAccount
			if tc.scenario == "budget" {
				registry, e := mpool.NewAllocationAccountRegistry(1, 4096)
				require.NoError(t, e)
				account, e = registry.Open(1)
				require.NoError(t, e)
				require.NoError(t, arg.SetAllocationAccount(account))
			} else {
				account = installODKUTestAllocation(t, arg)
			}
			t.Cleanup(func() {
				arg.freeAcceptedState(proc)
				require.NoError(t, arg.ClearAllocationAccount(account))
				require.Zero(t, account.Snapshot().Used)
			})
			input := batch.NewWithSize(1)
			input.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
			input.SetRowCount(1)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			ranges, reads, rows, closed := 0, 0, 0, 0
			injected := moerr.NewInternalErrorNoCtx("resident read/close failed")
			for range tc.constraints {
				rel := mock_frontend.NewMockRelation(ctrl)
				eng.EXPECT().GetRelationById(gomock.Any(), txn, uint64(99)).Return("db", "parent", rel, nil)
				rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, p engine.RangesParam) (engine.RelData, error) {
					ranges++
					require.Equal(t, 7, p.TxnOffset)
					require.Empty(t, p.BlockFilters)
					return nil, nil
				})
				reader := mock_frontend.NewMockReader(ctrl)
				rel.EXPECT().BuildReaders(gomock.Any(), proc, nil, nil, 1, 7, false, engine.TombstoneApplyPolicy(engine.Policy_CheckAll), engine.FilterHint{}).Return([]engine.Reader{reader}, nil)
				reader.EXPECT().Close().DoAndReturn(func() error {
					closed++
					if tc.scenario == "close" {
						return injected
					}
					return nil
				})
				offset := 0
				parentValue := func(row int) int32 {
					if tc.scenario == "distinct" {
						return int32(row + 1)
					}
					return int32(row/2 + 1)
				}
				reader.EXPECT().Read(gomock.Any(), []string{"parent_value", "id"}, nil, proc.Mp(), gomock.Any()).DoAndReturn(
					func(_ context.Context, _ []string, _ *plan.Expr, mp *mpool.MPool, out *batch.Batch) (bool, error) {
						reads++
						if tc.scenario == "read" {
							return false, injected
						}
						count := min(47, tc.parents-offset) // ingress boundaries differ from JoinMap chunks
						for j := range count {
							// Two physical parents per equal tuple; only one witness is needed.
							require.NoError(t, vector.AppendFixed(out.Vecs[0], parentValue(offset+j), false, mp))
							require.NoError(t, vector.AppendFixed(out.Vecs[1], int32(offset+j+1000), false, mp))
						}
						offset += count
						rows += count
						out.SetRowCount(count)
						return offset == tc.parents, nil
					}).AnyTimes()
			}
			for i := range tc.candidates {
				key := uint64(i + 1)
				if tc.parents > colexec.DefaultBatchSize {
					key = uint64(tc.parents/2 + 1)
				} // witness in the second retained chunk
				ok, e := arg.checkOrderedForeignKeys(proc, input, 0, true, key)
				if tc.scenario != "" && tc.scenario != "distinct" {
					if tc.scenario == "budget" {
						require.ErrorIs(t, e, mpool.ErrAllocationAccountCapacity)
					} else {
						require.ErrorIs(t, e, injected)
					}
					require.False(t, ok)
					require.False(t, arg.ctr.ordered.foreignKeyResults[0].lookup.ready)
					break
				}
				require.NoError(t, e)
				require.Equal(t, tc.parents != 0, ok)
				if ok {
					for j := range checks {
						l := arg.ctr.ordered.foreignKeyResults[j].lookup
						require.True(t, l.ready)
						witness := int32(1000 + 2*(key-1))
						if tc.scenario == "distinct" {
							witness = int32(1000 + key - 1)
						}
						require.Equal(t, witness, vector.GetFixedAtNoTypeCheck[int32](l.witness, 0))
					}
				}
			}
			require.Equal(t, tc.constraints, ranges, "one build per FK, independent of candidate count")
			require.Equal(t, ranges, closed, "reader ownership ends before map publication")
			if tc.scenario == "" || tc.scenario == "distinct" {
				require.Equal(t, tc.parents*tc.constraints, rows)
			}
			t.Logf("ranges=%d readers_closed=%d read_calls=%d returned_rows=%d candidates=%d constraints=%d charged_bytes=%d", ranges, closed, reads, rows, tc.candidates, tc.constraints, account.Snapshot().Used)
		})
	}
}

func TestOrderedForeignKeyResidentReleasesStringIterator(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
	arg := &PreInsertUnique{}
	account := installODKUTestAllocation(t, arg)
	var builder hashbuild.HashmapBuilder
	defer builder.Free(proc)
	require.NoError(t, builder.SetAllocationAccount(account))
	typ := types.NewWithCharset(types.T_varchar, 64, 0, types.CharsetUTF8MB4UnicodeCI)
	require.NoError(t, builder.Prepare([]*plan.Expr{plan2.GetColExpr(typ.PlanType(), 0, 0)}, -1, -1, nil, proc))
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(typ)
	require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte("Résumé"), false, proc.Mp()))
	input.SetRowCount(1)
	defer input.Clean(proc.Mp())
	require.NoError(t, builder.AppendResidentBatch(input, proc))
	parents, err := builder.FinishResidentBuild(proc)
	require.NoError(t, err)
	builder.Free(proc)
	lookup := &orderedFKLookup{parents: parents, iterator: parents.NewIterator()}
	defer func() {
		if lookup.parents != nil {
			lookup.free(proc)
		}
	}()
	probe := vector.NewVec(typ)
	require.NoError(t, vector.AppendBytes(probe, []byte("RESUME"), false, proc.Mp()))
	defer probe.Free(proc.Mp())
	before := account.Snapshot().Used
	ids, zs, err := lookup.iterator.Find(0, 1, []*vector.Vector{probe})
	require.NoError(t, err)
	require.EqualValues(t, 1, ids[0])
	require.EqualValues(t, 1, zs[0])
	require.Greater(t, account.Snapshot().Used, before, "probe iterator owns accounted key scratch")
	lookup.free(proc)
	lookup.iterator = nil
	require.Zero(t, account.Snapshot().Used, "detach iterator through its owner API before freeing the map")
	require.NoError(t, arg.ClearAllocationAccount(account))
}

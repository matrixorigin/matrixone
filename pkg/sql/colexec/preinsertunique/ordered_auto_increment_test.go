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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

func TestOrderedAutoIncrementAllocation(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		ids, keys                      []int32
		generated, conflicts, eligible []bool
		valuesRows, next               uint64
		want                           []int32
		wantNext                       uint64
		blocks                         []uint64
	}{
		{"rejected_explicit", []int32{100, 0, 0}, []int32{1, 2, 3}, []bool{false, true, true}, []bool{true, false, false}, nil, 3, 2, []int32{2, 3}, 5, []uint64{3}},
		{"explicit_prefix", []int32{100, 0, 0}, []int32{1, 2, 3}, []bool{false, true, true}, nil, nil, 3, 1, []int32{100, 101, 102}, 104, []uint64{3}},
		{"explicit_middle", []int32{0, 100, 0}, []int32{1, 2, 3}, []bool{true, false, true}, nil, nil, 3, 1, []int32{1, 100, 101}, 102, []uint64{3, 1}},
		{"fk_rejected_explicit", []int32{100, 0}, []int32{1, 2}, []bool{false, true}, nil, []bool{false, true}, 2, 1, []int32{1}, 3, []uint64{2}},
		{"bulk_repeated_uk", []int32{0, 0, 0, 0, 0, 0}, []int32{1, 1, 1, 1, 1, 1}, []bool{true, true, true, true, true, true}, nil, nil, 0, 1, []int32{1}, 4, []uint64{1, 2}},
		{"bulk_all_fk_rejected", []int32{0, 0, 0}, []int32{1, 2, 3}, []bool{true, true, true}, nil, []bool{false, false, false}, 0, 1, nil, 2, []uint64{1}},
		{"bulk_four_accepted", []int32{0, 0, 0, 0}, []int32{1, 2, 3, 4}, []bool{true, true, true, true}, nil, nil, 0, 1, []int32{1, 2, 3, 4}, 8, []uint64{1, 2, 4}},
		{"values_check_removed", []int32{0}, []int32{1}, []bool{true}, nil, nil, 6, 1, []int32{1}, 7, []uint64{6}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
			conflicts := tc.conflicts
			if conflicts == nil {
				conflicts = make([]bool, len(tc.ids))
			}
			input := makeInsertIgnoreAutoIncrementBatch(t, proc, tc.ids, tc.keys, make([]bool, len(tc.ids)), conflicts, tc.generated)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			arg := newInsertIgnoreAutoIncrementArgument(input)
			t.Cleanup(func() { arg.Free(proc, false, nil) })
			if tc.eligible != nil {
				flag := vector.NewVec(types.T_bool.ToType())
				input.Vecs = append(input.Vecs, flag)
				for _, v := range tc.eligible {
					require.NoError(t, vector.AppendFixed(flag, v, false, proc.Mp()))
				}
				arg.PreInsertCtx.RowEligibility = &plan.ColRef{ColPos: 5}
			}
			arg.PreInsertCtx.OrderedAutoIncrement = &plan.OrderedAutoIncrement{ValuesRows: tc.valuesRows,
				TableDef: &plan.TableDef{TblId: 44, IsTemporary: true, Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int32), AutoIncr: true}}}},
			}
			next := tc.next
			var blocks []uint64
			service := mock_frontend.NewMockAutoIncrementService(gomock.NewController(t))
			service.EXPECT().InsertValues(gomock.Any(), uint64(44), uint32(0), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, _ uint64, _ uint32, _ client.TxnOperator, vecs []*vector.Vector, rows int, _ int64) (uint64, error) {
					if request, ok := incrservice.AutoIncrementReservationFromContext(ctx); ok {
						require.Zero(t, rows)
						require.Empty(t, vecs)
						blocks = append(blocks, request.Count)
						request.Runs = []incrservice.AutoIncrementRun{{First: next, Step: 1, Count: request.Count}}
						next += request.Count
					} else {
						require.Equal(t, 1, rows)
						next = max(next, uint64(vector.GetFixedAtNoTypeCheck[int32](vecs[0], 0))+1)
					}
					return 0, nil
				}).AnyTimes()
			proc.Base.IncrService = service
			require.NoError(t, arg.Prepare(proc))
			result, err := arg.Call(proc)
			require.NoError(t, err)
			if len(tc.want) == 0 {
				require.True(t, result.Batch.IsEmpty())
			} else {
				require.Equal(t, tc.want, vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()])
			}
			require.Equal(t, tc.wantNext, next)
			require.Equal(t, tc.blocks, blocks)
			_, err = arg.Call(proc)
			require.NoError(t, err)
			require.Equal(t, tc.wantNext, next, "EOF must not allocate a second tail")
			arg.Reset(proc, false, nil)
			require.False(t, arg.ctr.ordered.started)
			require.Nil(t, arg.ctr.ordered.manual)
		})
	}
}

func TestOrderedAutoIncrementRejectsMalformedContext(t *testing.T) {
	for _, mode := range []string{"mode", "table", "column", "type", "fk", "parent"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProc(t)
			spec := &plan.OrderedAutoIncrement{TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int32), AutoIncr: true}}}}}
			p := &PreInsertUnique{PreInsertCtx: &plan.PreInsertUkCtx{InsertIgnoreMultiDedup: true, AutoIncrementReorder: true, OrderedAutoIncrement: spec}}
			switch mode {
			case "mode":
				p.PreInsertCtx.InsertIgnoreMultiDedup = false
			case "table":
				spec.TableDef = nil
			case "column":
				spec.ColumnIndex = 1
			case "type":
				spec.TableDef.Cols[0].Typ.Id = int32(types.T_varchar)
			case "fk":
				spec.ForeignKeys = []*plan.OrderedForeignKey{nil}
			case "parent":
				spec.ForeignKeys = []*plan.OrderedForeignKey{{TableDef: &plan.TableDef{Pkey: &plan.PrimaryKeyDef{}}, Predicate: &plan.Expr{}, ChildColumns: []*plan.ColRef{{}}, ParentColumns: []int32{1}}}
			}
			require.Error(t, p.validateOrderedAutoIncrement(proc))
		})
	}
}

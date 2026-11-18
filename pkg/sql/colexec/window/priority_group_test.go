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

package window

import (
	"fmt"
	"slices"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/partition"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestPriorityIsOrderWithinOneRowID(t *testing.T) {
	type item struct {
		id           int32
		active       bool
		priorityNull bool
		payloadNull  bool
	}
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%t", reverse), func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			inputs := []item{{20, false, false, false}, {10, true, false, true}, {21, false, false, false}, {30, false, true, false}}
			if reverse {
				slices.Reverse(inputs)
			}
			var blk types.Blockid
			rid := types.NewRowid(&blk, 7)
			batches := make([]*batch.Batch, 0, len(inputs))
			var p *partition.Partition
			var w *Window
			defer func() {
				if w != nil {
					w.Free(proc, false, nil)
				}
				if p != nil {
					p.Free(proc, false, nil)
				}
				for _, b := range batches {
					b.Clean(proc.Mp())
				}
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			}()
			columnTypes := []types.Type{types.T_Rowid.ToType(), types.T_bool.ToType(), types.T_int32.ToType(), types.T_int32.ToType()}
			for _, input := range inputs {
				b := batch.NewWithSize(len(columnTypes))
				batches = append(batches, b)
				for i, typ := range columnTypes {
					b.Vecs[i] = vector.NewVec(typ)
				}
				require.NoError(t, vector.AppendFixed(b.Vecs[0], rid, false, proc.Mp()))
				require.NoError(t, vector.AppendFixed(b.Vecs[1], input.active, input.priorityNull, proc.Mp()))
				require.NoError(t, vector.AppendFixed(b.Vecs[2], input.id, false, proc.Mp()))
				require.NoError(t, vector.AppendFixed(b.Vecs[3], input.id*10, input.payloadNull, proc.Mp()))
				b.SetRowCount(1)
			}
			rowID := newColExprWithType(0, columnTypes[0])
			priority := &plan.OrderBySpec{Expr: newColExprWithType(1, columnTypes[1]), Flag: plan.OrderBySpec_DESC | plan.OrderBySpec_INTERNAL}
			p = &partition.Partition{OrderBySpecs: []*plan.OrderBySpec{{Expr: rowID, Flag: plan.OrderBySpec_INTERNAL}}}
			// Each input run is sorted by the physical partition key.
			p.AppendChild(colexec.NewMockOperator().WithBatchs(batches))
			w = &Window{
				WinSpecList: []*plan.Expr{{Expr: &plan.Expr_W{W: &plan.WindowSpec{
					Name: "row_number", WindowFunc: newFunExpr("row_number"), Frame: makeFullFrame(),
					PartitionBy: []*plan.Expr{rowID}, OrderBy: []*plan.OrderBySpec{priority},
				}}}},
				Aggs: []aggexec.AggFuncExecExpression{newRowNumberAggExpr(t)},
			}
			w.AppendChild(p)
			require.NoError(t, p.Prepare(proc))
			require.NoError(t, w.Prepare(proc))
			type observation struct {
				id                        int32
				rank                      uint64
				active                    bool
				priorityNull, payloadNull bool
			}
			var actual []observation
			for {
				result, err := vm.Exec(w, proc)
				require.NoError(t, err)
				if b := result.Batch; b != nil {
					for i := 0; i < b.RowCount(); i++ {
						require.Equal(t, rid, vector.GetFixedAtNoTypeCheck[types.Rowid](b.Vecs[0], i))
						id := vector.GetFixedAtNoTypeCheck[int32](b.Vecs[2], i)
						v := observation{id, vector.GetFixedAtNoTypeCheck[uint64](b.Vecs[4], i), vector.GetFixedAtNoTypeCheck[bool](b.Vecs[1], i), b.Vecs[1].IsNull(uint64(i)), b.Vecs[3].IsNull(uint64(i))}
						if !v.payloadNull {
							require.Equal(t, id*10, vector.GetFixedAtNoTypeCheck[int32](b.Vecs[3], i))
						}
						actual = append(actual, v)
					}
				}
				if result.Status == vm.ExecStop {
					break
				}
			}
			require.Len(t, actual, 4)
			require.Equal(t, observation{10, 1, true, false, true}, actual[0])
			require.ElementsMatch(t, []int32{20, 21}, []int32{actual[1].id, actual[2].id})
			require.False(t, actual[1].active || actual[2].active || actual[1].priorityNull || actual[2].priorityNull)
			require.Equal(t, []uint64{1, 2, 3, 4}, []uint64{actual[0].rank, actual[1].rank, actual[2].rank, actual[3].rank})
			require.Equal(t, observation{30, 4, false, true, false}, actual[3])
			t.Logf("priority witness ranks=%v", actual)
		})
	}
}

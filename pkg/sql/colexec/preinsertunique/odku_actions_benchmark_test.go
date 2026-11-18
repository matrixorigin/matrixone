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
	"encoding/binary"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Match the two bulk SQL workloads in BenchmarkOrderedODKU, isolating the
// action owner from storage, network and cluster background work. Include
// Prepare/Call/Reset so per-statement admission and cleanup remain measured.
func BenchmarkODKUBulkActions(b *testing.B) {
	for _, noop := range []bool{true, false} {
		name, width := "noop_8192", 5
		if !noop {
			name, width = "varlen512_insert_8192", 512
		}
		b.Run(name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithMPool(b, "", mp)
			arg, child := &PreInsertUnique{}, colexec.NewMockOperator()
			arg.AppendChild(child)
			registry, err := mpool.NewAllocationAccountRegistry(1, 4096)
			require.NoError(b, err)
			account, err := registry.Open(64 << 20)
			require.NoError(b, err)
			require.NoError(b, arg.SetAllocationAccount(account))
			input := batch.NewWithSize(15)
			b.Cleanup(func() {
				arg.Reset(proc, false, nil)
				arg.Free(proc, false, nil)
				child.ResetBatchs() // The benchmark owns the reusable input.
				child.Free(proc, false, nil)
				input.Clean(mp)
				proc.Free()
				require.Zero(b, account.Snapshot().Used)
				require.Zero(b, mp.CurrNB())
				require.NoError(b, arg.ClearAllocationAccount(account))
			})
			ref := func(rel, pos int32, typ types.T) *plan.Expr {
				return &plan.Expr{Typ: plan.Type{Id: int32(typ)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rel, ColPos: pos}}}
			}
			c := &plan.OdkuActionCtx{
				InputColumns: []int32{0, 1, 2, 3, 4, -1}, PkColumn: 0, RowIdColumn: 5, ActionFinalColumn: -1,
				SourceOrdinal: ref(0, 5, types.T_int64), ChangeCheckAssignmentCount: 2,
				ChangeCheckColumns: []int32{3, 4}, UpdateColumns: []int32{3, 4},
				UpdateExprs: []*plan.Expr{ref(1, 3, types.T_int64), ref(1, 4, types.T_varchar)},
			}
			for i := range 6 {
				typ := types.T_int64
				if i == 4 {
					typ = types.T_varchar
				}
				c.SourceColumns = append(c.SourceColumns, ref(0, int32(i), typ))
				if i == 5 {
					typ = types.T_Rowid
				}
				c.OldColumns = append(c.OldColumns, ref(0, int32(i+9), typ))
			}
			for i, name := range []string{"id", "u1", "u2"} {
				c.Constraints = append(c.Constraints, &plan.OdkuConstraint{
					Name: name, Types: []plan.Type{{Id: int32(types.T_int64)}}, TargetEligible: true,
					InputKey: ref(0, int32(i), types.T_int64), PriorTarget: ref(0, int32(i+6), types.T_int64), CurrentKey: ref(0, int32(i), types.T_int64),
				})
			}
			arg.PreInsertCtx = &plan.PreInsertUkCtx{OrderedOdku: true, OdkuActions: c}
			for i := range input.Vecs {
				typ := types.T_int64
				if i == 4 || i == 13 {
					typ = types.T_varchar
				} else if i == 14 {
					typ = types.T_Rowid
				}
				input.Vecs[i] = vector.NewVec(types.New(typ, 0, 0))
			}
			payload := []byte(strings.Repeat("x", width))
			for row := int64(1); row <= 8192; row++ {
				values := []int64{1000000 + row, row * 10, row * 100, 1, 0, row, 0, row, row, row, row * 10, row * 100, 1}
				for i, n := range values {
					if i != 4 {
						require.NoError(b, vector.AppendFixed(input.Vecs[i], n, i == 6 || (i >= 7 && !noop), mp))
					}
				}
				require.NoError(b, vector.AppendBytes(input.Vecs[4], payload, false, mp))
				require.NoError(b, vector.AppendBytes(input.Vecs[13], payload, !noop, mp))
				var id types.Rowid
				binary.LittleEndian.PutUint64(id[:], uint64(row))
				require.NoError(b, vector.AppendFixed(input.Vecs[14], id, !noop, mp))
			}
			input.SetRowCount(8192)
			var affected uint64
			arg.PublishAffectedRows = func(n uint64) error { affected = n; return nil }
			b.ReportAllocs()
			for b.Loop() {
				child.WithBatchs([]*batch.Batch{input})
				require.NoError(b, arg.Prepare(proc))
				output := 0
				for {
					out, err := arg.Call(proc)
					require.NoError(b, err)
					if out.Batch == nil {
						break
					}
					output += out.Batch.RowCount()
				}
				want := 8192
				if noop {
					want = 0
				}
				require.Equal(b, want, output)
				require.Equal(b, uint64(want), affected)
				arg.Reset(proc, false, nil)
				child.ResetBatchs()
				child.Free(proc, false, nil)
			}
		})
	}
}

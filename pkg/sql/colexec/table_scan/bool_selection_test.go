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

package table_scan

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestBooleanFilterSparseNullBitmap(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(fmt.Sprintf("late=%t", late), func(t *testing.T) {
			proc := testutil.NewProc(t)
			t.Cleanup(func() { mpool.DeleteMPool(proc.Mp()) })
			t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
			registry, err := mpool.NewAllocationAccountRegistry(1, 32)
			require.NoError(t, err)
			account, err := registry.Open(1 << 20)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.Zero(t, account.Snapshot().Used)
				_, _, err := registry.CompleteTerminal(account)
				require.NoError(t, err)
			})
			allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerMin, 1, 2, 3, 4)
			require.NoError(t, err)
			boolType, intType := types.T_bool.ToType(), types.T_int32.ToType()
			scan := &TableScan{
				Attrs:       []string{"mask", "payload"},
				Types:       []pb.Type{plan.MakePlan2Type(&boolType), plan.MakePlan2Type(&intType)},
				FilterExprs: []*pb.Expr{lateTestCol(0, boolType)},
			}
			t.Cleanup(func() { scan.Free(proc, false, nil) })
			require.NoError(t, scan.Prepare(proc))
			input := batch.NewWithSize(2)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			input.Vecs[0] = vector.NewVec(boolType)
			input.Vecs[1] = vector.NewOffHeapVecWithType(intType)
			require.NoError(t, input.Vecs[1].SetAllocationAccount(allocation))
			for row := range 66 {
				require.NoError(t, vector.AppendFixed(input.Vecs[0], row != 0, false, proc.Mp()))
				require.NoError(t, vector.AppendFixed(input.Vecs[1], int32(row), row == 2, proc.Mp()))
			}
			input.SetRowCount(66)
			require.Equal(t, 1, input.Vecs[1].GetNulls().GetBitmap().ExternalStorageCapacity())
			before := account.Snapshot()
			var loaded []int
			if late {
				loaded = []int{0, 1}
			}
			result, err := scan.evalFilter(proc, input, loaded)
			require.NoError(t, err)
			require.Equal(t, 65, input.RowCount())
			require.Equal(t, []uint64{1}, input.Vecs[1].GetNulls().ToArray())
			for row, value := range vector.MustFixedColNoTypeCheck[int32](input.Vecs[1]) {
				if row != 1 {
					require.Equal(t, int32(row+1), value)
				}
				if late {
					require.Equal(t, int64(row+1), result.Sels[row])
				}
			}
			require.Equal(t, before.Used, account.Snapshot().Used)
			require.Equal(t, before.Peak, account.Snapshot().Peak)
		})
	}
}

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

package hashbuild

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestHashBuildRetainedVarlenAllocationBoundary(t *testing.T) {
	// Exercise the same per-allocation limit as the allocator, with a query
	// budget that independently admits all three fixtures.
	const allocationLimit = 1 << 20
	previousLimit := mpool.CapLimit
	mpool.CapLimit = allocationLimit
	t.Cleanup(func() { mpool.CapLimit = previousLimit })

	for _, tc := range []struct {
		name                                 string
		rowCount, payloadColumns, valueBytes int
		rowsPerInput                         int
		queryBudget                          uint64
		budgetFailure                        bool
	}{
		{name: "single-column-below-limit", rowCount: 11, payloadColumns: 1, valueBytes: 92 << 10, rowsPerInput: 6, queryBudget: 16 << 20},
		{name: "multiple-columns-over-limit-in-total", rowCount: 12, payloadColumns: 3, valueBytes: 48 << 10, rowsPerInput: 6, queryBudget: 16 << 20},
		{name: "single-column-over-limit", rowCount: 12, payloadColumns: 1, valueBytes: 92 << 10, rowsPerInput: 6, queryBudget: 16 << 20},
		{name: "next-physical-batch-rejected-by-query-budget", rowCount: 12, payloadColumns: 1, valueBytes: 92 << 10, rowsPerInput: 11, queryBudget: 1075 << 10, budgetFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keyType, payloadType := types.T_int64.ToType(), types.T_text.ToType()
			columnTypes := make([]types.Type, 2, 2+tc.payloadColumns)
			columnTypes[0], columnTypes[1] = keyType, keyType
			for range tc.payloadColumns {
				columnTypes = append(columnTypes, payloadType)
			}
			fixture := newTestCase(t, columnTypes, []*plan.Expr{newExpr(0, keyType)})
			fixture.arg.NeedBatches = true
			fixture.arg.NeedAllocateSels = true
			proc := fixture.proc
			mp := proc.Mp()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })

			ledger := process.MustNewExecutionResourceBudget(tc.queryBudget, tc.queryBudget)
			generation, err := ledger.OpenGeneration(1)
			require.NoError(t, err)
			t.Cleanup(generation.Close)
			require.NoError(t, proc.SetExecutionResourceBudgetForTesting(generation))
			installTestExecutionResourceBudget(t, fixture.arg, generation)
			t.Cleanup(func() { require.Zero(t, generation.Used()) })

			child := colexec.NewMockOperator()
			t.Cleanup(func() { child.Free(proc, false, nil) })
			for start := 0; start < tc.rowCount; start += tc.rowsPerInput {
				end := min(start+tc.rowsPerInput, tc.rowCount)
				input := batch.NewWithSize(len(columnTypes))
				child.WithBatchs([]*batch.Batch{input})
				for col, typ := range columnTypes {
					input.Vecs[col] = vector.NewVec(typ)
				}
				for row := start; row < end; row++ {
					require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(1), false, mp))
					require.NoError(t, vector.AppendFixed(input.Vecs[1], int64(row), false, mp))
					for col := range tc.payloadColumns {
						payload := bytes.Repeat([]byte{byte(row + col + 1)}, tc.valueBytes)
						require.NoError(t, vector.AppendBytes(input.Vecs[col+2], payload, false, mp))
					}
				}
				input.SetRowCount(end - start)
			}

			fixture.arg.SetChildren([]vm.Operator{child})
			require.NoError(t, child.Prepare(proc))
			var buildErr error
			t.Cleanup(func() { fixture.arg.Free(proc, buildErr != nil, buildErr) })
			require.NoError(t, fixture.arg.Prepare(proc))
			result, buildErr := vm.Exec(fixture.arg, proc)
			if tc.budgetFailure {
				require.True(t, moerr.IsMoErrCode(buildErr, moerr.ErrOOM), "%v", buildErr)
				retained := &fixture.arg.ctr.hashmapBuilder.Batches
				require.Len(t, retained.Buf, 1)
				require.Equal(t, tc.rowsPerInput, retained.RowCount())
				require.Zero(t, retained.MemSize)
				terminal, err := message.ReceiveJoinMapResult(fixture.arg.JoinMapTag, false, 0, proc.GetMessageBoard(), proc.Ctx)
				require.NoError(t, err)
				require.True(t, terminal.IsBuildError())
				require.Equal(t, buildErr.Error(), terminal.Err().Error())
				fixture.arg.Reset(proc, true, buildErr)
				require.Zero(t, generation.Used())
				return
			}
			require.NoError(t, buildErr)
			require.Equal(t, vm.ExecStop, result.Status)

			received, err := message.ReceiveJoinMapResult(fixture.arg.JoinMapTag, false, 0, proc.GetMessageBoard(), proc.Ctx)
			require.NoError(t, err)
			require.True(t, received.IsSuccess())
			joinMap := received.JoinMap()
			require.NotNil(t, joinMap)
			t.Cleanup(joinMap.Free)
			require.Equal(t, int64(tc.rowCount), joinMap.GetRowCount())
			require.Equal(t, uint64(1), joinMap.GetGroupCount())

			seen := make([]bool, tc.rowCount)
			for _, retained := range joinMap.GetBatches() {
				keys := vector.MustFixedColNoTypeCheck[int64](retained.Vecs[0])
				rowIDs := vector.MustFixedColNoTypeCheck[int64](retained.Vecs[1])
				for i, row := range rowIDs {
					require.Equal(t, int64(1), keys[i])
					require.GreaterOrEqual(t, row, int64(0))
					require.Less(t, row, int64(tc.rowCount))
					require.False(t, seen[row], "duplicate build row %d", row)
					seen[row] = true
					for col := range tc.payloadColumns {
						payload := retained.Vecs[col+2].GetBytesAt(i)
						require.Len(t, payload, tc.valueBytes)
						require.Equal(t, tc.valueBytes, bytes.Count(payload, []byte{byte(row + int64(col) + 1)}))
					}
				}
			}
			for row, ok := range seen {
				require.True(t, ok, "missing build row %d", row)
			}
		})
	}
}

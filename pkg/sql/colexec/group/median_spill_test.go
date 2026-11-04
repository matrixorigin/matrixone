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

package group

import (
	"bytes"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestGroupMedianAcrossChunksWithAndWithoutSpill(t *testing.T) {
	for _, tc := range []struct {
		name     string
		spillMem int64
		spill    bool
	}{
		{"resident", 1000000000, false},
		{"spill", 9000, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{
				aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMedian, false, []*plan.Expr{colExpr(0, types.T_int32)}, nil),
			})
			t.Cleanup(func() { g.Free(proc, false, nil); proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
			const rows = 16385
			values := make([]int32, rows)
			for i := range values {
				values[i] = int32(i + 1)
			}
			input := batch.NewWithSize(1)
			input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
			input.SetRowCount(rows)
			g.SpillMem = tc.spillMem
			g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
			require.NoError(t, g.Prepare(proc))
			seen := make([]bool, rows)
			count := 0
			for {
				result, err := vm.Exec(g, proc)
				require.NoError(t, err)
				if result.Status == vm.ExecStop || result.Batch == nil {
					break
				}
				require.Len(t, result.Batch.Vecs, 2)
				keys := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])
				medians := vector.MustFixedColNoTypeCheck[float64](result.Batch.Vecs[1])
				for i, key := range keys {
					if key < 1 || key > rows || seen[key-1] || medians[i] != float64(key) {
						t.Fatalf("incorrect result at row %d: key=%d median=%v", i, key, medians[i])
					}
					seen[key-1] = true
					count++
				}
			}
			require.Equal(t, rows, count)
			stats := g.OpAnalyzer.GetOpStats().ExtraStats
			if tc.spill {
				require.Positive(t, stats["GroupSpillRecords"])
				require.Positive(t, stats["GroupSpillReloadRecords"])
				require.GreaterOrEqual(t, stats["GroupSpillMaxGroups"], int64(8193))
			} else {
				require.Zero(t, stats["GroupSpillRecords"])
			}
		})
	}
}

type failingChunkSerializer struct {
	aggexec.AggFuncExec
	err error
}

func (a *failingChunkSerializer) SaveIntermediateResultOfChunk(_ int, buf *bytes.Buffer) error {
	if a.err != nil {
		_, _ = buf.WriteString("partial")
		return a.err
	}
	_, err := buf.WriteString("state")
	return err
}

func TestGroupIntermediateSerializationFailureIsNotPublished(t *testing.T) {
	want := errors.New("serialize failed")
	serializer := &failingChunkSerializer{err: want}
	g := NewArgument()
	g.ctr.groupByBatches = []*batch.Batch{batch.NewWithSize(0)}
	g.ctr.aggList = []aggexec.AggFuncExec{serializer}
	_, _, err := g.getNextIntermediateResult(nil)
	require.ErrorIs(t, err, want)
	require.Zero(t, g.ctr.currBatchIdx)
	require.Empty(t, g.ctr.groupByBatches[0].ExtraBuf)
	serializer.err = nil
	result, more, err := g.getNextIntermediateResult(nil)
	require.NoError(t, err)
	require.False(t, more)
	require.Equal(t, 1, g.ctr.currBatchIdx)
	require.Same(t, g.ctr.groupByBatches[0], result.Batch)
	require.Contains(t, string(result.Batch.ExtraBuf), "state")
}

func TestGroupStreamingDoesNotEmitEmptyTrailingChunk(t *testing.T) {
	proc := testutil.NewProcess(t)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMedian, false, []*plan.Expr{colExpr(0, types.T_int32)}, nil),
	})
	g.NeedEval = false
	g.SpillMem = 1000000000
	t.Cleanup(func() { g.Free(proc, false, nil); proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	values := make([]int32, 8192)
	for i := range values {
		values[i] = int32(i + 1)
	}
	first := batch.NewWithSize(1)
	first.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	first.SetRowCount(len(values))
	duplicate := batch.NewWithSize(1)
	duplicate.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	duplicate.SetRowCount(1)
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, duplicate}))
	require.NoError(t, g.Prepare(proc))
	count := 0
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		require.Positive(t, result.Batch.RowCount())
		count += result.Batch.RowCount()
	}
	require.Equal(t, len(values), count)
}

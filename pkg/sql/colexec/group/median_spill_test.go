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

func TestGroupMedianPartialMergeAcrossChunks(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	const count = 8193
	aggs := []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMedian, false, []*plan.Expr{colExpr(1, types.T_int32)}, nil),
	}
	var partials []*batch.Batch
	for pass := 0; pass < 2; pass++ {
		func() {
			groupCount := count - pass
			keys, values := make([]int32, groupCount), make([]int32, groupCount)
			var nulls []uint64
			for i := range keys {
				keys[i] = int32(i + 1)
				values[i] = keys[i] + int32(pass*2)
				if keys[i]%17 == 0 {
					nulls = append(nulls, uint64(i))
				}
			}
			input := batch.NewWithSize(2)
			input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
			input.Vecs[1] = testutil.MakeInt32Vector(values, nulls, proc.Mp())
			input.SetRowCount(groupCount)
			inputs := []*batch.Batch{input}
			if pass == 1 {
				// A duplicate after a full chunk must not create an empty batch.
				duplicate := batch.NewWithSize(2)
				duplicate.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
				duplicate.Vecs[1] = testutil.MakeInt32Vector([]int32{2}, nil, proc.Mp())
				duplicate.SetRowCount(1)
				inputs = append(inputs, duplicate)
			}
			child := colexec.NewMockOperator().WithBatchs(inputs)
			defer child.Free(proc, false, nil)
			partial := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, aggs)
			partial.NeedEval = false
			partial.SpillMem = 1000000000
			partial.AppendChild(child)
			defer partial.Free(proc, false, nil)
			require.NoError(t, partial.Prepare(proc))
			batches := collectBatches(t, partial, proc)
			require.Len(t, batches, 2-pass)
			for _, bat := range batches {
				require.Positive(t, bat.RowCount())
				copied := cloneBatch(t, proc, bat)
				t.Cleanup(func() { copied.Clean(proc.Mp()) })
				partials = append(partials, copied)
			}
		}()
	}
	require.Len(t, partials, 3)

	merge := newMergeGroupOp(aggs)
	merge.SpillMem = 1000000000
	merge.AppendChild(colexec.NewMockOperator().WithBatchs(partials))
	t.Cleanup(func() { merge.Free(proc, false, nil) })
	require.NoError(t, merge.Prepare(proc))
	seen := make([]bool, count)
	rows := 0
	for _, result := range collectBatches(t, merge, proc) {
		keys := vector.MustFixedColNoTypeCheck[int32](result.Vecs[0])
		values := vector.MustFixedColNoTypeCheck[float64](result.Vecs[1])
		for i, key := range keys {
			if key < 1 || key > count || seen[key-1] {
				t.Fatalf("invalid or duplicate key %d", key)
			}
			seen[key-1] = true
			if key%17 == 0 {
				if !result.Vecs[1].IsNull(uint64(i)) {
					t.Fatalf("key %d: expected NULL", key)
				}
			} else {
				want := float64(key + 1)
				if key == count {
					want = float64(key)
				}
				if result.Vecs[1].IsNull(uint64(i)) || values[i] != want {
					t.Fatalf("key %d: got %v, null %v; want %v", key, values[i], result.Vecs[1].IsNull(uint64(i)), want)
				}
			}
			rows++
		}
	}
	require.Equal(t, count, rows)
}

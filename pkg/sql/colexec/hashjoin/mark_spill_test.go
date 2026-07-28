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

package hashjoin

import (
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

type markResult struct {
	value  bool
	isNull bool
}

func newMarkSpillTestCase(t *testing.T) joinTestCase {
	tc := newTestCase(t,
		[]bool{true},
		[]types.Type{types.T_int32.ToType()},
		[]colexec.ResultPos{colexec.NewResultPos(0, 0)},
		[][]*plan.Expr{
			{newExpr(0, types.T_int32.ToType())},
			{newExpr(0, types.T_int32.ToType())},
		})
	tc.arg.JoinType = plan.Node_MARK
	tc.arg.NonEqCond = nil
	tc.arg.ResultCols = []colexec.ResultPos{
		colexec.NewResultPos(0, 0),
		colexec.NewResultPos(-1, 0),
	}
	tc.arg.IsShuffle = true
	tc.arg.ShuffleIdx = 0
	tc.barg.NeedAllocateSels = false
	tc.barg.NeedBatches = false
	tc.barg.TrackNullKeys = true
	tc.barg.IsShuffle = true
	tc.barg.ShuffleIdx = 0
	tc.barg.SpillThreshold = 1
	tc.barg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: tc.arg.JoinMapTag + 10000}
	return tc
}

func collectMarkResults(t *testing.T, tc *joinTestCase) map[int32]markResult {
	t.Helper()
	got := make(map[int32]markResult)
	for {
		res, err := vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		if res.Batch != nil && !res.Batch.IsEmpty() {
			keys := vector.GenerateFunctionFixedTypeParameter[int32](res.Batch.Vecs[0])
			marks := vector.GenerateFunctionFixedTypeParameter[bool](res.Batch.Vecs[1])
			for row := 0; row < res.Batch.RowCount(); row++ {
				key, _ := keys.GetValue(uint64(row))
				value, isNull := marks.GetValue(uint64(row))
				_, duplicated := got[key]
				require.False(t, duplicated, "probe row %d was emitted more than once", key)
				got[key] = markResult{value: value, isNull: isNull}
			}
		}
		if res.Status == vm.ExecStop {
			return got
		}
	}
}

func finishMarkSpillTest(t *testing.T, tc *joinTestCase) {
	t.Helper()
	tc.arg.Reset(tc.proc, false, nil)
	tc.barg.Reset(tc.proc, false, nil)
	tc.arg.Free(tc.proc, false, nil)
	tc.barg.Free(tc.proc, false, nil)
	tc.proc.Free()
	require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
}

func TestHashMarkJoinSpillThreeValuedSemantics(t *testing.T) {
	tests := []struct {
		name           string
		buildValues    []int32
		buildNulls     []uint64
		spillThreshold int64
		expected       map[int32]markResult
	}{
		{
			name:           "global build null survives partitioning",
			buildValues:    []int32{2, 0},
			buildNulls:     []uint64{1},
			spillThreshold: 1 << 30,
			expected: map[int32]markResult{
				0: {isNull: true},
				1: {isNull: true},
				2: {value: true},
				3: {isNull: true},
			},
		},
		{
			name:           "recursive spill preserves false and unknown",
			buildValues:    []int32{2, 4},
			spillThreshold: 1,
			expected: map[int32]markResult{
				0: {isNull: true},
				1: {value: false},
				2: {value: true},
				3: {value: false},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := newMarkSpillTestCase(t)
			tc.arg.SpillThreshold = tt.spillThreshold

			probe := batch.NewWithSize(1)
			probe.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 0}, []uint64{3}, tc.proc.Mp())
			probe.SetRowCount(4)
			resetChildrenWithBatch(tc.arg, probe)

			build := batch.NewWithSize(1)
			build.Vecs[0] = testutil.MakeInt32Vector(tt.buildValues, tt.buildNulls, tc.proc.Mp())
			build.SetRowCount(len(tt.buildValues))
			resetHashBuildChildrenWithBatch(tc.barg, build)

			require.NoError(t, tc.arg.Prepare(tc.proc))
			require.NoError(t, tc.barg.Prepare(tc.proc))
			res, err := vm.Exec(tc.barg, tc.proc)
			require.NoError(t, err)
			require.Equal(t, vm.ExecStop, res.Status)

			require.Equal(t, tt.expected, collectMarkResults(t, &tc))
			finishMarkSpillTest(t, &tc)
		})
	}
}

func TestHashMarkJoinEmptySpillBucketTruthTable(t *testing.T) {
	tests := []struct {
		name              string
		globalBuildRowCnt int64
		buildHasNullKey   bool
		expected          []markResult
	}{
		{
			name:              "globally empty build",
			globalBuildRowCnt: 0,
			expected:          []markResult{{value: false}, {value: false}},
		},
		{
			name:              "nonempty build without null",
			globalBuildRowCnt: 1,
			expected:          []markResult{{value: false}, {isNull: true}},
		},
		{
			name:              "nonempty build with null",
			globalBuildRowCnt: 1,
			buildHasNullKey:   true,
			expected:          []markResult{{isNull: true}, {isNull: true}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := newMarkSpillTestCase(t)
			require.NoError(t, tc.arg.Prepare(tc.proc))

			probe := batch.NewWithSize(1)
			probe.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 0}, []uint64{1}, tc.proc.Mp())
			probe.SetRowCount(2)
			tc.arg.ctr.leftBat = probe
			tc.arg.ctr.globalBuildRowCnt = tt.globalBuildRowCnt
			tc.arg.ctr.buildHasNullKey = tt.buildHasNullKey
			tc.arg.resetResultBat()

			var result vm.CallResult
			require.NoError(t, tc.arg.ctr.emptyProbe(tc.arg, tc.proc, &result))
			require.NotNil(t, result.Batch)
			marks := vector.GenerateFunctionFixedTypeParameter[bool](result.Batch.Vecs[1])
			for row, expected := range tt.expected {
				value, isNull := marks.GetValue(uint64(row))
				require.Equal(t, expected.isNull, isNull, "row %d null state", row)
				if !isNull {
					require.Equal(t, expected.value, value, "row %d value", row)
				}
			}

			probe.Clean(tc.proc.Mp())
			tc.arg.Reset(tc.proc, false, nil)
			tc.arg.Free(tc.proc, false, nil)
			tc.barg.Free(tc.proc, false, nil)
			tc.proc.Free()
			require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
		})
	}
}

// TestHashMarkJoinSpilledEmptyBuild verifies the complete operator path, not
// only emptyProbe: probe rows assigned to empty build buckets must survive the
// spill scatter phase, and an empty global build makes even NULL probes FALSE.
func TestHashMarkJoinSpilledEmptyBuild(t *testing.T) {
	tc := newMarkSpillTestCase(t)
	probe := batch.NewWithSize(1)
	probe.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 0}, []uint64{1}, tc.proc.Mp())
	probe.SetRowCount(2)
	resetChildrenWithBatch(tc.arg, probe)

	jm := message.NewJoinMap(message.GroupSels{}, nil, nil, nil, nil, tc.proc.Mp())
	jm.Spilled = true
	jm.SpillBuildFds = make([]*os.File, spillutil.SpillNumBuckets)
	jm.SetRowCount(0)
	jm.IncRef(1)
	message.SendMessage(message.JoinMapMsg{
		JoinMapPtr: jm,
		IsShuffle:  true,
		ShuffleIdx: tc.arg.ShuffleIdx,
		Tag:        tc.arg.JoinMapTag,
		Spilled:    true,
	}, tc.proc.GetMessageBoard())

	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.Equal(t, map[int32]markResult{
		0: {value: false},
		1: {value: false},
	}, collectMarkResults(t, &tc))
	finishMarkSpillTest(t, &tc)
}

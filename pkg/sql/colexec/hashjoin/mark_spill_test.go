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
	"strings"
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
	return newTypedMarkSpillTestCase(t, types.T_int32.ToType())
}

func newTypedMarkSpillTestCase(t *testing.T, typ types.Type) joinTestCase {
	tc := newTestCase(t,
		[]bool{true},
		[]types.Type{typ},
		[]colexec.ResultPos{colexec.NewResultPos(0, 0)},
		[][]*plan.Expr{
			{newExpr(0, typ)},
			{newExpr(0, typ)},
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

func collectStringMarkResults(t *testing.T, tc *joinTestCase) map[string]markResult {
	t.Helper()
	got := make(map[string]markResult)
	for {
		res, err := vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		if res.Batch != nil && !res.Batch.IsEmpty() {
			keys := res.Batch.Vecs[0]
			marks := vector.GenerateFunctionFixedTypeParameter[bool](res.Batch.Vecs[1])
			for row := 0; row < res.Batch.RowCount(); row++ {
				key := keys.GetStringAt(row)
				if keys.GetNulls().Contains(uint64(row)) {
					key = "<NULL>"
				}
				value, isNull := marks.GetValue(uint64(row))
				_, duplicated := got[key]
				require.False(t, duplicated, "probe row %q was emitted more than once", key)
				got[key] = markResult{value: value, isNull: isNull}
			}
		}
		if res.Status == vm.ExecStop {
			return got
		}
	}
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

// TestHashMarkJoinSpillResetAcrossBuildShapes exercises the lifecycle that a
// prepared/cached pipeline uses: the same HashBuild and HashJoin operators are
// prepared, executed, reset, and reused with materially different build-side
// shapes. In particular, a spilled build containing NULL must not leak either
// its global NULL flag, global row count, spill engine, or message-board state
// into the following empty and non-NULL generations.
func TestHashMarkJoinSpillResetAcrossBuildShapes(t *testing.T) {
	tc := newMarkSpillTestCase(t)
	defer finishMarkSpillTest(t, &tc)
	tc.arg.SpillThreshold = 1

	type generation struct {
		name        string
		probeValues []int32
		probeNulls  []uint64
		buildValues []int32
		buildNulls  []uint64
		expected    map[int32]markResult
		expectSpill bool
	}

	generations := []generation{
		{
			name:        "spilled build with null",
			probeValues: []int32{1, 2, 0},
			probeNulls:  []uint64{2},
			buildValues: []int32{2, 0},
			buildNulls:  []uint64{1},
			expected: map[int32]markResult{
				0: {isNull: true},
				1: {isNull: true},
				2: {value: true},
			},
			expectSpill: true,
		},
		{
			name:        "empty build after null spill",
			probeValues: []int32{1, 0},
			probeNulls:  []uint64{1},
			expected: map[int32]markResult{
				0: {value: false},
				1: {value: false},
			},
		},
		{
			name:        "non-null spill after empty build",
			probeValues: []int32{1, 2, 0},
			probeNulls:  []uint64{2},
			buildValues: []int32{2, 4},
			expected: map[int32]markResult{
				0: {isNull: true},
				1: {value: false},
				2: {value: true},
			},
			expectSpill: true,
		},
	}

	for i, gen := range generations {
		ok := t.Run(gen.name, func(t *testing.T) {
			probe := batch.NewWithSize(1)
			probe.Vecs[0] = testutil.MakeInt32Vector(gen.probeValues, gen.probeNulls, tc.proc.Mp())
			probe.SetRowCount(len(gen.probeValues))
			resetChildrenWithBatch(tc.arg, probe)

			build := batch.NewWithSize(1)
			build.Vecs[0] = testutil.MakeInt32Vector(gen.buildValues, gen.buildNulls, tc.proc.Mp())
			build.SetRowCount(len(gen.buildValues))
			resetHashBuildChildrenWithBatch(tc.barg, build)

			require.NoError(t, tc.arg.Prepare(tc.proc))
			require.NoError(t, tc.barg.Prepare(tc.proc))
			res, err := vm.Exec(tc.barg, tc.proc)
			require.NoError(t, err)
			require.Equal(t, vm.ExecStop, res.Status)

			require.Equal(t, gen.expected, collectMarkResults(t, &tc))
			if gen.expectSpill {
				require.NotNil(t, tc.arg.ctr.spillEngine, "generation must traverse the spill path")
			} else {
				require.Nil(t, tc.arg.ctr.spillEngine)
			}

			if i+1 < len(generations) {
				tc.arg.Reset(tc.proc, false, nil)
				tc.barg.Reset(tc.proc, false, nil)
				require.False(t, tc.arg.ctr.buildHasNullKey)
				require.Zero(t, tc.arg.ctr.globalBuildRowCnt)
				require.Nil(t, tc.arg.ctr.spillEngine)
				tc.proc.SetMessageBoard(tc.proc.GetMessageBoard().Reset())
				tc.proc.GetMessageBoard().BeforeRunonce()
			}
		})
		if !ok {
			return
		}
	}
}

// TestHashMarkJoinVarcharDuplicatesAcrossSpillReset covers the varlen hashmap
// and spill ownership path. Duplicate build keys must not duplicate probe rows,
// and neither their area-backed bytes nor the global NULL fact may survive a
// prepared/cached operator reset into a different build shape.
func TestHashMarkJoinVarcharDuplicatesAcrossSpillReset(t *testing.T) {
	tc := newTypedMarkSpillTestCase(t, types.T_varchar.ToType())
	defer finishMarkSpillTest(t, &tc)
	tc.arg.SpillThreshold = 1
	longKey := strings.Repeat("long-key-", 64)

	type generation struct {
		name        string
		probeValues []string
		probeNulls  []uint64
		buildValues []string
		buildNulls  []uint64
		expected    map[string]markResult
		expectSpill bool
	}
	generations := []generation{
		{
			name:        "duplicate inline and area keys with null",
			probeValues: []string{"dup", longKey, "miss", ""},
			probeNulls:  []uint64{3},
			buildValues: []string{"dup", "dup", longKey, longKey, ""},
			buildNulls:  []uint64{4},
			expected: map[string]markResult{
				"dup":    {value: true},
				longKey:  {value: true},
				"miss":   {isNull: true},
				"<NULL>": {isNull: true},
			},
			expectSpill: true,
		},
		{
			name:        "empty build after varlen spill",
			probeValues: []string{"dup", ""},
			probeNulls:  []uint64{1},
			expected: map[string]markResult{
				"dup":    {value: false},
				"<NULL>": {value: false},
			},
		},
		{
			name:        "non-null duplicates after empty build",
			probeValues: []string{"dup", "miss", ""},
			probeNulls:  []uint64{2},
			buildValues: []string{"dup", "dup", longKey},
			expected: map[string]markResult{
				"dup":    {value: true},
				"miss":   {value: false},
				"<NULL>": {isNull: true},
			},
			expectSpill: true,
		},
	}

	for i, gen := range generations {
		if !t.Run(gen.name, func(t *testing.T) {
			probe := batch.NewWithSize(1)
			probe.Vecs[0] = testutil.MakeVarcharVector(gen.probeValues, gen.probeNulls, tc.proc.Mp())
			probe.SetRowCount(len(gen.probeValues))
			resetChildrenWithBatch(tc.arg, probe)

			build := batch.NewWithSize(1)
			build.Vecs[0] = testutil.MakeVarcharVector(gen.buildValues, gen.buildNulls, tc.proc.Mp())
			build.SetRowCount(len(gen.buildValues))
			resetHashBuildChildrenWithBatch(tc.barg, build)

			require.NoError(t, tc.arg.Prepare(tc.proc))
			require.NoError(t, tc.barg.Prepare(tc.proc))
			res, err := vm.Exec(tc.barg, tc.proc)
			require.NoError(t, err)
			require.Equal(t, vm.ExecStop, res.Status)
			require.Equal(t, gen.expected, collectStringMarkResults(t, &tc))
			if gen.expectSpill {
				require.NotNil(t, tc.arg.ctr.spillEngine)
			} else {
				require.Nil(t, tc.arg.ctr.spillEngine)
			}

			if i+1 < len(generations) {
				tc.arg.Reset(tc.proc, false, nil)
				tc.barg.Reset(tc.proc, false, nil)
				require.False(t, tc.arg.ctr.buildHasNullKey)
				require.Zero(t, tc.arg.ctr.globalBuildRowCnt)
				require.Nil(t, tc.arg.ctr.spillEngine)
				tc.proc.SetMessageBoard(tc.proc.GetMessageBoard().Reset())
				tc.proc.GetMessageBoard().BeforeRunonce()
			}
		}) {
			return
		}
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
			require.NoError(t, tc.arg.resetResultBat())

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
	generation, registry, account := installHashJoinTestAllocation(t, tc.arg)
	probe := batch.NewWithSize(1)
	probe.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 0}, []uint64{1}, tc.proc.Mp())
	probe.SetRowCount(2)
	resetChildrenWithBatch(tc.arg, probe)

	jm := message.NewJoinMap(message.GroupSels{}, nil, nil, nil, nil, tc.proc.Mp())
	jm.SetRowCount(0)
	jm.IncRef(1)
	require.NoError(t, jm.SetSpillBuildPayload(message.SpillBuildPayload{
		Files:     make([]*message.SpillFile, spillutil.SpillNumBuckets),
		BudgetRef: generation,
	}))
	message.SendMessage(message.JoinMapMsg{
		Result:     message.NewJoinMapResult(jm),
		IsShuffle:  true,
		ShuffleIdx: tc.arg.ShuffleIdx,
		Tag:        tc.arg.JoinMapTag,
	}, tc.proc.GetMessageBoard())

	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.Equal(t, map[int32]markResult{
		0: {value: false},
		1: {value: false},
	}, collectMarkResults(t, &tc))
	finishMarkSpillTest(t, &tc)
	require.Zero(t, account.Snapshot().Used)
	_, _, err := registry.CompleteTerminal(account)
	require.NoError(t, err)
}

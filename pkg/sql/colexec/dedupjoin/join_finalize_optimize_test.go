// Copyright 2021 Matrix Origin
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

package dedupjoin

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// runFinalizeFixture wires a HashBuild + DedupJoin pair, runs the build
// phase, then drives the dedupjoin to completion by calling Exec until it
// returns nil. Result batches are collected (caller-owned via the returned
// slice; do NOT Free them here — they live inside dedupArg.ctr.buf).
func runFinalizeFixture(
	t *testing.T,
	dedupArg *DedupJoin,
	buildArg *hashbuild.HashBuild,
	proc *process.Process,
	buildBat, probeBat *batch.Batch,
) []*batch.Batch {
	t.Helper()

	buildArg.Children = nil
	buildArg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{buildBat}))
	dedupArg.Children = nil
	dedupArg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probeBat}))

	require.NoError(t, buildArg.Prepare(proc))
	require.NoError(t, dedupArg.Prepare(proc))

	res, err := vm.Exec(buildArg, proc)
	require.NoError(t, err)
	require.True(t, res.Batch == nil, "build phase should drain to nil")

	var out []*batch.Batch
	for {
		res, err = vm.Exec(dedupArg, proc)
		require.NoError(t, err)
		if res.Batch == nil {
			break
		}
		if res.Batch.RowCount() > 0 {
			out = append(out, res.Batch)
		}
	}
	return out
}

// TestDedupJoinFinalizeMatchedZero_NonCapture exercises the non-capture
// fast path at finalize() lines 312-360: matched.Count()==0 and no capture
// columns, so each Result column with rp.Rel==1 is transferred from the
// build batch (bat.Vecs[rp.Pos] = nil). Verifies output equals build content
// row-for-row, and Reset/Free do not panic on the nilled-out slots.
func TestDedupJoinFinalizeMatchedZero_NonCapture(t *testing.T) {
	proc, ctrl := newCaptureTestProc(t)
	defer ctrl.Finish()

	int32Typ := types.T_int32.ToType()
	tag++
	curTag := tag

	// Build keys [10,20,30] with payload col [100,200,300].
	buildBat := makeInt32Batch(proc.Mp(), [][]int32{{10, 20, 30}, {100, 200, 300}}, nil)
	// Probe key [99] misses every build bucket → matched stays at 0.
	probeBat := makeInt32Batch(proc.Mp(), [][]int32{{99}, {0}}, nil)

	conditions := [][]*plan.Expr{
		{newExpr(0, int32Typ)},
		{newExpr(0, int32Typ)},
	}

	dedupArg := &DedupJoin{
		LeftTypes:  []types.Type{int32Typ, int32Typ},
		RightTypes: []types.Type{int32Typ, int32Typ},
		Conditions: conditions,
		Result: []colexec.ResultPos{
			colexec.NewResultPos(1, 0), // build key
			colexec.NewResultPos(1, 1), // build payload — exercises transfer path
		},
		OnDuplicateAction: plan.Node_FAIL, // no capture, no matched marking
		JoinMapTag:        curTag,
		OperatorBase:      vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	buildArg := &hashbuild.HashBuild{
		NeedHashMap:   true,
		NeedBatches:   true,
		Conditions:    conditions[1],
		IsDedup:       true,
		DelColIdx:     -1,
		JoinMapTag:    curTag,
		JoinMapRefCnt: 1,
		OperatorBase:  vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}

	out := runFinalizeFixture(t, dedupArg, buildArg, proc, buildBat, probeBat)
	require.Len(t, out, 1)
	require.Equal(t, 3, out[0].RowCount())

	col0 := vector.MustFixedColNoTypeCheck[int32](out[0].Vecs[0])
	col1 := vector.MustFixedColNoTypeCheck[int32](out[0].Vecs[1])
	require.Equal(t, []int32{10, 20, 30}, col0)
	require.Equal(t, []int32{100, 200, 300}, col1)

	// Reset should walk JoinMap.batches whose Vecs slots are now nil-aliased
	// into the output buf — must not panic / double-free.
	dedupArg.Reset(proc, false, nil)
	buildArg.Reset(proc, false, nil)

	dedupArg.Free(proc, false, nil)
	buildArg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestDedupJoinFinalizeMatchedZero_WithCapture exercises the capture branch
// at finalize() lines 326-341: matched==0 AND ctr.captured.Count()==0, so
// the capture column is filled with NULLs via AppendMultiFixed (no UnionBatch
// from capturedVecs). The non-capture build column still goes through the
// transfer path.
func TestDedupJoinFinalizeMatchedZero_WithCapture(t *testing.T) {
	proc, ctrl := newCaptureTestProc(t)
	defer ctrl.Finish()

	int32Typ := types.T_int32.ToType()
	tag++
	curTag := tag

	// Build keys [10,20] with NULL placeholder col1.
	buildBat := makeInt32Batch(proc.Mp(), [][]int32{{10, 20}, {0, 0}}, [][]uint64{nil, {0, 1}})
	// Probe miss → captured stays empty.
	probeBat := makeInt32Batch(proc.Mp(), [][]int32{{99}, {0}}, nil)

	conditions := [][]*plan.Expr{
		{newExpr(0, int32Typ)},
		{newExpr(0, int32Typ)},
	}

	dedupArg := &DedupJoin{
		LeftTypes:  []types.Type{int32Typ, int32Typ},
		RightTypes: []types.Type{int32Typ, int32Typ},
		Conditions: conditions,
		Result: []colexec.ResultPos{
			colexec.NewResultPos(1, 0), // build key — non-capture column, transfer path
			colexec.NewResultPos(1, 1), // capture target — must be NULL-filled
		},
		OnDuplicateAction:               plan.Node_FAIL,
		OldColCapturePlaceholderIdxList: []int32{1},
		OldColCaptureProbeIdxList:       []int32{1},
		JoinMapTag:                      curTag,
		OperatorBase:                    vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	buildArg := &hashbuild.HashBuild{
		NeedHashMap:   true,
		NeedBatches:   true,
		Conditions:    conditions[1],
		IsDedup:       true,
		DelColIdx:     -1,
		JoinMapTag:    curTag,
		JoinMapRefCnt: 1,
		OperatorBase:  vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}

	out := runFinalizeFixture(t, dedupArg, buildArg, proc, buildBat, probeBat)
	require.Len(t, out, 1)
	require.Equal(t, 2, out[0].RowCount())

	// col 0: transferred build keys
	col0 := vector.MustFixedColNoTypeCheck[int32](out[0].Vecs[0])
	require.Equal(t, []int32{10, 20}, col0)

	// col 1: capture target — both rows must be NULL.
	require.True(t, out[0].Vecs[1].GetNulls().Contains(0), "row 0 capture col must be NULL")
	require.True(t, out[0].Vecs[1].GetNulls().Contains(1), "row 1 capture col must be NULL")
	require.Equal(t, 2, out[0].Vecs[1].Length(),
		"NULL-fill path should still allocate length==batSize")

	dedupArg.Reset(proc, false, nil)
	buildArg.Reset(proc, false, nil)
	dedupArg.Free(proc, false, nil)
	buildArg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestDedupJoinFinalize_UnionSelsByBatch_PartialMatch covers the matched>0
// non-UPDATE branch at finalize() lines 367-401. The new helper
// unionSelsByBatch is invoked here with a small sels slice (≤16 elements),
// so it takes the per-element UnionOne fallback path.
func TestDedupJoinFinalize_UnionSelsByBatch_PartialMatch(t *testing.T) {
	proc, ctrl := newCaptureTestProc(t)
	defer ctrl.Finish()

	int32Typ := types.T_int32.ToType()
	tag++
	curTag := tag

	// Build keys [10,20,30,40], payload [100,200,300,400].
	buildBat := makeInt32Batch(proc.Mp(), [][]int32{{10, 20, 30, 40}, {100, 200, 300, 400}}, nil)
	// Probe matches keys 10 and 30 → matched bitmap = {0, 2}.
	probeBat := makeInt32Batch(proc.Mp(), [][]int32{{10, 30}, {0, 0}}, nil)

	conditions := [][]*plan.Expr{
		{newExpr(0, int32Typ)},
		{newExpr(0, int32Typ)},
	}

	dedupArg := &DedupJoin{
		LeftTypes:  []types.Type{int32Typ, int32Typ},
		RightTypes: []types.Type{int32Typ, int32Typ},
		Conditions: conditions,
		Result: []colexec.ResultPos{
			colexec.NewResultPos(1, 0),
			colexec.NewResultPos(1, 1),
		},
		OnDuplicateAction: plan.Node_IGNORE, // sets matched bitmap on hit
		JoinMapTag:        curTag,
		OperatorBase:      vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	buildArg := &hashbuild.HashBuild{
		NeedHashMap:      true,
		NeedBatches:      true,
		Conditions:       conditions[1],
		IsDedup:          true,
		DelColIdx:        -1,
		JoinMapTag:       curTag,
		JoinMapRefCnt:    1,
		OnDuplicateAction: plan.Node_IGNORE,
		OperatorBase:     vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}

	out := runFinalizeFixture(t, dedupArg, buildArg, proc, buildBat, probeBat)
	require.Len(t, out, 1)
	require.Equal(t, 2, out[0].RowCount(), "should emit the 2 unmatched build rows")

	col0 := vector.MustFixedColNoTypeCheck[int32](out[0].Vecs[0])
	col1 := vector.MustFixedColNoTypeCheck[int32](out[0].Vecs[1])
	require.Equal(t, []int32{20, 40}, col0)
	require.Equal(t, []int32{200, 400}, col1)

	dedupArg.Reset(proc, false, nil)
	buildArg.Reset(proc, false, nil)
	dedupArg.Free(proc, false, nil)
	buildArg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// makeInt32Vec builds a single-vector helper for unionSelsByBatch tests.
func makeInt32Vec(t *testing.T, proc *process.Process, vals []int32) *vector.Vector {
	t.Helper()
	v := vector.NewOffHeapVecWithType(types.T_int32.ToType())
	for _, x := range vals {
		require.NoError(t, vector.AppendFixed(v, x, false, proc.Mp()))
	}
	return v
}

// TestUnionSelsByBatch directly exercises the helper, covering both the
// ≤16 fallback (per-element UnionOne) and the >16 grouped Union path.
func TestUnionSelsByBatch(t *testing.T) {
	proc, ctrl := newCaptureTestProc(t)
	defer ctrl.Finish()

	t.Run("small_sels_single_batch", func(t *testing.T) {
		vals := []int32{10, 20, 30, 40, 50}
		bat := batch.New([]string{"v"})
		bat.Vecs[0] = makeInt32Vec(t, proc, vals)
		bat.SetRowCount(len(vals))
		defer bat.Clean(proc.Mp())

		dst := vector.NewOffHeapVecWithType(types.T_int32.ToType())
		defer dst.Free(proc.Mp())

		sels := []int32{0, 2, 4} // length 3 ≤ 16
		require.NoError(t, unionSelsByBatch(dst, []*batch.Batch{bat}, 0, sels, proc))

		got := vector.MustFixedColNoTypeCheck[int32](dst)
		require.Equal(t, []int32{10, 30, 50}, got)
	})

	t.Run("large_sels_single_batch", func(t *testing.T) {
		// 32 values; sels picks 17 of them so we trip the >16 grouped path.
		vals := make([]int32, 32)
		for i := range vals {
			vals[i] = int32(i * 10)
		}
		bat := batch.New([]string{"v"})
		bat.Vecs[0] = makeInt32Vec(t, proc, vals)
		bat.SetRowCount(len(vals))
		defer bat.Clean(proc.Mp())

		sels := make([]int32, 17)
		want := make([]int32, 17)
		for i := range sels {
			sels[i] = int32(i)
			want[i] = vals[i]
		}

		dst := vector.NewOffHeapVecWithType(types.T_int32.ToType())
		defer dst.Free(proc.Mp())
		require.NoError(t, unionSelsByBatch(dst, []*batch.Batch{bat}, 0, sels, proc))

		got := vector.MustFixedColNoTypeCheck[int32](dst)
		require.Equal(t, want, got)
	})

	t.Run("large_sels_cross_batch", func(t *testing.T) {
		// Two batches; sels span batch boundary using global index sel /
		// DefaultBatchSize. Batch 0 holds 32 values, batch 1 holds 8.
		vals0 := make([]int32, 32)
		for i := range vals0 {
			vals0[i] = int32(i + 1000)
		}
		vals1 := make([]int32, 8)
		for i := range vals1 {
			vals1[i] = int32(i + 9000)
		}
		bat0 := batch.New([]string{"v"})
		bat0.Vecs[0] = makeInt32Vec(t, proc, vals0)
		bat0.SetRowCount(len(vals0))
		defer bat0.Clean(proc.Mp())

		bat1 := batch.New([]string{"v"})
		bat1.Vecs[0] = makeInt32Vec(t, proc, vals1)
		bat1.SetRowCount(len(vals1))
		defer bat1.Clean(proc.Mp())

		// 17 sels: 14 from batch 0, then 3 from batch 1. Crosses prevIdx
		// boundary, exercising the flush-on-batch-change branch.
		sels := []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
			colexec.DefaultBatchSize, colexec.DefaultBatchSize + 1, colexec.DefaultBatchSize + 2}
		want := []int32{
			vals0[0], vals0[1], vals0[2], vals0[3], vals0[4], vals0[5], vals0[6],
			vals0[7], vals0[8], vals0[9], vals0[10], vals0[11], vals0[12], vals0[13],
			vals1[0], vals1[1], vals1[2],
		}

		dst := vector.NewOffHeapVecWithType(types.T_int32.ToType())
		defer dst.Free(proc.Mp())
		require.NoError(t, unionSelsByBatch(dst, []*batch.Batch{bat0, bat1}, 0, sels, proc))

		got := vector.MustFixedColNoTypeCheck[int32](dst)
		require.Equal(t, want, got)
	})

	t.Run("equivalence_small_vs_large", func(t *testing.T) {
		// Same data, same projection, two paths: ≤16 sels (UnionOne) vs
		// >16 sels (grouped Union). Padding the input lets both cases hit
		// identical content so we can compare bit-for-bit.
		vals := make([]int32, 40)
		for i := range vals {
			vals[i] = int32(i * 7)
		}
		bat := batch.New([]string{"v"})
		bat.Vecs[0] = makeInt32Vec(t, proc, vals)
		bat.SetRowCount(len(vals))
		defer bat.Clean(proc.Mp())

		// Pick the same 17 indices in both runs.
		sels := []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		dstLarge := vector.NewOffHeapVecWithType(types.T_int32.ToType())
		defer dstLarge.Free(proc.Mp())
		require.NoError(t, unionSelsByBatch(dstLarge, []*batch.Batch{bat}, 0, sels, proc))

		// Drop one to force the ≤16 fallback path.
		dstSmall := vector.NewOffHeapVecWithType(types.T_int32.ToType())
		defer dstSmall.Free(proc.Mp())
		require.NoError(t, unionSelsByBatch(dstSmall, []*batch.Batch{bat}, 0, sels[:16], proc))

		require.Equal(t,
			vector.MustFixedColNoTypeCheck[int32](dstSmall),
			vector.MustFixedColNoTypeCheck[int32](dstLarge)[:16])
	})

	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

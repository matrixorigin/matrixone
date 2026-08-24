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

package table_function

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func ft2SearchRets() []*plan.ColDef {
	return []*plan.ColDef{
		{Name: catalog.FullText2Search_OutCol_DocId, Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: catalog.FullText2Search_OutCol_Score, Typ: plan.Type{Id: int32(types.T_float32), Width: 4}},
	}
}

// ft2Int64ColumnBuffer builds a box-free ColumnBuffer of int64 keys (the exact layout
// the streaming producer emits: N little-endian 8-byte values).
func ft2Int64ColumnBuffer(vals ...int64) *vectorindex.ColumnBuffer {
	k := fulltext2.GetColumnBuffer(types.T_int64)
	var b [8]byte
	for _, v := range vals {
		binary.LittleEndian.PutUint64(b[:], uint64(v))
		k.Data = append(k.Data, b[:]...)
		k.N++
	}
	return k
}

func TestFulltext2SearchPrepareLimit(t *testing.T) {
	proc := testutil.NewProc(t)
	base := []*plan.Expr{makeStrConstExpr("{}"), makeStrConstExpr("pat"), makeStrConstExpr("0")}

	mk := func(lit *plan.Expr) *fulltext2SearchState {
		arg := &TableFunction{Args: base, FuncName: "fulltext2_search", Limit: lit}
		st, err := fulltext2SearchPrepare(proc, arg)
		require.NoError(t, err)
		return st.(*fulltext2SearchState)
	}

	// no LIMIT → 0 (streaming path).
	require.Equal(t, uint64(0), mk(nil).limit)
	// U64 literal LIMIT (the shape the planner emits — makePlan2Uint64ConstExprWithType). A
	// prepared `LIMIT ?` parameter (a non-literal expr resolved at EXECUTE via evalLimitExpression)
	// is covered end-to-end by the fulltext2_prepare BVT.
	require.Equal(t, uint64(12), mk(&plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 12}}}}).limit)
}

func TestFulltext2ScoreAlgo(t *testing.T) {
	mp := mpool.MustNewZero()

	// default (resolve returns nil) → BM25.
	proc := testutil.NewProcessWithMPool(t, "", mp)
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return nil, nil })
	require.Equal(t, fulltext2.BM25, fulltext2ScoreAlgo(proc))

	// explicit TF-IDF.
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == fulltext2.Fulltext2RelevancyAlgo {
			return fulltext2.Fulltext2RelevancyAlgo_tfidf, nil
		}
		return nil, nil
	})
	require.Equal(t, fulltext2.TfIdf, fulltext2ScoreAlgo(proc))

	// resolve error → BM25 default stands.
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return nil, moerr.NewInternalErrorNoCtx("boom")
	})
	require.Equal(t, fulltext2.BM25, fulltext2ScoreAlgo(proc))
}

func TestFulltext2SearchStartValidation(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	tf := newFT2TF([]string{"doc_id", "score"}, ft2SearchRets())
	patVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(patVec, []byte("pat"), false, mp))
	modeVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](modeVec, int64(0), false, mp))

	// config not a const → invalid input.
	{
		st := &fulltext2SearchState{}
		tf.ctr.argVecs = []*vector.Vector{ft2NonConstStr(t, mp, "{}"), patVec, modeVec}
		require.ErrorContains(t, st.start(tf, proc, 0, nil), "must be a string constant")
	}
	// empty config → internal error.
	{
		st := &fulltext2SearchState{}
		tf.ctr.argVecs = []*vector.Vector{ft2ConstStr(t, mp, ""), patVec, modeVec}
		require.ErrorContains(t, st.start(tf, proc, 0, nil), "config is empty")
	}
	// invalid json config → error.
	{
		st := &fulltext2SearchState{}
		tf.ctr.argVecs = []*vector.Vector{ft2ConstStr(t, mp, "{bad"), patVec, modeVec}
		require.Error(t, st.start(tf, proc, 0, nil))
	}
	// valid config but a NULL pattern → early return nil, no search (no DB access).
	{
		st := &fulltext2SearchState{}
		patNull := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(patNull, nil, true, mp))
		tf.ctr.argVecs = []*vector.Vector{ft2ConstStr(t, mp, `{"index":"__idx"}`), patNull, modeVec}
		require.NoError(t, st.start(tf, proc, 0, nil))
		require.True(t, st.inited)
		st.free(tf, proc, false, nil)
	}
	// A malformed pushed score range is rejected before any index access. The planner only
	// ever emits this argument itself, so a bad value means the plan and the engine disagree
	// about the encoding -- fail loudly rather than silently searching without the bound.
	{
		st := &fulltext2SearchState{}
		tf.ctr.argVecs = []*vector.Vector{
			ft2ConstStr(t, mp, `{"index":"__idx"}`), patVec, modeVec,
			ft2ConstStr(t, mp, ""), ft2ConstStr(t, mp, "{not json"),
		}
		require.ErrorContains(t, st.start(tf, proc, 0, nil), "invalid score range")
	}
}

// TestFulltext2SearchCallMaterialized drives the non-streaming call() path: pre-loaded
// keys/distances are drained into result batches, then an empty batch stops.
func TestFulltext2SearchCallMaterialized(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	st := &fulltext2SearchState{
		limit: 10,
		out: &vectorindex.SearchOutput{
			Keys:  ft2Int64ColumnBuffer(11, 22, 33),
			Dists: []float32{0.5, 0.4, 0.3},
		},
	}
	st.batch = batch.NewWithSize(2)
	st.batch.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	st.batch.Vecs[1] = vector.NewVec(types.T_float32.ToType())
	st.pkVecIdx, st.scoreVecIdx = 0, 1 // non-covered [doc_id, score] layout (start() builds this from Attrs)

	tf := &TableFunction{}
	res, err := st.call(tf, proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, res.Status)
	require.Equal(t, 3, res.Batch.RowCount())
	require.Equal(t, int64(11), vector.GetFixedAtWithTypeCheck[int64](res.Batch.Vecs[0], 0))
	require.Equal(t, float32(0.3), vector.GetFixedAtWithTypeCheck[float32](res.Batch.Vecs[1], 2))

	// all keys consumed → CancelResult.
	res, err = st.call(tf, proc)
	require.NoError(t, err)
	require.Equal(t, vm.CancelResult, res)

	require.NoError(t, st.end(tf, proc))
	st.reset(tf, proc)
	st.free(tf, proc, false, nil)
}

// TestFulltext2SearchCallMaterializedCovered drives the COVERED non-streaming call() path:
// pre-loaded keys/distances plus a box-free Ft2IncludeResult (column-major, whole result set)
// are drained into a batch, the include column bulk-appended via AppendColumnBufferRange
// mapped by segPos — the pull-path mirror of the streaming covered consumer, NULL-aware.
func TestFulltext2SearchCallMaterializedCovered(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	// One include col ("prio" int64): [10, NULL, 30] — placeholder + Nulls flag (the
	// fillInclude layout). segPos 0 in the FULL include list.
	prio := &vectorindex.ColumnBuffer{Type: types.T_int64}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 10)
	prio.Data = append(prio.Data, b[:]...)
	prio.Data = append(prio.Data, make([]byte, 8)...) // NULL placeholder
	binary.LittleEndian.PutUint64(b[:], 30)
	prio.Data = append(prio.Data, b[:]...)
	prio.N = 3
	prio.Nulls = []bool{false, true, false}

	st := &fulltext2SearchState{
		limit: 10,
		out: &vectorindex.SearchOutput{
			Keys:    ft2Int64ColumnBuffer(11, 22, 33),
			Dists:   []float32{0.5, 0.4, 0.3},
			Include: []*vectorindex.ColumnBuffer{prio},
		},
		includeOut: []ft2IncludeOut{{vecIdx: 2, segPos: 0, name: "prio"}},
	}
	st.batch = batch.NewWithSize(3)
	st.batch.Attrs = []string{"doc_id", "score", "prio"}
	st.batch.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	st.batch.Vecs[1] = vector.NewVec(types.T_float32.ToType())
	st.batch.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	st.pkVecIdx, st.scoreVecIdx = 0, 1

	tf := &TableFunction{}
	res, err := st.call(tf, proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, res.Status)
	require.Equal(t, 3, res.Batch.RowCount())
	require.Equal(t, int64(11), vector.GetFixedAtWithTypeCheck[int64](res.Batch.Vecs[0], 0))
	require.Equal(t, int64(10), vector.GetFixedAtWithTypeCheck[int64](res.Batch.Vecs[2], 0))
	require.True(t, res.Batch.Vecs[2].IsNull(1)) // NULL include -> SQL NULL
	require.Equal(t, int64(30), vector.GetFixedAtWithTypeCheck[int64](res.Batch.Vecs[2], 2))

	st.free(tf, proc, false, nil)
}

// TestFulltext2SearchCallNilOut: when start() bails before running SearchInto (e.g. a NULL
// pattern), u.out stays nil; call() must return end-of-stream, NOT nil-deref u.out.Keys.
func TestFulltext2SearchCallNilOut(t *testing.T) {
	proc := testutil.NewProc(t)
	st := &fulltext2SearchState{limit: 10} // u.out == nil, not streaming
	st.batch = batch.NewWithSize(2)
	st.batch.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	st.batch.Vecs[1] = vector.NewVec(types.T_float32.ToType())
	st.pkVecIdx, st.scoreVecIdx = 0, 1

	tf := &TableFunction{}
	res, err := st.call(tf, proc)
	require.NoError(t, err)
	require.Equal(t, vm.CancelResult, res)
	st.free(tf, proc, false, nil)
}

// TestFulltext2SearchMaterializedAppendError drives the materialized call() drain with a
// capped mpool that is pre-filled to a few KB of headroom, so an append fails PARTWAY
// through the batch. The fix must surface that error (vm.CancelResult + err) instead of
// swallowing it and publishing a batch whose row count exceeds the rows actually appended
// to its vectors (the malformed, mismatched-length batch the pre-fix code produced).
func TestFulltext2SearchMaterializedAppendError(t *testing.T) {
	// cap enforcement fires only on the offHeap alloc path, so the vectors below are marked
	// offHeap and the pool is given a real (2 MiB) cap.
	mp, err := mpool.NewMPool("ft2_append_fail", 2<<20, mpool.NoFixed)
	require.NoError(t, err)
	proc := testutil.NewProcessWithMPool(t, "", mp)

	st := &fulltext2SearchState{limit: 10}
	st.batch = batch.NewWithSize(2)
	st.batch.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	st.batch.Vecs[1] = vector.NewVec(types.T_float32.ToType())
	st.pkVecIdx, st.scoreVecIdx = 0, 1 // non-covered [doc_id, score] layout (start() builds this from Attrs)
	st.batch.Vecs[0].SetOffHeap(true)  // route data allocations through the cap-enforced path
	st.batch.Vecs[1].SetOffHeap(true)

	// Far more keys than one call()'s 8192 window, and their append demand dwarfs the
	// headroom left below, so the drain loop hits an allocation failure mid-batch.
	n := 20000
	keysBuf := &vectorindex.ColumnBuffer{Type: types.T_int64}
	dists := make([]float32, n)
	var kb [8]byte
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint64(kb[:], uint64(i))
		keysBuf.Data = append(keysBuf.Data, kb[:]...)
		keysBuf.N++
		dists[i] = float32(i)
	}
	st.out = &vectorindex.SearchOutput{Keys: keysBuf, Dists: dists}

	// Consume the pool down to ~4 KB of headroom (offHeap → cap is enforced), so the first
	// call()'s appends succeed for a while, then fail — exercising the mid-loop error.
	fill, err := mp.Alloc(int(mp.Cap()-mp.CurrNB()-4096), true)
	require.NoError(t, err)
	defer mp.Free(fill)

	tf := &TableFunction{}
	res, err := st.call(tf, proc)
	require.Error(t, err, "an append allocation failure must propagate, not be swallowed")
	require.Equal(t, vm.CancelResult, res)

	// The batch must never claim more rows than were actually appended to EITHER vector —
	// SetRowCount is skipped on the error path, so the count stays 0 and the invariant holds
	// (pre-fix, RowCount could be n while a vector held fewer rows).
	require.LessOrEqual(t, st.batch.RowCount(), st.batch.Vecs[0].Length())
	require.LessOrEqual(t, st.batch.RowCount(), st.batch.Vecs[1].Length())

	st.free(tf, proc, false, nil)
}

// TestFulltext2SearchCallStreaming drives the streaming call() path: batches arriving on
// streamCh are decoded via the box-free ColumnBuffer, and channel close surfaces errCh.
func TestFulltext2SearchCallStreaming(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	newState := func() *fulltext2SearchState {
		st := &fulltext2SearchState{streaming: true}
		st.batch = batch.NewWithSize(2)
		st.batch.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		st.batch.Vecs[1] = vector.NewVec(types.T_float32.ToType())
		st.pkVecIdx, st.scoreVecIdx = 0, 1 // non-covered [doc_id, score] layout (start() builds this from Attrs)
		st.streamCh = make(chan *vectorindex.SearchOutput, 4)
		st.errCh = make(chan error, 1)
		return st
	}
	tf := &TableFunction{}

	// happy path: one batch then a clean (nil-error) close.
	{
		st := newState()
		st.streamCh <- &vectorindex.SearchOutput{Keys: ft2Int64ColumnBuffer(7, 8), Dists: []float32{1.5, 2.5}}
		close(st.streamCh)
		st.errCh <- nil

		res, err := st.call(tf, proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecNext, res.Status)
		require.Equal(t, 2, res.Batch.RowCount())
		require.Equal(t, int64(8), vector.GetFixedAtWithTypeCheck[int64](res.Batch.Vecs[0], 1))
		require.Equal(t, float32(1.5), vector.GetFixedAtWithTypeCheck[float32](res.Batch.Vecs[1], 0))

		res, err = st.call(tf, proc)
		require.NoError(t, err)
		require.Equal(t, vm.CancelResult, res)
		require.True(t, st.done)

		// once done, further calls short-circuit.
		res, err = st.call(tf, proc)
		require.NoError(t, err)
		require.Equal(t, vm.CancelResult, res)
	}

	// error path: producer reports a search error on close.
	{
		st := newState()
		close(st.streamCh)
		st.errCh <- moerr.NewInternalErrorNoCtx("search failed")
		res, err := st.call(tf, proc)
		require.Error(t, err)
		require.Equal(t, vm.CancelResult, res)
	}
}

// TestFulltext2SearchCallStreamingCovered drives the COVERED streaming path: a batch with
// an extra INCLUDE output vector, whose values arrive column-major (one nullable
// ColumnBuffer per include col) and are bulk-decoded box-free via AppendColumnBuffer,
// preserving NULLs. Mirrors what start() builds for `SELECT id, tag ... covered`.
func TestFulltext2SearchCallStreamingCovered(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	st := &fulltext2SearchState{streaming: true}
	st.batch = batch.NewWithSize(3)
	st.batch.Attrs = []string{"doc_id", "score", "tag"}
	st.batch.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	st.batch.Vecs[1] = vector.NewVec(types.T_float32.ToType())
	st.batch.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	st.pkVecIdx, st.scoreVecIdx = 0, 1
	st.includeOut = []ft2IncludeOut{{vecIdx: 2, segPos: 0, name: "tag"}}
	st.streamCh = make(chan *vectorindex.SearchOutput, 4)
	st.errCh = make(chan error, 1)

	// One include col ("tag" varchar): ["x", NULL] — a well-formed [u32 len][content] entry
	// plus a [u32 0] placeholder with Nulls[1]=true (the producer's cbAppendNull layout).
	tag := &vectorindex.ColumnBuffer{Type: types.T_varchar}
	tag.Data = append(tag.Data, 1, 0, 0, 0, 'x') // len=1, "x"
	tag.Data = append(tag.Data, 0, 0, 0, 0)      // len=0 (NULL placeholder)
	tag.N = 2
	tag.Nulls = []bool{false, true}

	st.streamCh <- &vectorindex.SearchOutput{Keys: ft2Int64ColumnBuffer(7, 8), Dists: []float32{1.5, 2.5}, Include: []*vectorindex.ColumnBuffer{tag}}
	close(st.streamCh)
	st.errCh <- nil

	tf := &TableFunction{}
	res, err := st.call(tf, proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, res.Status)
	require.Equal(t, 2, res.Batch.RowCount())
	require.Equal(t, int64(7), vector.GetFixedAtWithTypeCheck[int64](res.Batch.Vecs[0], 0))
	require.Equal(t, float32(2.5), vector.GetFixedAtWithTypeCheck[float32](res.Batch.Vecs[1], 1))
	require.Equal(t, "x", res.Batch.Vecs[2].GetStringAt(0))
	require.False(t, res.Batch.Vecs[2].IsNull(0))
	require.True(t, res.Batch.Vecs[2].IsNull(1)) // NULL include value surfaces as SQL NULL

	st.free(tf, proc, false, nil)
}

// TestFulltext2SearchStopStreamDrains verifies stopStream cancels the producer context
// and drains+recycles any buffered batch (no goroutine/buffer leak).
func TestFulltext2SearchStopStreamDrains(t *testing.T) {
	st := &fulltext2SearchState{streaming: true}
	_, cancel := context.WithCancel(context.Background())
	st.cancel = cancel
	st.streamCh = make(chan *vectorindex.SearchOutput, 4)
	st.streamCh <- &vectorindex.SearchOutput{Keys: ft2Int64ColumnBuffer(1), Dists: []float32{1}}
	close(st.streamCh)

	st.stopStream()
	require.Nil(t, st.cancel)
	require.Nil(t, st.streamCh)

	// idempotent.
	st.stopStream()
}

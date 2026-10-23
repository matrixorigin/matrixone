// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package table_function

import (
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestTokenizerByteChunksAndDocumentGeneration(t *testing.T) {
	for _, controlled := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "controlled"}[controlled], func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			ut := newFTTTestCase(t, mp, fttdefaultAttrs, "")
			t.Cleanup(func() { ut.arg.Free(ut.proc, false, nil) })
			ut.arg.Rets = append([]*plan.ColDef(nil), fftdefaultColdefs...)
			ut.arg.Rets[0] = &plan.ColDef{Name: "DOC_ID", Typ: plan.Type{Id: int32(types.T_varchar)}}
			ut.arg.Args = []*plan.Expr{
				{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
				{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}},
			}
			input := batch.NewWithSize(2)
			t.Cleanup(func() { input.Clean(ut.proc.Mp()) })
			input.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			input.Vecs[1] = vector.NewVec(types.T_text.ToType())
			for i, tokens := range []int{33, 17, 1} {
				require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(strings.Repeat(string(rune('a'+i)), 2048)), false, ut.proc.Mp()))
				require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("word ", tokens)), false, ut.proc.Mp()))
			}
			input.SetRowCount(3)
			require.NoError(t, ut.arg.Prepare(ut.proc))
			require.NoError(t, ut.arg.ApplyArgsEval(input, ut.proc))
			st := ut.arg.ctr.state.(*tokenizeState)
			for row, want := range []int{34, 18, 2} {
				unit := int64(0)
				if controlled {
					unit = 1024
				}
				ut.arg.SetMemoryOutputLimit(unit, input, row, []colexec.ResultPos{{Rel: 1, Pos: 0}, {Rel: 1, Pos: 1}, {Rel: 1, Pos: 2}})
				require.NoError(t, ut.arg.ApplyStart(row, ut.proc, nil))
				// Every tail slot from the preceding longer document must release
				// its copied doc-ID and word, while capacity remains reusable.
				for _, entry := range st.doc.Words[len(st.doc.Words):cap(st.doc.Words)] {
					require.Nil(t, entry.DocId)
					require.Empty(t, entry.Word)
				}
				got := 0
				for {
					result, err := ut.arg.ApplyCall(ut.proc)
					require.NoError(t, err)
					if result.Batch.IsDone() {
						break
					}
					if controlled {
						require.Equal(t, 1, result.Batch.RowCount(), "a legal wide ID is one row, never ID times8192")
					}
					got += result.Batch.RowCount()
				}
				require.Equal(t, want, got)
			}
			if controlled {
				ut.proc.Base.ImportMemory = &process.ImportMemory{UnitBytes: 1024}
			}
			ut.arg.Reset(ut.proc, false, nil)
			require.Zero(t, ut.arg.memoryOutputRows)
			if controlled {
				require.Nil(t, st.doc.Words)
				require.Nil(t, st.batch)
				require.Nil(t, ut.arg.ctr.executorsForArgs)
			}
			ut.proc.Base.ImportMemory = nil
		})
	}
}

func TestTokenizerBoundPreservesArgumentCorrelationAndByteUnit(t *testing.T) {
	args := make([]*plan.Expr, 4)
	cols := make([]MemoryColumnBound, 4)
	for i := range args {
		args[i] = &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: int32(i)}}}
		cols[i] = MemoryColumnBound{Type: types.T_varchar.ToType(), RowBytes: 1 << 20, BatchBytes: 1 << 20}
	}
	tf := &TableFunction{FuncName: "fulltext_index_tokenize", Args: args}
	one, ok := tf.MemoryBound(cols, 1<<20, 1024)
	require.True(t, ok)
	many, ok := tf.MemoryBound(cols, 3<<20, 1024)
	require.True(t, ok)
	require.Less(t, one.Bytes, many.Bytes, "distinct CSV argument columns share one record bound")
	require.Less(t, one.Bytes, int64(1<<30), "a wide legal ID must not be multiplied by8192")
	for _, params := range []string{`{"parser":"json"}`, `{"parser":"gojieba"}`, `{"parser":"unknown"}`} {
		tf.Params = []byte(params)
		require.False(t, tf.SupportsMemoryBound())
	}
	tf.Params = nil
	tf.Args[1].Typ.Id = int32(types.T_datalink)
	require.False(t, tf.SupportsMemoryBound())
}

func TestTokenizerAppendFailureNeverPublishesPartialBatch(t *testing.T) {
	for _, failingAppend := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(failingAppend), func(t *testing.T) {
			mp, err := mpool.NewMPool(t.Name(), 1<<20, mpool.NoFixed)
			require.NoError(t, err)
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			proc := testutil.NewProcessWithMPool(t, "", mp)
			st := &tokenizeState{batch: batch.NewWithSize(3), doc: Document{Words: []FullTextEntry{{DocId: int32(1), Pos: 1, Word: "word"}}}}
			t.Cleanup(func() { st.free(&TableFunction{}, proc, false, nil) })
			for i, typ := range []types.Type{types.T_int32.ToType(), types.T_int32.ToType(), types.T_varchar.ToType()} {
				// Native backing makes the existing MPool cap a deterministic
				// append failure seam; no oversized document/allocation is needed.
				st.batch.Vecs[i] = vector.NewOffHeapVecWithType(typ)
			}
			// Pre-grow preceding fixed results, then leave one allocatable byte.
			// This selects the exact failing append without assuming growth sizes.
			for i := 0; i < failingAppend; i++ {
				require.NoError(t, vector.AppendFixed(st.batch.Vecs[i], int32(0), false, mp))
				st.batch.Vecs[i].CleanOnlyData()
			}
			filler, err := mp.Alloc(int((1<<20)-mp.CurrNB()-1), true)
			require.NoError(t, err)
			t.Cleanup(func() { mp.Free(filler) })
			result, err := st.call(&TableFunction{memoryOutputRows: 1}, proc)
			require.Error(t, err)
			require.Nil(t, result.Batch)
			require.Zero(t, st.batch.RowCount())
		})
	}
}

func TestTokenizerConsumerHandlesExpandingUnicodeLowercase(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	ut := newFTTTestCase(t, mp, fttdefaultAttrs, "")
	t.Cleanup(func() { ut.arg.Free(ut.proc, false, nil) })
	ut.arg.Args = makeConstInputExprsFTT()
	ut.arg.Args[1].Expr = &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: strings.Repeat("\u023a", 8)}}}
	require.NoError(t, ut.arg.Prepare(ut.proc))
	require.NoError(t, ut.arg.ApplyArgsEval(batch.EmptyForConstFoldBatch, ut.proc))
	require.NoError(t, ut.arg.ApplyStart(0, ut.proc, nil))
	result, err := ut.arg.ApplyCall(ut.proc)
	require.NoError(t, err)
	require.Equal(t, 2, result.Batch.RowCount())
	require.Equal(t, strings.Repeat("\u2c65", 7), string(result.Batch.Vecs[2].GetBytesAt(0)))
	require.LessOrEqual(t, len(result.Batch.Vecs[2].GetBytesAt(0)), 23)
}

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package apply

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func tokenizerApplyFixture(t *testing.T, tokens []int, wideSource bool) (*Apply, *process.Process) {
	t.Helper()
	proc := testutil.NewProc(t)
	in := batch.NewWithSize(3)
	for i, typ := range []types.Type{types.T_int32.ToType(), types.T_text.ToType(), types.T_varchar.ToType()} {
		in.Vecs[i] = vector.NewVec(typ)
	}
	t.Cleanup(func() { in.Clean(proc.Mp()) })
	for i, n := range tokens {
		require.NoError(t, vector.AppendFixed(in.Vecs[0], int32(i+1), false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(in.Vecs[1], []byte(strings.Repeat("word ", n)), false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(in.Vecs[2], []byte(strings.Repeat("s", 1024)), false, proc.Mp()))
	}
	in.SetRowCount(len(tokens))
	tf := &table_function.TableFunction{FuncName: "fulltext_index_tokenize", Attrs: []string{"id", "pos", "word"}, Rets: []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int32)}}, {Typ: plan.Type{Id: int32(types.T_int32)}}, {Typ: plan.Type{Id: int32(types.T_varchar), Width: 128}}}, Args: []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
		{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}},
	}}
	op := &Apply{ApplyType: CROSS, TableFunction: tf, Typs: []types.Type{types.T_int32.ToType(), types.T_int32.ToType(), types.T_varchar.ToType()}, Result: []colexec.ResultPos{{Rel: 1, Pos: 0}, {Rel: 1, Pos: 1}, {Rel: 1, Pos: 2}}}
	if wideSource {
		op.Result = []colexec.ResultPos{{Rel: 0, Pos: 2}, {Rel: 1, Pos: 2}}
	}
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{in}))
	t.Cleanup(func() { op.Free(proc, false, nil) })
	return op, proc
}

func TestApplyTokenizerPartialAndChunkBoundary(t *testing.T) {
	// Two documents are the minimum witness for prior partial8191 + next
	// full8192. About80KiB of source text proves the actual16383-row ceiling.
	op, proc := tokenizerApplyFixture(t, []int{8190, 8191}, false)
	proc.Base.ImportMemory = &process.ImportMemory{UnitBytes: 1 << 20}
	t.Cleanup(func() { proc.Base.ImportMemory = nil })
	require.NoError(t, op.Prepare(proc))
	result, err := op.Call(proc)
	require.NoError(t, err)
	require.Equal(t, 16383, result.Batch.RowCount())
	count := result.Batch.RowCount()
	for {
		result, err = op.Call(proc)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
		count += result.Batch.RowCount()
	}
	require.Equal(t, 16383, count)
}

func TestApplyTokenizerBoundsLogicalRepeatedSourcePayload(t *testing.T) {
	op, proc := tokenizerApplyFixture(t, []int{200}, true)
	const unit int64 = 8 << 10
	proc.Base.ImportMemory = &process.ImportMemory{UnitBytes: unit}
	t.Cleanup(func() { proc.Base.ImportMemory = nil })
	require.NoError(t, op.Prepare(proc))
	count := 0
	for {
		result, err := op.Call(proc)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
		var logical int64
		for _, vec := range result.Batch.Vecs {
			logical += vec.WindowSize(0, result.Batch.RowCount())
		}
		require.LessOrEqual(t, logical, 2*unit+1100, "shared UnionMulti area must count once for every referenced output row")
		count += result.Batch.RowCount()
	}
	require.Equal(t, 201, count)
}

func TestApplyPropagatesArgumentEvaluationFailure(t *testing.T) {
	op, proc := tokenizerApplyFixture(t, []int{1}, false)
	get, err := function.GetFunctionByName(context.Background(), "cast", []types.Type{types.T_text.ToType(), types.T_int32.ToType()})
	require.NoError(t, err)
	source := op.TableFunction.Args[1]
	op.TableFunction.Args[0] = &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "cast", Obj: get.GetEncodedOverloadID()}, Args: []*plan.Expr{source, {Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_T{T: &plan.TargetType{}}}}}}}
	require.NoError(t, op.Prepare(proc))
	result, err := op.Call(proc)
	require.Error(t, err)
	require.Nil(t, result.Batch)
	require.Nil(t, op.ctr.rbat, "failed ArgsEval must not start or publish a tokenizer generation")
}

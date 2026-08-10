// Copyright 2024 Matrix Origin
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
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// mock batch schema: (a int32, b uuid, c varchar, d json, e datetime)
// col 0 = a int32

func colExpr(pos int32, t types.T) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(t)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}},
	}
}

func sumAgg(pos int32) aggexec.AggFuncExecExpression {
	e, _ := function.GetFunctionByName(context.Background(), "sum", []types.Type{types.T_int32.ToType()})
	return aggexec.MakeAggFunctionExpression(e.GetEncodedOverloadID(), false, []*plan.Expr{colExpr(pos, types.T_int32)}, nil)
}

func countStarAgg() aggexec.AggFuncExecExpression {
	return aggexec.MakeAggFunctionExpression(aggexec.AggIdOfCountStar, false, []*plan.Expr{colExpr(0, types.T_int32)}, nil)
}

func minPreparedParamAgg() aggexec.AggFuncExecExpression {
	return aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfMin,
		false,
		[]*plan.Expr{{
			Typ:  plan.Type{Id: int32(types.T_text)},
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
		}},
		nil,
	)
}

func minTextColumnAgg(pos int32) aggexec.AggFuncExecExpression {
	return aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfMin,
		false,
		[]*plan.Expr{colExpr(pos, types.T_text)},
		nil,
	)
}

func orderedGroupConcatAgg(distinct bool) aggexec.AggFuncExecExpression {
	config := []byte{2}
	config = binary.BigEndian.AppendUint32(config, 1)
	config = binary.BigEndian.AppendUint32(config, 1)
	config = append(config, 1)
	config = binary.BigEndian.AppendUint32(config, 1)
	config = binary.BigEndian.AppendUint32(config, 1)
	config = append(config, '|')
	return aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfGroupConcat,
		distinct,
		[]*plan.Expr{colExpr(1, types.T_varchar), colExpr(2, types.T_int64)},
		config,
		plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
	)
}

func newGroupOp(proc *process.Process, groupBy []*plan.Expr, aggs []aggexec.AggFuncExecExpression) *Group {
	g := NewArgument()
	g.GroupBy = groupBy
	g.Aggs = aggs
	g.NeedEval = true
	g.OperatorBase = vm.OperatorBase{
		OperatorInfo: vm.OperatorInfo{Idx: 0, IsFirst: false, IsLast: false},
	}
	return g
}

func newMergeGroupOp(aggs []aggexec.AggFuncExecExpression) *MergeGroup {
	mg := NewArgumentMergeGroup()
	mg.Aggs = aggs
	mg.OperatorBase = vm.OperatorBase{
		OperatorInfo: vm.OperatorInfo{Idx: 0, IsFirst: false, IsLast: false},
	}
	return mg
}

func TestSharedAggExpressionsPrepareConcurrently(t *testing.T) {
	groupProc := testutil.NewProcess(t)
	mergeProc := testutil.NewProcess(t)
	sharedAggs := []aggexec.AggFuncExecExpression{minPreparedParamAgg()}
	group := newGroupOp(groupProc, nil, sharedAggs)
	merge := newMergeGroupOp(sharedAggs)
	t.Cleanup(func() {
		group.Free(groupProc, true, nil)
		merge.Free(mergeProc, true, nil)
		require.Zero(t, groupProc.Mp().CurrNB())
		require.Zero(t, mergeProc.Mp().CurrNB())
		groupProc.Free()
		mergeProc.Free()
	})

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	prepare := func(op vm.Operator, proc *process.Process) {
		defer wg.Done()
		<-start
		for range 100 {
			if err := op.Prepare(proc); err != nil {
				errs <- err
				return
			}
		}
	}

	wg.Add(2)
	go prepare(group, groupProc)
	go prepare(merge, mergeProc)
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

type cancelOnDoneCheckContext struct {
	context.Context
	remaining int
	done      chan struct{}
}

type cancelAfterWriteWriter struct {
	cancel context.CancelFunc
	writes int
}

func (w *cancelAfterWriteWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == 1 {
		w.cancel()
	}
	return len(p), nil
}

func newCancelOnDoneCheckContext(parent context.Context, checks int) *cancelOnDoneCheckContext {
	return &cancelOnDoneCheckContext{
		Context:   parent,
		remaining: checks,
		done:      make(chan struct{}),
	}
}

func (ctx *cancelOnDoneCheckContext) Done() <-chan struct{} {
	if ctx.remaining > 0 {
		ctx.remaining--
		if ctx.remaining == 0 {
			close(ctx.done)
		}
	}
	return ctx.done
}

func (ctx *cancelOnDoneCheckContext) Err() error {
	select {
	case <-ctx.done:
		return context.Canceled
	default:
		return nil
	}
}

func resetChildren(g *Group, proc *process.Process) {
	bat := colexec.MakeMockBatchs(proc.Mp())
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	g.Children = nil
	g.AppendChild(op)
}

func collectBatches(t *testing.T, op vm.Operator, proc *process.Process) []*batch.Batch {
	t.Helper()

	var result []*batch.Batch
	for {
		ret, err := vm.Exec(op, proc)
		require.NoError(t, err)
		if ret.Status == vm.ExecStop || ret.Batch == nil {
			return result
		}
		result = append(result, ret.Batch)
	}
}

func cloneBatch(t *testing.T, proc *process.Process, bat *batch.Batch) *batch.Batch {
	t.Helper()

	cloned, err := bat.Dup(proc.Mp())
	require.NoError(t, err)
	cloned.ExtraBuf = append(cloned.ExtraBuf[:0], bat.ExtraBuf...)
	return cloned
}

func buildPartialH0Batch(t *testing.T, proc *process.Process, values []int32) *batch.Batch {
	t.Helper()

	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	input.SetRowCount(len(values))
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	partial := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{countStarAgg()})
	partial.NeedEval = false
	partial.AppendChild(child)
	defer func() {
		partial.Free(proc, false, nil)
		child.Free(proc, false, nil)
	}()

	require.NoError(t, partial.Prepare(proc))
	batches := collectBatches(t, partial, proc)
	require.Len(t, batches, 1)
	return cloneBatch(t, proc, batches[0])
}

type preparedPartialSpec struct {
	rows         int
	kind         vector.PrepareParamKind
	allNull      bool
	value        string
	binaryString bool
}

func buildPreparedMinPartial(
	t *testing.T,
	proc *process.Process,
	spec preparedPartialSpec,
) *batch.Batch {
	return buildPreparedPartial(t, proc, spec,
		[]aggexec.AggFuncExecExpression{minPreparedParamAgg()})
}

func buildPreparedPartial(
	t *testing.T,
	proc *process.Process,
	spec preparedPartialSpec,
	aggs []aggexec.AggFuncExecExpression,
) *batch.Batch {
	t.Helper()

	params := vector.NewVec(types.T_text.ToType())
	defer params.Free(proc.Mp())
	value := spec.value
	if value == "" {
		value = "5"
	}
	require.NoError(t, vector.AppendBytes(params, []byte(value), spec.allNull, proc.Mp()))
	params.SetIsBinaryString(spec.binaryString)
	proc.SetPrepareParamsWithMeta(
		params, nil, []vector.PrepareParamKind{spec.kind}, []bool{spec.binaryString})
	defer proc.SetPrepareParams(nil)

	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(make([]int32, spec.rows), nil, proc.Mp())
	input.SetRowCount(spec.rows)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	defer child.Free(proc, false, nil)

	partial := newGroupOp(proc, nil, aggs)
	partial.NeedEval = false
	partial.AppendChild(child)
	defer partial.Free(proc, false, nil)

	require.NoError(t, partial.Prepare(proc))
	partials := collectBatches(t, partial, proc)
	require.Len(t, partials, 1)
	return cloneBatch(t, proc, partials[0])
}

func TestMergeGroupPartialCarriesBinaryStringState(t *testing.T) {
	proc := testutil.NewProcess(t)
	setPrepareParamKindProtocolVersion(t, proc, defines.MORPCVersion14)
	t.Cleanup(func() {
		require.Zero(t, proc.Mp().CurrNB())
		proc.Free()
	})

	partial := buildPreparedMinPartial(t, proc, preparedPartialSpec{
		rows: 1, value: "你", binaryString: true,
	})
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{partial})
	merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{minPreparedParamAgg()})
	merge.AppendChild(child)
	require.NoError(t, merge.Prepare(proc))
	outputs := collectBatches(t, merge, proc)
	require.Len(t, outputs, 1)
	require.True(t, outputs[0].Vecs[0].GetIsBinaryString())

	merge.Free(proc, false, nil)
	child.Free(proc, false, nil)
	for _, output := range outputs {
		output.Clean(proc.Mp())
	}
}

func setPrepareParamKindProtocolVersion(t *testing.T, proc *process.Process, version int64) {
	t.Helper()
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
	t.Cleanup(func() {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
	})
}

func mergePreparedMinPartial(
	t *testing.T,
	proc *process.Process,
	partial *batch.Batch,
) *batch.Batch {
	return mergePreparedPartial(t, proc, partial,
		[]aggexec.AggFuncExecExpression{minPreparedParamAgg()})
}

func mergePreparedPartial(
	t *testing.T,
	proc *process.Process,
	partial *batch.Batch,
	aggs []aggexec.AggFuncExecExpression,
) *batch.Batch {
	t.Helper()
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{partial})
	t.Cleanup(func() { child.Free(proc, false, nil) })
	merge := newMergeGroupOp(aggs)
	merge.AppendChild(child)
	t.Cleanup(func() { merge.Free(proc, false, nil) })
	require.NoError(t, merge.Prepare(proc))
	outputs := collectBatches(t, merge, proc)
	require.Len(t, outputs, 1)
	return outputs[0]
}

func TestMergeGroupPreservesPreparedParamKind(t *testing.T) {
	tests := []struct {
		name     string
		partials []preparedPartialSpec
		wantKind vector.PrepareParamKind
	}{
		{
			name: "empty-before-float",
			partials: []preparedPartialSpec{
				{rows: 0, kind: vector.PrepareParamDecimal},
				{rows: 2, kind: vector.PrepareParamFloat},
			},
			wantKind: vector.PrepareParamFloat,
		},
		{
			name: "float-before-empty",
			partials: []preparedPartialSpec{
				{rows: 2, kind: vector.PrepareParamFloat},
				{rows: 0, kind: vector.PrepareParamDecimal},
			},
			wantKind: vector.PrepareParamFloat,
		},
		{
			name: "empty-before-integer",
			partials: []preparedPartialSpec{
				{rows: 0, kind: vector.PrepareParamFloat},
				{rows: 2, kind: vector.PrepareParamInteger},
			},
			wantKind: vector.PrepareParamInteger,
		},
		{
			name: "all-null-before-float",
			partials: []preparedPartialSpec{
				{rows: 1, kind: vector.PrepareParamDecimal, allNull: true},
				{rows: 2, kind: vector.PrepareParamFloat},
			},
			wantKind: vector.PrepareParamFloat,
		},
		{
			name: "string-before-float",
			partials: []preparedPartialSpec{
				{rows: 1, kind: vector.PrepareParamNone},
				{rows: 2, kind: vector.PrepareParamFloat},
			},
			wantKind: vector.PrepareParamNone,
		},
		{
			name: "float-before-string",
			partials: []preparedPartialSpec{
				{rows: 2, kind: vector.PrepareParamFloat},
				{rows: 1, kind: vector.PrepareParamNone},
			},
			// Equal MIN values fold conflicting provenance independent of
			// partial arrival order.
			wantKind: vector.PrepareParamNone,
		},
		{
			name: "float-before-integer",
			partials: []preparedPartialSpec{
				{rows: 2, kind: vector.PrepareParamFloat},
				{rows: 1, kind: vector.PrepareParamInteger},
			},
			// Equal values fold conflicting provenance independent of
			// partial arrival order.
			wantKind: vector.PrepareParamNone,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(func() {
				require.Zero(t, proc.Mp().CurrNB())
				proc.Free()
			})

			partials := make([]*batch.Batch, len(tc.partials))
			for i, spec := range tc.partials {
				partials[i] = buildPreparedMinPartial(t, proc, spec)
			}

			child := colexec.NewMockOperator().WithBatchs(partials)
			t.Cleanup(func() { child.Free(proc, false, nil) })
			merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{minPreparedParamAgg()})
			merge.AppendChild(child)
			t.Cleanup(func() { merge.Free(proc, false, nil) })

			require.NoError(t, merge.Prepare(proc))
			outputs := collectBatches(t, merge, proc)
			require.Len(t, outputs, 1)
			require.Equal(t, "5", outputs[0].Vecs[0].GetStringAt(0))
			require.Equal(t, tc.wantKind, outputs[0].Vecs[0].GetPrepareParamKind())
		})
	}
}

func TestMergeGroupUsesIncomingWinnerPrepareParamKind(t *testing.T) {
	tests := []struct {
		name     string
		partials []preparedPartialSpec
		want     vector.PrepareParamKind
	}{
		{
			name: "later-float-winner",
			partials: []preparedPartialSpec{
				{rows: 1, kind: vector.PrepareParamNone, value: "6"},
				{rows: 1, kind: vector.PrepareParamFloat, value: "5"},
			},
			want: vector.PrepareParamFloat,
		},
		{
			name: "later-ordinary-winner",
			partials: []preparedPartialSpec{
				{rows: 1, kind: vector.PrepareParamFloat, value: "5"},
				{rows: 1, kind: vector.PrepareParamNone, value: "4"},
			},
			want: vector.PrepareParamNone,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(func() {
				require.Zero(t, proc.Mp().CurrNB())
				proc.Free()
			})
			setPrepareParamKindProtocolVersion(t, proc, defines.MORPCVersion12)

			partials := make([]*batch.Batch, len(tc.partials))
			for i, spec := range tc.partials {
				partials[i] = buildPreparedMinPartial(t, proc, spec)
			}
			child := colexec.NewMockOperator().WithBatchs(partials)
			t.Cleanup(func() { child.Free(proc, false, nil) })
			merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{minPreparedParamAgg()})
			merge.AppendChild(child)
			t.Cleanup(func() { merge.Free(proc, false, nil) })

			require.NoError(t, merge.Prepare(proc))
			outputs := collectBatches(t, merge, proc)
			require.Len(t, outputs, 1)
			require.Equal(t, tc.want, outputs[0].Vecs[0].GetPrepareParamKindAt(0))
		})
	}
}

func TestMergeGroupPreservesHeterogeneousPartialProvenance(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	setPrepareParamKindProtocolVersion(t, proc, defines.MORPCVersion12)

	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{0, 1}, nil, proc.Mp())
	input.Vecs[1] = vector.NewVec(types.T_text.ToType())
	for range 2 {
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("5"), false, proc.Mp()))
	}
	input.Vecs[1].SetPrepareParamKinds([]vector.PrepareParamKind{
		vector.PrepareParamFloat,
		vector.PrepareParamNone,
	})
	input.SetRowCount(2)

	partial := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{minTextColumnAgg(1)})
	partial.NeedEval = false
	partial.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	require.NoError(t, partial.Prepare(proc))
	partials := collectBatches(t, partial, proc)
	require.Len(t, partials, 1)
	partialBatch := cloneBatch(t, proc, partials[0])
	partial.Free(proc, false, nil)
	input.Clean(proc.Mp())

	merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{minTextColumnAgg(1)})
	merge.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{partialBatch}))
	require.NoError(t, merge.Prepare(proc))
	outputs := collectBatches(t, merge, proc)
	require.Len(t, outputs, 1)
	seen := make(map[int32]vector.PrepareParamKind)
	keys := vector.MustFixedColNoTypeCheck[int32](outputs[0].Vecs[0])
	for row, key := range keys {
		seen[key] = outputs[0].Vecs[1].GetPrepareParamKindAt(row)
	}
	require.Equal(t, vector.PrepareParamFloat, seen[0])
	require.Equal(t, vector.PrepareParamNone, seen[1])
	merge.Free(proc, false, nil)
	partialBatch.Clean(proc.Mp())
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMergeGroupPartialWireCompatibility(t *testing.T) {
	tests := []struct {
		name          string
		writerVersion int64
		readerVersion int64
		wantKind      vector.PrepareParamKind
		wantErr       string
	}{
		{
			name:          "new writer and new reader",
			writerVersion: defines.MORPCVersion12,
			readerVersion: defines.MORPCVersion12,
			wantKind:      vector.PrepareParamFloat,
		},
		{
			name:          "legacy writer and new reader",
			writerVersion: defines.MORPCVersion11,
			readerVersion: defines.MORPCVersion12,
			wantKind:      vector.PrepareParamNone,
		},
		{
			name:          "new writer and legacy reader",
			writerVersion: defines.MORPCVersion12,
			readerVersion: defines.MORPCVersion11,
			wantErr:       "prepared parameter aggregate trailer requires MORPCVersion12",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(func() {
				require.Zero(t, proc.Mp().CurrNB())
				proc.Free()
			})
			setPrepareParamKindProtocolVersion(t, proc, tc.writerVersion)
			partial := buildPreparedMinPartial(t, proc, preparedPartialSpec{
				rows: 1,
				kind: vector.PrepareParamFloat,
			})
			moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(
				moruntime.MOProtocolVersion, tc.readerVersion)

			if tc.wantErr != "" {
				child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{partial})
				t.Cleanup(func() { child.Free(proc, false, nil) })
				merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{minPreparedParamAgg()})
				merge.AppendChild(child)
				t.Cleanup(func() { merge.Free(proc, true, nil) })
				require.NoError(t, merge.Prepare(proc))
				result, err := vm.Exec(merge, proc)
				require.Nil(t, result.Batch)
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			output := mergePreparedMinPartial(t, proc, partial)
			require.Equal(t, "5", output.Vecs[0].GetStringAt(0))
			require.Equal(t, tc.wantKind, output.Vecs[0].GetPrepareParamKind())
		})
	}
}

func TestOrdinaryGroupPartialKeepsLegacyWireFormat(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() {
		require.Zero(t, proc.Mp().CurrNB())
		proc.Free()
	})
	setPrepareParamKindProtocolVersion(t, proc, defines.MORPCVersion10)
	legacy := buildPartialH0Batch(t, proc, []int32{1, 2})
	t.Cleanup(func() { legacy.Clean(proc.Mp()) })
	moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(
		moruntime.MOProtocolVersion, defines.MORPCVersion12)
	current := buildPartialH0Batch(t, proc, []int32{1, 2})
	t.Cleanup(func() { current.Clean(proc.Mp()) })

	require.Equal(t, legacy.ExtraBuf, current.ExtraBuf)
}

func TestPrepareParamKindTrailerFollowsAllAggregateStates(t *testing.T) {
	tests := []struct {
		name     string
		aggs     []aggexec.AggFuncExecExpression
		minIndex int
	}{
		{
			name: "preserving aggregate first",
			aggs: []aggexec.AggFuncExecExpression{
				minPreparedParamAgg(), countStarAgg(),
			},
			minIndex: 0,
		},
		{
			name: "preserving aggregate last",
			aggs: []aggexec.AggFuncExecExpression{
				countStarAgg(), minPreparedParamAgg(),
			},
			minIndex: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(func() {
				require.Zero(t, proc.Mp().CurrNB())
				proc.Free()
			})
			partial := buildPreparedPartial(t, proc, preparedPartialSpec{
				rows: 2,
				kind: vector.PrepareParamFloat,
			}, tc.aggs)
			output := mergePreparedPartial(t, proc, partial, tc.aggs)

			require.Equal(t, "5", output.Vecs[tc.minIndex].GetStringAt(0))
			require.Equal(t, vector.PrepareParamFloat,
				output.Vecs[tc.minIndex].GetPrepareParamKind())
			countIndex := 1 - tc.minIndex
			require.Equal(t, int64(2),
				vector.MustFixedColNoTypeCheck[int64](output.Vecs[countIndex])[0])
		})
	}
}

func TestMergeGroupRejectsInvalidPrepareParamKindState(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func([]byte, int) []byte
		wantErr error
	}{
		{
			name: "out-of-range",
			mutate: func(extra []byte, stateOffset int) []byte {
				extra[stateOffset] = byte(vector.PrepareParamBoolean) + 2
				return extra
			},
		},
		{
			name: "truncated",
			mutate: func(extra []byte, stateOffset int) []byte {
				return extra[:stateOffset]
			},
			wantErr: io.EOF,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(func() {
				require.Zero(t, proc.Mp().CurrNB())
				proc.Free()
			})

			partial := buildPreparedMinPartial(t, proc, preparedPartialSpec{
				rows: 1,
				kind: vector.PrepareParamFloat,
			})
			trailerOffset := bytes.LastIndex(partial.ExtraBuf, []byte{
				prepareParamKindTrailerMagic0,
				prepareParamKindTrailerMagic1,
				prepareParamKindTrailerMagic2,
			})
			require.NotEqual(t, -1, trailerOffset)
			// The one-row prepared partial can use the compact v1 scalar
			// record; heterogeneous rows use the v2 marker + row count form.
			stateOffset := trailerOffset + 8
			if partial.ExtraBuf[trailerOffset+3] == prepareParamKindTrailerRowsVersion {
				stateOffset += 5 // marker plus int32 row count
			}
			partial.ExtraBuf = tc.mutate(partial.ExtraBuf, stateOffset)

			child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{partial})
			t.Cleanup(func() { child.Free(proc, false, nil) })
			merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{minPreparedParamAgg()})
			merge.AppendChild(child)
			t.Cleanup(func() { merge.Free(proc, true, tc.wantErr) })
			require.NoError(t, merge.Prepare(proc))

			result, err := vm.Exec(merge, proc)
			require.Nil(t, result.Batch)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				if partial.ExtraBuf[trailerOffset+3] == prepareParamKindTrailerRowsVersion {
					require.ErrorContains(t, err, "invalid aggregate prepared parameter row kind 6")
				} else {
					require.ErrorContains(t, err, "invalid aggregate prepared parameter state 6")
				}
			}
		})
	}
}

func TestMergeGroupRejectsInvalidPrepareParamKindTrailer(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func([]byte, int) []byte
		wantErr string
	}{
		{
			name: "invalid magic",
			mutate: func(extra []byte, trailerOffset int) []byte {
				extra[trailerOffset] = 'X'
				return extra
			},
			wantErr: "invalid aggregate prepared parameter trailer",
		},
		{
			name: "unsupported version",
			mutate: func(extra []byte, trailerOffset int) []byte {
				extra[trailerOffset+3] = 4
				return extra
			},
			wantErr: "unsupported aggregate prepared parameter trailer version 4",
		},
		{
			name: "aggregate count mismatch",
			mutate: func(extra []byte, trailerOffset int) []byte {
				count := int32(2)
				copy(extra[trailerOffset+4:trailerOffset+8], types.EncodeInt32(&count))
				return extra
			},
			wantErr: "aggregate prepared parameter count 2 does not match 1",
		},
		{
			name: "unexpected trailing bytes",
			mutate: func(extra []byte, _ int) []byte {
				return append(extra, 0)
			},
			wantErr: "unexpected aggregate prepared parameter trailer bytes",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(func() {
				require.Zero(t, proc.Mp().CurrNB())
				proc.Free()
			})

			partial := buildPreparedMinPartial(t, proc, preparedPartialSpec{
				rows: 1,
				kind: vector.PrepareParamFloat,
			})
			trailerOffset := bytes.LastIndex(partial.ExtraBuf, []byte{
				prepareParamKindTrailerMagic0,
				prepareParamKindTrailerMagic1,
				prepareParamKindTrailerMagic2,
			})
			require.NotEqual(t, -1, trailerOffset)
			partial.ExtraBuf = tc.mutate(partial.ExtraBuf, trailerOffset)

			child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{partial})
			t.Cleanup(func() { child.Free(proc, false, nil) })
			merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{minPreparedParamAgg()})
			merge.AppendChild(child)
			t.Cleanup(func() { merge.Free(proc, true, nil) })
			require.NoError(t, merge.Prepare(proc))

			result, err := vm.Exec(merge, proc)
			require.Nil(t, result.Batch)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestGroupRejectsInvalidPrepareParamKindState(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() {
		require.Zero(t, proc.Mp().CurrNB())
		proc.Free()
	})

	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(nil, nil, proc.Mp())
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	t.Cleanup(func() { child.Free(proc, true, nil) })
	partial := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{minPreparedParamAgg()})
	partial.NeedEval = false
	partial.AppendChild(child)
	t.Cleanup(func() { partial.Free(proc, true, nil) })

	require.NoError(t, partial.Prepare(proc))
	partial.ctr.prepareParamKind.Observe(0, vector.PrepareParamKind(255))
	result, err := vm.Exec(partial, proc)
	require.Nil(t, result.Batch)
	require.ErrorContains(t, err, "invalid aggregate prepared parameter kind 255")
}

func buildPartialGroupBatches(t *testing.T, proc *process.Process, sources []*batch.Batch, forceGroupTypesNotNull bool) []*batch.Batch {
	t.Helper()

	groupBy := []*plan.Expr{colExpr(0, types.T_int32), colExpr(1, types.T_int32)}
	partialBatches := make([]*batch.Batch, 0, len(sources))
	for _, source := range sources {
		partial := newGroupOp(proc, groupBy, []aggexec.AggFuncExecExpression{countStarAgg()})
		partial.NeedEval = false
		partial.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{source}))
		require.NoError(t, partial.Prepare(proc))
		rawPartialBatches := collectBatches(t, partial, proc)
		require.Len(t, rawPartialBatches, 1)
		for _, bat := range rawPartialBatches {
			cloned := cloneBatch(t, proc, bat)
			if forceGroupTypesNotNull {
				cloned.Vecs[0].GetType().SetNotNull(true)
				cloned.Vecs[1].GetType().SetNotNull(true)
			}
			partialBatches = append(partialBatches, cloned)
		}
		partial.Free(proc, false, nil)
	}
	return partialBatches
}

func assertMergedTicketCounts(t *testing.T, finals []*batch.Batch, wantNull, wantNonNull int64) {
	t.Helper()

	var nullCount, nonNullCount int64
	totalRows := 0
	for _, final := range finals {
		if final == nil || final.RowCount() == 0 || len(final.Vecs) == 0 {
			continue
		}
		require.Len(t, final.Vecs, 3)

		tickets := vector.MustFixedColNoTypeCheck[int32](final.Vecs[0])
		customers := vector.MustFixedColNoTypeCheck[int32](final.Vecs[1])
		counts := vector.MustFixedColNoTypeCheck[int64](final.Vecs[2])
		totalRows += final.RowCount()

		for i := 0; i < final.RowCount(); i++ {
			require.Equal(t, int32(1), tickets[i])
			if final.Vecs[1].GetNulls().Contains(uint64(i)) {
				nullCount = counts[i]
				continue
			}
			require.Equal(t, int32(10), customers[i])
			nonNullCount = counts[i]
		}
	}

	require.Equal(t, 2, totalRows)
	require.Equal(t, wantNull, nullCount)
	require.Equal(t, wantNonNull, nonNullCount)
}

func TestGroupString(t *testing.T) {
	proc := testutil.NewProcess(t)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{sumAgg(0)})
	buf := new(bytes.Buffer)
	g.String(buf)
	require.NotEmpty(t, buf.String())
}

func TestGroupPrepare(t *testing.T) {
	proc := testutil.NewProcess(t)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{sumAgg(0)})
	resetChildren(g, proc)
	require.NoError(t, g.Prepare(proc))
	g.Free(proc, false, nil)
}

// TestGroupByWithSum: GROUP BY a, SUM(a) — two distinct rows → two groups.
func TestGroupByWithSum(t *testing.T) {
	proc := testutil.NewProcess(t)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{sumAgg(0)})
	resetChildren(g, proc)
	require.NoError(t, g.Prepare(proc))

	var rowCount, execCalls int
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		execCalls++
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		rowCount += result.Batch.RowCount()
	}
	// mock batch has 2 rows with distinct values (1, 1000) → 2 groups
	require.Equal(t, 2, rowCount)
	require.Equal(t, execCalls, g.OpAnalyzer.GetOpStats().CallNum)

	g.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestGroupNoGroupBy: no GROUP BY, just COUNT(*) → single row result.
func TestGroupNoGroupBy(t *testing.T) {
	proc := testutil.NewProcess(t)
	g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{countStarAgg()})
	resetChildren(g, proc)
	require.NoError(t, g.Prepare(proc))

	var rowCount int
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		rowCount += result.Batch.RowCount()
	}
	require.Equal(t, 1, rowCount)

	g.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestGroupUsesReducedHashKeyAndKeepsFullOutput(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1, 2}, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector([]int32{10, 11, 20}, nil, proc.Mp())
	input.SetRowCount(3)

	g := newGroupOp(proc,
		[]*plan.Expr{colExpr(0, types.T_int32), colExpr(1, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countStarAgg()})
	g.GroupByHashKey = []int32{0}
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	require.NoError(t, g.Prepare(proc))
	finals := collectBatches(t, g, proc)
	require.Len(t, finals, 1)
	require.Equal(t, 2, finals[0].RowCount())
	require.Len(t, finals[0].Vecs, 3)
	require.Equal(t, []int32{1, 2}, vector.MustFixedColNoTypeCheck[int32](finals[0].Vecs[0]))
	require.Equal(t, []int32{10, 20}, vector.MustFixedColNoTypeCheck[int32](finals[0].Vecs[1]))
	require.Equal(t, []int64{2, 1}, vector.MustFixedColNoTypeCheck[int64](finals[0].Vecs[2]))
	g.Free(proc, false, nil)
	require.Zero(t, proc.Mp().CurrNB())
}

func BenchmarkGroupPhysicalHashKey(b *testing.B) {
	const (
		rows   = 8192
		groups = rows / 2
	)

	keys := make([]int32, rows)
	wide := make([]string, rows)
	padding := strings.Repeat("x", 96)
	for i := range rows {
		key := i % groups
		keys[i] = int32(key)
		wide[i] = fmt.Sprintf("%08d-%s", key, padding)
	}

	for _, test := range []struct {
		name    string
		hashKey []int32
	}{
		{name: "full-logical-key"},
		{name: "integer-primary-key", hashKey: []int32{0}},
	} {
		b.Run(test.name, func(b *testing.B) {
			proc := testutil.NewProcess(b)
			defer proc.Free()
			b.ReportAllocs()
			b.SetBytes(rows)

			for b.Loop() {
				b.StopTimer()
				input := batch.NewWithSize(5)
				input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
				for i := 1; i < len(input.Vecs); i++ {
					input.Vecs[i] = testutil.MakeVarcharVector(wide, nil, proc.Mp())
				}
				input.SetRowCount(rows)

				groupBy := []*plan.Expr{colExpr(0, types.T_int32)}
				for i := int32(1); i < 5; i++ {
					groupBy = append(groupBy, colExpr(i, types.T_varchar))
				}
				g := newGroupOp(proc, groupBy, []aggexec.AggFuncExecExpression{countStarAgg()})
				g.GroupByHashKey = test.hashKey
				g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
				b.StartTimer()

				if err := g.Prepare(proc); err != nil {
					b.Fatal(err)
				}
				outputRows := 0
				for {
					result, err := vm.Exec(g, proc)
					if err != nil {
						b.Fatal(err)
					}
					if result.Status == vm.ExecStop || result.Batch == nil {
						break
					}
					outputRows += result.Batch.RowCount()
				}
				if outputRows != groups {
					b.Fatalf("unexpected group count: got %d, want %d", outputRows, groups)
				}

				b.StopTimer()
				g.Free(proc, false, nil)
				if allocated := proc.Mp().CurrNB(); allocated != 0 {
					b.Fatalf("group leaked %d bytes", allocated)
				}
				b.StartTimer()
			}
		})
	}
}

func TestH0NeverRequestsGenericGroupSpill(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer func() {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	}()

	for _, tc := range []struct {
		name      string
		spillMem  int64
		allocated int
	}{
		{
			name:     "group count debug threshold",
			spillMem: 256,
		},
		{
			name:      "byte threshold",
			spillMem:  10 << 10,
			allocated: 16 << 10,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctr := container{
				mp:       proc.Mp(),
				mtyp:     H0,
				spillMem: tc.spillMem,
			}
			analyzer := process.NewAnalyzer(0, false, false, "group")

			var retained []byte
			if tc.allocated > 0 {
				var err error
				retained, err = proc.Mp().Alloc(tc.allocated, false)
				require.NoError(t, err)
				defer proc.Mp().Free(retained)
			}

			wantMem := ctr.memUsed()
			require.False(t, ctr.needSpill(analyzer))
			require.Equal(t, wantMem, analyzer.GetOpStats().MemorySize)
		})
	}
}

func TestGroupRejectsInvalidReducedHashKey(t *testing.T) {
	tests := []struct {
		name          string
		hashKey       []int32
		groupingFlags []bool
	}{
		{name: "not a strict subset", hashKey: []int32{0, 1, 2}},
		{name: "not ordered", hashKey: []int32{1, 0}},
		{name: "out of range", hashKey: []int32{3}},
		{name: "grouping set", hashKey: []int32{0}, groupingFlags: []bool{true, false, true}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			g := newGroupOp(proc,
				[]*plan.Expr{
					colExpr(0, types.T_int32), colExpr(1, types.T_int32), colExpr(2, types.T_int32),
				}, nil)
			g.GroupByHashKey = test.hashKey
			g.GroupingFlag = test.groupingFlags
			require.Error(t, g.Prepare(proc))
			g.Free(proc, true, nil)
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestMergeGroupUsesReducedHashKey(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	makeInput := func(payload int32) *batch.Batch {
		input := batch.NewWithSize(2)
		input.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
		input.Vecs[1] = testutil.MakeInt32Vector([]int32{payload}, nil, proc.Mp())
		input.SetRowCount(1)
		return input
	}

	partials := make([]*batch.Batch, 0, 2)
	for _, payload := range []int32{10, 11} {
		input := makeInput(payload)
		partial := newGroupOp(proc,
			[]*plan.Expr{colExpr(0, types.T_int32), colExpr(1, types.T_int32)},
			[]aggexec.AggFuncExecExpression{countStarAgg()})
		partial.NeedEval = false
		partial.GroupByHashKey = []int32{0}
		partial.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
		require.NoError(t, partial.Prepare(proc))
		outputs := collectBatches(t, partial, proc)
		require.Len(t, outputs, 1)
		partials = append(partials, cloneBatch(t, proc, outputs[0]))
		partial.Free(proc, false, nil)
		input.Clean(proc.Mp())
	}

	merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{countStarAgg()})
	merge.GroupByHashKey = []int32{0}
	merge.AppendChild(colexec.NewMockOperator().WithBatchs(partials))
	require.NoError(t, merge.Prepare(proc))
	finals := collectBatches(t, merge, proc)
	require.Len(t, finals, 1)
	require.Equal(t, 1, finals[0].RowCount())
	require.Equal(t, int32(1), vector.MustFixedColNoTypeCheck[int32](finals[0].Vecs[0])[0])
	require.Equal(t, int32(10), vector.MustFixedColNoTypeCheck[int32](finals[0].Vecs[1])[0])
	require.Equal(t, int64(2), vector.MustFixedColNoTypeCheck[int64](finals[0].Vecs[2])[0])
	merge.Free(proc, false, nil)
	for _, partial := range partials {
		partial.Clean(proc.Mp())
	}
	require.Zero(t, proc.Mp().CurrNB())
}

func TestGroupSpillReloadUsesReducedHashKey(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	const groups = 8192
	keys := make([]int32, groups*2)
	payloads := make([]int32, groups*2)
	for i := range groups {
		keys[i], keys[i+groups] = int32(i), int32(i)
		payloads[i], payloads[i+groups] = int32(i), int32(i+groups)
	}
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector(payloads, nil, proc.Mp())
	input.SetRowCount(len(keys))

	g := newGroupOp(proc,
		[]*plan.Expr{colExpr(0, types.T_int32), colExpr(1, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countStarAgg()})
	g.GroupByHashKey = []int32{0}
	g.SpillMem = 64
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	require.NoError(t, g.Prepare(proc))

	rows := 0
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		rows += result.Batch.RowCount()
		for _, count := range vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[2]) {
			require.Equal(t, int64(2), count)
		}
	}
	require.Equal(t, groups, rows)
	require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillReloadRecords"])
	g.Free(proc, false, nil)
	require.Zero(t, proc.Mp().CurrNB())
}

func TestGroupSpillPreservesPerGroupPrepareParamKind(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	const groups = 2
	const rows = 1024
	keys := make([]int32, rows)
	kinds := make([]vector.PrepareParamKind, rows)
	for i := range keys {
		keys[i] = int32(i % groups)
		if keys[i]%2 == 0 {
			kinds[i] = vector.PrepareParamFloat
		}
	}
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
	input.Vecs[1] = vector.NewVec(types.T_text.ToType())
	for range keys {
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("5"), false, proc.Mp()))
	}
	input.Vecs[1].SetPrepareParamKinds(kinds)
	input.SetRowCount(rows)

	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{minTextColumnAgg(1)})
	g.SpillMem = 10 << 10
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	require.NoError(t, g.Prepare(proc))
	seen := make(map[int32]vector.PrepareParamKind)
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		if result.Batch.RowCount() != 0 {
			outKeys := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])
			for row, key := range outKeys {
				seen[key] = result.Batch.Vecs[1].GetPrepareParamKindAt(row)
			}
		}
	}
	require.Len(t, seen, groups)
	for key, kind := range seen {
		want := vector.PrepareParamNone
		if key%2 == 0 {
			want = vector.PrepareParamFloat
		}
		require.Equal(t, want, kind, "group %d", key)
	}
	g.Free(proc, false, nil)
	input.Clean(proc.Mp())
	require.Zero(t, proc.Mp().CurrNB())
}

func TestGroupSpillPreservesUniformPreparedParamKind(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	const groups = 2
	const rows = 1024
	keys := make([]int32, rows)
	for i := range keys {
		keys[i] = int32(i % groups)
	}
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
	input.Vecs[1] = vector.NewVec(types.T_text.ToType())
	for range keys {
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("5"), false, proc.Mp()))
	}
	input.Vecs[1].SetPrepareParamKind(vector.PrepareParamFloat)
	input.SetRowCount(rows)

	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{minTextColumnAgg(1)})
	g.SpillMem = 10 << 10
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	require.NoError(t, g.Prepare(proc))
	seen := make(map[int32]vector.PrepareParamKind)
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		if result.Batch.RowCount() != 0 {
			outKeys := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])
			for row, key := range outKeys {
				seen[key] = result.Batch.Vecs[1].GetPrepareParamKindAt(row)
			}
		}
	}
	require.Len(t, seen, groups)
	for key, kind := range seen {
		require.Equal(t, vector.PrepareParamFloat, kind, "group %d", key)
	}
	g.Free(proc, false, nil)
	input.Clean(proc.Mp())
	require.Zero(t, proc.Mp().CurrNB())
}

func TestGroupSpillPreservesGroupKeyPreparedParamKind(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	const rows = 1024
	input := batch.NewWithSize(2)
	input.Vecs[0] = vector.NewVec(types.T_text.ToType())
	input.Vecs[1] = vector.NewVec(types.T_text.ToType())
	for i := 0; i < rows; i++ {
		key := []byte("0")
		if i%2 != 0 {
			key = []byte("1")
		}
		require.NoError(t, vector.AppendBytes(input.Vecs[0], key, false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("5"), false, proc.Mp()))
	}
	input.Vecs[0].SetPrepareParamKind(vector.PrepareParamFloat)
	input.Vecs[1].SetPrepareParamKind(vector.PrepareParamFloat)
	input.SetRowCount(rows)

	g := newGroupOp(proc, []*plan.Expr{
		colExpr(0, types.T_text),
	}, []aggexec.AggFuncExecExpression{minTextColumnAgg(1)})
	g.SpillMem = 10 << 10
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	require.NoError(t, g.Prepare(proc))

	seen := make(map[string]vector.PrepareParamKind)
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		keys := result.Batch.Vecs[0]
		for row := 0; row < keys.Length(); row++ {
			seen[string(keys.GetBytesAt(row))] = keys.GetPrepareParamKindAt(row)
		}
	}
	require.Equal(t, map[string]vector.PrepareParamKind{
		"0": vector.PrepareParamFloat,
		"1": vector.PrepareParamFloat,
	}, seen)
	g.Free(proc, false, nil)
	input.Clean(proc.Mp())
	require.Zero(t, proc.Mp().CurrNB())
}

func TestH0OrderedGroupConcatSpillsIndependently(t *testing.T) {
	proc := testutil.NewProcess(t)

	const rows = 512
	values := make([]string, rows)
	orderKeys := make([]int64, rows)
	for i := range rows {
		values[i] = fmt.Sprintf("%04d-%s", i, strings.Repeat("x", 256))
		orderKeys[i] = int64(rows - i)
	}
	input := batch.NewWithSize(3)
	input.Vecs[0] = testutil.MakeInt32Vector(make([]int32, rows), nil, proc.Mp())
	input.Vecs[1] = testutil.MakeVarcharVector(values, nil, proc.Mp())
	input.Vecs[2] = testutil.MakeInt64Vector(orderKeys, nil, proc.Mp())
	input.SetRowCount(rows)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{orderedGroupConcatAgg(false)})
	// ConfigureGroupConcatH0Spill clamps this to its independent run-size floor.
	g.SpillMem = 1
	g.AppendChild(child)
	t.Cleanup(func() {
		g.Free(proc, false, nil)
		child.Free(proc, false, nil)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	require.NoError(t, g.Prepare(proc))
	outputs := collectBatches(t, g, proc)
	require.Len(t, outputs, 1)
	parts := strings.Split(string(outputs[0].Vecs[0].GetBytesAt(0)), "|")
	require.Len(t, parts, rows)
	require.Equal(t, values[rows-1], parts[0])
	require.Equal(t, values[0], parts[rows-1])
	require.Positive(t, g.OpAnalyzer.GetOpStats().SpillRows)
	require.Zero(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillWriteCalls"])
}

func TestGroupSpillReloadKeepsPreallocationBounded(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	const rows = 65536
	values := make([]int32, rows)
	for i := range values {
		values[i] = int32(i)
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	input.SetRowCount(rows)

	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{countStarAgg()})
	// Values below 10K are interpreted as a group-count spill threshold.
	// One large input batch therefore establishes a 65,536-group high-water
	// mark, while each of the 32 reload buckets is only about 2K groups.
	g.SpillMem = 4096
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	require.NoError(t, g.Prepare(proc))

	var outputRows int
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		outputRows += result.Batch.RowCount()
	}

	require.Equal(t, rows, outputRows)
	require.Equal(t, uint64(rows), g.ctr.spillHashPreAllocSize)
	extra := g.OpAnalyzer.GetOpStats().ExtraStats
	require.Positive(t, extra["GroupSpillWriteCalls"])
	require.Positive(t, extra["GroupSpillWriteNanos"])
	require.Positive(t, extra["GroupSpillSerializedBytes"])
	require.Positive(t, extra["GroupSpillAggChunkHeadersOmitted"])
	require.Positive(t, extra["GroupSpillReloadBuckets"])
	require.Positive(t, extra["GroupSpillReloadRecords"])
	require.Positive(t, extra["GroupSpillAggExecReuseRecords"])
	require.Equal(t, int64(rows), extra["GroupSpillReloadRows"])
	require.Equal(t, int64(rows), extra["GroupSpillMaxGroups"])
	require.Greater(t, extra["GroupSpillPreallocRows"], int64(aggHtPreAllocSize))
	require.Positive(t, extra["GroupSpillReloadNanos"])
	require.Equal(t, int64(1), extra["GroupHashBuildGrowthBatches"])
	require.Positive(t, extra["GroupHashBuildGrowthBytes"])
	g.Free(proc, false, nil)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestGroupSpillReloadHonorsCancellationAfterInput(t *testing.T) {
	proc := testutil.NewProcess(t)

	const rows = 65536
	values := make([]int32, rows)
	for i := range values {
		values[i] = int32(i)
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	input.SetRowCount(rows)

	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}).WithEndOfDataCallback(cancel)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{countStarAgg()})
	g.SpillMem = 4096
	g.AppendChild(child)
	require.NoError(t, g.Prepare(proc))

	t.Cleanup(func() {
		g.Free(proc, true, context.Canceled)
		child.Free(proc, true, context.Canceled)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	result, err := vm.Exec(g, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Zero(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillReloadBuckets"])
}

func TestGroupSpillWriteHonorsCancellationAfterInputBatch(t *testing.T) {
	proc := testutil.NewProcess(t)
	baseCtx := proc.Ctx
	ctx, cancel := context.WithCancel(baseCtx)
	proc.Ctx = ctx

	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{4, 1, 3, 2}, nil, proc.Mp())
	input.SetRowCount(4)
	child := colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{input}).
		WithBatchCallback(func(int) { cancel() })
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{countStarAgg()})
	g.SpillMem = 1
	g.AppendChild(child)
	require.NoError(t, g.Prepare(proc))

	t.Cleanup(func() {
		proc.Ctx = baseCtx
		g.Free(proc, true, context.Canceled)
		child.Free(proc, true, context.Canceled)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	result, err := vm.Exec(g, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Zero(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])
	require.Nil(t, g.ctr.currentSpillBkt)
}

func TestGroupStreamingDoesNotPublishAfterInputCancellation(t *testing.T) {
	proc := testutil.NewProcess(t)
	baseCtx := proc.Ctx
	ctx, cancel := context.WithCancel(baseCtx)
	proc.Ctx = ctx

	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{4, 1, 3, 2}, nil, proc.Mp())
	input.SetRowCount(4)
	child := colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{input}).
		WithBatchCallback(func(int) { cancel() })
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{countStarAgg()})
	g.NeedEval = false
	g.SpillMem = 1
	g.AppendChild(child)
	require.NoError(t, g.Prepare(proc))

	t.Cleanup(func() {
		proc.Ctx = baseCtx
		g.Free(proc, true, context.Canceled)
		child.Free(proc, true, context.Canceled)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	result, err := vm.Exec(g, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Equal(t, vm.Eval, g.ctr.state)
	require.Zero(t, g.ctr.currBatchIdx)
	require.NotEmpty(t, g.ctr.groupByBatches)
	require.Empty(t, g.ctr.groupByBatches[0].ExtraBuf)

	g.Reset(proc, true, context.Canceled)
	require.Nil(t, g.ctr.mp)
	child.Free(proc, true, context.Canceled)

	proc.Ctx = baseCtx
	fresh := batch.NewWithSize(1)
	fresh.Vecs[0] = testutil.MakeInt32Vector([]int32{8, 5, 7, 6}, nil, proc.Mp())
	fresh.SetRowCount(4)
	child = colexec.NewMockOperator().WithBatchs([]*batch.Batch{fresh})
	g.Children = nil
	g.AppendChild(child)
	require.NoError(t, g.Prepare(proc))

	freshResult, err := vm.Exec(g, proc)
	require.NoError(t, err)
	require.NotNil(t, freshResult.Batch)
	require.Equal(t, 4, freshResult.Batch.RowCount())

	end, err := vm.Exec(g, proc)
	require.NoError(t, err)
	require.Nil(t, end.Batch)
}

func TestGroupSpillWriteStopsAtBucketBoundary(t *testing.T) {
	proc := testutil.NewProcess(t)
	baseCtx := proc.Ctx
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{countStarAgg()})
	g.SpillMem = 1
	require.NoError(t, g.Prepare(proc))

	const rows = 1024
	values := make([]int32, rows)
	for i := range values {
		values[i] = int32(i)
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	input.SetRowCount(rows)
	cleaned := false
	t.Cleanup(func() {
		if cleaned {
			return
		}
		proc.Ctx = baseCtx
		g.Free(proc, true, context.Canceled)
		input.Clean(proc.Mp())
		proc.Free()
	})
	needSpill, err := g.buildOneBatch(proc, input)
	require.NoError(t, err)
	require.True(t, needSpill)

	hashCodes := make([]uint64, g.ctr.hr.Hash.GroupCount())
	hashCodes = g.ctr.hr.Hash.FillGroupHashes(hashCodes)
	g.ctr.computeBucketIndex(hashCodes, 1)
	usedBuckets := make(map[int]struct{})
	firstBucket := spillNumBuckets
	for _, hashCode := range hashCodes {
		bucketIndex := int(hashCode & (spillNumBuckets - 1))
		usedBuckets[bucketIndex] = struct{}{}
		firstBucket = min(firstBucket, bucketIndex)
	}
	require.Greater(t, len(usedBuckets), 1)

	g.ctr.currentSpillBkt = make([]*spillBucket, spillNumBuckets)
	for i := range g.ctr.currentSpillBkt {
		g.ctr.currentSpillBkt[i] = &spillBucket{lv: 1, name: fmt.Sprintf("cancel-boundary-%d", i)}
	}
	file, err := os.CreateTemp(t.TempDir(), "group-cancel-boundary-*")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(baseCtx)
	writer := &cancelAfterWriteWriter{cancel: cancel}
	g.ctr.currentSpillBkt[firstBucket].file = file
	g.ctr.currentSpillBkt[firstBucket].writer = bufio.NewWriterSize(writer, 1)
	proc.Ctx = ctx

	_, _, err = g.ctr.spillDataToDisk(proc, g.OpAnalyzer, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, writer.writes)
	require.Equal(t, int64(1), g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])

	proc.Ctx = baseCtx
	g.Free(proc, true, context.Canceled)
	input.Clean(proc.Mp())
	proc.Free()
	cleaned = true
	require.Zero(t, proc.Mp().CurrNB())
}

func TestGroupSpillWriteStopsAfterLastBucket(t *testing.T) {
	proc := testutil.NewProcess(t)
	baseCtx := proc.Ctx
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{countStarAgg()})
	g.SpillMem = 1
	require.NoError(t, g.Prepare(proc))

	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	input.SetRowCount(1)
	t.Cleanup(func() {
		proc.Ctx = baseCtx
		if g.ctr.mp != nil {
			g.Free(proc, true, context.Canceled)
		}
		input.Clean(proc.Mp())
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	needSpill, err := g.buildOneBatch(proc, input)
	require.NoError(t, err)
	require.True(t, needSpill)
	hashCodes := make([]uint64, g.ctr.hr.Hash.GroupCount())
	hashCodes = g.ctr.hr.Hash.FillGroupHashes(hashCodes)
	g.ctr.computeBucketIndex(hashCodes, 1)
	require.Len(t, hashCodes, 1)
	bucketIndex := int(hashCodes[0] & (spillNumBuckets - 1))

	g.ctr.currentSpillBkt = make([]*spillBucket, spillNumBuckets)
	for i := range g.ctr.currentSpillBkt {
		g.ctr.currentSpillBkt[i] = &spillBucket{lv: 1, name: fmt.Sprintf("cancel-last-%d", i)}
	}
	file, err := os.CreateTemp(t.TempDir(), "group-cancel-last-*")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(baseCtx)
	writer := &cancelAfterWriteWriter{cancel: cancel}
	g.ctr.currentSpillBkt[bucketIndex].file = file
	g.ctr.currentSpillBkt[bucketIndex].writer = bufio.NewWriterSize(writer, 1)
	proc.Ctx = ctx

	_, _, err = g.ctr.spillDataToDisk(proc, g.OpAnalyzer, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, writer.writes)
	require.Equal(t, int64(1), g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])
}

func TestGroupSpillReloadCancellationCleansAndReuses(t *testing.T) {
	const (
		cancelAtLoadEntry = iota
		cancelDuringBucketTransfer
		cancelAfterBucketTransfer
		cancelAfterFirstRecord
	)

	proc := testutil.NewProcess(t)
	baseCtx := proc.Ctx

	const rows = 65536
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{countStarAgg()})
	g.SpillMem = 4096
	var spillFiles []*os.File
	var child *colexec.MockOperator
	var nonEmptyBuckets int
	installSpillInput := func(cancelPoint int) {
		values := make([]int32, rows)
		for i := range values {
			values[i] = int32(i)
		}
		input := batch.NewWithSize(1)
		input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
		input.SetRowCount(rows)
		spillFiles = nil
		nonEmptyBuckets = 0
		child = colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}).WithEndOfDataCallback(func() {
			for _, bkt := range g.ctr.currentSpillBkt {
				if bkt.file != nil {
					spillFiles = append(spillFiles, bkt.file)
				}
				if bkt.cnt > 0 {
					nonEmptyBuckets++
				}
			}
			checksAfterEOF := 3 // EOF boundary, final empty spill, load entry.
			switch cancelPoint {
			case cancelDuringBucketTransfer:
				// Pass the first bucket-flush check and cancel before the second.
				checksAfterEOF = 5
			case cancelAfterBucketTransfer:
				checksAfterEOF = nonEmptyBuckets + 4
			case cancelAfterFirstRecord:
				// Also pass every bucket flush, the post-transfer boundary, and
				// the first record checkpoint; cancel before the second record.
				checksAfterEOF = nonEmptyBuckets + 6
			}
			proc.Ctx = newCancelOnDoneCheckContext(baseCtx, checksAfterEOF)
		})
		g.Children = nil
		g.AppendChild(child)
	}
	installSpillInput(cancelAtLoadEntry)
	require.NoError(t, g.Prepare(proc))

	t.Cleanup(func() {
		proc.Ctx = baseCtx
		g.Free(proc, false, nil)
		child.Free(proc, false, nil)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	// Cancel exactly at loadSpilledData entry. Current buckets have not
	// transferred yet and Reset remains their sole cleanup owner.
	result, err := g.Call(proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Zero(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillReloadBuckets"])
	require.NotEmpty(t, g.ctr.currentSpillBkt)
	require.NotEmpty(t, spillFiles)
	entryFiles := append([]*os.File(nil), spillFiles...)

	g.Reset(proc, true, context.Canceled)
	for _, file := range entryFiles {
		_, statErr := file.Stat()
		require.Error(t, statErr)
	}
	child.Free(proc, true, context.Canceled)

	proc.Ctx = baseCtx
	installSpillInput(cancelDuringBucketTransfer)
	require.NoError(t, g.Prepare(proc))

	// One bucket has transferred to spillBkts; all remaining bucket files stay
	// uniquely owned by currentSpillBkt.
	result, err = g.Call(proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Equal(t, spillNumBuckets, nonEmptyBuckets)
	require.NotNil(t, g.ctr.spillBkts)
	require.Equal(t, 1, g.ctr.spillBkts.Len())
	require.NotNil(t, g.ctr.currentSpillBkt)
	transferredSlots := 0
	for _, bkt := range g.ctr.currentSpillBkt {
		if bkt == nil {
			transferredSlots++
		}
	}
	require.Equal(t, 1, transferredSlots)

	transferFiles := append([]*os.File(nil), spillFiles...)
	g.Reset(proc, true, context.Canceled)
	for _, file := range transferFiles {
		_, statErr := file.Stat()
		require.Error(t, statErr)
	}
	child.Free(proc, true, context.Canceled)

	proc.Ctx = baseCtx
	installSpillInput(cancelAfterBucketTransfer)
	require.NoError(t, g.Prepare(proc))

	// All buckets have transferred to spillBkts, but cancellation is observed
	// before a bucket is popped for reload.
	result, err = g.Call(proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Nil(t, g.ctr.currentSpillBkt)
	require.NotNil(t, g.ctr.spillBkts)
	require.Equal(t, nonEmptyBuckets, g.ctr.spillBkts.Len())
	require.Zero(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillReloadBuckets"])

	postTransferFiles := append([]*os.File(nil), spillFiles...)
	g.Reset(proc, true, context.Canceled)
	for _, file := range postTransferFiles {
		_, statErr := file.Stat()
		require.Error(t, statErr)
	}
	child.Free(proc, true, context.Canceled)

	proc.Ctx = baseCtx
	installSpillInput(cancelAfterFirstRecord)
	require.NoError(t, g.Prepare(proc))

	// After EOF: pass the phase boundaries and every bucket ownership transfer,
	// then process one record and cancel at the next per-record checkpoint.
	result, err = g.Call(proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillReloadRecords"])
	require.NotEmpty(t, spillFiles)
	require.NotNil(t, g.ctr.spillBkts)
	require.Positive(t, g.ctr.spillBkts.Len())

	g.Reset(proc, true, context.Canceled)
	require.Nil(t, g.ctr.mp)
	require.Nil(t, g.ctr.aggList)
	require.Nil(t, g.ctr.spillAggList)
	require.Nil(t, g.ctr.currentSpillBkt)
	for _, file := range spillFiles {
		_, statErr := file.Stat()
		require.Error(t, statErr)
	}
	child.Free(proc, true, context.Canceled)

	proc.Ctx = baseCtx
	fresh := batch.NewWithSize(1)
	fresh.Vecs[0] = testutil.MakeInt32Vector([]int32{4, 1, 3, 2}, nil, proc.Mp())
	fresh.SetRowCount(4)
	child = colexec.NewMockOperator().WithBatchs([]*batch.Batch{fresh})
	g.Children = nil
	g.AppendChild(child)
	require.NoError(t, g.Prepare(proc))

	var outputRows int
	for _, output := range collectBatches(t, g, proc) {
		outputRows += output.RowCount()
	}
	require.Equal(t, 4, outputRows)
}

func TestGroupedOrderedGroupConcatComposesWithGenericSpill(t *testing.T) {
	for _, distinct := range []bool{false, true} {
		t.Run(fmt.Sprintf("distinct=%t", distinct), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()

			const rows = 512
			groups := make([]int32, rows)
			values := make([]string, rows)
			orderKeys := make([]int64, rows)
			for i := range rows {
				groups[i] = 1
				values[i] = fmt.Sprintf("%04d-%s", i, strings.Repeat("x", 256))
				orderKeys[i] = int64(rows - i)
			}
			input := batch.NewWithSize(3)
			input.Vecs[0] = testutil.MakeInt32Vector(groups, nil, proc.Mp())
			input.Vecs[1] = testutil.MakeVarcharVector(values, nil, proc.Mp())
			input.Vecs[2] = testutil.MakeInt64Vector(orderKeys, nil, proc.Mp())
			input.SetRowCount(rows)

			g := newGroupOp(
				proc,
				[]*plan.Expr{colExpr(0, types.T_int32)},
				[]aggexec.AggFuncExecExpression{orderedGroupConcatAgg(distinct), countStarAgg()},
			)
			g.SpillMem = 64 << 10
			g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
			require.NoError(t, g.Prepare(proc))

			results := collectBatches(t, g, proc)
			require.Len(t, results, 1)
			require.Equal(t, 1, results[0].RowCount())
			require.Equal(t, int64(rows), vector.MustFixedColNoTypeCheck[int64](results[0].Vecs[2])[0])
			parts := strings.Split(string(results[0].Vecs[1].GetBytesAt(0)), "|")
			require.Len(t, parts, rows)
			require.Equal(t, values[rows-1], parts[0])
			require.Equal(t, values[0], parts[rows-1])

			extra := g.OpAnalyzer.GetOpStats().ExtraStats
			require.Equal(t, int64(spillMaxPass), extra["GroupSpillMaxLevel"])
			require.Positive(t, extra["GroupSpillRespills"])
			g.Free(proc, false, nil)
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestSpillReloadPreallocationRespectsByteLimit(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	const requested = uint64(1 << 20)
	ctr := container{
		mp:                    proc.Mp(),
		mtyp:                  H8,
		spillMem:              1 << 20,
		spillHashPreAllocSize: requested,
	}
	got := ctr.boundedSpillReloadPreAlloc(int64(requested))
	require.Less(t, got, requested)
	require.LessOrEqual(t,
		hashtable.Int64HashMapInitialAllocationBytes()+hashtable.EstimateInt64HashMapSize(got),
		uint64(ctr.spillMem))

	// The sub-10K test mode is a group-count threshold rather than a byte
	// budget, but the proven high-water cap still applies.
	ctr.spillMem = 4096
	ctr.spillHashPreAllocSize = 2048
	require.Equal(t, uint64(2048), ctr.boundedSpillReloadPreAlloc(8192))
}

// TestGroupResetAndReuse: verify Reset allows the operator to be reused correctly.
func TestGroupResetAndReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	g := newGroupOp(proc,
		[]*plan.Expr{colExpr(0, types.T_int32), colExpr(1, types.T_uuid)},
		[]aggexec.AggFuncExecExpression{sumAgg(0)})
	g.GroupByHashKey = []int32{0}

	for i := 0; i < 2; i++ {
		resetChildren(g, proc)
		require.NoError(t, g.Prepare(proc))
		for {
			result, err := vm.Exec(g, proc)
			require.NoError(t, err)
			if result.Status == vm.ExecStop || result.Batch == nil {
				break
			}
		}
		g.Reset(proc, false, nil)
	}

	g.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestMergeGroupPreservesLateNullableGroupKeys(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	first := batch.NewWithSize(2)
	first.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1}, nil, proc.Mp())
	first.Vecs[1] = testutil.MakeInt32Vector([]int32{10, 10}, nil, proc.Mp())
	first.SetRowCount(2)

	second := batch.NewWithSize(2)
	second.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1}, nil, proc.Mp())
	second.Vecs[1] = testutil.MakeInt32Vector([]int32{0, 0}, []uint64{0, 1}, proc.Mp())
	second.SetRowCount(2)

	partialBatches := buildPartialGroupBatches(t, proc, []*batch.Batch{first, second}, true)

	merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{countStarAgg()})
	merge.AppendChild(colexec.NewMockOperator().WithBatchs(partialBatches))
	require.NoError(t, merge.Prepare(proc))
	finalBatches := collectBatches(t, merge, proc)
	require.Len(t, finalBatches, 1)
	require.Equal(t, len(finalBatches)+1, merge.OpAnalyzer.GetOpStats().CallNum)
	assertMergedTicketCounts(t, finalBatches, 2, 2)
	merge.Free(proc, false, nil)
}

func TestMergeGroupH0SkipsGenericSpillAndReuses(t *testing.T) {
	proc := testutil.NewProcess(t)
	merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{countStarAgg()})
	var child *colexec.MockOperator
	t.Cleanup(func() {
		merge.Free(proc, false, nil)
		if child != nil {
			child.Free(proc, false, nil)
		}
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	run := func(values [][]int32, want int64) {
		partials := make([]*batch.Batch, 0, len(values))
		for _, input := range values {
			partials = append(partials, buildPartialH0Batch(t, proc, input))
		}

		child = colexec.NewMockOperator().WithBatchs(partials)
		merge.Children = nil
		merge.SpillMem = 256
		merge.AppendChild(child)
		require.NoError(t, merge.Prepare(proc))

		outputs := collectBatches(t, merge, proc)
		require.Len(t, outputs, 1)
		require.Len(t, outputs[0].Vecs, 1)
		require.Equal(t, want, vector.MustFixedColNoTypeCheck[int64](outputs[0].Vecs[0])[0])
		require.Equal(t, int32(H0), merge.ctr.mtyp)
		require.True(t, merge.ctr.hr.IsEmpty())
		extra := merge.OpAnalyzer.GetOpStats().ExtraStats
		require.Zero(t, extra["GroupSpillWriteCalls"])
		require.Zero(t, extra["GroupSpillBucketsCreated"])
		require.Zero(t, extra["GroupSpillReloadBuckets"])

		merge.Reset(proc, false, nil)
		require.Nil(t, merge.ctr.mp)
		child.Free(proc, false, nil)
		child = nil
	}

	// Multiple partial groups prove the real Group -> MergeGroup H0 hand-off.
	run([][]int32{{1, 2}, {3}}, 3)
	// Reset must leave no old H0 state in the next generation.
	run([][]int32{{4}, {5, 6}}, 3)
}

func TestMergeGroupHonorsCancellationAfterInput(t *testing.T) {
	proc := testutil.NewProcess(t)
	baseCtx := proc.Ctx
	partial := batch.NewWithSize(1)
	partial.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	partial.SetRowCount(3)

	var extra bytes.Buffer
	mtyp := int32(H8)
	nullable := false
	nAggs := int32(0)
	extra.Write(types.EncodeInt32(&mtyp))
	extra.Write(types.EncodeBool(&nullable))
	extra.Write(types.EncodeInt32(&nAggs))
	partial.ExtraBuf = extra.Bytes()

	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{partial}).WithEndOfDataCallback(cancel)
	merge := newMergeGroupOp(nil)
	merge.AppendChild(child)
	require.NoError(t, merge.Prepare(proc))

	t.Cleanup(func() {
		proc.Ctx = baseCtx
		merge.Free(proc, false, nil)
		child.Free(proc, false, nil)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	result, err := vm.Exec(merge, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)

	merge.Reset(proc, true, context.Canceled)
	require.Nil(t, merge.ctr.mp)
	child.Free(proc, true, context.Canceled)

	proc.Ctx = baseCtx
	freshPartial := batch.NewWithSize(1)
	freshPartial.Vecs[0] = testutil.MakeInt32Vector([]int32{3, 1, 2}, nil, proc.Mp())
	freshPartial.SetRowCount(3)
	freshPartial.ExtraBuf = append(freshPartial.ExtraBuf[:0], extra.Bytes()...)
	child = colexec.NewMockOperator().WithBatchs([]*batch.Batch{freshPartial})
	merge.Children = nil
	merge.AppendChild(child)
	require.NoError(t, merge.Prepare(proc))

	outputs := collectBatches(t, merge, proc)
	require.Len(t, outputs, 1)
	require.Equal(t, 3, outputs[0].RowCount())
}

func TestMergeGroupSpillWriteHonorsCancellationAfterInputBatch(t *testing.T) {
	proc := testutil.NewProcess(t)
	baseCtx := proc.Ctx
	partial := batch.NewWithSize(1)
	partial.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	partial.SetRowCount(3)

	var extra bytes.Buffer
	mtyp := int32(H8)
	nullable := false
	nAggs := int32(0)
	extra.Write(types.EncodeInt32(&mtyp))
	extra.Write(types.EncodeBool(&nullable))
	extra.Write(types.EncodeInt32(&nAggs))
	partial.ExtraBuf = extra.Bytes()

	ctx, cancel := context.WithCancel(baseCtx)
	proc.Ctx = ctx
	child := colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{partial}).
		WithBatchCallback(func(int) { cancel() })
	merge := newMergeGroupOp(nil)
	merge.SpillMem = 1
	merge.AppendChild(child)
	require.NoError(t, merge.Prepare(proc))

	t.Cleanup(func() {
		proc.Ctx = baseCtx
		merge.Free(proc, true, context.Canceled)
		child.Free(proc, true, context.Canceled)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	result, err := vm.Exec(merge, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Zero(t, merge.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])
	require.Nil(t, merge.ctr.currentSpillBkt)
}

func TestMergeGroupFreesSpillAggListAfterBatchMerge(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	first := batch.NewWithSize(2)
	first.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1}, nil, proc.Mp())
	first.Vecs[1] = testutil.MakeInt32Vector([]int32{10, 10}, nil, proc.Mp())
	first.SetRowCount(2)

	second := batch.NewWithSize(2)
	second.Vecs[0] = testutil.MakeInt32Vector([]int32{2, 2}, nil, proc.Mp())
	second.Vecs[1] = testutil.MakeInt32Vector([]int32{20, 20}, nil, proc.Mp())
	second.SetRowCount(2)

	partialBatches := buildPartialGroupBatches(t, proc, []*batch.Batch{first, second}, false)

	merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{countStarAgg()})
	require.NoError(t, merge.Prepare(proc))
	defer merge.Free(proc, false, nil)

	for _, partial := range partialBatches {
		_, err := merge.buildOneBatch(proc, partial)
		require.NoError(t, err)
		require.Nil(t, merge.ctr.spillAggList)
	}
}

func TestFreeAggListPartial(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	aggList := make([]aggexec.AggFuncExec, 3)
	for i := 0; i < 3; i++ {
		agg, err := aggexec.MakeAgg(proc.Mp(), aggexec.AggIdOfCountStar, false, types.T_int64.ToType())
		require.NoError(t, err)
		aggList[i] = agg
	}

	freeAggListPartial(aggList, 2)
	freeAggListPartial(aggList, 3)
}

func TestFreeAggList(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	aggList := make([]aggexec.AggFuncExec, 2)
	for i := 0; i < 2; i++ {
		agg, err := aggexec.MakeAgg(proc.Mp(), aggexec.AggIdOfCountStar, false, types.T_int64.ToType())
		require.NoError(t, err)
		aggList[i] = agg
	}

	freeAggList(aggList)
}

func TestFreeAggListPartialWithNilEntries(t *testing.T) {
	aggList := make([]aggexec.AggFuncExec, 3)

	freeAggListPartial(aggList, 3)
	freeAggList(aggList)
}

func TestMakeAggListFreesPartialOnCreationError(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	ctr := &container{mp: proc.Mp()}
	_, err := ctr.makeAggList([]aggexec.AggFuncExecExpression{
		countStarAgg(),
		aggexec.MakeAggFunctionExpression(-1, false, []*plan.Expr{colExpr(0, types.T_int32)}, nil),
	})
	require.Error(t, err)
}

func TestMakeAggListFreesPartialOnExtraConfigError(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	ctr := &container{mp: proc.Mp()}
	_, err := ctr.makeAggList([]aggexec.AggFuncExecExpression{
		countStarAgg(),
		aggexec.MakeAggFunctionExpression(
			aggexec.AggIdOfMin,
			false,
			[]*plan.Expr{colExpr(0, types.T_int32)},
			[]byte("bad-config"),
		),
	})
	require.Error(t, err)
}

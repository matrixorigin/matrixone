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

package indexbuild

import (
	"errors"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func indexBuildPlanType(typ types.Type) *plan.Type {
	return &plan.Type{
		Id:    int32(typ.Oid),
		Width: typ.Width,
		Scale: typ.Scale,
	}
}

func indexBuildColExpr(typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: *indexBuildPlanType(typ),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{ColPos: 0},
		},
	}
}

func indexBuildRawSpec(tag, upperLimit int32, typ types.Type) *plan.RuntimeFilterSpec {
	return &plan.RuntimeFilterSpec{
		Tag:         tag,
		UpperLimit:  upperLimit,
		BuildExpr:   indexBuildColExpr(typ),
		KeyEncoding: plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
		ProbeType:   indexBuildPlanType(typ),
	}
}

func indexBuildTestProcess(t *testing.T) *process.Process {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	return proc
}

func indexBuildBatch(vec *vector.Vector, rows int) *batch.Batch {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vec
	bat.SetRowCount(rows)
	return bat
}

type indexBuildErrorOperator struct {
	*colexec.MockOperator
	err error
}

func (op *indexBuildErrorOperator) Call(
	_ *process.Process,
) (vm.CallResult, error) {
	return vm.NewCallResult(), op.err
}

func receiveIndexBuildRuntimeFilter(
	t *testing.T,
	proc *process.Process,
	tag int32,
) message.RuntimeFilterMessage {
	receiver := message.NewMessageReceiver(
		[]int32{tag},
		message.AddrBroadCastOnCurrentCN(),
		proc.GetMessageBoard(),
	)
	msgs, done, err := receiver.ReceiveMessage(false, proc.Ctx)
	require.NoError(t, err)
	require.False(t, done)
	require.Len(t, msgs, 1)
	runtimeFilter, ok := msgs[0].(message.RuntimeFilterMessage)
	require.True(t, ok)
	return runtimeFilter
}

func executeIndexBuild(
	t *testing.T,
	spec *plan.RuntimeFilterSpec,
	batches ...*batch.Batch,
) (*IndexBuild, *colexec.MockOperator, *process.Process) {
	proc := indexBuildTestProcess(t)
	child := colexec.NewMockOperator().WithBatchs(append(batches, nil))
	arg := NewArgument()
	arg.RuntimeFilterSpec = spec
	arg.AppendChild(child)

	require.NoError(t, child.Prepare(proc))
	require.NoError(t, arg.Prepare(proc))
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	return arg, child, proc
}

func cleanupIndexBuild(
	t *testing.T,
	arg *IndexBuild,
	child *colexec.MockOperator,
	proc *process.Process,
) {
	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	arg.Release()
	proc.GetMessageBoard().Reset()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestIndexBuildExactRuntimeFilterContract(t *testing.T) {
	t.Run("nil spec is a safe no-op", func(t *testing.T) {
		arg, child, proc := executeIndexBuild(t, nil)
		cleanupIndexBuild(t, arg, child, proc)
	})

	t.Run("nil payload fails open", func(t *testing.T) {
		spec := indexBuildRawSpec(101, 16, types.T_int32.ToType())
		proc := indexBuildTestProcess(t)
		child := colexec.NewMockOperator()
		arg := NewArgument()
		arg.RuntimeFilterSpec = spec
		require.NoError(t, arg.Prepare(proc))
		arg.ctr.buf = indexBuildBatch(nil, 1)
		require.NotPanics(t, func() {
			require.NoError(t, arg.ctr.handleRuntimeFilter(arg, proc))
		})
		runtimeFilter := receiveIndexBuildRuntimeFilter(t, proc, spec.Tag)
		require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)
		require.Zero(t, runtimeFilter.Card)
		require.Empty(t, runtimeFilter.Data)
		cleanupIndexBuild(t, arg, child, proc)
	})

	t.Run("legacy spec fails open", func(t *testing.T) {
		spec := &plan.RuntimeFilterSpec{
			Tag:        102,
			UpperLimit: 16,
			Expr:       indexBuildColExpr(types.T_int32.ToType()),
		}
		arg, child, proc := executeIndexBuild(t, spec)
		runtimeFilter := receiveIndexBuildRuntimeFilter(t, proc, spec.Tag)
		require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)
		require.Zero(t, runtimeFilter.Card)
		require.Empty(t, runtimeFilter.Data)
		cleanupIndexBuild(t, arg, child, proc)
	})

	t.Run("stale decimal contract fails open", func(t *testing.T) {
		probeType := types.New(types.T_decimal64, 18, 2)
		buildType := types.New(types.T_decimal64, 18, 3)
		spec := &plan.RuntimeFilterSpec{
			Tag:         103,
			UpperLimit:  16,
			BuildExpr:   indexBuildColExpr(buildType),
			KeyEncoding: plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
			ProbeType:   indexBuildPlanType(probeType),
		}
		arg, child, proc := executeIndexBuild(t, spec)
		runtimeFilter := receiveIndexBuildRuntimeFilter(t, proc, spec.Tag)
		require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)
		require.Zero(t, runtimeFilter.Card)
		require.Empty(t, runtimeFilter.Data)
		cleanupIndexBuild(t, arg, child, proc)
	})

	t.Run("actual payload drift fails open", func(t *testing.T) {
		spec := indexBuildRawSpec(104, 16, types.T_int32.ToType())
		proc := indexBuildTestProcess(t)
		vec := testutil.MakeInt64Vector([]int64{7}, nil, proc.Mp())
		child := colexec.NewMockOperator().WithBatchs(
			[]*batch.Batch{indexBuildBatch(vec, vec.Length()), nil})
		arg := NewArgument()
		arg.RuntimeFilterSpec = spec
		arg.AppendChild(child)
		require.NoError(t, child.Prepare(proc))
		require.NoError(t, arg.Prepare(proc))
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecStop, result.Status)

		runtimeFilter := receiveIndexBuildRuntimeFilter(t, proc, spec.Tag)
		require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)
		require.Zero(t, runtimeFilter.Card)
		require.Empty(t, runtimeFilter.Data)
		cleanupIndexBuild(t, arg, child, proc)
	})

	t.Run("explicit raw contract emits in", func(t *testing.T) {
		spec := indexBuildRawSpec(105, 16, types.T_int32.ToType())
		proc := indexBuildTestProcess(t)
		vec := testutil.MakeInt32Vector([]int32{7, 3}, nil, proc.Mp())
		child := colexec.NewMockOperator().WithBatchs(
			[]*batch.Batch{indexBuildBatch(vec, vec.Length()), nil})
		arg := NewArgument()
		arg.RuntimeFilterSpec = spec
		arg.AppendChild(child)
		require.NoError(t, child.Prepare(proc))
		require.NoError(t, arg.Prepare(proc))
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecStop, result.Status)

		runtimeFilter := receiveIndexBuildRuntimeFilter(t, proc, spec.Tag)
		require.Equal(t, int32(message.RuntimeFilter_IN), runtimeFilter.Typ)
		require.Equal(t, int32(2), runtimeFilter.Card)
		payload := vector.NewVec(types.T_any.ToType())
		require.NoError(t, payload.UnmarshalBinary(runtimeFilter.Data))
		require.Equal(t, types.T_int32, payload.GetType().Oid)
		require.Equal(t, []int32{3, 7},
			vector.MustFixedColNoTypeCheck[int32](payload))
		payload.Free(proc.Mp())
		runtimeFilter.Destroy()
		cleanupIndexBuild(t, arg, child, proc)
	})
}

func TestIndexBuildFloatRuntimeFilterClosesConstSignedZero(t *testing.T) {
	service := ""
	rt := moruntime.ServiceRuntime(service)
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(
				moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion8)

	for _, test := range []struct {
		name       string
		upperLimit int32
		wantType   int32
	}{
		{
			name:       "closure emits both representations",
			upperLimit: 2,
			wantType:   message.RuntimeFilter_IN,
		},
		{
			name:       "closure respects upper limit",
			upperLimit: 1,
			wantType:   message.RuntimeFilter_PASS,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			typ := types.T_float64.ToType()
			spec := &plan.RuntimeFilterSpec{
				Tag:         106,
				UpperLimit:  test.upperLimit,
				BuildExpr:   indexBuildColExpr(typ),
				KeyEncoding: plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1,
				ProbeType:   indexBuildPlanType(typ),
			}
			proc := indexBuildTestProcess(t)
			const logicalRows = 1000
			vec, err := vector.NewConstFixed(
				typ, float64(0), logicalRows, proc.Mp())
			require.NoError(t, err)
			child := colexec.NewMockOperator().WithBatchs(
				[]*batch.Batch{indexBuildBatch(vec, logicalRows), nil})
			arg := NewArgument()
			arg.RuntimeFilterSpec = spec
			arg.AppendChild(child)
			require.NoError(t, child.Prepare(proc))
			require.NoError(t, arg.Prepare(proc))
			result, err := vm.Exec(arg, proc)
			require.NoError(t, err)
			require.Equal(t, vm.ExecStop, result.Status)

			runtimeFilter := receiveIndexBuildRuntimeFilter(t, proc, spec.Tag)
			require.Equal(t, test.wantType, runtimeFilter.Typ)
			if test.wantType == message.RuntimeFilter_IN {
				require.Equal(t, int32(2), runtimeFilter.Card)
				payload := vector.NewVec(types.T_any.ToType())
				require.NoError(t, payload.UnmarshalBinary(runtimeFilter.Data))
				require.Equal(t, 2, payload.Length())
				var positiveZero, negativeZero bool
				for _, value := range vector.MustFixedColNoTypeCheck[float64](payload) {
					switch math.Float64bits(value) {
					case 0:
						positiveZero = true
					case uint64(1) << 63:
						negativeZero = true
					}
				}
				require.True(t, positiveZero)
				require.True(t, negativeZero)
				payload.Free(proc.Mp())
				runtimeFilter.Destroy()
			} else {
				require.Zero(t, runtimeFilter.Card)
				require.Empty(t, runtimeFilter.Data)
			}
			cleanupIndexBuild(t, arg, child, proc)
		})
	}
}

func TestIndexBuildRuntimeFilterCopyFailureFailsOpen(t *testing.T) {
	typ := types.T_varchar.ToType()
	spec := indexBuildRawSpec(109, 16, typ)
	limited, err := mpool.NewMPool(
		"index-runtime-filter-fail-open",
		mpool.MB,
		mpool.NoFixed,
	)
	require.NoError(t, err)
	proc := testutil.NewProcessWithMPool(t, "", limited)
	proc.SetMessageBoard(message.NewMessageBoard())

	sourceMP := mpool.MustNewZero()
	sourceVec := vector.NewVec(typ)
	require.NoError(t, vector.AppendBytes(
		sourceVec, []byte("one-key"), false, sourceMP))
	sourceBatch := batch.NewWithSize(1)
	sourceBatch.Vecs[0] = sourceVec
	sourceBatch.SetRowCount(1)
	child := colexec.NewMockOperator().WithBatchs(
		[]*batch.Batch{sourceBatch, nil})
	arg := NewArgument()
	arg.RuntimeFilterSpec = spec
	arg.AppendChild(child)
	require.NoError(t, child.Prepare(proc))
	require.NoError(t, arg.Prepare(proc))
	filler, err := limited.Alloc(
		int(limited.Cap()-limited.CurrNB()), true)
	require.NoError(t, err)

	// Exhaust only the producer pool before copying the one-row optional
	// payload. IndexBuild must still complete and unblock its consumer.
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	require.False(t, arg.ctr.runtimeFilterUsable)
	require.Nil(t, arg.ctr.buf)
	runtimeFilter := receiveIndexBuildRuntimeFilter(t, proc, spec.Tag)
	require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)
	require.Zero(t, runtimeFilter.Card)
	require.Empty(t, runtimeFilter.Data)
	require.Equal(t, int64(1), arg.OpAnalyzer.GetOpStats().ExtraStats["IndexBuildRuntimeFilterAllocationFallbacks"])
	require.Zero(t, arg.OpAnalyzer.GetOpStats().ExtraStats["IndexBuildRuntimeFilterBudgetFallbacks"])

	arg.Free(proc, false, nil)
	child.ResetBatchs()
	child.Free(proc, false, nil)
	arg.Release()
	limited.Free(filler)
	sourceBatch.Clean(sourceMP)
	require.Zero(t, sourceMP.CurrNB())
	proc.GetMessageBoard().Reset()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestIndexBuildRuntimeFilterClosureFailureFailsOpen(t *testing.T) {
	service := ""
	rt := moruntime.ServiceRuntime(service)
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(
		moruntime.MOProtocolVersion, defines.MORPCVersion8)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(
				moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	typ := types.T_float64.ToType()
	spec := indexBuildRawSpec(110, math.MaxInt32, typ)
	spec.KeyEncoding = plan.
		RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1
	limited, err := mpool.NewMPool(
		"index-runtime-filter-closure-fail-open",
		mpool.MB,
		mpool.NoFixed,
	)
	require.NoError(t, err)
	proc := testutil.NewProcessWithMPool(t, service, limited)
	proc.SetMessageBoard(message.NewMessageBoard())
	arg := NewArgument()
	arg.RuntimeFilterSpec = spec
	require.NoError(t, arg.Prepare(proc))

	arg.ctr.buf = batch.NewOffHeapWithSize(1)
	arg.ctr.buf.Vecs[0] = vector.NewOffHeapVecWithType(typ)
	require.NoError(t, vector.AppendFixed(
		arg.ctr.buf.Vecs[0], float64(0), false, limited))
	for arg.ctr.buf.Vecs[0].Length() < arg.ctr.buf.Vecs[0].Capacity() {
		require.NoError(t, vector.AppendFixed(
			arg.ctr.buf.Vecs[0], float64(1), false, limited))
	}
	arg.ctr.buf.SetRowCount(arg.ctr.buf.Vecs[0].Length())
	filler, err := limited.Alloc(
		int(limited.Cap()-limited.CurrNB()), true)
	require.NoError(t, err)

	require.NoError(t, arg.ctr.handleRuntimeFilter(arg, proc))
	require.False(t, arg.ctr.runtimeFilterUsable)
	require.Nil(t, arg.ctr.buf)
	runtimeFilter := receiveIndexBuildRuntimeFilter(t, proc, spec.Tag)
	require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)
	require.Zero(t, runtimeFilter.Card)
	require.Empty(t, runtimeFilter.Data)
	require.Equal(t, int64(1), arg.OpAnalyzer.GetOpStats().ExtraStats["IndexBuildRuntimeFilterAllocationFallbacks"])
	require.Zero(t, arg.OpAnalyzer.GetOpStats().ExtraStats["IndexBuildRuntimeFilterBudgetFallbacks"])

	limited.Free(filler)
	arg.Free(proc, false, nil)
	arg.Release()
	proc.GetMessageBoard().Reset()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestIndexBuildRuntimeFilterBudgetErrorPolicy(t *testing.T) {
	for _, test := range []struct {
		name   string
		closed bool
	}{
		{name: "admission fails open"},
		{name: "closed remains fatal", closed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := indexBuildTestProcess(t)
			typ := types.T_int32.ToType()
			spec := indexBuildRawSpec(111, 16, typ)
			arg := NewArgument()
			arg.RuntimeFilterSpec = spec
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.buf = indexBuildBatch(
				testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp()),
				3,
			)

			generation, err := proc.GetHashBuildBudget()
			require.NoError(t, err)
			var held *process.HashBuildReservation
			if test.closed {
				generation.Close()
			} else {
				held, err = generation.Reserve(generation.Cap())
				require.NoError(t, err)
			}

			err = arg.ctr.handleRuntimeFilter(arg, proc)
			stats := arg.OpAnalyzer.GetOpStats().ExtraStats
			if test.closed {
				require.ErrorIs(t, err, process.ErrHashBuildBudgetClosed)
				require.NotErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
				require.False(t, arg.ctr.runtimeFilterDone)
				require.Zero(t,
					stats["IndexBuildRuntimeFilterBudgetFallbacks"])
				arg.finalizeBuildFailure(proc)
			} else {
				require.NoError(t, err)
				require.True(t, arg.ctr.runtimeFilterDone)
				require.Equal(t, int64(1),
					stats["IndexBuildRuntimeFilterBudgetFallbacks"])
				require.True(t, held.Release())
			}
			require.False(t, arg.ctr.runtimeFilterUsable)
			require.Nil(t, arg.ctr.buf)
			runtimeFilter := receiveIndexBuildRuntimeFilter(
				t, proc, spec.Tag)
			require.Equal(t, int32(message.RuntimeFilter_PASS),
				runtimeFilter.Typ)

			arg.Free(proc, test.closed, err)
			arg.Release()
			proc.GetMessageBoard().Reset()
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestIndexBuildResetOnBuildErrorFailsOpen(t *testing.T) {
	proc := indexBuildTestProcess(t)
	spec := indexBuildRawSpec(107, 16, types.T_int32.ToType())
	arg := NewArgument()
	arg.RuntimeFilterSpec = spec

	arg.Reset(proc, true, errors.New("build failed"))
	arg.Reset(proc, true, errors.New("build failed"))
	runtimeFilter := receiveIndexBuildRuntimeFilter(t, proc, spec.Tag)
	require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)
	require.Zero(t, runtimeFilter.Card)
	require.Empty(t, runtimeFilter.Data)

	arg.Release()
	proc.GetMessageBoard().Reset()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestIndexBuildCallErrorUnblocksRuntimeFilterBeforeReset(t *testing.T) {
	proc := indexBuildTestProcess(t)
	spec := indexBuildRawSpec(108, 16, types.T_int32.ToType())
	buildErr := errors.New("child build failed")
	child := &indexBuildErrorOperator{
		MockOperator: colexec.NewMockOperator(),
		err:          buildErr,
	}
	arg := NewArgument()
	arg.RuntimeFilterSpec = spec
	arg.AppendChild(child)
	receiver := message.NewMessageReceiver(
		[]int32{spec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		proc.GetMessageBoard(),
	)

	require.NoError(t, child.Prepare(proc))
	require.NoError(t, arg.Prepare(proc))
	_, err := vm.Exec(arg, proc)
	require.ErrorIs(t, err, buildErr)

	messages, done, receiveErr := receiver.ReceiveMessage(false, proc.Ctx)
	require.NoError(t, receiveErr)
	require.False(t, done)
	require.Len(t, messages, 1)
	runtimeFilter, ok := messages[0].(message.RuntimeFilterMessage)
	require.True(t, ok)
	require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)

	arg.Reset(proc, true, err)
	messages, done, receiveErr = receiver.ReceiveMessage(false, proc.Ctx)
	require.NoError(t, receiveErr)
	require.False(t, done)
	require.Empty(t, messages, "Reset must not publish a second terminal value")
	arg.Reset(proc, true, err)
	messages, done, receiveErr = receiver.ReceiveMessage(false, proc.Ctx)
	require.NoError(t, receiveErr)
	require.False(t, done)
	require.Empty(t, messages,
		"repeated Reset must remain idempotent within one generation")
	require.True(t, arg.ctr.runtimeFilterDone)

	proc.GetMessageBoard().Reset()
	require.NoError(t, arg.Prepare(proc))
	require.False(t, arg.ctr.runtimeFilterDone,
		"Prepare must open the terminal gate for the next generation")
	arg.finalizeBuildFailure(proc)
	runtimeFilter = receiveIndexBuildRuntimeFilter(t, proc, spec.Tag)
	require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)

	arg.Free(proc, true, err)
	child.Free(proc, true, err)
	arg.Release()
	proc.GetMessageBoard().Reset()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

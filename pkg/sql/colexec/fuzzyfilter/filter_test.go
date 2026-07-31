// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package fuzzyfilter

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type fuzzyTestCase struct {
	arg   *FuzzyFilter
	types []types.Type
	proc  *process.Process
}

type fuzzyErrorOperator struct {
	*colexec.MockOperator
	err error
}

func (op *fuzzyErrorOperator) Call(
	_ *process.Process,
) (vm.CallResult, error) {
	return vm.NewCallResult(), op.err
}

var (
	rowCnts []float64
)

func init() {
	// rowCnts = []float64{1000000, 10000000}

	rowCnts = []float64{1000, 10000}

	// https://hur.st/bloomfilter/?n=100000&p=0.00001&m=&k=3
	// referM = []float64{
	// 	68871111,
	// 	137742221,
	// }
}

func makeTestCases(t *testing.T) []fuzzyTestCase {
	return []fuzzyTestCase{
		{
			arg:  newArgument(types.T_int32.ToType()),
			proc: newProcess(t),
			types: []types.Type{
				types.T_int32.ToType(),
			},
		},
		// {
		// 	arg:  newArgument(types.T_date.ToType()),
		// 	proc: newProcess(),
		// 	types: []types.Type{
		// 		types.T_date.ToType(),
		// 	},
		// },
		// {
		// 	arg:  newArgument(types.T_float32.ToType()),
		// 	proc: newProcess(),
		// 	types: []types.Type{
		// 		types.T_float32.ToType(),
		// 	},
		// },
		// {
		// 	arg:  newArgument(types.T_varchar.ToType()),
		// 	proc: newProcess(),
		// 	types: []types.Type{
		// 		types.T_varchar.ToType(),
		// 	},
		// },
		// {
		// 	arg:  newArgument(types.T_binary.ToType()),
		// 	proc: newProcess(),
		// 	types: []types.Type{
		// 		types.T_binary.ToType(),
		// 	},
		// },
	}
}

func newArgument(typ types.Type) *FuzzyFilter {
	arg := new(FuzzyFilter)
	arg.PkTyp = plan.MakePlan2Type(&typ)
	arg.Callback = func(bat *batch.Batch) error {
		if bat == nil || bat.IsEmpty() {
			return nil
		}
		return nil
	}
	return arg
}

func newProcess(t *testing.T) *process.Process {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	return proc
}

func setProcForTest(fuzzyFilter *FuzzyFilter, proc *process.Process, typs []types.Type, rowCnt float64) {
	fuzzyFilter.Children = nil

	leftBatches := newBatch(typs, proc, int64(rowCnt))
	rightBatches := newBatch(typs, proc, int64(rowCnt))

	leftChild := colexec.NewMockOperator().WithBatchs(leftBatches)
	rightChild := colexec.NewMockOperator().WithBatchs(rightBatches)

	fuzzyFilter.AppendChild(leftChild)
	fuzzyFilter.AppendChild(rightChild)
}

func TestString(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		buf := new(bytes.Buffer)
		tc.arg.String(buf)
		require.Equal(t, "fuzzy_filter: fuzzy check duplicate constraint", buf.String())
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestRuntimeFilterContract(t *testing.T) {
	t.Run("stale decimal shape fails open without retaining payload", func(t *testing.T) {
		probeType := types.New(types.T_decimal64, 18, 2)
		payloadType := types.New(types.T_decimal64, 18, 3)
		spec := newRuntimeFilterSpec(101, probeType, payloadType)
		arg, proc := newRuntimeFilterTest(t, spec, payloadType)

		require.NoError(t, arg.Prepare(proc))
		require.False(t, arg.ctr.runtimeFilterUsable)
		require.Nil(t, arg.ctr.pass2RuntimeFilter)

		payload := vector.NewVec(payloadType)
		require.NoError(t, vector.AppendFixed(
			payload, types.Decimal64(1000), false, proc.Mp()))
		beforeAppend := proc.Mp().CurrNB()
		require.NoError(t, arg.appendPassToRuntimeFilter(payload, proc))
		require.Equal(t, beforeAppend, proc.Mp().CurrNB())
		require.Nil(t, arg.ctr.pass2RuntimeFilter)

		require.NoError(t, arg.handleRuntimeFilter(proc))
		runtimeFilter := receiveRuntimeFilter(t, proc, spec.Tag)
		require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)
		require.Zero(t, runtimeFilter.Card)
		require.Empty(t, runtimeFilter.Data)

		payload.Free(proc.Mp())
		freeRuntimeFilterTest(t, arg, proc)
	})

	t.Run("valid empty input drops", func(t *testing.T) {
		typ := types.T_int32.ToType()
		spec := newRuntimeFilterSpec(102, typ, typ)
		arg, proc := newRuntimeFilterTest(t, spec, typ)

		require.NoError(t, arg.Prepare(proc))
		require.True(t, arg.ctr.runtimeFilterUsable)
		require.NotNil(t, arg.ctr.pass2RuntimeFilter)
		require.Zero(t, arg.ctr.pass2RuntimeFilter.Length())

		require.NoError(t, arg.handleRuntimeFilter(proc))
		runtimeFilter := receiveRuntimeFilter(t, proc, spec.Tag)
		require.Equal(t, int32(message.RuntimeFilter_DROP), runtimeFilter.Typ)
		require.Zero(t, runtimeFilter.Card)
		require.Empty(t, runtimeFilter.Data)

		freeRuntimeFilterTest(t, arg, proc)
	})

	t.Run("actual payload drift fails open and releases retained copy", func(t *testing.T) {
		typ := types.T_int32.ToType()
		spec := newRuntimeFilterSpec(103, typ, typ)
		arg, proc := newRuntimeFilterTest(t, spec, typ)

		require.NoError(t, arg.Prepare(proc))
		require.True(t, arg.ctr.runtimeFilterUsable)
		payload := testutil.MakeInt64Vector([]int64{7}, nil, proc.Mp())
		require.NoError(t, arg.appendPassToRuntimeFilter(payload, proc))
		require.False(t, arg.ctr.runtimeFilterUsable)
		require.Nil(t, arg.ctr.pass2RuntimeFilter)
		require.NoError(t, arg.handleRuntimeFilter(proc))
		runtimeFilter := receiveRuntimeFilter(t, proc, spec.Tag)
		require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)

		payload.Free(proc.Mp())
		freeRuntimeFilterTest(t, arg, proc)
	})

	t.Run("valid nonempty input publishes decoded cardinality", func(t *testing.T) {
		typ := types.T_int32.ToType()
		spec := newRuntimeFilterSpec(104, typ, typ)
		arg, proc := newRuntimeFilterTest(t, spec, typ)

		require.NoError(t, arg.Prepare(proc))
		payload := testutil.MakeInt32Vector([]int32{7, 3}, nil, proc.Mp())
		require.NoError(t, arg.appendPassToRuntimeFilter(payload, proc))

		require.NoError(t, arg.handleRuntimeFilter(proc))
		runtimeFilter := receiveRuntimeFilter(t, proc, spec.Tag)
		require.Equal(t, int32(message.RuntimeFilter_IN), runtimeFilter.Typ)
		require.NotEmpty(t, runtimeFilter.Data)

		decoded := vector.NewVec(types.T_any.ToType())
		require.NoError(t, decoded.UnmarshalBinary(runtimeFilter.Data))
		require.Equal(t, runtimeFilter.Card, int32(decoded.Length()))
		require.Equal(t, []int32{3, 7},
			vector.MustFixedColWithTypeCheck[int32](decoded))

		decoded.Free(proc.Mp())
		payload.Free(proc.Mp())
		freeRuntimeFilterTest(t, arg, proc)
	})

	t.Run("build error reset fails open", func(t *testing.T) {
		typ := types.T_int32.ToType()
		spec := newRuntimeFilterSpec(105, typ, typ)
		arg, proc := newRuntimeFilterTest(t, spec, typ)

		arg.Reset(proc, false, errors.New("build failed"))
		arg.Reset(proc, false, errors.New("build failed"))
		runtimeFilter := receiveRuntimeFilter(t, proc, spec.Tag)
		require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)
		require.Zero(t, runtimeFilter.Card)
		require.Empty(t, runtimeFilter.Data)

		freeRuntimeFilterTest(t, arg, proc)
	})
}

func TestFuzzyRuntimeFilterCopyFailureFailsOpen(t *testing.T) {
	typ := types.T_varchar.ToType()
	spec := newRuntimeFilterSpec(107, typ, typ)
	limited, err := mpool.NewMPool(
		"fuzzy-runtime-filter-fail-open",
		mpool.MB,
		mpool.NoFixed,
	)
	require.NoError(t, err)
	proc := testutil.NewProcessWithMPool(t, "", limited)
	proc.SetMessageBoard(message.NewMessageBoard())
	arg := newArgument(typ)
	arg.N = 1
	arg.RuntimeFilterSpec = spec
	require.NoError(t, arg.Prepare(proc))

	sourceMP := mpool.MustNewZero()
	payload := vector.NewVec(typ)
	require.NoError(t, vector.AppendBytes(
		payload, []byte("one-key"), false, sourceMP))
	filler, err := limited.Alloc(
		int(limited.Cap()-limited.CurrNB()), true)
	require.NoError(t, err)

	// Exhaust only the producer pool before its optional retained copy.
	// Duplicate detection must remain usable.
	require.NoError(t, arg.appendPassToRuntimeFilter(payload, proc))
	require.False(t, arg.ctr.runtimeFilterUsable)
	require.Nil(t, arg.ctr.pass2RuntimeFilter)
	require.NoError(t, arg.handleBuild(proc, payload))
	require.NoError(t, arg.handleRuntimeFilter(proc))
	runtimeFilter := receiveRuntimeFilter(t, proc, spec.Tag)
	require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)
	require.Zero(t, runtimeFilter.Card)
	require.Empty(t, runtimeFilter.Data)
	require.Equal(t, int64(1), arg.OpAnalyzer.GetOpStats().ExtraStats["FuzzyFilterRuntimeFilterAllocationFallbacks"])
	require.Zero(t, arg.OpAnalyzer.GetOpStats().ExtraStats["FuzzyFilterRuntimeFilterBudgetFallbacks"])

	limited.Free(filler)
	payload.Free(sourceMP)
	require.Zero(t, sourceMP.CurrNB())
	arg.Free(proc, false, nil)
	proc.GetMessageBoard().Reset()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestFuzzyRuntimeFilterClosureFailureFailsOpen(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(
		moruntime.MOProtocolVersion, defines.MORPCVersion7)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(
				moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	typ := types.T_float64.ToType()
	spec := newRuntimeFilterSpec(108, typ, typ)
	spec.KeyEncoding = planpb.
		RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1
	spec.UpperLimit = math.MaxInt32
	limited, err := mpool.NewMPool(
		"fuzzy-runtime-filter-closure-fail-open",
		mpool.MB,
		mpool.NoFixed,
	)
	require.NoError(t, err)
	proc := testutil.NewProcessWithMPool(t, "", limited)
	proc.SetMessageBoard(message.NewMessageBoard())
	arg := newArgument(typ)
	arg.N = 1
	arg.RuntimeFilterSpec = spec
	require.NoError(t, arg.Prepare(proc))
	require.NoError(t, vector.AppendFixed(
		arg.ctr.pass2RuntimeFilter, float64(0), false, limited))
	for arg.ctr.pass2RuntimeFilter.Length() <
		arg.ctr.pass2RuntimeFilter.Capacity() {
		require.NoError(t, vector.AppendFixed(
			arg.ctr.pass2RuntimeFilter, float64(1), false, limited))
	}
	filler, err := limited.Alloc(
		int(limited.Cap()-limited.CurrNB()), true)
	require.NoError(t, err)

	// Signed-zero closure needs one more slot for -0. Exhausting only that
	// optional growth must produce PASS, not fail the uniqueness operator.
	require.NoError(t, arg.handleRuntimeFilter(proc))
	require.False(t, arg.ctr.runtimeFilterUsable)
	require.Nil(t, arg.ctr.pass2RuntimeFilter)
	runtimeFilter := receiveRuntimeFilter(t, proc, spec.Tag)
	require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)
	require.Zero(t, runtimeFilter.Card)
	require.Empty(t, runtimeFilter.Data)
	require.Equal(t, int64(1), arg.OpAnalyzer.GetOpStats().ExtraStats["FuzzyFilterRuntimeFilterAllocationFallbacks"])
	require.Zero(t, arg.OpAnalyzer.GetOpStats().ExtraStats["FuzzyFilterRuntimeFilterBudgetFallbacks"])

	limited.Free(filler)
	arg.Free(proc, false, nil)
	proc.GetMessageBoard().Reset()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestFuzzyRuntimeFilterBudgetErrorPolicy(t *testing.T) {
	for _, test := range []struct {
		name   string
		closed bool
	}{
		{name: "admission fails open"},
		{name: "closed remains fatal", closed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			typ := types.T_int32.ToType()
			spec := newRuntimeFilterSpec(109, typ, typ)
			arg, proc := newRuntimeFilterTest(t, spec, typ)
			require.NoError(t, arg.Prepare(proc))
			require.NoError(t, vector.AppendFixed(
				arg.ctr.pass2RuntimeFilter, int32(1), false, proc.Mp()))

			generation, err := proc.GetHashBuildBudget()
			require.NoError(t, err)
			var held *process.HashBuildReservation
			if test.closed {
				generation.Close()
			} else {
				held, err = generation.Reserve(generation.Cap())
				require.NoError(t, err)
			}

			err = arg.handleRuntimeFilter(proc)
			stats := arg.OpAnalyzer.GetOpStats().ExtraStats
			if test.closed {
				require.ErrorIs(t, err, process.ErrHashBuildBudgetClosed)
				require.NotErrorIs(t, err,
					process.ErrHashBuildBudgetAdmission)
				require.False(t, arg.ctr.runtimeFilterDone)
				require.Zero(t,
					stats["FuzzyFilterRuntimeFilterBudgetFallbacks"])
				arg.finalizeBuildFailure(proc)
			} else {
				require.NoError(t, err)
				require.True(t, arg.ctr.runtimeFilterDone)
				require.Equal(t, int64(1),
					stats["FuzzyFilterRuntimeFilterBudgetFallbacks"])
				require.True(t, held.Release())
			}
			require.False(t, arg.ctr.runtimeFilterUsable)
			require.Nil(t, arg.ctr.pass2RuntimeFilter)
			runtimeFilter := receiveRuntimeFilter(t, proc, spec.Tag)
			require.Equal(t, int32(message.RuntimeFilter_PASS),
				runtimeFilter.Typ)

			arg.Free(proc, test.closed, err)
			proc.GetMessageBoard().Reset()
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestFuzzyCallErrorUnblocksRuntimeFilterBeforeReset(t *testing.T) {
	typ := types.T_int32.ToType()
	spec := newRuntimeFilterSpec(106, typ, typ)
	arg, proc := newRuntimeFilterTest(t, spec, typ)
	buildErr := errors.New("fuzzy build child failed")
	buildChild := &fuzzyErrorOperator{
		MockOperator: colexec.NewMockOperator(),
		err:          buildErr,
	}
	probeChild := colexec.NewMockOperator()
	arg.BuildIdx = 0
	arg.AppendChild(buildChild)
	arg.AppendChild(probeChild)
	receiver := message.NewMessageReceiver(
		[]int32{spec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		proc.GetMessageBoard(),
	)

	require.NoError(t, buildChild.Prepare(proc))
	require.NoError(t, probeChild.Prepare(proc))
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
	runtimeFilter = receiveRuntimeFilter(t, proc, spec.Tag)
	require.Equal(t, int32(message.RuntimeFilter_PASS), runtimeFilter.Typ)

	buildChild.Free(proc, true, err)
	probeChild.Free(proc, true, err)
	arg.Free(proc, true, err)
	proc.GetMessageBoard().Reset()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestFuzzyFilter(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		for _, r := range rowCnts {
			setProcForTest(tc.arg, tc.proc, tc.types, r)
			tc.arg.N = r
			tc.arg.OperatorBase.OperatorInfo = vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			}
			err := tc.arg.Prepare(tc.proc)
			require.NoError(t, err)

			for {
				result, err := vm.Exec(tc.arg, tc.proc)

				if result.Status != vm.ExecStop {
					if IfCanUseRoaringFilter(tc.types[0].Oid) {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
						require.Greater(t, tc.arg.ctr.rbat.RowCount(), int64(0))
					}
				} else {
					break
				}
			}

			tc.arg.GetChildren(0).Reset(tc.proc, false, nil)
			tc.arg.GetChildren(1).Reset(tc.proc, false, nil)
			tc.arg.Reset(tc.proc, false, nil)

			err = tc.arg.Prepare(tc.proc)
			require.NoError(t, err)

			for {
				result, err := vm.Exec(tc.arg, tc.proc)
				if result.Status != vm.ExecStop {
					if IfCanUseRoaringFilter(tc.types[0].Oid) {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
						require.Greater(t, tc.arg.ctr.rbat.RowCount(), int64(0))
					}
				} else {
					break
				}
			}
			tc.arg.GetChildren(0).Reset(tc.proc, false, nil)
			tc.arg.GetChildren(1).Reset(tc.proc, false, nil)
			tc.arg.Reset(tc.proc, false, nil)
			tc.arg.GetChildren(0).Free(tc.proc, false, nil)
			tc.arg.GetChildren(1).Free(tc.proc, false, nil)
			tc.arg.Free(tc.proc, false, nil)
			tc.proc.Free()
			require.Equal(t, int64(0), tc.proc.GetMPool().CurrNB())
		}
	}
}

func newRuntimeFilterSpec(
	tag int32,
	probeType types.Type,
	buildType types.Type,
) *planpb.RuntimeFilterSpec {
	probePlanType := plan.MakePlan2Type(&probeType)
	return &planpb.RuntimeFilterSpec{
		Tag:        tag,
		UpperLimit: 16,
		BuildExpr: &planpb.Expr{
			Typ: plan.MakePlan2Type(&buildType),
			Expr: &planpb.Expr_Col{
				Col: &planpb.ColRef{ColPos: 0},
			},
		},
		KeyEncoding: planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
		ProbeType:   &probePlanType,
	}
}

func newRuntimeFilterTest(
	t *testing.T,
	spec *planpb.RuntimeFilterSpec,
	pkType types.Type,
) (*FuzzyFilter, *process.Process) {
	proc := newProcess(t)
	proc.SetMessageBoard(message.NewMessageBoard())
	arg := newArgument(pkType)
	arg.N = 1
	arg.RuntimeFilterSpec = spec
	return arg, proc
}

func receiveRuntimeFilter(
	t *testing.T,
	proc *process.Process,
	tag int32,
) message.RuntimeFilterMessage {
	receiver := message.NewMessageReceiver(
		[]int32{tag},
		message.AddrBroadCastOnCurrentCN(),
		proc.GetMessageBoard(),
	)
	messages, done, err := receiver.ReceiveMessage(false, proc.Ctx)
	require.NoError(t, err)
	require.False(t, done)
	require.Len(t, messages, 1)
	runtimeFilter, ok := messages[0].(message.RuntimeFilterMessage)
	require.True(t, ok)
	return runtimeFilter
}

func freeRuntimeFilterTest(
	t *testing.T,
	arg *FuzzyFilter,
	proc *process.Process,
) {
	arg.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

// create a new block based on the type information
func newBatch(ts []types.Type, proc *process.Process, rows int64) []*batch.Batch {
	// not random
	bat := testutil.NewBatch(ts, false, int(rows), proc.Mp())
	pkAttr := make([]string, 1)
	pkAttr[0] = "pkCol"
	bat.SetAttributes(pkAttr)
	return []*batch.Batch{bat, nil}
}

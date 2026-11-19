// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package window

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

var ordinalChildError = errors.New("ordinal child failed")

type ordinalChild struct {
	*colexec.MockOperator
	results []vm.CallResult
	calls   int
	fail    bool
	cancel  context.CancelFunc
	onCall  func(int)
}

func (c *ordinalChild) Call(_ *process.Process) (vm.CallResult, error) {
	c.calls++
	if c.onCall != nil {
		c.onCall(c.calls)
	}
	if c.cancel != nil {
		c.cancel()
	}
	if c.fail || c.calls > len(c.results) {
		return vm.NewCallResult(), ordinalChildError
	}
	return c.results[c.calls-1], nil
}
func newStreamingRowNumber(t *testing.T) *Window {
	ag := newRowNumberAggExpr(t)
	typ := plan.Type{Id: int32(types.T_uint64)}
	return &Window{Aggs: []aggexec.AggFuncExecExpression{ag}, WinSpecList: []*plan.Expr{{Typ: typ, Expr: &plan.Expr_W{W: &plan.WindowSpec{
		Name:       "row_number",
		WindowFunc: &plan.Expr{Typ: typ, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: ag.GetAggID(), ObjName: "row_number"}}}},
		Frame:      &plan.FrameClause{Type: plan.FrameClause_ROWS, Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true}, End: &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, UnBounded: true}},
	}}}}}
}
func rowNumberInput(t *testing.T, proc *process.Process, selection *vector.AllocationAccountSelection, start, n int) *batch.Batch {
	b := batch.NewWithSize(2)
	t.Cleanup(func() { b.Clean(proc.Mp()) })
	for col, typ := range []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()} {
		var err error
		if selection != nil {
			b.Vecs[col], err = vector.NewOffHeapVecWithTypeAndAllocation(typ, selection)
			require.NoError(t, err)
		} else {
			b.Vecs[col] = vector.NewVec(typ)
		}
		for i := range n {
			if col == 0 {
				err = vector.AppendFixed(b.Vecs[col], int64(start+i), false, proc.Mp())
			} else {
				err = vector.AppendBytes(b.Vecs[col], []byte(strings.Repeat("x", 64)), i%3 == 1, proc.Mp())
			}
			require.NoError(t, err)
		}
	}
	require.NoError(t, b.Vecs[1].PreExtendGrouping(n, proc.Mp()))
	for i := range n {
		if i%5 == 2 {
			b.Vecs[1].GetGrouping().Set(uint64(i))
		}
	}
	b.SetRowCount(n)
	return b
}

func TestRowNumberStreamStreaming(t *testing.T) {
	for _, sourceAccounted := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary producer", true: "accounted producer"}[sourceAccounted], func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
			registry, err := mpool.NewAllocationAccountRegistry(1, 64)
			require.NoError(t, err)
			sourceRegistry, err := mpool.NewAllocationAccountRegistry(1, 64)
			require.NoError(t, err)
			var source *mpool.AllocationAccount
			var selection *vector.AllocationAccountSelection
			if sourceAccounted {
				source, err = sourceRegistry.Open(4 << 20)
				require.NoError(t, err)
				selection, err = vector.NewAllocationAccountSelection(source, mpool.AllocationOwnerOrder, 1, 2, 3, 4)
				require.NoError(t, err)
			}
			first := rowNumberInput(t, proc, selection, 0, 3)
			last := rowNumberInput(t, proc, selection, 3, colexec.DefaultBatchSize+3)
			child := &ordinalChild{MockOperator: colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, last}), results: []vm.CallResult{{Status: vm.ExecYield}, {Batch: batch.EmptyBatch, Status: vm.ExecNext}, {Batch: first, Status: vm.ExecNext}, {Status: vm.ExecYield}, {Batch: last, Status: vm.ExecStop}}}
			arg := newStreamingRowNumber(t)
			arg.AppendChild(child)
			defer func() {
				arg.Free(proc, false, nil)
				child.Free(proc, false, nil)
				if source != nil {
					require.Zero(t, source.Snapshot().Used)
					_, _, err := sourceRegistry.CompleteTerminal(source)
					require.NoError(t, err)
				}
				proc.Free()
				require.Zero(t, mp.CurrNB())
			}()
			sourceUsed := uint64(0)
			if source != nil {
				sourceUsed = source.Snapshot().Used
			}
			for generation := range 2 {
				child.calls = 0
				account, err := registry.Open(1 << 20)
				require.NoError(t, err)
				require.NoError(t, arg.SetAllocationAccount(account))
				require.NoError(t, arg.Prepare(proc))
				count, yields := 0, 0
				for {
					result, err := vm.Exec(arg, proc)
					require.NoError(t, err)
					if result.Status == vm.ExecYield {
						yields++
						continue
					}
					if result.Status == vm.ExecStop {
						break
					}
					require.NotNil(t, result.Batch)
					require.LessOrEqual(t, result.Batch.RowCount(), colexec.DefaultBatchSize)
					for i := range result.Batch.RowCount() {
						require.Equal(t, int64(count), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], i))
						local := count
						if count >= 3 {
							local = count - 3
						}
						require.Equal(t, local%3 == 1, result.Batch.Vecs[1].GetNulls().Contains(uint64(i)))
						require.Equal(t, local%5 == 2, result.Batch.Vecs[1].GetGrouping().Contains(uint64(i)))
						if local%3 != 1 {
							require.Equal(t, strings.Repeat("x", 64), result.Batch.Vecs[1].GetStringAt(i))
						}

						require.Equal(t, uint64(count+1), vector.GetFixedAtNoTypeCheck[uint64](result.Batch.Vecs[2], i))
						count++
					}
					require.Nil(t, arg.ctr.bat, "no materialized input copy")
					require.LessOrEqual(t, account.Snapshot().Used, uint64(8*colexec.DefaultBatchSize+4096))
					if generation == 1 {
						break
					} // Reset while the current output still borrows input.
				}
				if generation == 0 {
					require.Equal(t, colexec.DefaultBatchSize+6, count)
					require.Equal(t, 2, yields)
					require.Equal(t, 5, child.calls)
				}
				if source != nil {
					require.Equal(t, sourceUsed, source.Snapshot().Used)
				}
				arg.Reset(proc, false, nil)
				require.Zero(t, account.Snapshot().Used)
				require.Nil(t, arg.ctr.rowNumberInput)
				require.Zero(t, arg.ctr.rowNumberCounter)
				require.Equal(t, int64(3), vector.GetFixedAtNoTypeCheck[int64](last.Vecs[0], 0), "child backing remains owned by child")
				require.NoError(t, arg.ClearAllocationAccount(account))
				terminal, _, err := registry.CompleteTerminal(account)
				require.NoError(t, err)
				require.Zero(t, terminal.LiveAllocations)
			}
		})
	}
}

func TestRowNumberStreamTerminalFailures(t *testing.T) {
	for _, kind := range []string{"ordinal admission", "view admission", "overflow", "child error", "pre cancellation", "fill cancellation"} {
		t.Run(kind, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
			b := rowNumberInput(t, proc, nil, 0, 4)
			child := &ordinalChild{MockOperator: colexec.NewMockOperator().WithBatchs([]*batch.Batch{b}), results: []vm.CallResult{{Batch: b, Status: vm.ExecStop}}}
			arg := newStreamingRowNumber(t)
			arg.AppendChild(child)
			defer func() {
				arg.Free(proc, false, nil)
				child.Free(proc, false, nil)
				proc.Free()
				require.Zero(t, mp.CurrNB())
			}()
			registry, err := mpool.NewAllocationAccountRegistry(1, 32)
			require.NoError(t, err)
			limit := uint64(1024)
			if kind == "ordinal admission" {
				limit = 1
			}
			if kind == "view admission" {
				limit = 32
			}
			account, err := registry.Open(limit)
			require.NoError(t, err)
			require.NoError(t, arg.SetAllocationAccount(account))
			require.NoError(t, arg.Prepare(proc))
			switch kind {
			case "overflow":
				arg.ctr.rowNumberCounter = math.MaxUint64 - 2
			case "child error":
				child.fail = true
			case "pre cancellation", "fill cancellation":
				ctx, cancel := context.WithCancel(proc.Ctx)
				defer cancel()
				proc.Ctx = ctx
				if kind == "pre cancellation" {
					cancel()
				} else {
					child.cancel = cancel
				}
			}
			_, err = vm.Exec(arg, proc)
			require.Error(t, err)
			if kind == "ordinal admission" || kind == "view admission" {
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			}
			if kind == "child error" {
				require.ErrorIs(t, err, ordinalChildError)
			}
			require.Zero(t, account.Snapshot().Used)
			require.Nil(t, arg.ctr.rowNumberInput)
			require.Zero(t, arg.ctr.rowNumberCounter)
			require.Nil(t, arg.ctr.rBat)
			arg.Free(proc, false, err)
			require.NoError(t, arg.ClearAllocationAccount(account))
			terminal, _, err := registry.CompleteTerminal(account)
			require.NoError(t, err)
			require.Zero(t, terminal.LiveAllocations)
		})
	}
}

func TestRowNumberStreamEligibility(t *testing.T) {
	for _, kind := range []string{"eligible", "order", "partition", "function", "arguments", "multiple", "topn"} {
		t.Run(kind, func(t *testing.T) {
			arg := newStreamingRowNumber(t)
			spec := arg.WinSpecList[0].GetW()
			switch kind {
			case "order":
				spec.OrderBy = []*plan.OrderBySpec{{}}
			case "partition":
				spec.PartitionBy = []*plan.Expr{{}}
			case "function":
				arg.Aggs[0] = aggexec.MakeAggFunctionExpression(aggexec.WinIdOfRank, false, nil, nil)
			case "arguments":
				arg.Aggs[0] = aggexec.MakeAggFunctionExpression(aggexec.WinIdOfRowNumber, false, []*plan.Expr{{}}, nil)
			case "multiple":
				arg.WinSpecList = append(arg.WinSpecList, arg.WinSpecList[0])
			case "topn":
				arg.PartitionTopN = true
			}
			require.Equal(t, kind == "eligible", arg.canStreamRowNumber())
		})
	}
}

func TestRowNumberStreamSharedAccountAndInputReuse(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.Open(4 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerOrder, 1, 2, 3, 4)
	require.NoError(t, err)
	rows := colexec.DefaultBatchSize + 3
	input := rowNumberInput(t, proc, selection, 0, rows)
	// Constant vectors have one physical value but span every logical output row.
	constant, err := vector.NewConstFixed(types.T_int64.ToType(), int64(42), rows, mp)
	require.NoError(t, err)
	input.Vecs = append(input.Vecs, constant)
	sourceUsed := account.Snapshot().Used
	child := &ordinalChild{MockOperator: colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}), results: []vm.CallResult{{Batch: input, Status: vm.ExecNext}, {Batch: input, Status: vm.ExecStop}}}
	child.onCall = func(call int) {
		if call == 2 {
			for i := range rows {
				require.NoError(t, vector.SetFixedAtNoTypeCheck(input.Vecs[0], i, int64(rows+i)))
			}
		}
	}
	arg := newStreamingRowNumber(t)
	arg.AppendChild(child)
	require.NoError(t, arg.SetAllocationAccount(account))
	defer func() {
		arg.Free(proc, false, nil)
		child.Free(proc, false, nil)
		require.Zero(t, account.Snapshot().Used)
		require.NoError(t, arg.ClearAllocationAccount(account))
		terminal, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
		require.Zero(t, terminal.LiveAllocations)
		proc.Free()
		require.Zero(t, mp.CurrNB())
	}()
	require.NoError(t, arg.Prepare(proc))
	count := 0
	for {
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop {
			break
		}
		require.NotNil(t, result.Batch)
		for i := range result.Batch.RowCount() {
			require.Equal(t, int64(count), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], i))
			require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[2], i))
			require.Equal(t, uint64(count+1), vector.GetFixedAtNoTypeCheck[uint64](result.Batch.Vecs[3], i))
			count++
		}
		require.LessOrEqual(t, account.Snapshot().Used-sourceUsed, uint64(8*colexec.DefaultBatchSize+8192))
		if count < rows {
			require.Equal(t, 1, child.calls, "must not prefetch while an input chunk remains")
		}
	}
	require.Equal(t, 2*rows, count)
	require.Equal(t, 2, child.calls)
	arg.Reset(proc, false, nil)
	require.Equal(t, sourceUsed, account.Snapshot().Used)
	// Prepare reuse (without another Reset) must release the old output and start at one.
	child.calls = 0
	child.onCall = nil
	require.NoError(t, arg.Prepare(proc))
	first, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Equal(t, uint64(1), vector.GetFixedAtNoTypeCheck[uint64](first.Batch.Vecs[3], 0))
	child.calls = 0
	require.NoError(t, arg.Prepare(proc))
	first, err = vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Equal(t, uint64(1), vector.GetFixedAtNoTypeCheck[uint64](first.Batch.Vecs[3], 0))
	arg.Reset(proc, false, nil)
	require.Equal(t, sourceUsed, account.Snapshot().Used)
}

func TestRowNumberStreamRetiresGenericCacheOnPrepare(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	input := rowNumberInput(t, proc, nil, 0, 16)
	child := &ordinalChild{MockOperator: colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}), results: []vm.CallResult{{Batch: input, Status: vm.ExecNext}, {Status: vm.ExecStop}}}
	arg := &Window{WinSpecList: []*plan.Expr{makeAggWindowSpec("count")}, Aggs: []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(aggexec.AggIdOfCountStar, false, nil, nil)}}
	arg.AppendChild(child)
	defer func() {
		arg.Free(proc, false, nil)
		child.Free(proc, false, nil)
		proc.Free()
		require.Zero(t, mp.CurrNB())
	}()
	require.NoError(t, arg.Prepare(proc))
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Equal(t, 16, result.Batch.RowCount())
	arg.Reset(proc, false, nil)
	require.NotNil(t, arg.ctr.bat, "ordinary unaccounted windows retain reusable capacity")
	require.Positive(t, arg.ctr.bat.Allocated())
	streamed := newStreamingRowNumber(t)
	arg.WinSpecList = streamed.WinSpecList
	arg.Aggs = streamed.Aggs
	child.calls = 0
	require.NoError(t, arg.Prepare(proc))
	require.Nil(t, arg.ctr.bat, "a streaming generation must retire the unused full-input cache")
	result, err = vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Equal(t, uint64(1), vector.GetFixedAtNoTypeCheck[uint64](result.Batch.Vecs[2], 0))
	require.Equal(t, uint64(16), vector.GetFixedAtNoTypeCheck[uint64](result.Batch.Vecs[2], 15))
}

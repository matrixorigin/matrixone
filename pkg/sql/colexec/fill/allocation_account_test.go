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

package fill

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type fillTestAllocation struct {
	generation *process.ExecutionResourceGeneration
	registry   *mpool.AllocationAccountRegistry
	account    *mpool.AllocationAccount
}

func installFillTestAllocation(
	t testing.TB,
	op *Fill,
	proc *process.Process,
	limit uint64,
) fillTestAllocation {
	t.Helper()
	proc.Base.Lim.Size = int64(limit)
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<14)
	require.NoError(t, err)
	account, err := registry.OpenWithController(limit, generation)
	require.NoError(t, err)
	require.NoError(t, op.SetAllocationAccount(account))
	return fillTestAllocation{
		generation: generation,
		registry:   registry,
		account:    account,
	}
}

func finalizeFillTestAllocation(
	t testing.TB,
	op *Fill,
	state fillTestAllocation,
) {
	t.Helper()
	require.Zero(t, state.account.Snapshot().Used)
	require.NoError(t, op.ClearAllocationAccount(state.account))
	require.Zero(t, state.generation.Snapshot().Used)
	require.Zero(t, state.generation.SpillDiskUsed())
	require.Zero(t, state.generation.SpillFDUsed())
	snapshot, first, err := state.registry.CompleteTerminal(state.account)
	require.NoError(t, err)
	require.True(t, first)
	require.Zero(t, snapshot.Used)
	for _, owner := range snapshot.Owners {
		require.Zero(t, owner.Current)
	}
}

func newAccountedNextFill(spillThreshold int64) *Fill {
	return &Fill{
		ColLen:          1,
		FillType:        plan.Node_NEXT,
		PartitionColIdx: []int32{1},
		SpillThreshold:  spillThreshold,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
}

func collectFillInt64(
	t testing.TB,
	op *Fill,
	proc *process.Process,
) (values []int64, nulls []bool, err error) {
	t.Helper()
	for {
		result, callErr := vm.Exec(op, proc)
		if callErr != nil {
			return values, nulls, callErr
		}
		if result.Batch == nil || result.Status == vm.ExecStop {
			return values, nulls, nil
		}
		vec := result.Batch.Vecs[0]
		for row := 0; row < result.Batch.RowCount(); row++ {
			isNull := vec.IsNull(uint64(row))
			nulls = append(nulls, isNull)
			if isNull {
				values = append(values, 0)
			} else {
				values = append(values,
					vector.GetFixedAtNoTypeCheck[int64](vec, row))
			}
		}
	}
}

func TestAccountedFillResidentLifecycle(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedNextFill(1 << 30)
	state := installFillTestAllocation(t, op, proc, 64<<20)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		partitionedBatch(proc.Mp(), []int64{0, 2}, []uint64{0}, []int64{1, 1}),
		partitionedBatch(proc.Mp(), []int64{3, 4}, nil, []int64{1, 1}),
	})
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	values, nulls, err := collectFillInt64(t, op, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 2, 3, 4}, values)
	require.Equal(t, []bool{false, false, false, false}, nulls)
	require.Zero(t, op.OpAnalyzer.GetOpStats().SpillSize)
	owner, ok := state.account.OwnerUsage(mpool.AllocationOwnerFill)
	require.True(t, ok)
	require.Positive(t, owner.Peak)

	child.Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeFillTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedFillValueAndPrevLifecycle(t *testing.T) {
	tests := []struct {
		name     string
		fillType plan.Node_FillType
		fillVal  []*plan.Expr
		expected []int64
	}{
		{
			name:     "value",
			fillType: plan.Node_VALUE,
			fillVal: []*plan.Expr{{
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{
					Value: &plan.Literal_I64Val{I64Val: 5},
				}},
				Typ: plan.Type{Id: int32(types.T_int64)},
			}},
			expected: []int64{5, 2},
		},
		{
			name:     "previous",
			fillType: plan.Node_PREV,
			expected: []int64{1, 1, 3},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			op := &Fill{
				ColLen:          1,
				FillType:        test.fillType,
				FillVal:         test.fillVal,
				PartitionColIdx: []int32{1},
			}
			state := installFillTestAllocation(t, op, proc, 64<<20)
			var source *batch.Batch
			if test.fillType == plan.Node_VALUE {
				source = partitionedBatch(
					proc.Mp(), []int64{0, 2}, []uint64{0}, []int64{1, 1},
				)
			} else {
				source = partitionedBatch(
					proc.Mp(), []int64{1, 0, 3}, []uint64{1}, []int64{1, 1, 1},
				)
			}
			child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{source})
			op.AppendChild(child)
			require.NoError(t, op.Prepare(proc))
			values, nulls, err := collectFillInt64(t, op, proc)
			require.NoError(t, err)
			require.Equal(t, test.expected, values)
			require.Equal(t, make([]bool, len(values)), nulls)
			child.Free(proc, false, nil)
			op.Free(proc, false, nil)
			finalizeFillTestAllocation(t, op, state)
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestAccountedFillLinearSpillLifecycle(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := &Fill{
		ColLen:          1,
		FillType:        plan.Node_LINEAR,
		PartitionColIdx: []int32{1},
		SpillThreshold:  2,
	}
	state := installFillTestAllocation(t, op, proc, 64<<20)
	typ := types.New(types.T_decimal128, 38, 0)
	makeBatch := func(value int64, isNull bool) *batch.Batch {
		vec := vector.NewVec(typ)
		require.NoError(t, vector.AppendFixed(
			vec, types.Decimal128FromInt64(value), isNull, proc.Mp(),
		))
		bat := batch.NewWithSize(2)
		bat.SetVector(0, vec)
		bat.SetVector(1, testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp()))
		bat.SetRowCount(1)
		return bat
	}
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		makeBatch(10, false),
		makeBatch(0, true),
		makeBatch(0, true),
		makeBatch(90, false),
	})
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	var values []types.Decimal128
	for {
		result, err := vm.Exec(op, proc)
		require.NoError(t, err)
		if result.Batch == nil || result.Status == vm.ExecStop {
			break
		}
		values = append(values,
			vector.MustFixedColNoTypeCheck[types.Decimal128](result.Batch.Vecs[0])...)
	}
	require.Equal(t, []types.Decimal128{
		types.Decimal128FromInt64(10),
		types.Decimal128FromInt64(37),
		types.Decimal128FromInt64(63),
		types.Decimal128FromInt64(90),
	}, values)
	require.Positive(t, op.OpAnalyzer.GetOpStats().SpillSize)
	require.Zero(t, state.generation.SpillDiskUsed())
	require.Zero(t, state.generation.SpillFDUsed())

	child.Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeFillTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func makeAccountedLinearEndpointBatch(
	t testing.TB,
	proc *process.Process,
	value int64,
	isNull bool,
) *batch.Batch {
	t.Helper()
	vec := vector.NewVec(types.New(types.T_decimal256, 76, 0))
	require.NoError(t, vector.AppendFixed(vec, types.Decimal256FromInt64(value), isNull, proc.Mp()))
	bat := batch.NewWithSize(2)
	bat.SetVector(0, vec)
	bat.SetVector(1, testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp()))
	bat.SetRowCount(1)
	return bat
}

func TestAccountedFillLinearDecimal256ExpressionSelection(t *testing.T) {
	tests := []struct {
		name string
		typ  types.Type
	}{
		{name: "decimal128", typ: types.New(types.T_decimal128, 38, 0)},
		{name: "decimal256", typ: types.New(types.T_decimal256, 76, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			op := &Fill{ColLen: 1, FillType: plan.Node_LINEAR}
			state := installFillTestAllocation(t, op, proc, 64<<20)
			input := batch.NewWithSize(1)
			vec := vector.NewVec(test.typ)
			if test.typ.Oid == types.T_decimal256 {
				require.NoError(t, vector.AppendFixed(vec, types.Decimal256FromInt64(100), false, proc.Mp()))
				require.NoError(t, vector.AppendFixed(vec, types.Decimal256FromInt64(130), false, proc.Mp()))
			} else {
				require.NoError(t, vector.AppendFixed(vec, types.Decimal128FromInt64(100), false, proc.Mp()))
				require.NoError(t, vector.AppendFixed(vec, types.Decimal128FromInt64(130), false, proc.Mp()))
			}
			input.SetVector(0, vec)
			input.SetRowCount(2)

			result, owned, err := linearFillValue(&op.ctr, proc, 0, input, 0, input, 1)
			require.NoError(t, err)
			require.True(t, owned)
			require.Same(t, op.ctr.expressionAllocation, result.AllocationAccountSelection())
			if test.typ.Oid == types.T_decimal256 {
				require.Equal(t, types.Decimal256FromInt64(115), vector.GetFixedAtNoTypeCheck[types.Decimal256](result, 0))
			} else {
				require.Equal(t, types.Decimal128FromInt64(115), vector.GetFixedAtNoTypeCheck[types.Decimal128](result, 0))
			}
			owner, ok := state.account.OwnerUsage(mpool.AllocationOwnerFill)
			require.True(t, ok)
			require.Positive(t, owner.Peak)

			result.Free(proc.Mp())
			input.Clean(proc.Mp())
			op.Free(proc, false, nil)
			finalizeFillTestAllocation(t, op, state)
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestUnaccountedFillLinearDecimal256KeepsRegularVector(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := &Fill{ColLen: 1, FillType: plan.Node_LINEAR}
	input := batch.NewWithSize(1)
	vec := vector.NewVec(types.New(types.T_decimal256, 76, 0))
	require.NoError(t, vector.AppendFixed(vec, types.Decimal256FromInt64(100), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(vec, types.Decimal256FromInt64(130), false, proc.Mp()))
	input.SetVector(0, vec)
	input.SetRowCount(2)

	result, owned, err := linearFillValue(&op.ctr, proc, 0, input, 0, input, 1)
	require.NoError(t, err)
	require.True(t, owned)
	require.Nil(t, result.AllocationAccountSelection())
	require.Equal(t, types.Decimal256FromInt64(115), vector.GetFixedAtNoTypeCheck[types.Decimal256](result, 0))
	result.Free(proc.Mp())
	input.Clean(proc.Mp())
	op.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func runAccountedDecimal256LinearValue(t *testing.T, capacity uint64) (uint64, error) {
	t.Helper()
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := &Fill{ColLen: 1, FillType: plan.Node_LINEAR}
	state := installFillTestAllocation(t, op, proc, capacity)
	typ := types.New(types.T_decimal256, 76, 0)
	input := batch.NewWithSize(1)
	vec := vector.NewVec(typ)
	require.NoError(t, vector.AppendFixed(vec, types.Decimal256FromInt64(100), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(vec, types.Decimal256FromInt64(130), false, proc.Mp()))
	input.SetVector(0, vec)
	input.SetRowCount(2)
	result, owned, err := linearFillValue(&op.ctr, proc, 0, input, 0, input, 1)
	if err != nil {
		require.Nil(t, result)
		require.False(t, owned)
	} else {
		require.NotNil(t, result)
		require.True(t, owned)
	}
	if result != nil && owned {
		result.Free(proc.Mp())
	}
	owner, ok := state.account.OwnerUsage(mpool.AllocationOwnerFill)
	require.True(t, ok)
	peak := owner.Peak
	input.Clean(proc.Mp())
	op.Free(proc, err != nil, err)
	finalizeFillTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
	return peak, err
}

func TestAccountedFillLinearDecimal256BudgetBoundary(t *testing.T) {
	amplePeak, err := runAccountedDecimal256LinearValue(t, 64<<20)
	require.NoError(t, err)
	require.Positive(t, amplePeak)

	exactPeak, err := runAccountedDecimal256LinearValue(t, amplePeak)
	require.NoError(t, err)
	require.Equal(t, amplePeak, exactPeak)

	_, err = runAccountedDecimal256LinearValue(t, amplePeak-1)
	require.Error(t, err)
	require.True(t, mpool.IsRetryableAllocationCapacity(err))
}

func TestAccountedFillLinearDecimal256ResidentAndSpill(t *testing.T) {
	tests := []struct {
		name           string
		spillThreshold int64
		values         []int64
		expected       []types.Decimal256
		spills         bool
	}{
		{name: "resident", spillThreshold: 1 << 30, values: []int64{100, 0, 130}, expected: []types.Decimal256{
			types.Decimal256FromInt64(100), types.Decimal256FromInt64(115), types.Decimal256FromInt64(130),
		}},
		{name: "spill", spillThreshold: 2, values: []int64{100, 0, 0, 130}, expected: []types.Decimal256{
			types.Decimal256FromInt64(100), types.Decimal256FromInt64(110), types.Decimal256FromInt64(120), types.Decimal256FromInt64(130),
		}, spills: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			op := &Fill{ColLen: 1, FillType: plan.Node_LINEAR, PartitionColIdx: []int32{1}, SpillThreshold: test.spillThreshold}
			state := installFillTestAllocation(t, op, proc, 64<<20)
			batches := make([]*batch.Batch, 0, len(test.values))
			for i, value := range test.values {
				batches = append(batches, makeAccountedLinearEndpointBatch(t, proc, value, i != 0 && i != len(test.values)-1))
			}
			child := colexec.NewMockOperator().WithBatchs(batches)
			op.AppendChild(child)
			require.NoError(t, op.Prepare(proc))
			var got []types.Decimal256
			for {
				result, err := vm.Exec(op, proc)
				require.NoError(t, err)
				if result.Batch == nil || result.Status == vm.ExecStop {
					break
				}
				got = append(got, vector.MustFixedColNoTypeCheck[types.Decimal256](result.Batch.Vecs[0])...)
			}
			require.Equal(t, test.expected, got)
			if test.spills {
				require.Positive(t, op.OpAnalyzer.GetOpStats().SpillSize)
			} else {
				require.Zero(t, op.OpAnalyzer.GetOpStats().SpillSize)
			}
			child.Free(proc, false, nil)
			op.Reset(proc, false, nil)
			require.Zero(t, state.account.Snapshot().Used)
			require.Zero(t, state.generation.SpillDiskUsed())
			require.Zero(t, state.generation.SpillFDUsed())
			op.Free(proc, false, nil)
			finalizeFillTestAllocation(t, op, state)
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

type fillRunResult struct {
	peak      uint64
	spillSize int64
	values    []int64
	nulls     []bool
}

func runFillCapacityCase(t *testing.T, capacity uint64) (fillRunResult, error) {
	t.Helper()
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedNextFill(1 << 30)
	state := installFillTestAllocation(t, op, proc, capacity)
	const rows = 2048
	leftValues := make([]int64, rows)
	leftNulls := make([]uint64, rows)
	rightValues := make([]int64, rows)
	parts := make([]int64, rows)
	for i := range rows {
		leftNulls[i] = uint64(i)
		parts[i] = 1
		rightValues[i] = 7
	}
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		partitionedBatch(proc.Mp(), leftValues, leftNulls, parts),
		partitionedBatch(proc.Mp(), rightValues, nil, parts),
	})
	op.AppendChild(child)
	err := op.Prepare(proc)
	var values []int64
	var nulls []bool
	if err == nil {
		values, nulls, err = collectFillInt64(t, op, proc)
	}
	owner, _ := state.account.OwnerUsage(mpool.AllocationOwnerFill)
	result := fillRunResult{
		peak:      owner.Peak,
		spillSize: op.OpAnalyzer.GetOpStats().SpillSize,
		values:    values,
		nulls:     nulls,
	}
	child.Free(proc, err != nil, err)
	op.Free(proc, err != nil, err)
	finalizeFillTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
	return result, err
}

func TestAccountedFillOneByteShortSpillsAndPreservesResult(t *testing.T) {
	baseline, err := runFillCapacityCase(t, 64<<20)
	require.NoError(t, err)
	require.Positive(t, baseline.peak)
	require.Zero(t, baseline.spillSize)
	require.Len(t, baseline.values, 4096)
	for i := range baseline.values {
		require.Equal(t, int64(7), baseline.values[i])
		require.False(t, baseline.nulls[i])
	}

	exact, err := runFillCapacityCase(t, baseline.peak)
	require.NoError(t, err)
	require.Equal(t, baseline.values, exact.values)
	require.Equal(t, baseline.nulls, exact.nulls)
	require.Zero(t, exact.spillSize)

	short, err := runFillCapacityCase(t, baseline.peak-1)
	require.NoError(t, err)
	require.Equal(t, baseline.values, short.values)
	require.Equal(t, baseline.nulls, short.nulls)
	require.Positive(t, short.spillSize)
}

func TestAccountedFillSpillResourceAdmissionCleans(t *testing.T) {
	tests := []struct {
		name      string
		component process.ExecutionResourceComponent
		reserve   func(*process.ExecutionResourceGeneration) (func(), error)
	}{
		{
			name:      "disk",
			component: process.ExecutionResourceComponentSpillDisk,
			reserve: func(g *process.ExecutionResourceGeneration) (func(), error) {
				token, err := g.ReserveSpillDisk(g.SpillDiskCap())
				return func() {
					if token != nil {
						token.Release()
					}
				}, err
			},
		},
		{
			name:      "file-descriptor",
			component: process.ExecutionResourceComponentSpillFD,
			reserve: func(g *process.ExecutionResourceGeneration) (func(), error) {
				token, err := g.ReserveSpillFD(g.SpillFDCap())
				return func() {
					if token != nil {
						token.Release()
					}
				}, err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			op := newAccountedNextFill(1)
			state := installFillTestAllocation(t, op, proc, 64<<20)
			releaseBlocker, err := test.reserve(state.generation)
			require.NoError(t, err)
			child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
				partitionedBatch(proc.Mp(), []int64{0}, []uint64{0}, []int64{1}),
			})
			op.AppendChild(child)
			require.NoError(t, op.Prepare(proc))
			_, err = vm.Exec(op, proc)
			var resourceErr *process.ExecutionResourceError
			require.True(t, errors.As(err, &resourceErr))
			require.Equal(t, test.component, resourceErr.Component)

			child.Free(proc, true, err)
			op.Free(proc, true, err)
			releaseBlocker()
			finalizeFillTestAllocation(t, op, state)
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestAccountedFillResetAndReuse(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedNextFill(1 << 30)
	state := installFillTestAllocation(t, op, proc, 64<<20)
	first := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		partitionedBatch(proc.Mp(), []int64{0, 2}, []uint64{0}, []int64{1, 1}),
	})
	op.AppendChild(first)
	require.NoError(t, op.Prepare(proc))
	values, _, err := collectFillInt64(t, op, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 2}, values)
	first.Free(proc, false, nil)
	op.Reset(proc, false, nil)
	require.Zero(t, state.account.Snapshot().Used)
	require.Zero(t, state.generation.SpillDiskUsed())
	require.Zero(t, state.generation.SpillFDUsed())

	second := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		partitionedBatch(proc.Mp(), []int64{0, 9}, []uint64{0}, []int64{1, 1}),
	})
	op.SetChild(second, 0)
	require.NoError(t, op.Prepare(proc))
	values, _, err = collectFillInt64(t, op, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{9, 9}, values)
	second.Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeFillTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedFillBoundsPendingBatchMetadata(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedNextFill(1 << 30)
	state := installFillTestAllocation(t, op, proc, 64<<20)
	batches := make([]*batch.Batch, 0, maxFillPendingBatches+1)
	for range maxFillPendingBatches {
		batches = append(batches, partitionedBatch(
			proc.Mp(), []int64{0}, []uint64{0}, []int64{1},
		))
	}
	batches = append(batches, partitionedBatch(
		proc.Mp(), []int64{7}, nil, []int64{1},
	))
	child := colexec.NewMockOperator().WithBatchs(batches)
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	values, nulls, err := collectFillInt64(t, op, proc)
	require.NoError(t, err)
	require.Len(t, values, maxFillPendingBatches+1)
	require.Equal(t, make([]bool, len(nulls)), nulls)
	for _, value := range values {
		require.Equal(t, int64(7), value)
	}
	require.Positive(t, op.OpAnalyzer.GetOpStats().SpillSize)
	require.LessOrEqual(t, cap(op.ctr.bats), maxFillPendingBatches)

	child.Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeFillTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedFillCancellationReleasesSpill(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedNextFill(1)
	state := installFillTestAllocation(t, op, proc, 64<<20)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		partitionedBatch(proc.Mp(), []int64{0}, []uint64{0}, []int64{1}),
		partitionedBatch(proc.Mp(), []int64{0}, []uint64{0}, []int64{1}),
		partitionedBatch(proc.Mp(), []int64{7}, nil, []int64{1}),
	})
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	result, err := vm.Exec(op, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.NotNil(t, op.ctr.spill)
	cancelCtx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = cancelCtx
	cancel()
	_, err = vm.Exec(op, proc)
	require.ErrorIs(t, err, context.Canceled)

	child.Free(proc, true, err)
	op.Free(proc, true, err)
	finalizeFillTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedFillResetReleasesSpillSuffix(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := &Fill{
		ColLen:          2,
		FillType:        plan.Node_NEXT,
		PartitionColIdx: []int32{2},
		SpillThreshold:  2,
	}
	state := installFillTestAllocation(t, op, proc, 64<<20)
	input := batch.NewWithSize(3)
	input.SetVector(0,
		testutil.MakeInt64Vector([]int64{10, 0}, []uint64{1}, proc.Mp()))
	input.SetVector(1,
		testutil.MakeInt64Vector([]int64{0, 20}, []uint64{0}, proc.Mp()))
	input.SetVector(2,
		testutil.MakeInt64Vector([]int64{1, 1}, nil, proc.Mp()))
	input.SetRowCount(2)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	result, err := vm.Exec(op, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.NotNil(t, op.ctr.spill)
	require.NotNil(t, op.ctr.spill.next)

	child.Free(proc, false, nil)
	op.Reset(proc, false, nil)
	require.Zero(t, state.account.Snapshot().Used)
	require.Zero(t, state.generation.SpillDiskUsed())
	require.Zero(t, state.generation.SpillFDUsed())
	op.Free(proc, false, nil)
	finalizeFillTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestFillAllocationAccountIdentity(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(2, 8)
	require.NoError(t, err)
	first, err := registry.Open(1 << 20)
	require.NoError(t, err)
	second, err := registry.Open(1 << 20)
	require.NoError(t, err)
	op := newAccountedNextFill(0)
	require.NoError(t, op.SetAllocationAccount(first))
	require.NoError(t, op.SetAllocationAccount(first))
	require.ErrorIs(t, op.SetAllocationAccount(second), mpool.ErrAllocationAccountMismatch)
	require.ErrorIs(t, op.ClearAllocationAccount(second), mpool.ErrAllocationAccountMismatch)
	require.NoError(t, op.ClearAllocationAccount(first))
	_, _, err = registry.CompleteTerminal(first)
	require.NoError(t, err)
	_, _, err = registry.CompleteTerminal(second)
	require.NoError(t, err)
}

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

package colexec

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccountedFixedMaskedResultNullsAndReuse(t *testing.T) {
	for _, oid := range []types.T{types.T_int64, types.T_decimal128} {
		t.Run(oid.String(), func(t *testing.T) {
			proc, allocation, _ := dependentTestAllocation(t)
			typ := types.New(oid, 18, 0)
			input := batch.NewWithSize(1)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			input.Vecs[0] = vector.NewVec(typ)
			for row := range 12 {
				if oid == types.T_int64 {
					require.NoError(t, vector.AppendFixed(input.Vecs[0], -int64(row+1), row == 1, proc.Mp()))
				} else {
					value := types.Decimal128{B0_63: uint64(row + 1)}.Minus()
					require.NoError(t, vector.AppendFixed(input.Vecs[0], value, row == 1, proc.Mp()))
				}
			}
			input.SetRowCount(12)
			column := &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			executor, err := NewExpressionExecutorWithAllocation(proc, bindTestFunction(t, proc, "abs", column), allocation)
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			for index, mask := range [][]bool{
				{true, true, false, false, false, false, false, false, false, false, false, true},
				{false, true, true, true, true, true, true, true, true, true, true, false},
				{false, false, false, false, false, false, false, false, false, false, false, false},
				{true, true, true, true, true, true, true, true, true, true, true, true},
				{false, false, true, false, false, false, false, false, false, false, false, true},
			} {
				result, err := executor.Eval(proc, []*batch.Batch{input}, mask)
				require.NoError(t, err)
				if index == 0 {
					require.Nil(t, executor.(*FunctionExpressionExecutor).selectedNullResult,
						"sparse fixed output must avoid NULL-placeholder appends")
				}
				require.Equal(t, 12, result.Length())
				for row, selected := range mask {
					isNull := !selected || row == 1
					require.Equal(t, isNull, result.IsNull(uint64(row)))
					if isNull {
						continue
					}
					if oid == types.T_int64 {
						require.Equal(t, int64(row+1), vector.GetFixedAtNoTypeCheck[int64](result, row))
					} else {
						require.Equal(t, types.Decimal128{B0_63: uint64(row + 1)}, vector.GetFixedAtNoTypeCheck[types.Decimal128](result, row))
					}
				}
				require.Same(t, allocation, result.AllocationAccountSelection())
			}
		})
	}
}

func TestAccountedExpressionTreeCoversNestedAndSelectedResults(t *testing.T) {
	pool := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(pool)
		assert.Zero(t, pool.CurrNB(), "native storage before pool deletion")
		bytes, objects := pool.OnHeapOutstanding()
		assert.Zero(t, bytes, "heap storage before pool deletion")
		assert.Zero(t, objects, "heap objects before pool deletion")
	})
	proc := testutil.NewProcess(t, testutil.WithMPool(pool), testutil.WithFileService(nil))
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.Open(4 << 10)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.Zero(t, account.Snapshot().Used)
		assert.Zero(t, registry.LiveAllocationMetadata())
		snapshot := account.Seal()
		assert.Zero(t, snapshot.Used)
		_, err := registry.Finalize(account)
		assert.NoError(t, err)
	})
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	typ := types.T_varchar.ToType()
	column := &plan.Expr{
		Typ:  plan.Type{Id: int32(typ.Oid), Width: typ.Width},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}

	literal := &plan.Expr{
		Typ: plan.Type{Id: int32(typ.Oid), Width: typ.Width},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Sval{Sval: "-"},
		}},
	}
	expression := bindTestFunction(t, proc, "concat", bindTestFunction(t, proc, "lower", column), column, literal)
	executor, err := NewExpressionExecutorWithAllocation(proc, expression, selection)
	require.NoError(t, err)
	t.Cleanup(func() {
		if executor != nil {
			executor.Free()
		}
	})
	root := executor.(*FunctionExpressionExecutor)
	nested := root.parameterExecutor[0].(*FunctionExpressionExecutor)

	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeVarcharVector(
		[]string{"AA", "BB", "CC", "DD"}, nil, proc.Mp(),
	)
	input.SetRowCount(4)
	defer input.Clean(proc.Mp())
	result, err := executor.Eval(
		proc, []*batch.Batch{input}, []bool{true, false, true, false},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"aaAA-", "", "ccCC-", ""}, vector.InefficientMustStrCol(result))

	assertFunctionStorage := func(function *FunctionExpressionExecutor) {
		t.Helper()
		require.Same(t, selection, function.resultVector.GetResultVector().AllocationAccountSelection())
		require.Same(t, selection, function.selectedResult.GetResultVector().AllocationAccountSelection())
		for _, selected := range function.selectedParameterVectors {
			if selected != nil {
				require.Same(t, selection, selected.AllocationAccountSelection())
			}
		}
	}
	// Grow the selected path beyond its account budget, then retry the same
	// executor with a small partial batch. Admission failure must not poison it.
	large := batch.NewWithSize(1)
	defer large.Clean(proc.Mp())
	large.Vecs[0] = vector.NewVec(typ)
	for range 4 {
		require.NoError(t, vector.AppendBytes(large.Vecs[0], []byte(strings.Repeat("A", 2048)), false, proc.Mp()))
	}
	large.SetRowCount(4)
	_, err = executor.Eval(proc, []*batch.Batch{large}, []bool{true, false, true, false})
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.LessOrEqual(t, account.Snapshot().Used, account.Snapshot().Limit)
	result, err = executor.Eval(proc, []*batch.Batch{input}, []bool{true, false, true, false})
	require.NoError(t, err)
	require.Equal(t, []string{"aaAA-", "", "ccCC-", ""}, vector.InefficientMustStrCol(result))

	assertFunctionStorage(root)
	assertFunctionStorage(nested)
	fixed := root.parameterExecutor[2].(*FixedVectorExpressionExecutor)
	require.Same(t, selection, fixed.resultVector.AllocationAccountSelection())
	require.Positive(t, account.Snapshot().Used)

	// Reuse the allocation-accounted result vectors after the first evaluation.
	// ResetWithSameType clears the bitmap logical length while retaining its
	// external storage. NULL propagation through lower/concat must still mark
	// the second row as NULL instead of treating the reset bitmap as empty.
	second := batch.NewWithSize(1)
	second.Vecs[0] = testutil.MakeVarcharVector(
		[]string{"AA", "", "CC", "DD"}, []uint64{1}, proc.Mp(),
	)
	second.SetRowCount(4)
	defer second.Clean(proc.Mp())
	result, err = executor.Eval(proc, []*batch.Batch{second}, nil)
	require.NoError(t, err)
	require.True(t, result.IsNull(1))

	// Exercise the nullable string comparison from issue #27432. The reused
	// input deliberately retains a NULL beyond both the current four-row batch
	// and the result's external bitmap capacity. The in-range NULL must still
	// propagate without growing the result to the source's stale logical length.
	equalLiteral := &plan.Expr{
		Typ: plan.Type{Id: int32(typ.Oid), Width: typ.Width},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Sval{Sval: "AA"},
		}},
	}
	equalExecutor, err := NewExpressionExecutorWithAllocation(
		proc, bindTestFunction(t, proc, "=", column, equalLiteral), selection,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if equalExecutor != nil {
			equalExecutor.Free()
		}
	})
	_, err = equalExecutor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	equalRoot := equalExecutor.(*FunctionExpressionExecutor)
	require.Zero(t, equalRoot.resultVector.GetResultVector().GetNulls().GetBitmap().ExternalStorageCapacity(),
		"the first non-NULL generation needs no validity reservation")
	// Establish real nullable storage before probing a stale NULL beyond its
	// bound. The non-NULL fast path intentionally has no bitmap to exceed.
	equalResult, err := equalExecutor.Eval(proc, []*batch.Batch{second}, nil)
	require.NoError(t, err)
	require.True(t, equalResult.IsNull(1))
	capacityRows := uint64(equalRoot.resultVector.GetResultVector().
		GetNulls().GetBitmap().ExternalStorageCapacity() * 64)
	require.GreaterOrEqual(t, capacityRows, uint64(4))
	second.Vecs[0].GetNulls().Add(capacityRows)

	equalResult, err = equalExecutor.Eval(proc, []*batch.Batch{second}, nil)
	require.NoError(t, err)
	require.True(t, equalResult.IsNull(1))
	require.False(t, equalResult.IsNull(capacityRows))
	equalExecutor.Free()
	equalExecutor = nil

	executor.Free()
	executor = nil
	require.Zero(t, account.Snapshot().Used)
	// The valid first literal allocates, but a later invalid literal must
	// roll the entire partially constructed tree back to its entry state.
	func() {
		first, err := NewExpressionExecutorWithAllocation(proc, literal, selection)
		require.NoError(t, err)
		defer first.Free()
		require.Positive(t, account.Snapshot().Used)
	}()
	invalid := &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{
		Value: &plan.Literal_Sval{Sval: "invalid source"}, StringSource: 257,
	}}}
	partial, err := NewExpressionExecutorWithAllocation(proc,
		bindTestFunction(t, proc, "concat", literal, invalid), selection)
	if partial != nil {
		t.Cleanup(partial.Free)
	}
	require.ErrorContains(t, err, "invalid literal string source")
	require.Nil(t, partial)
	require.Zero(t, account.Snapshot().Used, "failed later child must release the first literal")
	require.Zero(t, registry.LiveAllocationMetadata())
}

func TestAccountedFixedCrossDomainConstBroadcastUsesPhysicalMetadata(t *testing.T) {
	pool := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(pool)
		assert.Zero(t, pool.CurrNB(), "native storage before pool deletion")
		bytes, objects := pool.OnHeapOutstanding()
		assert.Zero(t, bytes, "heap storage before pool deletion")
		assert.Zero(t, objects, "heap objects before pool deletion")
	})
	proc := testutil.NewProcess(t, testutil.WithMPool(pool), testutil.WithFileService(nil))
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.Zero(t, account.Snapshot().Used)
		assert.Zero(t, registry.LiveAllocationMetadata())
		snapshot := account.Seal()
		assert.Zero(t, snapshot.Used)
		_, err := registry.Finalize(account)
		assert.NoError(t, err)
	})
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	expression := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_varbinary)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value:       &plan.Literal_Sval{Sval: "selected"},
			LiteralForm: plan.StringLiteralForm_STRING_LITERAL_TEXT,
		}},
	}
	executor, err := NewExpressionExecutorWithAllocation(proc, expression, selection)
	require.NoError(t, err)
	t.Cleanup(func() {
		if executor != nil {
			executor.Free()
		}
	})
	used := account.Snapshot().Used
	input := batch.NewWithSize(0)
	input.SetRowCount(65)

	var result *vector.Vector
	require.NotPanics(t, func() {
		result, err = executor.Eval(proc, []*batch.Batch{input}, nil)
	})
	require.NoError(t, err)
	require.Equal(t, 65, result.Length())
	require.Equal(t, types.RuntimeStringText, result.GetRuntimeStringDomainAt(0))
	require.Equal(t, types.RuntimeStringText, result.GetRuntimeStringDomainAt(64))
	require.Equal(t, used, account.Snapshot().Used)

	executor.Free()
	executor = nil
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, registry.LiveAllocationMetadata())
}

func TestAccountedLiteralConstructorReleasesPayloadOnDomainDenial(t *testing.T) {
	pool := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(pool)
		assert.Zero(t, pool.CurrNB(), "native storage before pool deletion")
		bytes, objects := pool.OnHeapOutstanding()
		assert.Zero(t, bytes, "heap storage before pool deletion")
		assert.Zero(t, objects, "heap objects before pool deletion")
	})
	proc := testutil.NewProcess(t, testutil.WithMPool(pool), testutil.WithFileService(nil))
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	// One inline varlen cell and its NULL bitmap fit; the text-domain bitmap does not.
	account, err := registry.Open(types.VarlenaSize + 16)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.Zero(t, account.Snapshot().Used)
		assert.Zero(t, registry.LiveAllocationMetadata())
		snapshot := account.Seal()
		assert.Zero(t, snapshot.Used)
		_, err := registry.Finalize(account)
		assert.NoError(t, err)
	})
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	func() {
		payload, err := vector.NewConstBytesWithAllocation(types.T_varbinary.ToType(), []byte("selected"), 1, proc.Mp(), selection)
		require.NoError(t, err, "payload allocation must succeed before domain admission")
		defer payload.Free(proc.Mp())
		require.Positive(t, account.Snapshot().Used)
	}()
	executor, err := NewExpressionExecutorWithAllocation(proc, &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_varbinary)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Sval{Sval: "selected"}, LiteralForm: plan.StringLiteralForm_STRING_LITERAL_TEXT,
		}},
	}, selection)
	if executor != nil {
		t.Cleanup(executor.Free)
	}
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Nil(t, executor)
	require.Zero(t, account.Snapshot().Used, "constructor must release the already materialized payload")
	require.Zero(t, registry.LiveAllocationMetadata())
}

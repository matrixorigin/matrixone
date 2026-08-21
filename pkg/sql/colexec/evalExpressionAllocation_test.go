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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestAccountedExpressionTreeCoversNestedAndSelectedResults(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	typ := types.T_varchar.ToType()
	column := &plan.Expr{
		Typ:  plan.Type{Id: int32(typ.Oid), Width: typ.Width},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	bindFunction := func(name string, args ...*plan.Expr) *plan.Expr {
		argTypes := make([]types.Type, len(args))
		for i := range args {
			argTypes[i] = types.New(
				types.T(args[i].Typ.Id), args[i].Typ.Width, args[i].Typ.Scale,
			)
		}
		fn, bindErr := function.GetFunctionByName(proc.Ctx, name, argTypes)
		require.NoError(t, bindErr)
		retType := fn.GetReturnType()
		return &plan.Expr{
			Typ: plan.Type{
				Id: int32(retType.Oid), Width: retType.Width, Scale: retType.Scale,
			},
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: name},
				Args: args,
			}},
		}
	}
	literal := &plan.Expr{
		Typ: plan.Type{Id: int32(typ.Oid), Width: typ.Width},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Sval{Sval: "-"},
		}},
	}
	expression := bindFunction("concat", bindFunction("lower", column), column, literal)
	executor, err := NewExpressionExecutorWithAllocation(proc, expression, selection)
	require.NoError(t, err)
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

	executor.Free()
	require.Zero(t, account.Snapshot().Used)
}

func TestAccountedFixedCrossDomainConstBroadcastUsesPhysicalMetadata(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
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
	require.Zero(t, account.Snapshot().Used)
	snapshot := account.Seal()
	require.Zero(t, snapshot.Used)
	require.Zero(t, registry.LiveAllocationMetadata())
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

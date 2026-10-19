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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDependentSlotPreservesNativeValueAndLifetime(t *testing.T) {
	proc, allocation, _ := dependentTestAllocation(t)
	typ := types.T_varbinary.ToType()
	typ.Width = 4
	source := vector.NewVec(typ)
	defer source.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(source, []byte{0, 255, 1}, false, proc.Mp()))
	child, err := proc.NewDependentProcess(3, []*vector.Vector{source}, 0)
	require.NoError(t, err)
	defer child.CloseDependentProcess()
	expression := &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Slot{Slot: &plan.CorrelationSlot{ProgramId: 3}}}
	executor, err := NewExpressionExecutorWithAllocation(child, expression, allocation)
	require.NoError(t, err)
	defer executor.Free()
	input := batch.NewWithSize(0)
	input.SetRowCount(3)
	result, err := executor.Eval(child, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.True(t, result.IsConst())
	require.Equal(t, 3, result.Length())
	require.Equal(t, []byte{0, 255, 1}, result.GetBytesAt(2))
	require.Equal(t, typ, *result.GetType())
	source.CleanOnlyData()
	require.NoError(t, vector.AppendBytes(source, []byte{2}, false, proc.Mp()))
	require.Equal(t, []byte{0, 255, 1}, result.GetBytesAt(0))
	// An empty selection neither reads a retired slot nor preserves stale bytes.
	child.CloseDependentProcess()
	result, err = executor.Eval(child, []*batch.Batch{input}, []bool{false, false, false})
	require.NoError(t, err)
	require.True(t, result.IsConstNull())
	_, err = executor.Eval(child, []*batch.Batch{input}, nil)
	require.ErrorContains(t, err, "retired dependent slot")
}

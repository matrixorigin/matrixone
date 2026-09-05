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

package dedupjoin

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestODKUAffectedRowsRules(t *testing.T) {
	require.EqualValues(t, 2, odkuAffectedRows(true, false))
	require.EqualValues(t, 0, odkuAffectedRows(false, false))
	require.EqualValues(t, 1, odkuAffectedRows(false, true))
}

func TestODKUValueEqualityUsesSQLFloatSemantics(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	left := vector.NewVec(types.T_float64.ToType())
	right := vector.NewVec(types.T_float64.ToType())
	defer left.Free(proc.Mp())
	defer right.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(left, math.Copysign(0, -1), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(right, float64(0), false, proc.Mp()))
	require.True(t, odkuValuesEqual(left, right), "-0 and +0 are SQL-equal and must remain a no-op")
}

func TestODKUSequentialValuesSurviveProbeAdvance(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := types.T_int32.ToType()
	leftValue := vector.NewVec(typ)
	rightValue := vector.NewVec(typ)
	defer leftValue.Free(proc.Mp())
	defer rightValue.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(leftValue, int32(10), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(rightValue, int32(11), false, proc.Mp()))

	exec, err := colexec.NewExpressionExecutor(proc, &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int32)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0}},
	})
	require.NoError(t, err)
	defer exec.Free()
	leftBat := &batch.Batch{Vecs: []*vector.Vector{leftValue}}
	rightBat := &batch.Batch{Vecs: []*vector.Vector{rightValue}}
	leftBat.SetRowCount(1)
	rightBat.SetRowCount(1)
	ctr := &container{
		joinBat1:   leftBat,
		joinBat2:   rightBat,
		exprExecs:  []colexec.ExpressionExecutor{exec},
		stableCols: []int32{0}, // normally derived once by Prepare
	}
	defer ctr.cleanStableUpdateVecs(proc)

	changed, err := ctr.applyUpdateExpressions(proc, []int32{0}, []int32{0})
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, int32(11), vector.GetFixedAtNoTypeCheck[int32](ctr.joinBat1.Vecs[0], 0))
	require.NotSame(t, rightValue, ctr.joinBat1.Vecs[0],
		"the current row must not alias the next incoming VALUES vector")

	rightValue.CleanOnlyData()
	require.NoError(t, vector.AppendFixed(rightValue, int32(12), false, proc.Mp()))
	require.Equal(t, int32(11), vector.GetFixedAtNoTypeCheck[int32](ctr.joinBat1.Vecs[0], 0),
		"advancing the probe row must not mutate the prior logical result")
	changed, err = ctr.applyUpdateExpressions(proc, []int32{0}, []int32{0})
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, int32(12), vector.GetFixedAtNoTypeCheck[int32](ctr.joinBat1.Vecs[0], 0))

	for value := int32(13); value < 100; value++ {
		rightValue.CleanOnlyData()
		require.NoError(t, vector.AppendFixed(rightValue, value, false, proc.Mp()))
		changed, err = ctr.applyUpdateExpressions(proc, []int32{0}, []int32{0})
		require.NoError(t, err)
		require.True(t, changed)
	}
	require.LessOrEqual(t, len(ctr.stableUpdateVecs[0]), 2,
		"replaying an arbitrarily long duplicate group must use bounded scratch vectors")
}

func TestODKUPhysicalChangeSeparatesImplicitColumnsFromNoOp(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := types.T_int64.ToType()
	oldValue := vector.NewVec(typ)
	oldTimestamp := vector.NewVec(typ)
	finalValue := vector.NewVec(typ)
	finalTimestamp := vector.NewVec(typ)
	for _, vec := range []*vector.Vector{oldValue, oldTimestamp, finalValue, finalTimestamp} {
		defer vec.Free(proc.Mp())
	}
	require.NoError(t, vector.AppendFixed(oldValue, int64(10), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(oldTimestamp, int64(100), false, proc.Mp()))
	// Repeated logical updates restored the explicit value but an implicit ON
	// UPDATE column retained the effect of the earlier changing action.
	require.NoError(t, vector.AppendFixed(finalValue, int64(10), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(finalTimestamp, int64(101), false, proc.Mp()))
	final := &batch.Batch{Vecs: []*vector.Vector{finalValue, finalTimestamp}}
	final.SetRowCount(1)

	require.True(t, odkuPhysicalChanged(
		true, []*vector.Vector{oldValue, oldTimestamp}, final, []int32{0, 1}),
		"an implicit-column change must survive when an earlier logical action changed the row")
	require.False(t, odkuPhysicalChanged(
		false, []*vector.Vector{oldValue, oldTimestamp}, final, []int32{0, 1}),
		"a pure no-op must not fire an implicit ON UPDATE expression")
}

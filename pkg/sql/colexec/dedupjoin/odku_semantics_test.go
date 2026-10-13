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
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
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

func TestODKUMetadataContractRejectsMalformedPlans(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	keyType := types.T_int32.ToType()
	uint64Type := types.T_uint64.ToType()
	boolType := types.T_bool.ToType()
	condition := newExpr(0, keyType)

	newValidArg := func() *DedupJoin {
		return &DedupJoin{
			LeftTypes:  []types.Type{keyType},
			RightTypes: []types.Type{uint64Type, boolType, boolType, boolType},
			Conditions: [][]*plan.Expr{{condition}, {condition}},
			Result: []colexec.ResultPos{
				colexec.NewResultPos(1, 0),
				colexec.NewResultPos(1, 1),
				colexec.NewResultPos(1, 2),
				colexec.NewResultPos(1, 3),
			},
			HasODKUAffectedRows:      true,
			AffectedRowsResultPos:    0,
			PhysicalChangedResultPos: 1,
			EmitActionRows:           true,
			ActionFinalResultPos:     2,
		}
	}

	tests := []struct {
		name    string
		wantErr string
		mutate  func(*DedupJoin)
	}{
		{
			name:    "update column and expression counts differ",
			wantErr: "update column/expression count mismatch",
			mutate:  func(arg *DedupJoin) { arg.UpdateColIdxList = []int32{0} },
		},
		{
			name:    "update column is out of range",
			wantErr: "update column out of range",
			mutate: func(arg *DedupJoin) {
				arg.UpdateColIdxList = []int32{1}
				arg.UpdateColExprList = []*plan.Expr{condition}
			},
		},
		{
			name:    "metadata result column is out of range",
			wantErr: "result column out of range",
			mutate:  func(arg *DedupJoin) { arg.AffectedRowsResultPos = 4 },
		},
		{
			name:    "metadata left source column is out of range",
			wantErr: "source column out of range",
			mutate:  func(arg *DedupJoin) { arg.Result[0] = colexec.NewResultPos(0, 1) },
		},
		{
			name:    "action marker has the wrong type with left-side affected-row metadata",
			wantErr: "expected BOOL",
			mutate: func(arg *DedupJoin) {
				arg.LeftTypes[0] = uint64Type
				arg.Result[0] = colexec.NewResultPos(0, 0)
				arg.Result[2] = colexec.NewResultPos(1, 0)
			},
		},
		{
			name:    "metadata right source column is out of range",
			wantErr: "source column out of range",
			mutate:  func(arg *DedupJoin) { arg.Result[0] = colexec.NewResultPos(1, 4) },
		},
		{
			name:    "metadata source has the wrong type",
			wantErr: "expected BIGINT UNSIGNED",
			mutate:  func(arg *DedupJoin) { arg.Result[0] = colexec.NewResultPos(1, 1) },
		},
		{
			name:    "metadata result position is shared",
			wantErr: "result column is shared",
			mutate:  func(arg *DedupJoin) { arg.PhysicalChangedResultPos = 0 },
		},
		{
			name:    "FK eligibility result column is out of range",
			wantErr: "result column out of range",
			mutate: func(arg *DedupJoin) {
				arg.ForeignKeyChecks = []ODKUForeignKeyCheck{{ColIdxList: []int32{0}, EligibilityResultPos: 4}}
			},
		},
		{
			name:    "FK eligibility marker has the wrong type",
			wantErr: "expected BOOL",
			mutate: func(arg *DedupJoin) {
				arg.ForeignKeyChecks = []ODKUForeignKeyCheck{{ColIdxList: []int32{0}, EligibilityResultPos: 3}}
				arg.Result[3] = colexec.NewResultPos(1, 0)
			},
		},
		{
			name:    "FK check has no child columns",
			wantErr: "FK check has no columns",
			mutate: func(arg *DedupJoin) {
				arg.ForeignKeyChecks = []ODKUForeignKeyCheck{{EligibilityResultPos: 3}}
			},
		},
		{
			name:    "FK child column is out of range",
			wantErr: "FK column out of range",
			mutate: func(arg *DedupJoin) {
				arg.ForeignKeyChecks = []ODKUForeignKeyCheck{{ColIdxList: []int32{1}, EligibilityResultPos: 3}}
			},
		},
		{
			name:    "ODKU comparison column is out of range",
			wantErr: "ODKU check column out of range",
			mutate:  func(arg *DedupJoin) { arg.UpdateCheckColIdxList = []int32{1} },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			arg := newValidArg()
			test.mutate(arg)
			installTestAllocation(t, arg)
			t.Cleanup(func() { arg.Free(proc, false, nil) })
			require.ErrorContains(t, arg.Prepare(proc), test.wantErr)
		})
	}
}

func TestODKUValueEqualityUsesSQLJSONAndScaledFloatSemantics(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	jsonLeft := vector.NewVec(types.T_json.ToType())
	jsonRight := vector.NewVec(types.T_json.ToType())
	defer jsonLeft.Free(proc.Mp())
	defer jsonRight.Free(proc.Mp())
	one, err := bytejson.ParseJsonByteFromString("1")
	require.NoError(t, err)
	onePointZero, err := bytejson.ParseJsonByteFromString("1.0")
	require.NoError(t, err)
	require.NoError(t, vector.AppendBytes(jsonLeft, one, false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(jsonRight, onePointZero, false, proc.Mp()))
	require.True(t, odkuValuesEqual(jsonLeft, jsonRight),
		"JSON numeric encodings that compare equal must remain a no-op")

	floatType := types.T_float32.ToType()
	floatType.Scale = 2
	floatLeft := vector.NewVec(floatType)
	floatRight := vector.NewVec(floatType)
	defer floatLeft.Free(proc.Mp())
	defer floatRight.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(floatLeft, float32(1.234), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(floatRight, float32(1.231), false, proc.Mp()))
	require.True(t, odkuValuesEqual(floatLeft, floatRight),
		"FLOAT32 comparisons normalize values to the declared scale")
}

func TestODKUNoOpActionRestoresImplicitColumnsImmediately(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	storedValue := testutil.MakeInt64Vector([]int64{10}, nil, proc.Mp())
	storedTimestamp := testutil.MakeInt64Vector([]int64{100}, nil, proc.Mp())
	incomingValue := testutil.MakeInt64Vector([]int64{10}, nil, proc.Mp())
	incomingTimestamp := testutil.MakeInt64Vector([]int64{101}, nil, proc.Mp())
	for _, vec := range []*vector.Vector{storedValue, storedTimestamp, incomingValue, incomingTimestamp} {
		defer vec.Free(proc.Mp())
	}
	left := &batch.Batch{Vecs: []*vector.Vector{storedValue, storedTimestamp}}
	right := &batch.Batch{Vecs: []*vector.Vector{incomingValue, incomingTimestamp}}
	left.SetRowCount(1)
	right.SetRowCount(1)
	execs := make([]colexec.ExpressionExecutor, 2)
	for i := range execs {
		var err error
		execs[i], err = colexec.NewExpressionExecutor(proc, &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: int32(i)}},
		})
		require.NoError(t, err)
		defer execs[i].Free()
	}
	ctr := &container{
		joinBat1: left, joinBat2: right, exprExecs: execs, stableCols: []int32{0, 1},
	}
	defer ctr.cleanStableUpdateVecs(proc)
	changed, err := ctr.applyUpdateExpressions(proc, []int32{0, 1}, []int32{0})
	require.NoError(t, err)
	require.False(t, changed)
	require.Same(t, storedValue, ctr.joinBat1.Vecs[0])
	require.Same(t, storedTimestamp, ctr.joinBat1.Vecs[1],
		"a no-op action must not leak an implicit value into validation or the next action")
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

	left.CleanOnlyData()
	right.CleanOnlyData()
	require.NoError(t, vector.AppendFixed(left, math.Float64frombits(0x7ff8000000000001), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(right, math.Float64frombits(0x7ff8000000000002), false, proc.Mp()))
	require.True(t, odkuValuesEqual(left, right), "all FLOAT NaN values are one SQL comparison peer")

	float32Left := vector.NewVec(types.T_float32.ToType())
	float32Right := vector.NewVec(types.T_float32.ToType())
	defer float32Left.Free(proc.Mp())
	defer float32Right.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(float32Left, math.Float32frombits(0x7fc00001), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(float32Right, math.Float32frombits(0x7fc00002), false, proc.Mp()))
	require.True(t, odkuValuesEqual(float32Left, float32Right),
		"FLOAT32 NaN payloads must not turn an otherwise unchanged action into a write")
}

func TestODKUValueEqualityUsesSQLNarrowVectorSemantics(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	for _, tc := range []struct {
		name  string
		typ   types.Type
		left  []byte
		right []byte
	}{
		{
			name: "float16 signed zero", typ: types.T_array_float16.ToType(),
			left:  types.ArrayToBytes([]types.Float16{types.Float16(0x8000)}),
			right: types.ArrayToBytes([]types.Float16{types.Float16(0x0000)}),
		},
		{
			name: "float16 NaN payload", typ: types.T_array_float16.ToType(),
			left:  types.ArrayToBytes([]types.Float16{types.Float16(0x7e01)}),
			right: types.ArrayToBytes([]types.Float16{types.Float16(0x7e02)}),
		},
		{
			name: "bf16 signed zero", typ: types.T_array_bf16.ToType(),
			left:  types.ArrayToBytes([]types.BF16{types.BF16(0x8000)}),
			right: types.ArrayToBytes([]types.BF16{types.BF16(0x0000)}),
		},
		{
			name: "bf16 NaN payload", typ: types.T_array_bf16.ToType(),
			left:  types.ArrayToBytes([]types.BF16{types.BF16(0x7fc1)}),
			right: types.ArrayToBytes([]types.BF16{types.BF16(0x7fc2)}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			left := vector.NewVec(tc.typ)
			right := vector.NewVec(tc.typ)
			defer left.Free(proc.Mp())
			defer right.Free(proc.Mp())
			require.NoError(t, vector.AppendBytes(left, tc.left, false, proc.Mp()))
			require.NoError(t, vector.AppendBytes(right, tc.right, false, proc.Mp()))
			require.True(t, odkuValuesEqual(left, right))
		})
	}
}

func TestODKUFixedValueEqualityDoesNotAllocate(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	left := testutil.MakeInt64Vector([]int64{42}, nil, proc.Mp())
	right := testutil.MakeInt64Vector([]int64{42}, nil, proc.Mp())
	defer left.Free(proc.Mp())
	defer right.Free(proc.Mp())

	require.Zero(t, testing.AllocsPerRun(1000, func() {
		if !odkuValuesEqual(left, right) {
			t.Fatal("equal fixed values compared unequal")
		}
	}))
}

func TestODKUValueEqualityTreatsNullsAsEqualOnlyWhenBothAreNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	left := vector.NewVec(types.T_int32.ToType())
	right := vector.NewVec(types.T_int32.ToType())
	defer left.Free(proc.Mp())
	defer right.Free(proc.Mp())
	require.NoError(t, vector.AppendNull(left, proc.Mp()))
	require.NoError(t, vector.AppendNull(right, proc.Mp()))

	after := &batch.Batch{Vecs: []*vector.Vector{right}}
	after.SetRowCount(1)
	require.False(t, snapshotChanged([]*vector.Vector{left}, after, []int32{0}),
		"NULL to NULL is a no-op for ODKU change detection")

	right.CleanOnlyData()
	require.NoError(t, vector.AppendFixed(right, int32(1), false, proc.Mp()))
	require.True(t, snapshotChanged([]*vector.Vector{left}, after, []int32{0}),
		"NULL to a value is a logical change")
}

func TestODKUValueEqualityUsesSQLStringSemantics(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	charType := types.T_char.ToType()
	charType.Width = 4
	varcharType := types.T_varchar.ToType()
	varcharType.Width = 4

	for _, tc := range []struct {
		name  string
		typ   types.Type
		equal bool
	}{
		{name: "CHAR ignores trailing spaces", typ: charType, equal: true},
		{name: "VARCHAR preserves trailing spaces", typ: varcharType, equal: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			left := vector.NewVec(tc.typ)
			right := vector.NewVec(tc.typ)
			defer left.Free(proc.Mp())
			defer right.Free(proc.Mp())
			require.NoError(t, vector.AppendBytes(left, []byte("a"), false, proc.Mp()))
			require.NoError(t, vector.AppendBytes(right, []byte("a   "), false, proc.Mp()))
			require.Equal(t, tc.equal, odkuValuesEqual(left, right))
		})
	}
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

func TestODKUStableVectorPoolSurvivesJoinBatchWidthChange(t *testing.T) {
	proc := testutil.NewProcess(t)
	baseline := proc.Mp().CurrNB()
	typ := types.T_varchar.ToType()
	first := vector.NewVec(typ)
	second := vector.NewVec(typ)
	extra := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendBytes(first, []byte("first allocation"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(second, []byte("second allocation"), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(extra, int32(1), false, proc.Mp()))

	ctr := container{stableCols: []int32{0}}
	ctr.joinBat1 = &batch.Batch{Vecs: []*vector.Vector{first}}
	ctr.joinBat1.SetRowCount(1)
	require.NoError(t, ctr.stabilizeUpdateVectors(proc))
	owned := ctr.stableUpdateVecs[0][0]

	ctr.joinBat1 = &batch.Batch{Vecs: []*vector.Vector{second, extra}}
	ctr.joinBat1.SetRowCount(1)
	require.NoError(t, ctr.stabilizeUpdateVectors(proc))
	require.Contains(t, ctr.stableUpdateVecs[0], owned,
		"widening the join batch must preserve ownership of prior scratch vectors")

	ctr.cleanStableUpdateVecs(proc)
	first.Free(proc.Mp())
	second.Free(proc.Mp())
	extra.Free(proc.Mp())
	require.Equal(t, baseline, proc.Mp().CurrNB())
	proc.Free()
}

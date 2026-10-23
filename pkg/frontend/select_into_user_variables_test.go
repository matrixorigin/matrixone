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

package frontend

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestSelectIntoUserVariablesCapturesAndAssignsOneRow(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	ses := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
	collector := newSelectIntoUserVariables([]*tree.VarExpr{{Name: "out"}})

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(5), false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	require.NoError(t, collector.capture(ctx, ses, bat))
	require.NoError(t, collector.apply(ctx, ses, "select abs(-5) into @out"))
	variable, err := ses.GetUserDefinedVar("OUT")
	require.NoError(t, err)
	require.Equal(t, int64(5), variable.Value)
	require.Equal(t, "select abs(-5) into @out", variable.Sql)
}

func TestSelectIntoUserVariablesRejectsArityMismatchOnZeroRows(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	ses := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
	require.NoError(t, ses.SetUserDefinedVar("out1", "old1", "set @out1 = 'old1'"))
	require.NoError(t, ses.SetUserDefinedVar("out2", "old2", "set @out2 = 'old2'"))
	collector := newSelectIntoUserVariables([]*tree.VarExpr{{Name: "out1"}, {Name: "out2"}})

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.SetRowCount(0)
	defer bat.Clean(mp)

	err := collector.capture(ctx, ses, bat)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrWrongNumberOfColumnsInSelect))
	variable, err := ses.GetUserDefinedVar("out1")
	require.NoError(t, err)
	require.Equal(t, "old1", variable.Value)
	variable, err = ses.GetUserDefinedVar("out2")
	require.NoError(t, err)
	require.Equal(t, "old2", variable.Value)
}

func TestValidateSelectIntoArityBeforeExecution(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, validateSelectIntoArity(ctx, newResultColumnTestPlan(1), 1))
	err := validateSelectIntoArity(ctx, newResultColumnTestPlan(1), 2)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrWrongNumberOfColumnsInSelect))
	// A nil plan can occur before compilation; the callback still performs the
	// result-batch arity check once execution supplies a batch.
	require.NoError(t, validateSelectIntoArity(ctx, nil, 2))
}

func TestSelectIntoUserVariablesPreservesBinaryFlagPerColumn(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	ses := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
	collector := newSelectIntoUserVariables([]*tree.VarExpr{{Name: "bin_out"}, {Name: "text_out"}})

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_binary.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("AB\x00\x00"), false, mp))
	// SQL HEX/BIT literal provenance is attached after the value is materialized;
	// a raw bytes append is an ordinary (non-literal) value append.
	bat.Vecs[0].SetIsBin(true)
	require.NoError(t, bat.Vecs[0].SetRuntimeStringDomainWithMP(types.RuntimeStringText, mp))
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("text"), false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	require.NoError(t, collector.capture(ctx, ses, bat))
	require.NoError(t, collector.apply(ctx, ses, "select x'41420000', 'text' into @bin_out, @text_out"))
	binVar, err := ses.GetUserDefinedVar("bin_out")
	require.NoError(t, err)
	require.Equal(t, []byte("AB\x00\x00"), binVar.Value)
	require.True(t, binVar.IsBin)
	require.Equal(t, types.RuntimeStringText, binVar.RuntimeStringDomain)
	textVar, err := ses.GetUserDefinedVar("text_out")
	require.NoError(t, err)
	require.Equal(t, []byte("text"), textVar.Value)
	require.False(t, textVar.IsBin)
}

func TestSelectIntoUserVariablesReadsMappedKindPassengers(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	ses := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
	collector := newSelectIntoUserVariables([]*tree.VarExpr{{Name: "hex_out"}, {Name: "text_out"}})

	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("31"), false, mp))
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("ordinary"), false, mp))
	bat.Vecs[2] = vector.NewVec(types.T_bool.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], true, false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	require.NoError(t, collector.capture(ctx, ses, bat, []int32{2, -1}))
	require.Len(t, collector.row, 2, "the physical BOOL passenger is not a visible SQL result")
	require.NoError(t, collector.apply(ctx, ses, "select value into @hex_out, @text_out"))
	hex, err := ses.GetUserDefinedVar("hex_out")
	require.NoError(t, err)
	require.Equal(t, []byte("31"), hex.Value)
	require.True(t, hex.IsBin, "the mapped kind passenger carries selected HEX provenance")
	ordinary, err := ses.GetUserDefinedVar("text_out")
	require.NoError(t, err)
	require.Equal(t, []byte("ordinary"), ordinary.Value)
	require.False(t, ordinary.IsBin, "-1 preserves the ordinary string path")
}

func TestSelectIntoUserVariablesMappedKindLayoutFailsClosed(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	ses := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
	collector := newSelectIntoUserVariables([]*tree.VarExpr{{Name: "out"}})

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("31"), false, mp))
	bat.Vecs[1] = vector.NewVec(types.T_int8.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], int8(1), false, mp))
	bat.SetRowCount(0)
	defer bat.Clean(mp)

	err := collector.capture(ctx, ses, bat, []int32{1})
	require.Error(t, err, "even an empty result must validate the mapped physical BOOL slot")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrWrongNumberOfColumnsInSelect))
}

func TestSetUserDefinedVarWithIsBinPreservesFlagThroughBackSession(t *testing.T) {
	ses := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
	backSes := &backSession{feSessionImpl: feSessionImpl{upstream: ses}}

	require.NoError(t, setUserDefinedVarWithIsBin(backSes, "bin_out", []byte("AB\x00\x00"), "select ... into @bin_out", true))
	variable, err := ses.GetUserDefinedVar("bin_out")
	require.NoError(t, err)
	require.Equal(t, []byte("AB\x00\x00"), variable.Value)
	require.True(t, variable.IsBin)
}

func TestSelectIntoUserVariablesZeroOrManyRowsDoNotAssign(t *testing.T) {
	ctx := context.Background()
	ses := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
	require.NoError(t, ses.SetUserDefinedVar("out", "old", "set @out = 'old'"))

	zeroRows := newSelectIntoUserVariables([]*tree.VarExpr{{Name: "out"}})
	require.NoError(t, zeroRows.apply(ctx, ses, "select value into @out from empty_table"))
	variable, err := ses.GetUserDefinedVar("out")
	require.NoError(t, err)
	require.Equal(t, "old", variable.Value)

	manyRows := newSelectIntoUserVariables([]*tree.VarExpr{{Name: "out"}})
	manyRows.row = []any{"new"}
	manyRows.rowCount = 2
	err = manyRows.apply(ctx, ses, "select value into @out from two_rows")
	require.ErrorContains(t, err, "Result consisted of more than one row")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTooManyRows))
	variable, err = ses.GetUserDefinedVar("out")
	require.NoError(t, err)
	require.Equal(t, "old", variable.Value)
}

func TestSelectIntoUserVariablesStopsOnSecondRowDuringCapture(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	ses := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
	require.NoError(t, ses.SetUserDefinedVar("out", "old", "set @out = 'old'"))
	collector := newSelectIntoUserVariables([]*tree.VarExpr{{Name: "out"}})

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(2), false, mp))
	bat.SetRowCount(2)
	defer bat.Clean(mp)

	err := collector.capture(ctx, ses, bat)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTooManyRows))
	variable, err := ses.GetUserDefinedVar("out")
	require.NoError(t, err)
	require.Equal(t, "old", variable.Value)
}

func TestSelectIntoUserVariablesOutputCallbackReinitializesCollector(t *testing.T) {
	execCtx := &ExecCtx{reqCtx: context.Background()}
	ses := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
	firstStmt := &tree.Select{IntoVars: []*tree.VarExpr{{Name: "first"}}}
	secondStmt := &tree.Select{IntoVars: []*tree.VarExpr{{Name: "second"}}}

	selectIntoUserVariablesOutputCallback(execCtx, ses, firstStmt, func(*batch.Batch, *perfcounter.CounterSet) error {
		return nil
	}, nil)
	firstCollector := execCtx.selectInto
	require.NotNil(t, firstCollector)

	selectIntoUserVariablesOutputCallback(execCtx, ses, secondStmt, func(*batch.Batch, *perfcounter.CounterSet) error {
		return nil
	}, nil)
	require.NotNil(t, execCtx.selectInto)
	require.NotSame(t, firstCollector, execCtx.selectInto)
	require.Equal(t, "second", execCtx.selectInto.vars[0].Name)
}

func TestSelectIntoUserVariablesCallbackUsesActivePlanLayout(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	ses := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
	stmt := &tree.Select{IntoVars: []*tree.VarExpr{{Name: "hex_out"}, {Name: "text_out"}}}
	makePlan := func(kindColumns []int32) *planpb.Plan {
		return &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
			StmtType: planpb.Query_SELECT,
			Steps:    []int32{0},
			Headings: []string{"hex_out", "text_out"},
			Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_varchar)}},
				{Typ: planpb.Type{Id: int32(types.T_varchar)}},
				{Typ: planpb.Type{Id: int32(types.T_bool)}},
				{Typ: planpb.Type{Id: int32(types.T_bool)}},
			}}},
			IntoUserVariableKindColumns: kindColumns,
		}}}
	}
	initialPlan := makePlan([]int32{2, 3})
	activePlan := makePlan([]int32{3, 2})
	execCtx := &ExecCtx{
		reqCtx: context.Background(),
		cw:     &TxnComputationWrapper{plan: initialPlan},
	}
	current := initialPlan
	callback := selectIntoUserVariablesOutputCallback(
		execCtx, ses, stmt, func(*batch.Batch, *perfcounter.CounterSet) error { return nil },
		func() *planpb.Plan { return current },
	)

	bat := batch.NewWithSize(4)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("31"), false, mp))
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("31"), false, mp))
	bat.Vecs[2] = vector.NewVec(types.T_bool.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], false, false, mp))
	bat.Vecs[3] = vector.NewVec(types.T_bool.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[3], true, false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	// A retry can replace the active compiled plan after the wrapper was
	// created. The output callback must capture the matching physical layout.
	current = activePlan
	require.NoError(t, callback(bat, nil))
	require.NoError(t, execCtx.selectInto.apply(ctx, ses, "select ... into @hex_out, @text_out"))
	hexValue, err := ses.GetUserDefinedVar("hex_out")
	require.NoError(t, err)
	require.True(t, hexValue.IsBin, "the active plan maps hex_out to passenger column 3")
	textValue, err := ses.GetUserDefinedVar("text_out")
	require.NoError(t, err)
	require.False(t, textValue.IsBin, "the active plan maps text_out to passenger column 2")
}

func TestScalarNumericKindCallbackCapturesPassengerAndForwardsVisibleBatch(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	ses := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
	activePlan := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{0},
		Headings: []string{"value"},
		Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_varchar)}},
			{Typ: planpb.Type{Id: int32(types.T_bool)}},
		}}},
		IntoUserVariableKindColumns: []int32{1},
	}}}
	execCtx := &ExecCtx{
		reqCtx:                  ctx,
		numericValueKindCapture: &scalarNumericValueKindCapture{},
	}
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("31"), false, mp))
	// Legacy vector metadata is intentionally ordinary; the paired BOOL owns
	// the selected HEX provenance for this result.
	bat.Vecs[1] = vector.NewVec(types.T_bool.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], true, false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	var forwarded *batch.Batch
	callback := selectIntoUserVariablesOutputCallback(
		execCtx, ses, &tree.Select{}, func(value *batch.Batch, _ *perfcounter.CounterSet) error {
			forwarded = value
			return nil
		}, func() *planpb.Plan { return activePlan })
	require.NoError(t, callback(bat, nil))
	require.NotNil(t, forwarded)
	require.Len(t, forwarded.Vecs, 1, "the result batch exposes only the visible value")
	require.Len(t, forwarded.Attrs, 0)
	kind, available := execCtx.numericValueKindCapture.snapshot()
	require.True(t, available)
	require.True(t, kind)

	// A mapped false passenger must override any stale legacy vector marker.
	bat.Vecs[0].SetIsBin(true)
	bat.Vecs[1].ResetWithSameType()
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], false, false, mp))
	bat.SetRowCount(1)
	require.NoError(t, callback(bat, nil))
	kind, available = execCtx.numericValueKindCapture.snapshot()
	require.True(t, available)
	require.False(t, kind)
}

func TestSelectIntoUserVariablesZeroRowsAddsNoDataDiagnostic(t *testing.T) {
	ctx := context.Background()
	ses := &Session{
		userDefinedVars: make(map[string]*UserDefinedVar),
		errInfo:         &errInfo{maxCnt: MoDefaultErrorCount},
	}
	collector := newSelectIntoUserVariables([]*tree.VarExpr{{Name: "out"}})

	require.NoError(t, collector.apply(ctx, ses, "select value from empty_table into @out"))
	info := ses.diagnosticsSnapshot()
	require.Contains(t, info.codes, moerr.ER_SP_FETCH_NO_DATA)
	require.Contains(t, info.msgs, "No data - zero rows fetched, selected, or processed")
}

func TestSelectIntoDeprecatedWarning(t *testing.T) {
	ses := &Session{errInfo: &errInfo{maxCnt: MoDefaultErrorCount}}
	appendSelectIntoDeprecatedWarning(ses, true)

	info := ses.diagnosticsSnapshot()
	require.Contains(t, info.codes, moerr.ER_WARN_DEPRECATED_INNER_INTO)
	require.NotEmpty(t, info.msgs)
	require.Contains(t, info.msgs[0], "The INTO clause is deprecated inside query blocks of query expressions")
}

func TestSelectIntoUserVariableNormalizesDisplayOnlyGeometryTypes(t *testing.T) {
	for _, typ := range []types.Type{types.T_geometry.ToType(), types.T_geometry32.ToType()} {
		value, planType := selectIntoUserVariableValueAndType([]byte("POINT(1 2)"), typ)
		require.Equal(t, []byte("POINT(1 2)"), value)
		require.Equal(t, int32(types.T_varchar), planType.Id)
	}
}

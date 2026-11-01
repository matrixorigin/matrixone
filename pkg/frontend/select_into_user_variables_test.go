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
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	bat.Vecs[0].SetIsBin(true)
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("AB\x00\x00"), false, mp))
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
	})
	firstCollector := execCtx.selectInto
	require.NotNil(t, firstCollector)

	selectIntoUserVariablesOutputCallback(execCtx, ses, secondStmt, func(*batch.Batch, *perfcounter.CounterSet) error {
		return nil
	})
	require.NotNil(t, execCtx.selectInto)
	require.NotSame(t, firstCollector, execCtx.selectInto)
	require.Equal(t, "second", execCtx.selectInto.vars[0].Name)
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

// Retained-output discard for SELECT ... INTO @var (#30359 review, tier-2
// statement retry N1-extension): the compiler reuses the statement's
// capture closure across retry generations, so an unreset collector would
// carry the failed generation's row into the replay — a legitimate one-row
// retry then surfaces ErrTooManyRows, and an empty retry would assign the
// failed generation's stale row. The production factory arms the
// transition-time discard when it installs the collector; these arms drive
// the real capture/apply path around the armed hook exactly as a retry
// transition would (after quiescence, before replay).
func TestRetryDiscardOutputResetsSelectIntoCapture(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	newFixture := func(t *testing.T) (*Session, *ExecCtx, func(*batch.Batch, *perfcounter.CounterSet) error) {
		ses := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
		stmt := &tree.Select{IntoVars: []*tree.VarExpr{{Name: "out"}}}
		execCtx := &ExecCtx{
			reqCtx: ctx,
			ses:    ses,
			proc:   &process.Process{Base: &process.BaseProcess{}},
		}
		fillCalled := false
		callback := selectIntoUserVariablesOutputCallback(execCtx, ses, stmt,
			func(*batch.Batch, *perfcounter.CounterSet) error {
				fillCalled = true
				return nil
			})
		require.NotNil(t, execCtx.selectInto)
		require.False(t, fillCalled, "the INTO capture replaces the client output path")
		require.NotNil(t, execCtx.proc.GetSessionInfo().RetryDiscardOutput,
			"SELECT ... INTO @var retains rows off the wire, so the retry transition needs a discard")
		return ses, execCtx, callback
	}

	rowBatch := func(t *testing.T, value int64) *batch.Batch {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], value, false, mp))
		bat.SetRowCount(1)
		return bat
	}

	t.Run("one-row retry assigns the retried row", func(t *testing.T) {
		ses, execCtx, callback := newFixture(t)

		// The failed generation captures its candidate row before the
		// sequence exhaustion aborts the attempt.
		failed := rowBatch(t, 7)
		defer failed.Clean(mp)
		require.NoError(t, callback(failed, nil))
		require.Equal(t, uint64(1), execCtx.selectInto.rowCount)

		// The retry transition runs the armed discard after the failed
		// generation quiesced...
		require.NoError(t, execCtx.proc.GetSessionInfo().RetryDiscardOutput())
		require.Zero(t, execCtx.selectInto.rowCount)
		require.Nil(t, execCtx.selectInto.row)

		// ...and the replayed generation captures through the same closure.
		retried := rowBatch(t, 8)
		defer retried.Clean(mp)
		require.NoError(t, callback(retried, nil),
			"the reset collector accepts the retried generation's single row")
		require.NoError(t, execCtx.selectInto.apply(ctx, ses, "select nextval('s') into @out"))
		variable, err := ses.GetUserDefinedVar("out")
		require.NoError(t, err)
		require.Equal(t, int64(8), variable.Value,
			"the retried generation's row wins; the failed generation's row is gone")
	})

	t.Run("empty retry assigns nothing stale", func(t *testing.T) {
		ses, execCtx, callback := newFixture(t)
		require.NoError(t, ses.SetUserDefinedVar("out", "old", "set @out = 'old'"))

		failed := rowBatch(t, 7)
		defer failed.Clean(mp)
		require.NoError(t, callback(failed, nil))
		require.Equal(t, uint64(1), execCtx.selectInto.rowCount)
		require.NoError(t, execCtx.proc.GetSessionInfo().RetryDiscardOutput())

		// The retried generation produces no rows: the apply observes the
		// zero state, warns, and leaves the previous value alone.
		require.NoError(t, execCtx.selectInto.apply(ctx, ses, "select nextval('s') into @out from empty_table"))
		variable, err := ses.GetUserDefinedVar("out")
		require.NoError(t, err)
		require.Equal(t, "old", variable.Value,
			"an empty retry must not resurrect the failed generation's stale row")
	})

	t.Run("without the discard the retried row double-counts", func(t *testing.T) {
		// Documents the defect the hook repairs: two generations capturing
		// into an unreset collector surface ErrTooManyRows at the retried
		// generation's first row, failing a statement that validly
		// produced exactly one row.
		_, _, callback := newFixture(t)

		failed := rowBatch(t, 7)
		defer failed.Clean(mp)
		require.NoError(t, callback(failed, nil))
		retried := rowBatch(t, 8)
		defer retried.Clean(mp)
		err := callback(retried, nil)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTooManyRows))
	})
}

func TestSelectIntoUserVariableNormalizesDisplayOnlyGeometryTypes(t *testing.T) {
	for _, typ := range []types.Type{types.T_geometry.ToType(), types.T_geometry32.ToType()} {
		value, planType := selectIntoUserVariableValueAndType([]byte("POINT(1 2)"), typ)
		require.Equal(t, []byte("POINT(1 2)"), value)
		require.Equal(t, int32(types.T_varchar), planType.Id)
	}
}

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// This file pins the internal executor's prepared specialization route of
// issue #30938 against approved design revision 42 entry 88 (sha256
// e61574405aaf906ea1ed3761780b5be535fa1a72cf89a20bb4f401a24c87d4c8), honoring
// the design review's P3 advisory (direct coverage for the internal session
// and generation plumbing):
//
//   - internalSessionDivPrecisionIncrement recovers the attached session's
//     div_precision_increment through the minimal-interface idiom (entry 88
//     (iv)(d)), mirroring sessionDivPrecisionIncrement
//     (pkg/frontend/session.go:1520-1531) without importing pkg/frontend:
//     every incapability - absent process, absent session, missing
//     capability method, read error, non-int64 value - degrades to
//     function.DefaultDivPrecisionIncrement exactly as the frontend's
//     nil-session guard does.
//   - specializeInternalPreparedPlan enters a decimal-boundary generation on
//     the executor's own per-execution process around the fill and releases
//     it on every return path (entry 88 (iv)(c)), so the fill's
//     provenance-witness and b#-channel reads behave exactly as on the
//     frontend routes, and rebound entries die with the generation - no
//     ledger crosses the route boundary (entry 88 (iv)(e)).
//
// The witnesses are table-free derived-column shapes: the SQL-authored casts
// are fixed-domain barriers whose parent-side types equal the round-13
// declared columns' ((38,0) operands, a (65,65) high-scale peer), and the
// derived columns' products are non-foldable column multiplications carrying
// the widened provenance that opens the lineage gate. The increment
// discriminant is the round-13 division witness: the parent common of
// [(65,0),(65+N,N),(20,1)] is DECIMAL(75,10) at increment 10 and
// DECIMAL(69,4) at the default 4, so a route that ignores the session
// setting cannot reach (75,10).

// internalPreparedFillMockSession implements process.Session plus the
// GetSessionSysVar capability the adapter asserts.
type internalPreparedFillMockSession struct {
	increment any
	readErr   error
}

func (s *internalPreparedFillMockSession) GetTempTable(dbName, alias string) (string, bool) {
	return "", false
}
func (s *internalPreparedFillMockSession) AddTempTable(dbName, alias, realName string) {}
func (s *internalPreparedFillMockSession) RemoveTempTable(dbName, alias string)        {}
func (s *internalPreparedFillMockSession) RemoveTempTableByRealName(realName string)   {}
func (s *internalPreparedFillMockSession) GetSqlModeNoAutoValueOnZero() (bool, bool) {
	return false, false
}
func (s *internalPreparedFillMockSession) GetSessionSysVar(name string) (interface{}, error) {
	if s.readErr != nil {
		return nil, s.readErr
	}
	if name == "div_precision_increment" {
		return s.increment, nil
	}
	return nil, errors.New("unknown system variable")
}

// internalPreparedFillPlainSession implements process.Session without the
// GetSessionSysVar capability, so the interface assertion fails.
type internalPreparedFillPlainSession struct{}

func (s *internalPreparedFillPlainSession) GetTempTable(dbName, alias string) (string, bool) {
	return "", false
}
func (s *internalPreparedFillPlainSession) AddTempTable(dbName, alias, realName string) {}
func (s *internalPreparedFillPlainSession) RemoveTempTable(dbName, alias string)        {}
func (s *internalPreparedFillPlainSession) RemoveTempTableByRealName(realName string)   {}
func (s *internalPreparedFillPlainSession) GetSqlModeNoAutoValueOnZero() (bool, bool) {
	return false, false
}

// The round-13 division recovery witness over table-free derived columns.
const internalDivisionWitnessSQL = "select coalesce(d1.a*d1.b, (d1.c*d1.d)/cast(3 as decimal(1,0)), ?) as v from " +
	"(select cast(1 as decimal(38,0)) as a, cast(2 as decimal(38,0)) as b, " +
	"cast(3 as decimal(38,0)) as c, cast(4 as decimal(38,0)) as d limit 100) d1"

// The parent-common-overflow witness: the (65,65) high-scale peer makes
// every common of the three arms overflow the carrier, so the fill's merge
// retry declines and the original checked bind failure surfaces - the same
// checked fill-decline family as the round-18 lineage-withheld ablation.
const internalOverflowWitnessSQL = "select coalesce(d1.a*d1.b, d1.h, ?) as v from " +
	"(select cast(1 as decimal(38,0)) as a, cast(2 as decimal(38,0)) as b, " +
	"cast(0 as decimal(65,65)) as h limit 100) d1"

// buildInternalPreparedSelect mirrors the internal executor's prepared flow
// (sql_executor.go:604-645): BuildPlan with prepared=true, then
// ResetPreparePlan, over the executor's own process.
func buildInternalPreparedSelect(t *testing.T, proc *process.Process, sql string) *plan2.Plan {
	t.Helper()
	optimizer := plan2.NewMockOptimizer(true, proc)
	ctx := optimizer.CurrentContext()
	statements, err := mysql.Parse(context.Background(), sql, 1)
	require.NoError(t, err)
	defer statements[0].Free()
	pn, err := plan2.BuildPlan(ctx, statements[0], true)
	require.NoError(t, err)
	_, _, err = plan2.ResetPreparePlan(ctx, pn)
	require.NoError(t, err)
	return pn
}

// requireInternalGenerationRestored asserts the entered generation left with
// the fill: the field lifecycle holds on success, error and panic unwind
// alike (entry 88 (iv)(c), entry-58's defer-restored discipline).
func requireInternalGenerationRestored(t *testing.T, proc *process.Process) {
	t.Helper()
	require.Zero(t, proc.DecimalBoundaryGenerationDepth(),
		"the entered generation is released on every return path")
	require.Nil(t, proc.DecimalBoundaryDecisions(),
		"no generation stays active after the internal fill, so no ledger crosses the route boundary")
	require.False(t, proc.DecimalBoundaryBarrierActive(),
		"the decision barrier leaves with the generation")
}

// requireInternalFilledHeadTyp asserts the bound type of the filled SELECT's
// head projection expression.
func requireInternalFilledHeadTyp(t *testing.T, filled *plan2.Plan, width, scale int32, msgAndArgs ...any) {
	t.Helper()
	query := filled.GetQuery()
	require.NotNil(t, query)
	require.NotEmpty(t, query.Steps)
	root := query.Nodes[query.Steps[len(query.Steps)-1]]
	require.NotNil(t, root)
	require.NotEmpty(t, root.ProjectList)
	head := root.ProjectList[0]
	message := append([]any{head.Typ.String()}, msgAndArgs...)
	require.Equal(t, int32(types.T_decimal256), head.Typ.Id, message...)
	require.Equal(t, width, head.Typ.Width, message...)
	require.Equal(t, scale, head.Typ.Scale, message...)
}

func internalDecimalBinding() executor.ParamValue {
	return executor.ParamValue{Value: "0.5",
		SourceType:       types.New(types.T_decimal128, 20, 1),
		HasSourceType:    true,
		PrepareParamKind: vector.PrepareParamDecimal}
}

func TestInternalSessionDivPrecisionIncrementIssue30938(t *testing.T) {
	newProcWithSession := func(t *testing.T, session process.Session) *process.Process {
		proc := newPlanTestProcess(t)
		t.Cleanup(proc.Free)
		proc.Session = session
		return proc
	}

	t.Run("session increment recovered", func(t *testing.T) {
		proc := newProcWithSession(t, &internalPreparedFillMockSession{increment: int64(10)})
		require.Equal(t, int32(10), internalSessionDivPrecisionIncrement(proc),
			"the adapter recovers the attached session's div_precision_increment")
	})

	t.Run("nil process falls back to default", func(t *testing.T) {
		require.Equal(t, function.DefaultDivPrecisionIncrement,
			internalSessionDivPrecisionIncrement(nil),
			"a session-less background execution degrades to the engine default")
	})

	t.Run("nil session falls back to default", func(t *testing.T) {
		require.Equal(t, function.DefaultDivPrecisionIncrement,
			internalSessionDivPrecisionIncrement(newProcWithSession(t, nil)),
			"the frontend's nil-session guard mirrored")
	})

	t.Run("session without the capability falls back to default", func(t *testing.T) {
		require.Equal(t, function.DefaultDivPrecisionIncrement,
			internalSessionDivPrecisionIncrement(newProcWithSession(t, &internalPreparedFillPlainSession{})),
			"a session lacking GetSessionSysVar keeps the engine default")
	})

	t.Run("read error falls back to default", func(t *testing.T) {
		proc := newProcWithSession(t, &internalPreparedFillMockSession{readErr: errors.New("boom")})
		require.Equal(t, function.DefaultDivPrecisionIncrement,
			internalSessionDivPrecisionIncrement(proc),
			"a failed sysvar read keeps the engine default")
	})

	t.Run("non-int64 value falls back to default", func(t *testing.T) {
		proc := newProcWithSession(t, &internalPreparedFillMockSession{increment: "10"})
		require.Equal(t, function.DefaultDivPrecisionIncrement,
			internalSessionDivPrecisionIncrement(proc),
			"a non-int64 sysvar value keeps the engine default")
	})
}

func TestSpecializeInternalPreparedPlanReviewRound18Issue30938(t *testing.T) {
	binding := internalDecimalBinding()

	t.Run("sessionIncrementRecoveredThroughAdapter", func(t *testing.T) {
		// The P3 witness: the division recovery consumes the increment the
		// adapter reads from the attached session - the route cannot reach
		// the parent's increment-10 common (75,10) unless the session
		// setting crosses into the lineage scope.
		proc := newPlanTestProcess(t)
		t.Cleanup(proc.Free)
		proc.Session = &internalPreparedFillMockSession{increment: int64(10)}
		pn := buildInternalPreparedSelect(t, proc, internalDivisionWitnessSQL)
		divIncrement := internalSessionDivPrecisionIncrement(proc)
		require.Equal(t, int32(10), divIncrement)
		filled, err := specializeInternalPreparedPlan(
			proc.Ctx, proc, pn, []executor.ParamValue{binding}, divIncrement)
		require.NoError(t, err)
		requireInternalFilledHeadTyp(t, filled, 75, 10,
			"the internal fill recovers the parent common at the session's increment-10 setting")
		requireInternalGenerationRestored(t, proc)
	})

	t.Run("defaultSessionControl", func(t *testing.T) {
		proc := newPlanTestProcess(t)
		t.Cleanup(proc.Free)
		proc.Session = &internalPreparedFillMockSession{
			increment: int64(function.DefaultDivPrecisionIncrement)}
		pn := buildInternalPreparedSelect(t, proc, internalDivisionWitnessSQL)
		filled, err := specializeInternalPreparedPlan(
			proc.Ctx, proc, pn, []executor.ParamValue{binding},
			internalSessionDivPrecisionIncrement(proc))
		require.NoError(t, err)
		requireInternalFilledHeadTyp(t, filled, 69, 4,
			"the default increment keeps the parent's (69,4) on the internal route")
		requireInternalGenerationRestored(t, proc)
	})

	t.Run("sessionlessExecutionUsesDefault", func(t *testing.T) {
		proc := newPlanTestProcess(t)
		t.Cleanup(proc.Free)
		pn := buildInternalPreparedSelect(t, proc, internalDivisionWitnessSQL)
		filled, err := specializeInternalPreparedPlan(
			proc.Ctx, proc, pn, []executor.ParamValue{binding},
			internalSessionDivPrecisionIncrement(proc))
		require.NoError(t, err)
		requireInternalFilledHeadTyp(t, filled, 69, 4,
			"a session-less background execution degrades to the engine default")
		requireInternalGenerationRestored(t, proc)
	})

	t.Run("generationRestoredAfterCheckedFillError", func(t *testing.T) {
		// The (65,65) high-scale arm makes every common of the merge overflow
		// the carrier, so the fill declines with the engine's checked
		// merge-bind family even with the lineage view installed; the entered
		// generation must leave with the failed fill exactly as on success.
		proc := newPlanTestProcess(t)
		t.Cleanup(proc.Free)
		proc.Session = &internalPreparedFillMockSession{increment: int64(10)}
		pn := buildInternalPreparedSelect(t, proc, internalOverflowWitnessSQL)
		_, err := specializeInternalPreparedPlan(
			proc.Ctx, proc, pn, []executor.ParamValue{binding},
			internalSessionDivPrecisionIncrement(proc))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid argument function coalesce, bad value",
			"the checked fill decline is the engine's own merge-bind family")
		requireInternalGenerationRestored(t, proc)
	})
}

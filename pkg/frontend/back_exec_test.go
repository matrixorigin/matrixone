// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestBindBackExecSession(t *testing.T) {
	clientSessionID := uuid.New()
	backSessionID := uuid.New()
	clientSession := &Session{
		feSessionImpl: feSessionImpl{uuid: clientSessionID},
		tempTables:    make(map[string]string),
		tempTablesRev: make(map[string]string),
	}
	backSes := &backSession{
		feSessionImpl: feSessionImpl{
			uuid:     backSessionID,
			upstream: clientSession,
		},
	}
	proc := &process.Process{Base: &process.BaseProcess{}}

	parentSink := &struct{}{}
	ctx := process.ContextWithWarningSink(context.Background(), parentSink)
	bindBackExecSession(proc, backSes, ctx)
	require.Same(t, parentSink, proc.WarningSink)

	require.Same(t, backSes, proc.GetSession())
	require.Equal(t, clientSessionID, proc.Base.SessionInfo.SessionId)
	proc.GetSession().AddTempTable("db1", "tmp1", "real_tmp1")
	realName, ok := clientSession.GetTempTable("db1", "tmp1")
	require.True(t, ok)
	require.Equal(t, "real_tmp1", realName)
	require.True(t, backSes.OwnsTemporaryTable("db1", "real_tmp1"))
	require.False(t, backSes.OwnsTemporaryTable("other-db", "real_tmp1"))
	cleaner, ok := proc.GetSession().(interface {
		RemoveTempTablesByDatabase(string)
	})
	require.True(t, ok)
	cleaner.RemoveTempTablesByDatabase("db1")
	_, ok = clientSession.GetTempTable("db1", "tmp1")
	require.False(t, ok)
}

func TestBackSessionForgetsMissingTemporaryTable(t *testing.T) {
	var absent *backSession
	absent.ForgetMissingTemporaryTable("db", "t", "physical")
	(&backSession{}).ForgetMissingTemporaryTable("db", "t", "physical")
	ses := &Session{}
	ses.PublishTemporaryTable("db", "t", "physical")
	parent := &backSession{feSessionImpl: feSessionImpl{upstream: ses}}
	parent.ForgetMissingTemporaryTable("db", "t", "old")
	_, ok := ses.GetTempTable("db", "t")
	require.True(t, ok, "stale observation must not erase replacement")
	parent.ForgetMissingTemporaryTable("db", "t", "physical")
	_, ok = ses.GetTempTable("db", "t")
	require.False(t, ok)
	require.Empty(t, ses.retiredTempTables)
}

func TestBackSessionInheritsForeignKeyChecks(t *testing.T) {
	ctx := context.Background()
	ses := newFeatureLimitTestSession(t)
	require.NoError(t, ses.SetSessionSysVar(ctx, "foreign_key_checks", int64(0)))
	backSes := &backSession{}
	backSes.upstream = ses

	value, err := backSes.GetSessionSysVar("foreign_key_checks")
	require.NoError(t, err)
	require.Equal(t, int8(0), value)
}

func TestBackSessionInheritsPlanningSettings(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{"experimental_hnsw_index", "mo_pk_check_by_cn"} {
		t.Run(name, func(t *testing.T) {
			ses := newFeatureLimitTestSession(t)
			parent := &backSession{feSessionImpl: feSessionImpl{upstream: ses}}
			child := &backSession{parentBackSession: parent}
			for _, backSes := range []*backSession{parent, child} {
				value, err := backSes.GetSessionSysVar(name)
				require.NoError(t, err)
				require.Equal(t, int8(0), value, "new owning sessions default OFF")
			}
			for _, enabled := range []int64{1, 0, 1} {
				require.NoError(t, ses.SetSessionSysVar(ctx, name, enabled))
				for _, backSes := range []*backSession{parent, child} {
					value, err := backSes.GetSessionSysVar(strings.ToUpper(name))
					require.NoError(t, err)
					require.Equal(t, int8(enabled), value)
				}
			}

			// A direct owner takes precedence over a differently configured parent.
			other := newFeatureLimitTestSession(t)
			child.upstream = other
			value, err := child.GetSessionSysVar(name)
			require.NoError(t, err)
			require.Equal(t, int8(0), value)
			// Independent background sessions keep their executor-default fallback.
			value, err = (&backSession{}).GetSessionSysVar(name)
			require.NoError(t, err)
			require.Nil(t, value)
			// Caller settings do not change background transaction/SQL-mode overrides.
			for name, want := range map[string]interface{}{"autocommit": true, "sql_mode": "", "lower_case_table_names": int64(1)} {
				value, err := child.GetSessionSysVar(name)
				require.NoError(t, err)
				require.Equal(t, want, value)
			}
		})
	}
}

// TestBackSessionInheritsLowerCaseTableNames pins the persisted-case mode a
// background parse and compile must resolve identifier case with. Publication
// binding probes re-parse a stored view definition inside this session, and
// the marker's binding machinery reads exactly this variable for its own
// relation resolution, so the probe must inherit the validating session's
// mode instead of assuming mode 1: on an instance running with
// lower_case_table_names = 0 a probe that guesses mode 1 resolves a
// case-distinct definition against the wrong catalog object, certifying a
// different relation than the session and every subscriber resolve. Background
// sessions without an upstream keep the registered default, and a restore
// replay keeps its own mode-0 parse context regardless of the upstream.
func TestBackSessionInheritsLowerCaseTableNames(t *testing.T) {
	modeZero := newFeatureLimitTestSession(t)
	modeZero.sesSysVars = &SystemVariables{mp: map[string]interface{}{
		"lower_case_table_names": int64(0),
	}}
	inheriting := &backSession{}
	inheriting.upstream = modeZero

	value, err := inheriting.GetSessionSysVar("lower_case_table_names")
	require.NoError(t, err)
	require.Equal(t, int64(0), value,
		"a background probe inherits the upstream session's case mode")

	defaultBackSes := &backSession{}
	value, err = defaultBackSes.GetSessionSysVar("lower_case_table_names")
	require.NoError(t, err)
	require.Equal(t, int64(1), value,
		"a background session without an upstream keeps the registered default mode")

	modeOne := newFeatureLimitTestSession(t)
	inheritingOne := &backSession{}
	inheritingOne.upstream = modeOne

	value, err = inheritingOne.GetSessionSysVar("lower_case_table_names")
	require.NoError(t, err)
	require.Equal(t, int64(1), value,
		"an upstream at the registered default still resolves mode 1")

	restore := &backSession{}
	restore.upstream = modeOne
	restore.SetRestore(true)

	value, err = restore.GetSessionSysVar("lower_case_table_names")
	require.NoError(t, err)
	require.Equal(t, int64(0), value,
		"a restore replay keeps its own mode-0 parse context")
}

func TestBackSessionDelegatesWarningRetentionLimit(t *testing.T) {
	ctx := context.Background()
	ses := newFeatureLimitTestSession(t)
	ses.errInfo = &errInfo{maxCnt: MoDefaultErrorCount}
	require.NoError(t, ses.SetSessionSysVar(ctx, "max_error_count", int64(7)))
	ses.beginWarningDiagnostics()
	backSes := &backSession{feSessionImpl: feSessionImpl{upstream: ses}}

	require.Equal(t, 7, backSes.GetWarningRetentionLimit())
}

func TestBackSessionWarningRetentionDelegationBoundaries(t *testing.T) {
	ctx := context.Background()
	ses := newFeatureLimitTestSession(t)
	ses.errInfo = &errInfo{maxCnt: MoDefaultErrorCount}
	require.NoError(t, ses.SetSessionSysVar(ctx, "max_error_count", int64(7)))
	ses.beginWarningDiagnostics()

	var nilBackSes *backSession
	require.Equal(t, process.WarningDiagnosticDefaultRetentionLimit, nilBackSes.GetWarningRetentionLimit())
	defaultBackSes := &backSession{}
	require.Equal(t, process.WarningDiagnosticDefaultRetentionLimit, defaultBackSes.GetWarningRetentionLimit())

	parent := &backSession{feSessionImpl: feSessionImpl{upstream: ses}}
	child := &backSession{parentBackSession: parent}
	require.Equal(t, 7, child.GetWarningRetentionLimit())
	value, err := child.GetSessionSysVar("max_error_count")
	require.NoError(t, err)
	require.Equal(t, int64(7), value)

	proc := &process.Process{Base: &process.BaseProcess{}}
	refreshBackgroundStatementScopedSessionInfo(parent, nil, proc)
	require.Equal(t, 7, proc.Base.SessionInfo.MaxErrorCount)
	require.True(t, proc.Base.SessionInfo.MaxErrorCountSet)

	value, err = defaultBackSes.GetSessionSysVar("max_error_count")
	require.NoError(t, err)
	require.Equal(t, int64(process.WarningDiagnosticDefaultRetentionLimit), value)
}

func TestBindBackExecSessionWithoutUpstream(t *testing.T) {
	backSessionID := uuid.New()
	backSes := &backSession{
		feSessionImpl: feSessionImpl{uuid: backSessionID},
	}
	proc := &process.Process{Base: &process.BaseProcess{}}

	bindBackExecSession(proc, backSes, context.Background())

	require.Nil(t, proc.GetSession())
	require.Equal(t, uuid.Nil, proc.Base.SessionInfo.SessionId)
}

func TestExecInFrontendInBackRequiresUpstreamForPreparedStatements(t *testing.T) {
	ctx := context.Background()
	varExpr := tree.NewVarExpr("sql", false, false, nil)
	prepareVar := tree.NewPrepareVar("stmt", varExpr)

	tests := []struct {
		name string
		stmt tree.Statement
	}{
		{name: "prepare statement", stmt: tree.NewPrepareStmt("stmt", &tree.Select{})},
		{name: "prepare string", stmt: tree.NewPrepareString("stmt", "select 1")},
		{name: "prepare variable", stmt: prepareVar},
		{name: "deallocate", stmt: tree.NewDeallocate("stmt", false)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer test.stmt.Free()
			backSes := &backSession{}
			execCtx := &ExecCtx{reqCtx: ctx, ses: backSes, stmt: test.stmt}

			err := execInFrontendInBack(backSes, execCtx)

			require.ErrorContains(t, err, "requires an upstream session")
		})
	}
}

func TestInstallBackExecStatsInfoPreservesRootAndClaimsOnce(t *testing.T) {
	root := resource.NewRoot(resource.ConnInternal)
	parent := resource.ContextWithRoot(context.Background(), root)
	start := time.Unix(123, 456)
	duration := 17 * time.Millisecond

	firstCtx, firstStats := installBackExecStatsInfo(parent, start, duration)
	secondCtx, secondStats := installBackExecStatsInfo(parent, start, duration)

	if firstStats == secondStats {
		t.Fatal("successive substatements must use distinct StatsInfo values")
	}
	if resource.RootFromContext(firstCtx) != root || resource.RootFromContext(secondCtx) != root {
		t.Fatal("substatement context must preserve the parent resource root")
	}
	if statistic.StatsInfoFromContext(firstCtx) != firstStats {
		t.Fatal("first substatement context does not contain its StatsInfo")
	}
	if statistic.StatsInfoFromContext(secondCtx) != secondStats {
		t.Fatal("second substatement context does not contain its StatsInfo")
	}
	if firstStats.ParseStage.ParseStartTime != start || firstStats.ParseStage.ParseDuration != duration {
		t.Fatal("first StatsInfo parse timing was not installed")
	}
	if secondStats.ParseStage.ParseStartTime != start || secondStats.ParseStage.ParseDuration != duration {
		t.Fatal("second StatsInfo parse timing was not installed")
	}
	if _, ok := firstStats.ClaimRootPhaseResource(); !ok {
		t.Fatal("first StatsInfo claim should succeed")
	}
	if _, ok := firstStats.ClaimRootPhaseResource(); ok {
		t.Fatal("first StatsInfo claim should not succeed twice")
	}
	if _, ok := secondStats.ClaimRootPhaseResource(); !ok {
		t.Fatal("second StatsInfo claim should succeed")
	}
	if _, ok := secondStats.ClaimRootPhaseResource(); ok {
		t.Fatal("second StatsInfo claim should not succeed twice")
	}
}

func TestLegacyCompositeStatsProjection(t *testing.T) {
	ctx, _ := installBackExecStatsInfo(context.Background(), time.Unix(0, 0), 7)
	h := &marshalPlanHandler{isInternalSubStmt: true, query: &plan.Query{Nodes: []*plan.Node{{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    &plan.TableDef{Name: "t"},
		AnalyzeInfo: &plan.AnalyzeInfo{TimeConsumed: 123, InputRows: 9, InputSize: 77},
	}}}}
	stats, details := h.Stats(ctx, nil)
	if stats.GetTimeConsumed() != 130 {
		t.Fatalf("legacy engine projection time = %v, want 130", stats.GetTimeConsumed())
	}
	if details.RowsRead != 9 || details.BytesScan != 77 {
		t.Fatalf("legacy engine projection scan stats = (%d,%d), want (9,77)", details.RowsRead, details.BytesScan)
	}
}

func TestTopLevelStatsDoesNotRunLegacyResourceProjection(t *testing.T) {
	ctx, _ := installBackExecStatsInfo(context.Background(), time.Unix(0, 0), 7)
	h := &marshalPlanHandler{query: &plan.Query{Nodes: []*plan.Node{{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    &plan.TableDef{Name: "t"},
		AnalyzeInfo: &plan.AnalyzeInfo{TimeConsumed: 123, InputRows: 9, InputSize: 77},
	}}}}
	stats, details := h.Stats(ctx, nil)
	if stats.GetTimeConsumed() != 0 {
		t.Fatalf("top-level shadow resource projection time = %v, want 0", stats.GetTimeConsumed())
	}
	if details.RowsRead != 9 || details.BytesScan != 77 {
		t.Fatalf("top-level scan stats = (%d,%d), want (9,77)", details.RowsRead, details.BytesScan)
	}
}

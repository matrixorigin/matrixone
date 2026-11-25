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
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

// errExplainLocalLoadReachedCompile is returned by the stub computation wrapper
// when executeStmt lets the parsed statement pass the dispatch cases and reach
// Compile. Controls must observe it: it proves the guard did not reject them and
// that they keep their existing compile behavior.
var errExplainLocalLoadReachedCompile = errors.New("stub compile: dispatch passed the executable-explain local-load guard")

// explainLocalLoadStubCW pins the dispatch boundary at the same Compile call
// executeStmt makes. The embedded nil interface fails the test loudly if that
// path unexpectedly touches any other wrapper method.
type explainLocalLoadStubCW struct {
	ComputationWrapper
	compiled bool
}

func (cw *explainLocalLoadStubCW) Compile(
	_ any,
	_ func(*batch.Batch, *perfcounter.CounterSet) error,
) (interface{}, error) {
	cw.compiled = true
	return nil, errExplainLocalLoadReachedCompile
}

// parseAndDispatchExplainLocalLoad runs real parser output through the real
// executeStmt dispatch with a stub computation wrapper. The session carries no
// protocol and no upload pipe owner, exactly like an explain wrapper that
// cannot own a LOCAL transfer.
func parseAndDispatchExplainLocalLoad(t *testing.T, sql string) (tree.Statement, error, bool) {
	t.Helper()
	ctx := context.Background()
	stmts, err := parsers.Parse(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err, "parse sql: %s", sql)
	require.Len(t, stmts, 1, "parse sql: %s", sql)
	cw := &explainLocalLoadStubCW{}
	execCtx := &ExecCtx{
		reqCtx: ctx,
		stmt:   stmts[0],
		cw:     cw,
		input:  &UserInput{},
	}
	err = executeStmt(&Session{}, execCtx)
	return stmts[0], err, cw.compiled
}

func TestExecutableExplainLocalLoadRejectedBeforeCompile(t *testing.T) {
	rejected := []struct {
		name         string
		sql          string
		wantStmtType tree.Statement
		wantErrMsg   string
	}{
		{
			name: "analyze jsonline local",
			sql: "EXPLAIN ANALYZE LOAD DATA LOCAL INFILE " +
				"{'filepath'='/tmp/qa-explain-local.jsonl','format'='jsonline','jsondata'='object'} INTO TABLE t",
			wantStmtType: &tree.ExplainAnalyze{},
			wantErrMsg:   "EXPLAIN ANALYZE LOAD DATA LOCAL INFILE is not supported",
		},
		{
			name: "analyze csv local lowercase spelling",
			sql: "explain analyze load data local infile " +
				"{'filepath'='/tmp/qa-explain-local.csv','format'='csv'} into table t",
			wantStmtType: &tree.ExplainAnalyze{},
			wantErrMsg:   "EXPLAIN ANALYZE LOAD DATA LOCAL INFILE is not supported",
		},
		{
			name: "analyze option list spelling",
			sql: "EXPLAIN (ANALYZE TRUE) LOAD DATA LOCAL INFILE " +
				"{'filepath'='/tmp/qa-explain-local.jsonl','format'='jsonline','jsondata'='object'} INTO TABLE t",
			wantStmtType: &tree.ExplainAnalyze{},
			wantErrMsg:   "EXPLAIN ANALYZE LOAD DATA LOCAL INFILE is not supported",
		},
		{
			name: "phyplan local",
			sql: "EXPLAIN PHYPLAN LOAD DATA LOCAL INFILE " +
				"{'filepath'='/tmp/qa-explain-local.csv','format'='csv'} INTO TABLE t",
			wantStmtType: &tree.ExplainPhyPlan{},
			wantErrMsg:   "EXPLAIN PHYPLAN LOAD DATA LOCAL INFILE is not supported",
		},
	}

	for _, c := range rejected {
		t.Run(c.name, func(t *testing.T) {
			stmt, err, compiled := parseAndDispatchExplainLocalLoad(t, c.sql)
			require.IsType(t, c.wantStmtType, stmt)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "want NotSupported, got: %v", err)
			require.Contains(t, err.Error(), c.wantErrMsg)
			require.False(t, compiled, "rejected wrapper must not reach Compile: no plan, no upload pipe, no result metadata")
		})
	}
}

func TestExplainLocalLoadControlsReachCompile(t *testing.T) {
	var controls = []struct {
		name         string
		sql          string
		wantStmtType tree.Statement
	}{
		{
			name: "analyze executes nonlocal load",
			sql: "EXPLAIN ANALYZE LOAD DATA INFILE " +
				"{'filepath'='/tmp/qa-explain-remote.csv','format'='csv'} INTO TABLE t",
			wantStmtType: &tree.ExplainAnalyze{},
		},
		{
			name: "phyplan executes nonlocal load",
			sql: "EXPLAIN PHYPLAN LOAD DATA INFILE " +
				"{'filepath'='/tmp/qa-explain-remote.csv','format'='csv'} INTO TABLE t",
			wantStmtType: &tree.ExplainPhyPlan{},
		},
		{
			name: "plain local load",
			sql: "LOAD DATA LOCAL INFILE " +
				"{'filepath'='/tmp/qa-explain-local.jsonl','format'='jsonline','jsondata'='object'} INTO TABLE t",
			wantStmtType: &tree.Load{},
		},
		{
			name:         "analyze select",
			sql:          "EXPLAIN ANALYZE SELECT 1",
			wantStmtType: &tree.ExplainAnalyze{},
		},
	}

	for _, c := range controls {
		t.Run(c.name, func(t *testing.T) {
			stmt, err, compiled := parseAndDispatchExplainLocalLoad(t, c.sql)
			require.IsType(t, c.wantStmtType, stmt)
			require.ErrorIs(t, err, errExplainLocalLoadReachedCompile)
			require.True(t, compiled, "guard must not reject preserved explain/local behaviors: %s", c.name)
		})
	}
}

// TestPlainExplainLocalLoadStaysFrontendPlanned pins why plain EXPLAIN is not
// part of the rejection: with the wrapper from issue #32453 it is classified
// EXEC_IN_FRONTEND (compositeResRowType), so its plan-only route
// (execInFrontend -> handleExplainStmt) never enters the guarded
// executable-explain cases in executeStmt. The local-load rejection therefore
// cannot affect a wrapped plain EXPLAIN.
func TestPlainExplainLocalLoadStaysFrontendPlanned(t *testing.T) {
	sql := "EXPLAIN LOAD DATA LOCAL INFILE " +
		"{'filepath'='/tmp/qa-explain-local.jsonl','format'='jsonline','jsondata'='object'} INTO TABLE t"
	stmts, err := parsers.Parse(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err, "parse sql: %s", sql)
	require.Len(t, stmts, 1, sql)
	require.IsType(t, &tree.ExplainStmt{}, stmts[0])
	require.False(t, executableExplainWrapsLocalLoad(stmts[0]),
		"plain EXPLAIN is plan-only and must not be classified as an executable wrapper")
	require.Equal(t, tree.EXEC_IN_FRONTEND, stmts[0].StmtKind().ExecLocation(),
		"plain EXPLAIN keeps its frontend planning route, outside the guarded engine explain cases")
}

// --- prepared-statement admission (prepared-route consumer closure) ---

// newPrepareGuardSession builds the minimal real session shape the prepare
// handlers use (same construction as newPreparedExecuteEnvForSQLWithCompiler-
// Context in computation_wrapper_test), with the owner policy pre-captured so
// admission runs the captured-policy branches the text and binary prepare
// handlers deliver after processQuery setup.
func newPrepareGuardSession(t *testing.T) (*Session, *ExecCtx, context.Context) {
	t.Helper()
	ctx := statistic.ContextWithStatsInfo(context.Background(), statistic.NewStatsInfo())
	ctx = defines.AttachAccount(ctx, sysAccountID, rootID, moAdminRoleID)
	setPu("", config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))
	ses := NewSession(ctx, "", &testMysqlWriter{}, nil)
	require.NotNil(t, ses.GetProc())
	ses.GetProc().Base.SessionInfo.StorageEngine = &disttae.Engine{}
	execCtx := &ExecCtx{
		reqCtx: ctx,
		ses:    ses,
		proc:   ses.GetProc(),
		resper: ses.GetResponser(),
		input:  &UserInput{rewritePolicy: &rewritePolicySnapshot{captured: true}},
	}
	return ses, execCtx, ctx
}

// isExplainLocalLoadRejection distinguishes the canonical executable-explain
// LOCAL admission rejection from the legitimate errors legal prepared shapes
// produce at deeper prepare stages.
func isExplainLocalLoadRejection(err error) bool {
	return err != nil && moerr.IsMoErrCode(err, moerr.ErrNotSupported) &&
		strings.Contains(err.Error(), "LOAD DATA LOCAL INFILE is not supported")
}

const prepareGuardStmtName = tree.Identifier("p_el")

// TestPrepareRejectsExecutableExplainLocalLoad proves prepared-statement
// admission rejects an executable EXPLAIN wrapping a LOCAL load on every
// prepare route (issue #32453): the wrapped load would execute at every later
// EXECUTE without the LOCAL transfer pipe only a plain tree.Load admission
// creates. Every route must reject before planning, caching, publication or
// storage, with the canonical direct-text error, and must store nothing.
func TestPrepareRejectsExecutableExplainLocalLoad(t *testing.T) {
	rejected := []struct {
		name       string
		sql        string
		wantErrMsg string
	}{
		{
			name: "analyze jsonline local",
			sql: "EXPLAIN ANALYZE LOAD DATA LOCAL INFILE " +
				"{'filepath'='/tmp/qa-explain-local.jsonl','format'='jsonline','jsondata'='object'} INTO TABLE t",
			wantErrMsg: "EXPLAIN ANALYZE LOAD DATA LOCAL INFILE is not supported",
		},
		{
			name: "analyze csv local lowercase spelling",
			sql: "explain analyze load data local infile " +
				"{'filepath'='/tmp/qa-explain-local.csv','format'='csv'} into table t",
			wantErrMsg: "EXPLAIN ANALYZE LOAD DATA LOCAL INFILE is not supported",
		},
		{
			name: "analyze option list spelling",
			sql: "EXPLAIN (ANALYZE TRUE) LOAD DATA LOCAL INFILE " +
				"{'filepath'='/tmp/qa-explain-local.jsonl','format'='jsonline','jsondata'='object'} INTO TABLE t",
			wantErrMsg: "EXPLAIN ANALYZE LOAD DATA LOCAL INFILE is not supported",
		},
		{
			name: "phyplan local",
			sql: "EXPLAIN PHYPLAN LOAD DATA LOCAL INFILE " +
				"{'filepath'='/tmp/qa-explain-local.csv','format'='csv'} INTO TABLE t",
			wantErrMsg: "EXPLAIN PHYPLAN LOAD DATA LOCAL INFILE is not supported",
		},
	}

	// Every prepare route funnels through createPrepareStmtInSession with the
	// parsed inner statement as saveStmt: the admission owner itself, text
	// PREPARE ... FROM 'sql', and PREPARE ... FROM <stmt> (the shape binary
	// COM_STMT_PREPARE also feeds).
	admissionRoutes := []struct {
		name  string
		route func(ses *Session, execCtx *ExecCtx, inner tree.Statement, sql string) error
	}{
		{"admission owner", func(ses *Session, execCtx *ExecCtx, inner tree.Statement, sql string) error {
			_, err := createPrepareStmtInSession(execCtx, ses, ses, sql,
				tree.NewPrepareStmt(prepareGuardStmtName, inner), inner)
			return err
		}},
		{"text prepare from string", func(ses *Session, execCtx *ExecCtx, inner tree.Statement, sql string) error {
			_, err := doPrepareStringWithParamTypes(ses, ses, execCtx,
				tree.NewPrepareString(prepareGuardStmtName, sql), nil)
			return err
		}},
		{"prepare from parsed statement", func(ses *Session, execCtx *ExecCtx, inner tree.Statement, sql string) error {
			_, err := doPrepareStmtInSession(execCtx, ses, ses,
				tree.NewPrepareStmt(prepareGuardStmtName, inner),
				"prepare "+string(prepareGuardStmtName)+" from "+sql, nil)
			return err
		}},
	}

	for _, c := range rejected {
		for _, route := range admissionRoutes {
			t.Run(c.name+"/"+route.name, func(t *testing.T) {
				ses, execCtx, _ := newPrepareGuardSession(t)
				stmts, err := parsers.Parse(execCtx.reqCtx, dialect.MYSQL, c.sql, 1)
				require.NoError(t, err, "parse sql: %s", c.sql)
				require.Len(t, stmts, 1, c.sql)
				err = route.route(ses, execCtx, stmts[0], c.sql)
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "want NotSupported, got: %v", err)
				require.Contains(t, err.Error(), c.wantErrMsg)
				require.Empty(t, ses.prepareStmts,
					"a rejected prepared explain must never be stored for a later EXECUTE")
			})
		}
	}
}

// TestPrepareLocalLoadControlsNotRejectedAtAdmission proves the admission
// guard does not ban anything legal: a prepared plain EXPLAIN over a LOCAL
// load, a prepared nonLOCAL executable explain, a prepared plain LOCAL load
// and an ordinary prepared EXPLAIN ANALYZE SELECT all pass the admission
// rejection. A control that fails later in prepare handling keeps its
// existing behavior; a control that succeeds must be stored as before.
func TestPrepareLocalLoadControlsNotRejectedAtAdmission(t *testing.T) {
	controls := []struct {
		name string
		sql  string
	}{
		{"prepare analyze select", "EXPLAIN ANALYZE SELECT 1"},
		{
			name: "prepare analyze nonlocal load",
			sql: "EXPLAIN ANALYZE LOAD DATA INFILE " +
				"{'filepath'='/tmp/qa-explain-remote.csv','format'='csv'} INTO TABLE t",
		},
		{
			name: "prepare plain explain local load",
			sql: "EXPLAIN LOAD DATA LOCAL INFILE " +
				"{'filepath'='/tmp/qa-explain-local.jsonl','format'='jsonline','jsondata'='object'} INTO TABLE t",
		},
		{
			name: "prepare plain local load",
			sql: "LOAD DATA LOCAL INFILE " +
				"{'filepath'='/tmp/qa-explain-local.jsonl','format'='jsonline','jsondata'='object'} INTO TABLE t",
		},
	}

	for _, c := range controls {
		t.Run(c.name, func(t *testing.T) {
			ses, execCtx, _ := newPrepareGuardSession(t)
			stmts, err := parsers.Parse(execCtx.reqCtx, dialect.MYSQL, c.sql, 1)
			require.NoError(t, err, "parse sql: %s", c.sql)
			require.Len(t, stmts, 1, c.sql)
			// Drive the production prepare-from-parsed-statement route (the
			// shape text PREPARE ... FROM <stmt> and binary COM_STMT_PREPARE
			// both deliver), so an admitted control also proves its storage.
			_, err = doPrepareStmtInSession(execCtx, ses, ses,
				tree.NewPrepareStmt(prepareGuardStmtName, stmts[0]),
				"prepare "+string(prepareGuardStmtName)+" from "+c.sql, nil)
			require.False(t, isExplainLocalLoadRejection(err),
				"legal prepared shape must not hit the explain-LOCAL admission rejection, got: %v", err)
			if err == nil {
				require.Len(t, ses.prepareStmts, 1,
					"an admitted control must be stored exactly as before this change")
			}
		})
	}
}

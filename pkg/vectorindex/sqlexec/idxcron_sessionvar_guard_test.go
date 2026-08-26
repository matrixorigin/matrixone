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

package sqlexec

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// reindexRequiredSessionVars is the canonical set of session/system variables the background
// idxcron reindex build SQL path resolves. It is the CONTRACT this guard enforces: every var
// here MUST be answerable by Metadata.ResolveVariableWithSessionDefaults — either a captured
// algo knob or a deliberate background default in sessionSystemVarDefaults. When the
// reindex/build/INSERT path grows a NEW session-var dependency, add it here; the guard then
// FAILS until it is also enumerated in sessionSystemVarDefaults — forcing a conscious background
// default instead of a silent reindex abort (the #25438 sql_mode failure: the strict resolver
// errored "key sql_mode not found" and aborted every reindex).
var reindexRequiredSessionVars = []string{
	"sql_mode",          // #25438 zero-temporal-date write-policy check (process/sql_mode.go)
	"lock_wait_timeout", // lockop / process_codec
}

// mockResolveExecutor is a stand-in InternalSQLExecutor whose Exec RESOLVES a fixed set of
// session vars through the Options resolver — exactly as the real internal executor plumbs
// Options.ResolveVariableFunc into plan/exec (RunSql -> WithResolveVariableFunc -> here). It lets
// this unit test drive that plumbing end-to-end with the idxcron Metadata resolver, without an
// engine. The first resolve error is surfaced (a reindex would likewise abort).
type mockResolveExecutor struct{ vars []string }

func (m *mockResolveExecutor) Exec(_ context.Context, _ string, opts executor.Options) (executor.Result, error) {
	resolve := opts.ResolveVariableFunc()
	if resolve == nil {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("no resolveVariableFunc plumbed into Options")
	}
	for _, v := range m.vars {
		if _, err := resolve(v, true, false); err != nil {
			return executor.Result{}, err
		}
	}
	return executor.Result{}, nil
}

func (m *mockResolveExecutor) ExecTxn(context.Context, func(executor.TxnExecutor) error, executor.Options) error {
	return nil
}

// TestIdxcronReindexSessionVarGuard drives RunSql through a mock InternalSQLExecutor that resolves
// the reindex's required session vars via the installed idxcron Metadata resolver
// (ResolveVariableWithSessionDefaults). It guards the session-var contract: a new required var
// that is not enumerated in sessionSystemVarDefaults breaks this test.
//
// IF THIS TEST FAILS with `un-enumerated session variable "<name>"`: the background idxcron
// reindex path now resolves a session variable that has no background default. FIX IT in
// pkg/vectorindex/sqlexec/metadata.go — add "<name>" to the sessionSystemVarDefaults map with the
// correct BACKGROUND value (e.g. sql_mode "" not the strict default), then add it to
// reindexRequiredSessionVars above. Do NOT just capture the user's live value: a background
// rebuild must use a safe default (see the sessionSystemVarDefaults doc comment). This is the
// #25438 failure mode surfaced at test time instead of aborting reindex in production.
func TestIdxcronReindexSessionVarGuard(t *testing.T) {
	const sid = "ut-idxcron-sessionvar-guard"
	moruntime.SetupServiceBasedRuntime(sid, moruntime.DefaultRuntime())

	// An algo-only capture blob — exactly what an idxcron task's Metadata carries (no session
	// vars); session vars must come from the background defaults.
	md, err := NewMetadataFromJson(`{"cfg":{"kmeans_train_percent":{"t":"F","v":10}}}`)
	require.NoError(t, err)

	run := func(vars []string) error {
		moruntime.ServiceRuntime(sid).SetGlobalVariables(moruntime.InternalSQLExecutor, &mockResolveExecutor{vars: vars})
		sp := NewSqlProcessWithContext(NewSqlContext(context.Background(), sid, nil, 0, md.ResolveVariableWithSessionDefaults))
		_, e := RunSql(sp, "select 1")
		return e
	}

	// GUARD: every currently-required reindex session var resolves without error through the
	// full RunSql -> Options.ResolveVariableFunc -> Metadata resolver plumbing. If the reindex
	// path grows a new dependency added to reindexRequiredSessionVars but NOT to
	// sessionSystemVarDefaults, ResolveVariableWithSessionDefaults fail-fasts and this fails.
	require.NoErrorf(t, run(reindexRequiredSessionVars),
		"a required reindex session var has no background default: add it to the "+
			"sessionSystemVarDefaults map in pkg/vectorindex/sqlexec/metadata.go with its correct "+
			"BACKGROUND value (see the fix note on TestIdxcronReindexSessionVarGuard)")

	// NEGATIVE: an un-enumerated session var fail-fasts through the same plumbing — proving the
	// guard actually catches a newly-plumbed, unwhitelisted dependency (not a silent nil).
	err = run([]string{"some_new_unenumerated_var"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "some_new_unenumerated_var")

	// A CAPTURED algo knob still resolves strictly (it is not treated as an un-enumerated
	// session var), so real build-config is honored on the reindex path.
	require.NoError(t, run([]string{"kmeans_train_percent"}))
}

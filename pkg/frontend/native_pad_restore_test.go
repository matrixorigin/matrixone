// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type collationRestoreBackgroundExec struct {
	BackgroundExec
	service string
	execute func(context.Context, string, string) error
}

func (b *collationRestoreBackgroundExec) Service() string { return b.service }
func (b *collationRestoreBackgroundExec) ExecWithSQLMode(ctx context.Context, sql, mode string) error {
	return b.execute(ctx, sql, mode)
}

func TestNativePADViewRestoreBindingGeneration(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	// The lightweight process has no lock service and therefore an empty CN
	// ID. The background executor must name a service; both use the same gate.
	service := t.Name()
	previousRuntime := moruntime.ServiceRuntime(service)
	moruntime.SetupServiceBasedRuntime(service, rt)
	t.Cleanup(func() {
		if previousRuntime != nil {
			moruntime.SetupServiceBasedRuntime(service, previousRuntime)
		} else {
			moruntime.CompareAndDeleteServiceBasedRuntime(service, rt)
		}
	})
	for _, key := range []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolFloor, moruntime.PersistedExpressionProtocolAuthoringFloor} {
		old, exists := rt.GetGlobalVariables(key)
		t.Cleanup(func() {
			if exists {
				rt.SetGlobalVariables(key, old)
			} else if value, ok := rt.GetGlobalVariables(key); ok {
				rt.CompareAndDeleteGlobalVariables(key, value)
			}
		})
		rt.SetGlobalVariables(key, defines.MORPCVersion130)
	}
	const sql = "create view v as select 'A' collate utf8mb4_general_ci = 'a' as b"
	for _, source := range []struct {
		revision uint32
		floor    int64
		marked   bool
	}{
		{0, 0, false},
		{0, 0, true},
		{0, defines.MORPCVersion124, true},
		{0, defines.MORPCVersion130, true},
		{1, defines.MORPCVersion130, true},
	} {
		revision := source.revision
		data := plan.ViewData{Stmt: sql, CollationBindingRevision: revision}
		if source.floor != 0 {
			data.RequiredProtocolVersion = &source.floor
		}
		encoded, err := json.Marshal(data)
		require.NoError(t, err)
		if source.marked && revision == 0 {
			var fields map[string]any
			require.NoError(t, json.Unmarshal(encoded, &fields))
			fields["collation_binding_revision"] = 0
			encoded, err = json.Marshal(fields)
			require.NoError(t, err)
		}
		// Neither the restorer's generation nor a high floor chooses the source's.
		parent := plan.WithCollationBindingRevision(defines.AttachAccountId(t.Context(), 42), 1-revision)
		called := false
		b := &collationRestoreBackgroundExec{service: service}
		b.execute = func(ctx context.Context, gotSQL, mode string) error {
			called = true
			require.Equal(t, sql, gotSQL)
			account, err := defines.GetAccountId(ctx)
			require.NoError(t, err)
			require.Equal(t, uint32(42), account)
			compiler := plan.NewMockCompilerContext(false, proc)
			compiler.SetContext(ctx)
			statements, err := parsers.ParseWithSQLMode(ctx, dialect.MYSQL, gotSQL, 1, mode)
			if err != nil {
				return err
			}
			defer freeStatements(statements)
			built, err := plan.BuildPlan(compiler, statements[0], false)
			if err != nil {
				return err
			}
			var restored plan.ViewData
			require.NoError(t, json.Unmarshal([]byte(built.GetDdl().GetCreateView().TableDef.ViewSql.View), &restored))
			require.Equal(t, revision, restored.CollationBindingRevision)
			return nil
		}
		require.NoError(t, executeViewCreateSQLForRestore(parent, b, &tableInfo{dbName: "tpch", tblName: "v", viewDef: string(encoded), createSql: sql}))
		require.True(t, called)

		compiler := plan.NewMockCompilerContext(false, proc)
		compiler.SetContext(parent)
		if source.floor != 0 {
			parsed, err := parsers.ParseOne(parent, dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, defines.MORPCVersion120)
			_, err = buildViewPlanForRestore(compiler, parsed, &tableInfo{viewDef: string(encoded)})
			parsed.Free()
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, defines.MORPCVersion130)
			require.ErrorContains(t, err, fmt.Sprintf("protocol version %d", source.floor), "source read admission is independent of target authoring")
			require.Same(t, parent, compiler.GetContext())
		}
		for _, statement := range []string{"create view v as select true as b", "create view v as select missing_column"} {
			parsed, err := parsers.ParseOne(parent, dialect.MYSQL, statement, 1)
			require.NoError(t, err)
			built, err := buildViewPlanForRestore(compiler, parsed, &tableInfo{viewDef: string(encoded)})
			parsed.Free()
			require.Same(t, parent, compiler.GetContext(), "dependency sorting must restore its compiler context")
			if statement == "create view v as select missing_column" {
				require.Error(t, err)
				continue
			}
			require.NoError(t, err)
			var restored plan.ViewData
			require.NoError(t, json.Unmarshal([]byte(built.GetDdl().GetCreateView().TableDef.ViewSql.View), &restored))
			require.Equal(t, revision, restored.CollationBindingRevision, "a folded SQL skeleton cannot erase source provenance")
			if source.floor != 0 {
				require.NotNil(t, restored.RequiredProtocolVersion)
				require.Equal(t, source.floor, *restored.RequiredProtocolVersion)
			}
		}
	}
}

func TestNativePADViewRestoreRejectsInvalidProvenanceBeforeSQL(t *testing.T) {
	for _, view := range []string{
		`{"collation_binding_revision":1}`,
		`{"collation_binding_revision":1,"required_protocol_version":120}`,
		`{"collation_binding_revision":1,"required_protocol_version":124}`,
		`{"collation_binding_revision":1,"required_protocol_version":125}`,
		`{"collation_binding_revision":1,"required_protocol_version":128}`,
		`{"collation_binding_revision":1,"required_protocol_version":129}`,
		`{"collation_binding_revision":2}`,
	} {
		called := false
		b := &collationRestoreBackgroundExec{execute: func(context.Context, string, string) error { called = true; return nil }}
		err := executeViewCreateSQLForRestore(t.Context(), b, &tableInfo{viewDef: view, createSql: "create view v as select true"})
		require.Error(t, err)
		require.False(t, called)
		require.False(t, isUnservableViewError(err))
		require.False(t, canSkipRestoreViewError(err))
	}
}

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

package plan

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

type historicalColumnsContext struct {
	*MockCompilerContext
	resolve func(string, string, *Snapshot) (*ObjectRef, *TableDef, error)
}

func (c *historicalColumnsContext) Resolve(db, table string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
	return c.resolve(db, table, snapshot)
}

// preChangeCurrentColumnsDDL reproduces the undecorated current template a
// pre-change binary persists as information_schema.COLUMNS: the V109 lineage
// plus the MEDIUMINT DATA_TYPE arm. The sysview package pins this exact
// construction byte-identical to its frozen pass-through identity and to the
// base-era derivation of the current template
// (pkg/util/sysview/view_columns_history_test.go).
func preChangeCurrentColumnsDDL() string {
	const v109DataType = "upper(mo_show_visible_bin(mc.atttyp,2)) = 'BOOL' then 'TINYINT' "
	const v110DataType = v109DataType + "when upper(mo_show_visible_bin(mc.atttyp,3)) like 'MEDIUMINT%' then 'MEDIUMINT' "
	return strings.ReplaceAll(sysview.InformationSchemaColumnsV109DDL(), v109DataType, v110DataType)
}

func TestHistoricalViewColumnsSQL(t *testing.T) {
	legacy := sysview.InformationSchemaColumnsV58DDL()
	replacement := strings.Replace(sysview.InformationSchemaColumnsDDL, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1)
	denied := errors.New("catalog permission denied")
	for _, tc := range []struct {
		name          string
		current       string
		definition    string
		noSnapshot    bool
		otherView     bool
		crossTenant   bool
		resolveErr    error
		accountErr    bool
		missing       bool
		badJSON       bool
		oldProtocol   bool
		wantAdapt     bool
		wantError     bool
		wantResolve   bool
		wantErrorText string
	}{
		{name: "current query keeps migration semantics", noSnapshot: true},
		{name: "unrelated View", otherView: true},
		{name: "already current", definition: sysview.InformationSchemaColumnsDDL},
		{name: "already current replacement", definition: replacement},
		{name: "before migration", current: legacy, wantResolve: true},
		{name: "protocol100 migration remains active before protocol109", current: sysview.InformationSchemaColumnsV100DDL(), wantResolve: true, wantAdapt: true},
		{name: "current label-aware definition adapts v109 template", current: sysview.InformationSchemaColumnsDDL, definition: sysview.InformationSchemaColumnsV109DDL(), wantResolve: true, wantAdapt: true},
		{name: "current label-aware definition normalizes lowercase v109 template", current: sysview.InformationSchemaColumnsDDL, definition: strings.Replace(sysview.InformationSchemaColumnsV109DDL(), "information_schema.COLUMNS", "information_schema.columns", 1), wantResolve: true, wantAdapt: true},
		{name: "current label-aware definition rejects modified v109 template", current: sysview.InformationSchemaColumnsDDL, definition: sysview.InformationSchemaColumnsV109DDL() + " ", wantResolve: true, wantError: true},
		// One generation back, a binary from before the current identity
		// change persists the pre-change current template; the merged
		// current identity still adapts it for historical reads.
		{name: "current merged definition adapts pre-change current template", current: sysview.InformationSchemaColumnsDDL, definition: preChangeCurrentColumnsDDL(), wantResolve: true, wantAdapt: true},
		{name: "current merged definition normalizes lowercase pre-change template", current: sysview.InformationSchemaColumnsDDL, definition: strings.Replace(preChangeCurrentColumnsDDL(), "information_schema.COLUMNS", "information_schema.columns", 1), wantResolve: true, wantAdapt: true},
		{name: "current merged definition rejects modified pre-change template", current: sysview.InformationSchemaColumnsDDL, definition: preChangeCurrentColumnsDDL() + " ", wantResolve: true, wantError: true},
		// A tenant whose current COLUMNS definition is still the historical
		// V110 template keeps adapting historical snapshot definitions.
		{name: "v110 tenant current definition adapts legacy template", current: sysview.InformationSchemaColumnsV110DDL(), wantResolve: true, wantAdapt: true},
		{name: "v110 tenant current definition normalizes lowercase legacy template", current: sysview.InformationSchemaColumnsV110DDL(), definition: strings.Replace(legacy, "information_schema.COLUMNS", "information_schema.columns", 1), wantResolve: true, wantAdapt: true},
		{name: "v110 tenant current definition rejects unknown template", current: sysview.InformationSchemaColumnsV110DDL(), definition: "create view columns as select 1", wantResolve: true, wantError: true},
		// H1b: the pre-upgrade snapshot transition - a base-era snapshot
		// recorded under the previous current identity (the NULL-selector
		// MEDIUMINT template) binds after the 4.0.19 hop refreshed the live
		// view to the merged identity: the recorded text survives
		// byte-stable, and unknown definitions stay refused.
		{name: "base-era nil selector snapshot binds against merged live identity",
			current:     sysview.InformationSchemaColumnsDDL,
			definition:  sysview.InformationSchemaColumnsBinaryFamilyNullDDL(),
			wantResolve: true, wantAdapt: true},
		{name: "base-era nil selector snapshot normalizes lowercase view name",
			current:     sysview.InformationSchemaColumnsDDL,
			definition:  strings.Replace(sysview.InformationSchemaColumnsBinaryFamilyNullDDL(), "information_schema.COLUMNS", "information_schema.columns", 1),
			wantResolve: true, wantAdapt: true},
		{name: "merged live identity still rejects unknown snapshot definition",
			current: sysview.InformationSchemaColumnsDDL, definition: "CREATE VIEW information_schema.COLUMNS AS SELECT 1", wantResolve: true,
			wantError: true, wantErrorText: "unsupported historical information_schema.COLUMNS definition"},
		// D14-4 I6/H1a: a snapshot recorded while the live definition was
		// the published upstream 4.0.18 identity still binds once the 4.0.19
		// refresh rewrote the live view to the merged identity: the recorded
		// text adapts byte-stably and edited or unknown definitions stay
		// refused.
		{name: "upstream 4.0.18 snapshot binds against merged live identity",
			current:     sysview.InformationSchemaColumnsDDL,
			definition:  sysview.InformationSchemaColumns4018DDL(),
			wantResolve: true, wantAdapt: true},
		{name: "upstream 4.0.18 snapshot normalizes lowercase view name",
			current:     sysview.InformationSchemaColumnsDDL,
			definition:  strings.Replace(sysview.InformationSchemaColumns4018DDL(), "information_schema.COLUMNS", "information_schema.columns", 1),
			wantResolve: true, wantAdapt: true},
		{name: "upstream 4.0.18 snapshot rejects modified template",
			current:     sysview.InformationSchemaColumnsDDL,
			definition:  sysview.InformationSchemaColumns4018DDL() + " ",
			wantResolve: true, wantError: true},
		// The pre-refresh gate leg (D14-4): a tenant whose live COLUMNS
		// definition is still the 4.0.18 identity keeps adapting historical
		// snapshot definitions instead of serving them unchanged.
		{name: "4.0.18 live identity gates legacy snapshot adaptation",
			current: sysview.InformationSchemaColumns4018DDL(), wantResolve: true, wantAdapt: true},
		{name: "pre-migration gate keeps v109 template unchanged", current: legacy, definition: sysview.InformationSchemaColumnsV109DDL(), wantResolve: true},
		{name: "after migration", current: sysview.InformationSchemaColumnsDDL, wantResolve: true, wantAdapt: true},
		{name: "after identity-preserving migration", current: replacement, wantResolve: true, wantAdapt: true},
		{name: "replaced historical template", current: replacement, definition: strings.Replace(sysview.InformationSchemaColumnsV100DDL(), "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1), wantResolve: true, wantAdapt: true},
		{name: "modified replacement is not a migration gate", current: replacement + " ", wantResolve: true},
		{name: "cross tenant", current: sysview.InformationSchemaColumnsDDL, crossTenant: true, wantResolve: true, wantAdapt: true},
		{name: "cross tenant replacement", current: replacement, crossTenant: true, wantResolve: true, wantAdapt: true},
		{name: "cross tenant protocol100 migration", current: sysview.InformationSchemaColumnsV100DDL(), crossTenant: true, wantResolve: true, wantAdapt: true},
		{name: "unknown historical template", current: sysview.InformationSchemaColumnsDDL, definition: "create view columns as select 1", wantResolve: true, wantError: true},
		{name: "permission failure", resolveErr: denied, wantResolve: true, wantError: true},
		{name: "cross tenant failure restores context", crossTenant: true, resolveErr: denied, wantResolve: true, wantError: true},
		{name: "account failure", accountErr: true, wantError: true},
		{name: "missing current template", missing: true, wantResolve: true, wantError: true},
		{name: "malformed current template", badJSON: true, wantResolve: true, wantError: true},
		{name: "protocol not ready", current: sysview.InformationSchemaColumnsDDL, oldProtocol: true, wantResolve: true, wantError: true},
		{name: "replacement protocol not ready", current: replacement, oldProtocol: true, wantResolve: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockCompilerContext(true, newPlanTestProcess(t))
			mock.GetAccountIdFunc = func() (uint32, error) {
				if tc.accountErr {
					return 0, denied
				}
				return 7, nil
			}
			original := defines.AttachAccountId(context.Background(), 7)
			mock.SetContext(original)
			if tc.oldProtocol {
				rt := moruntime.ServiceRuntime(mock.GetProcess().GetService())
				previous, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion58)
				t.Cleanup(func() { rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous) })
			}
			calls := 0
			ctx := &historicalColumnsContext{MockCompilerContext: mock}
			ctx.resolve = func(db, table string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
				calls++
				require.Equal(t, "information_schema", db)
				require.Equal(t, "columns", table)
				require.Nil(t, snapshot, "the migration gate is current, not historical")
				account, err := defines.GetAccountId(ctx.GetContext())
				require.NoError(t, err)
				if tc.crossTenant {
					require.Equal(t, uint32(13), account)
				} else {
					require.Equal(t, uint32(7), account)
				}
				if tc.resolveErr != nil || tc.missing {
					return nil, nil, tc.resolveErr
				}
				data, err := json.Marshal(ViewData{Stmt: tc.current})
				require.NoError(t, err)
				if tc.badJSON {
					data = []byte("invalid json")
				}
				return &ObjectRef{SchemaName: db, ObjName: table, Obj: 99},
					&TableDef{TblId: 99, Version: 3, ViewSql: &planpb.ViewDef{View: string(data)}}, nil
			}
			snapshot := &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 42}, Tenant: &planpb.SnapshotTenant{TenantID: 7}}
			if tc.crossTenant {
				snapshot.Tenant.TenantID = 13
			}
			if tc.noSnapshot {
				snapshot = nil
			}
			before := DeepCopySnapshot(snapshot)
			definition := legacy
			if tc.definition != "" {
				definition = tc.definition
			}
			schema := "information_schema"
			if tc.otherView {
				schema = "user_db"
			}
			builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
			got, err := builder.historicalViewColumnsSQL(schema, "COLUMNS", definition, snapshot)
			if tc.wantError {
				require.Error(t, err)
				require.Empty(t, got)
				if tc.wantErrorText != "" {
					require.ErrorContains(t, err, tc.wantErrorText)
				}
				if tc.resolveErr != nil || tc.accountErr {
					require.ErrorIs(t, err, denied)
				}
			} else {
				require.NoError(t, err)
				if tc.wantAdapt {
					expected, ok := sysview.AdaptLegacyInformationSchemaColumnsDDL(definition)
					require.True(t, ok)
					require.Equal(t, expected, got)
				} else {
					require.Equal(t, definition, got)
				}
			}
			require.Equal(t, tc.wantResolve, calls == 1)
			require.Same(t, original, ctx.GetContext(), "borrowed tenant context must be restored")
			require.Equal(t, before, snapshot, "the user schema snapshot must not be advanced")
			if tc.wantResolve && !tc.wantError {
				require.Len(t, builder.qry.CatalogDependencies, 1, "also track the gate before migration, so cached plans see upgrades")
				dependency := builder.qry.CatalogDependencies[0]
				require.Equal(t, int64(99), dependency.Obj)
				require.Nil(t, dependency.Snapshot)
				if tc.crossTenant {
					require.Equal(t, int32(13), dependency.PubInfo.TenantId)
				}
			}
		})
	}
}

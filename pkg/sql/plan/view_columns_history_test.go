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

func TestHistoricalViewColumnsSQL(t *testing.T) {
	legacy := sysview.InformationSchemaColumnsV58DDL()
	denied := errors.New("catalog permission denied")
	for _, tc := range []struct {
		name        string
		current     string
		definition  string
		noSnapshot  bool
		otherView   bool
		crossTenant bool
		resolveErr  error
		accountErr  bool
		missing     bool
		badJSON     bool
		oldProtocol bool
		wantAdapt   bool
		wantError   bool
		wantResolve bool
	}{
		{name: "current query keeps migration semantics", noSnapshot: true},
		{name: "unrelated View", otherView: true},
		{name: "already current", definition: sysview.InformationSchemaColumnsDDL},
		{name: "before migration", current: legacy, wantResolve: true},
		{name: "after migration", current: sysview.InformationSchemaColumnsDDL, wantResolve: true, wantAdapt: true},
		{name: "cross tenant", current: sysview.InformationSchemaColumnsDDL, crossTenant: true, wantResolve: true, wantAdapt: true},
		{name: "unknown historical template", current: sysview.InformationSchemaColumnsDDL, definition: "create view columns as select 1", wantResolve: true, wantError: true},
		{name: "permission failure", resolveErr: denied, wantResolve: true, wantError: true},
		{name: "cross tenant failure restores context", crossTenant: true, resolveErr: denied, wantResolve: true, wantError: true},
		{name: "account failure", accountErr: true, wantError: true},
		{name: "missing current template", missing: true, wantResolve: true, wantError: true},
		{name: "malformed current template", badJSON: true, wantResolve: true, wantError: true},
		{name: "protocol not ready", current: sysview.InformationSchemaColumnsDDL, oldProtocol: true, wantResolve: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockCompilerContext(true)
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
				if tc.resolveErr != nil || tc.accountErr {
					require.ErrorIs(t, err, denied)
				}
			} else {
				require.NoError(t, err)
				if tc.wantAdapt {
					require.Equal(t, sysview.InformationSchemaColumnsDDL, got)
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

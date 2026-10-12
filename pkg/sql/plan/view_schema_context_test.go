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
	"errors"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

// The session identity remains fixed, while a catalog resolver reports the
// account in its actual read context. Historical system-View migration gates
// use that distinction without moving the caller's historical snapshot.
type viewSchemaContextCompiler struct {
	*viewSchemaTestCompiler
	identityError error
}

func (c *viewSchemaContextCompiler) ResolveViewDependencyAccount(_ *ObjectRef, _ *TableDef, _ *Snapshot) (uint32, error) {
	if c.identityError != nil {
		return 0, c.identityError
	}
	return defines.GetAccountId(c.GetContext())
}

func TestViewSchemaHistoricalMigrationGateContextAndDependency(t *testing.T) {
	for _, failureAt := range []string{"none", "resolve", "identity"} {
		t.Run(failureAt, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			const callerAccount, historicalAccount uint32 = 7, 13
			f.compiler.GetAccountIdFunc = func() (uint32, error) { return callerAccount, nil }
			compiler := &viewSchemaContextCompiler{viewSchemaTestCompiler: f.compiler}
			f.binding.Compiler = compiler
			gate := f.addDefinition(t, "columns", sysview.InformationSchemaColumnsDDL)
			gate.DbName = "information_schema"
			f.compiler.objects["columns"].SchemaName = "information_schema"

			rt := moruntime.ServiceRuntime(compiler.GetProcess().GetService())
			for _, key := range []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolFloor} {
				previous, present := rt.GetGlobalVariables(key)
				t.Cleanup(func() {
					if present {
						rt.SetGlobalVariables(key, previous)
					} else if current, ok := rt.GetGlobalVariables(key); ok {
						rt.CompareAndDeleteGlobalVariables(key, current)
					}
				})
				rt.SetGlobalVariables(key, int64(defines.MORPCLatestVersion))
			}

			failure := errors.New("historical migration gate read failed")
			if failureAt == "identity" {
				compiler.identityError = failure
			}
			calls := 0
			f.compiler.resolve = func(ctx context.Context, database, name string, snapshot *Snapshot) error {
				calls++
				require.Equal(t, "information_schema", database)
				require.Equal(t, "columns", name)
				require.Nil(t, snapshot, "the migration gate is read at current visibility")
				account, err := defines.GetAccountId(ctx)
				require.NoError(t, err)
				require.Equal(t, historicalAccount, account, "the actual resolver must see the temporary tenant context")
				if failureAt == "resolve" {
					return failure
				}
				return nil
			}

			request := NewViewSchemaRequest(defines.AttachAccountId(t.Context(), callerAccount), f.provider)
			t.Cleanup(request.Close)
			require.NoError(t, request.open())
			originalContext := compiler.GetContext()
			state := newViewSchemaDerivation(request)
			defer state.close()
			bindingContext := state.compiler.GetContext()
			require.Same(t, bindingContext, compiler.GetContext(), "the owned child starts in the request's binding context")
			builder := NewQueryBuilder(planpb.Query_SELECT, state.compiler, false, false)
			snapshot := &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 42}, Tenant: &planpb.SnapshotTenant{TenantID: historicalAccount}}
			beforeSnapshot := DeepCopySnapshot(snapshot)
			definition, err := builder.historicalViewColumnsSQL("information_schema", "columns", sysview.InformationSchemaColumnsV58DDL(), snapshot)
			require.Equal(t, 1, calls)
			if failureAt == "none" {
				require.NoError(t, err)
				// The current template admits the migration, not a projection upgrade.
				require.Equal(t, sysview.InformationSchemaColumnsV100DDL(), definition)
				require.NotEqual(t, sysview.InformationSchemaColumnsDDL, definition)
				dependencies := state.capture.dependencies()
				require.Len(t, dependencies, 1)
				require.Equal(t, historicalAccount, dependencies[0].AccountID)
				require.Equal(t, "information_schema", dependencies[0].DatabaseName)
				require.Equal(t, "columns", dependencies[0].RelationName)
				require.Nil(t, dependencies[0].Snapshot, "the captured gate identity is current, not historical")
				require.Len(t, builder.qry.CatalogDependencies, 1)
				require.Equal(t, int32(historicalAccount), builder.qry.CatalogDependencies[0].PubInfo.TenantId)
			} else {
				require.ErrorIs(t, err, failure)
				require.Empty(t, definition)
				require.Empty(t, state.capture.deps)
			}
			require.Equal(t, beforeSnapshot, snapshot)
			require.Same(t, bindingContext, state.compiler.GetContext())
			require.Same(t, bindingContext, compiler.GetContext(), "success and errors both restore the scoped tenant override")
			account, err := defines.GetAccountId(compiler.GetContext())
			require.NoError(t, err)
			require.Equal(t, callerAccount, account)
			state.close()
			require.Same(t, originalContext, compiler.GetContext(), "derivation cleanup restores the provider-owned context")
			require.Zero(t, f.generation.Used())
			state.close()
			require.Same(t, originalContext, compiler.GetContext(), "cleanup is idempotent")
			request.Close()
			require.Equal(t, int64(1), f.closes.Load())
			require.False(t, f.generation.Closed())
		})
	}
}

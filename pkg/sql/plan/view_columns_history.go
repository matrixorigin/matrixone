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
	"encoding/json"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

// historicalViewColumnsSQL adapts a historical system template only after the
// tenant's current COLUMNS view has migrated to the existing on-demand format.
// It neither persists SQL nor changes the snapshot used to bind user objects.
// This is not activation of the broader v2 View metadata contract.
func (builder *QueryBuilder) historicalViewColumnsSQL(
	schema, table, definition string, snapshot *Snapshot,
) (string, error) {
	if !IsSnapshotValid(snapshot) || !strings.EqualFold(schema, "information_schema") ||
		!strings.EqualFold(table, "columns") || sysview.IsCurrentInformationSchemaColumnsDDL(definition) {
		return definition, nil
	}

	ctx := builder.compCtx
	account, err := ctx.GetAccountId()
	if err != nil {
		return "", err
	}
	crossAccount := snapshot.Tenant != nil && snapshot.Tenant.TenantID != account
	if crossAccount {
		previous := ctx.GetContext()
		ctx.SetContext(defines.AttachAccountId(previous, snapshot.Tenant.TenantID))
		defer ctx.SetContext(previous)
	}
	// Only the implementation/migration gate is current. All relations in the
	// adapted SQL will still be bound through the caller's historical snapshot.
	currentRef, currentDef, err := ctx.Resolve("information_schema", "columns", nil)
	if err != nil {
		return "", err
	}
	if currentDef == nil || currentDef.ViewSql == nil {
		return "", moerr.NewNotSupported(builder.GetContext(), "current information_schema.COLUMNS migration gate is unavailable")
	}
	if dependency := prepareSchemaRefWithSnapshot(currentRef, currentDef, nil); dependency != nil {
		if crossAccount {
			// This has no subscriber-local name. Cache reuse must rebind instead
			// of resolving the gate in the caller's account.
			dependency.PubInfo = &planpb.PubInfo{TenantId: int32(snapshot.Tenant.TenantID)}
		}
		builder.qry.CatalogDependencies = appendPrepareSchemas(builder.qry.CatalogDependencies, dependency)
	}
	var current ViewData
	if err := json.Unmarshal([]byte(currentDef.ViewSql.View), &current); err != nil {
		return "", err
	}
	if !sysview.IsCurrentInformationSchemaColumnsDDL(current.Stmt) {
		return definition, nil
	}
	if err := RequirePersistedProtocolVersion(builder.GetContext(), ctx.GetProcess(), defines.MORPCVersion100); err != nil {
		return "", err
	}
	adapted, ok := sysview.AdaptLegacyInformationSchemaColumnsDDL(definition)
	if !ok {
		return "", moerr.NewNotSupported(builder.GetContext(), "unsupported historical information_schema.COLUMNS definition")
	}
	return adapted, nil
}

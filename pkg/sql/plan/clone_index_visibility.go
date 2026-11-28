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
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type cloneCatalogReadKey struct{}

type cloneCatalogReadCache struct {
	discovery  bool
	visibility map[cloneCatalogReadScope]map[uint64]*tableIndexVisibility
	defaults   map[cloneCatalogReadScope]plan.DatabaseDefaults
}

type cloneCatalogReadScope struct {
	database uint64
	account  uint32
	physical int64
	logical  uint32
}

type tableIndexVisibility struct {
	names    map[string]bool
	conflict error
}

// WithCloneIndexVisibility shares catalog reads between the serial table
// planners of a database clone. Only an explicit immutable snapshot is
// eligible. Live catalog reads continue to observe the statement frontier.
func WithCloneIndexVisibility(ctx context.Context) context.Context {
	return withCloneCatalogReads(ctx, false)
}

// WithCloneSchemaDiscovery is for a read-only inventory pass whose
// child statements share one transaction frontier and see its workspace.
func WithCloneSchemaDiscovery(ctx context.Context) context.Context {
	return withCloneCatalogReads(ctx, true)
}

func withCloneCatalogReads(ctx context.Context, discovery bool) context.Context {
	return context.WithValue(ctx, cloneCatalogReadKey{}, &cloneCatalogReadCache{
		discovery:  discovery,
		visibility: make(map[cloneCatalogReadScope]map[uint64]*tableIndexVisibility),
		defaults:   make(map[cloneCatalogReadScope]plan.DatabaseDefaults),
	})
}

func cloneSnapshotIndexVisibility(
	ctx CompilerContext, def *TableDef, snapshot *Snapshot,
) (map[uint64]*tableIndexVisibility, error) {
	cache, _ := ctx.GetContext().Value(cloneCatalogReadKey{}).(*cloneCatalogReadCache)
	if cache == nil || def.DbId == 0 {
		return nil, nil
	}
	var scope cloneCatalogReadScope
	switch {
	case IsSnapshotValid(snapshot):
		scope.physical, scope.logical = snapshot.TS.PhysicalTime, snapshot.TS.LogicalTime
	case cache.discovery:
		ts := ctx.GetProcess().GetTxnOperator().SnapshotTS()
		scope.physical, scope.logical = ts.PhysicalTime, ts.LogicalTime
	default:
		return nil, nil
	}
	account := uint32(0)
	if snapshot != nil && snapshot.Tenant != nil {
		account = snapshot.Tenant.TenantID
	} else {
		var err error
		account, err = defines.GetAccountId(ViewSchemaCatalogContext(ctx.GetProcess().GetTopContext()))
		if err != nil {
			return nil, err
		}
	}
	scope.database, scope.account = def.DbId, account
	if visibility := cache.visibility[scope]; visibility != nil {
		return visibility, nil
	}
	// Use the table's catalog identity to scope the read. The per-table
	// lookup is keyed by table_id even for historical index catalog rows.
	sql := fmt.Sprintf(`SELECT i.name, i.is_visible, i.table_id
FROM mo_catalog.mo_indexes i JOIN mo_catalog.mo_tables t ON i.table_id = t.rel_id
WHERE t.reldatabase_id = %d AND t.account_id = %d`, def.DbId, account)
	visibility, err := readIndexVisibility(ctx, snapshot, sql, 0, true)
	if err != nil {
		return nil, err
	}
	cache.visibility[scope] = visibility
	return visibility, nil
}

func readIndexVisibility(ctx CompilerContext, snapshot *Snapshot, sql string, tableID uint64, batch bool) (map[uint64]*tableIndexVisibility, error) {
	result, err := runSqlWithSnapshot(ctx, sql, snapshot)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	visibility := make(map[uint64]*tableIndexVisibility)
	expectedColumns := 2
	if batch {
		expectedColumns = 3
	}
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if len(cols) != expectedColumns {
			err = moerr.NewInternalErrorf(ctx.GetContext(),
				"invalid mo_indexes visibility result: expected %d columns, got %d", expectedColumns, len(cols))
			return false
		}
		names := executor.GetStringRows(cols[0])
		visible := executor.GetFixedRows[int8](cols[1])
		var tableIDs []uint64
		if batch {
			tableIDs = executor.GetFixedRows[uint64](cols[2])
		}
		if len(names) != rows || len(visible) != rows || (batch && len(tableIDs) != rows) {
			err = moerr.NewInternalErrorf(ctx.GetContext(),
				"invalid mo_indexes visibility result: expected %d rows", rows)
			return false
		}
		for i, name := range names {
			if batch {
				tableID = tableIDs[i]
			}
			table := visibility[tableID]
			if table == nil {
				table = &tableIndexVisibility{names: make(map[string]bool)}
				visibility[tableID] = table
			}
			key, value := indexNameKey(name), visible[i] != 0
			if previous, ok := table.names[key]; ok && previous != value && table.conflict == nil {
				table.conflict = moerr.NewInternalErrorf(ctx.GetContext(),
					"inconsistent visibility metadata for index '%s'", name)
			}
			table.names[strings.Clone(key)] = value
		}
		return true
	})
	return visibility, err
}

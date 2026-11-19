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

package compile

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// ResolveIndexMaintenanceSpec resolves a caller-verified index group in the
// caller's current transaction. The caller must establish that defs came from
// the owned base's current index group (or its CREATE plan), not from SQL.
func ResolveIndexMaintenanceSpec(ctx context.Context, database string, db engine.Database, baseName string, baseID uint64, function string, defs map[string]*plan.IndexDef) (*executor.IndexMaintenanceSpec, error) {
	if ctx == nil || db == nil || database == "" || baseName == "" || baseID == 0 || function == "" || len(defs) == 0 || len(defs) > 16 {
		return nil, moerr.NewInvalidInputNoCtx("missing index maintenance owner")
	}
	account, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	dbID, err := strconv.ParseUint(db.GetDatabaseId(ctx), 10, 64)
	if err != nil || dbID == 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid index maintenance database identity")
	}
	base, err := db.Relation(ctx, baseName, nil)
	if err != nil {
		return nil, err
	}
	if base == nil || base.GetTableID(ctx) != baseID {
		return nil, moerr.NewInvalidInputNoCtx("index maintenance base identity changed")
	}
	roles := make([]string, 0, len(defs))
	for role := range defs {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	spec := &executor.IndexMaintenanceSpec{
		Function: function, AccountID: account, Database: database, DatabaseID: dbID,
		BaseName: baseName, BaseID: baseID,
	}
	var indexName, indexAlgo string
	for _, role := range roles {
		def := defs[role]
		if def == nil || role == "" || role != def.IndexAlgoTableType || def.IndexName == "" || def.IndexTableName == "" {
			return nil, moerr.NewInvalidInputNoCtx("invalid index maintenance definition")
		}
		if indexName == "" {
			indexName, indexAlgo = def.IndexName, def.IndexAlgo
		} else if def.IndexName != indexName || def.IndexAlgo != indexAlgo {
			return nil, moerr.NewInvalidInputNoCtx("index maintenance definitions do not form one group")
		}
		relation, err := db.Relation(ctx, def.IndexTableName, nil)
		if err != nil {
			return nil, err
		}
		if relation == nil {
			return nil, moerr.NewInvalidInputNoCtx("missing index maintenance relation")
		}
		table := relation.GetTableDef(ctx)
		if table == nil || !strings.EqualFold(relation.GetTableName(), def.IndexTableName) {
			return nil, moerr.NewInvalidInputNoCtx("index maintenance relation definition changed")
		}
		extra := relation.GetExtraInfo()
		if extra != nil && extra.ParentTableID != 0 && extra.ParentTableID != baseID {
			return nil, moerr.NewInvalidInputNoCtx("index maintenance relation has a different parent")
		}
		flagged := features.IsIndexTable(table.FeatureFlag)
		if extra != nil {
			flagged = flagged || features.IsIndexTable(extra.FeatureFlag)
		}
		kind := table.TableType
		// New temporary children need the durable marker; their catalog kind
		// is deliberately temporary rather than an algorithm's normal kind.
		if strings.EqualFold(kind, catalog.SystemTemporaryTable) {
			if !flagged {
				return nil, moerr.NewInvalidInputNoCtx("temporary index maintenance relation is unmarked")
			}
		} else if !strings.EqualFold(kind, role) && !strings.EqualFold(kind, catalog.SystemIndexRel) &&
			!(strings.EqualFold(kind, catalog.SystemOrdinaryRel) && (flagged || strings.HasPrefix(strings.ToLower(def.IndexTableName), strings.ToLower(catalog.IndexTableNamePrefix)))) {
			return nil, moerr.NewInvalidInputNoCtx("index maintenance relation has an unexpected kind")
		}
		spec.Targets = append(spec.Targets, executor.IndexMaintenanceTarget{Role: role, Name: def.IndexTableName, ID: relation.GetTableID(ctx)})
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	return spec, nil
}

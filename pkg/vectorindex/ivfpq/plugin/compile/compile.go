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

// Package compile implements the IVF-PQ plugin's compile-layer (DDL) hooks.
//
// Scope: anything that runs during DDL execution — CREATE INDEX, ALTER
// REINDEX, DROP INDEX, ALTER TABLE column changes that touch an indexed
// column. The four Hooks methods cover those four operations.
//
// The lifted logic operates through a CompileContext provided by the SQL
// layer (pkg/sql/compile/plugin_context.go), so this package does not
// import pkg/sql/compile — that would create a cycle.
//
// What CompileContext exposes (see pkg/vectorindex/plugin/compile/hooks.go
// for the contract):
//
//	Ctx()                   — request context.Context
//	Database()              — engine.Database for the indexed table's db
//	QryDatabase()           — database name from the parsed query
//	OriginalTableDef()      — plan.TableDef of the parent (indexed) table
//	IndexInfo()             — plan.CreateTable carrying the hidden-table
//	                          DDL during CREATE; nil during ALTER REINDEX
//	MainTableID()           — parent table ID
//	MainExtra()             — parent table SchemaExtra (gets mutated to
//	                          record new index-table IDs)
//	RunSql(sql)             — execute a SQL statement in the current txn
//	BuildIndexTable(def)    — create one hidden table from its TableDef
//	ResolveVariable(...)    — system-variable lookup (proc.GetResolveVar)
//
// Lifted from:
//   - pkg/sql/compile/ddl_index_algo.go:802 (handleVectorIvfpqIndex)
//   - pkg/sql/compile/util.go:740,757      (gen{Delete,Build}IvfpqIndex)
package compile

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	compileplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/compile"
)

// insertIntoIvfpqIndexTableFormat is the SQL template used to populate the
// IVF-PQ index storage table. Lifted from pkg/sql/compile/util.go:126.
const insertIntoIvfpqIndexTableFormat = "SELECT f.* from `%s`.`%s` AS %s CROSS APPLY ivfpq_create('%s', '%s', %s, %s) AS f;"

// Hooks implements plugin/compile.Hooks for IVF-PQ.
//
// All four methods below are required by the framework. If you add a hook
// to plugin/compile/hooks.go and don't implement it here, the
// `var _ compileplugin.Hooks = Hooks{}` interface check (at the bottom of
// this file) breaks the build.
type Hooks struct{}

// HandleCreateIndex runs during CREATE INDEX (and as the worker for
// HandleReindex). Called once per multi-table index; indexDefs is keyed by
// IndexAlgoTableType (the same strings CatalogHooks.HiddenTableTypes()
// returns). For IVF-PQ that's {"ivfpq_meta", "ivfpq_index"}.
//
// Responsibilities:
//  1. Validate the indexDefs shape (number of tables, key parts).
//  2. Create the hidden tables via ctx.BuildIndexTable.
//  3. Clear stale runtime cache entries (vectorindex/cache).
//  4. Wipe any pre-existing rows in the hidden tables (DELETE FROM ...).
//  5. Populate the storage table from the source table — IVF-PQ uses
//     CROSS APPLY ivfpq_create(...) which the engine routes to the
//     ivfpq_create table-function builder in pkg/sql/plan/ivfpq.go.
//
// Lifted from Scope.handleVectorIvfpqIndex
// (pkg/sql/compile/ddl_index_algo.go:802).
func (Hooks) HandleCreateIndex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) error {
	// 1. static check
	if len(indexDefs) != 2 {
		return moerr.NewInternalErrorNoCtx("invalid ivfpq index table definition")
	}
	if len(indexDefs[catalog.Ivfpq_TblType_Metadata].Parts) != 1 {
		return moerr.NewInternalErrorNoCtx("invalid ivfpq index part must be 1.")
	}

	// 2. create hidden tables
	if info := ctx.IndexInfo(); info != nil {
		for _, table := range info.GetIndexTables() {
			if err := ctx.BuildIndexTable(table); err != nil {
				return err
			}
		}
	}

	// 3. clear the cache
	key := indexDefs[catalog.Ivfpq_TblType_Storage].IndexTableName
	cache.Cache.Remove(key)

	// 4. delete old data first
	sqls, err := genDeleteSQL(indexDefs, ctx.QryDatabase())
	if err != nil {
		return err
	}
	for _, sql := range sqls {
		if err = ctx.RunSql(sql); err != nil {
			return err
		}
	}

	// 5. build ivfpq index
	sqls, err = genBuildSQL(ctx, indexDefs)
	if err != nil {
		return err
	}
	for _, sql := range sqls {
		if err = ctx.RunSql(sql); err != nil {
			return err
		}
	}
	return nil
}

// HandleReindex runs during ALTER … REINDEX. For IVF-PQ this is just the
// same as a fresh CREATE — the same delete-old + populate-new flow. The
// forceSync flag mirrors IVF-FLAT's semantics (run synchronously inside
// the transaction); IVF-PQ ignores it.
//
// New algorithms: if your rebuild strategy diverges from CREATE (e.g.
// incremental rebuild), write a separate implementation here.
func (h Hooks) HandleReindex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef, _ bool) error {
	return h.HandleCreateIndex(ctx, indexDefs)
}

// ValidateReindexParams is the per-algo arm of the ALTER … REINDEX
// parameter-change validator (originally pkg/sql/compile/ddl.go:929 switch).
// Receive `old` (the current params map for the index) and a
// ReindexParamUpdate carrying the user's new values; return the merged
// params or an error.
//
// IVF-PQ has no online parameter updates today, so this is a no-op
// passthrough — matching the legacy ddl.go:961 fall-through.
func (Hooks) ValidateReindexParams(old map[string]string, _ compileplugin.ReindexParamUpdate) (map[string]string, error) {
	return old, nil
}

// HandleDropIndex runs algorithm-specific cleanup beyond the generic
// hidden-table deletion the SQL layer already performs.
//
// Implementations typically: unregister CDC tasks (DropIndexCdcTask),
// remove idxcron schedules, clear runtime caches.
//
// IVF-PQ does none of those — generic hidden-table deletion is enough —
// so this is a no-op. Compare HNSW, which does maintain CDC tasks.
func (Hooks) HandleDropIndex(_ compileplugin.CompileContext, _ map[string]*plan.IndexDef) error {
	return nil
}

// IdxcronMetadata: IVF-PQ has no idxcron action wired today
// (SyncDescriptor().IdxcronAction==""). Returns (nil, nil) until the executor
// learns Action_Ivfpq_Reindex.
func (Hooks) IdxcronMetadata(_ compileplugin.CompileContext) ([]byte, error) {
	return nil, nil
}

// Compile-time interface check.
var _ compileplugin.Hooks = Hooks{}

// genDeleteSQL is lifted from pkg/sql/compile/util.go:740.
func genDeleteSQL(indexDefs map[string]*plan.IndexDef, qryDatabase string) ([]string, error) {
	meta, ok := indexDefs[catalog.Ivfpq_TblType_Metadata]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("ivfpq_meta index definition not found")
	}
	idx, ok := indexDefs[catalog.Ivfpq_TblType_Storage]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("ivfpq_index index definition not found")
	}
	return []string{
		fmt.Sprintf("DELETE FROM `%s`.`%s`", qryDatabase, meta.IndexTableName),
		fmt.Sprintf("DELETE FROM `%s`.`%s`", qryDatabase, idx.IndexTableName),
	}, nil
}

// genBuildSQL is lifted from pkg/sql/compile/util.go:757.
func genBuildSQL(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) ([]string, error) {
	originalTableDef := ctx.OriginalTableDef()
	qryDatabase := ctx.QryDatabase()
	const srcAlias = "src"
	pkColName := srcAlias + "." + originalTableDef.Pkey.PkeyColName

	meta, ok := indexDefs[catalog.Ivfpq_TblType_Metadata]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("ivfpq_meta index definition not found")
	}
	idx, ok := indexDefs[catalog.Ivfpq_TblType_Storage]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("ivfpq_index index definition not found")
	}

	cfg := vectorindex.IndexTableConfig{
		MetadataTable: meta.IndexTableName,
		IndexTable:    idx.IndexTableName,
		DbName:        qryDatabase,
		SrcTable:      originalTableDef.Name,
		PKey:          pkColName,
		KeyPart:       idx.Parts[0],
	}

	threads, err := ctx.ResolveVariable("ivfpq_threads_build", true, false)
	if err != nil {
		return nil, err
	}
	cfg.ThreadsBuild = threads.(int64)

	idxcap, err := ctx.ResolveVariable("ivfpq_max_index_capacity", true, false)
	if err != nil {
		return nil, err
	}
	cfg.IndexCapacity = idxcap.(int64)

	cfgbytes, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	params := idx.IndexAlgoParams
	part := srcAlias + "." + idx.Parts[0] + filterColumnsFromParams(params, srcAlias)

	sql := fmt.Sprintf(insertIntoIvfpqIndexTableFormat,
		qryDatabase, originalTableDef.Name,
		srcAlias,
		params,
		string(cfgbytes),
		pkColName,
		part)
	return []string{sql}, nil
}

// filterColumnsFromParams is lifted from pkg/sql/compile/util.go:640.
// Reads the comma-joined "included_columns" entry from the JSON algo-params
// blob and returns ", src.col1, src.col2, …".
func filterColumnsFromParams(indexAlgoParams, srcAlias string) string {
	if len(indexAlgoParams) == 0 {
		return ""
	}
	val, err := sonic.Get([]byte(indexAlgoParams), catalog.IncludedColumns)
	if err != nil {
		return ""
	}
	joined, err := val.StrictString()
	if err != nil || len(joined) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, name := range strings.Split(joined, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		sb.WriteString(", ")
		sb.WriteString(srcAlias)
		sb.WriteByte('.')
		sb.WriteString(name)
	}
	return sb.String()
}

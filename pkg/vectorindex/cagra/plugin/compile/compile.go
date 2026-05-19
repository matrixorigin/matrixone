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

// Package compile implements the CAGRA plugin's compile-layer (DDL) hooks.
// See pkg/vectorindex/ivfpq/plugin/compile for the canonical template.
//
// Lifted from:
//   - pkg/sql/compile/ddl_index_algo.go:732 (handleVectorCagraIndex)
//   - pkg/sql/compile/util.go:666,688      (gen{Delete,Build}CagraIndex)
package compile

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	cagraruntime "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin/runtime"
)

// insertIntoCagraIndexTableFormat is the SQL template used to populate the
// CAGRA index storage table. Lifted from pkg/sql/compile/util.go:122.
const insertIntoCagraIndexTableFormat = "SELECT f.* from `%s`.`%s` AS %s CROSS APPLY cagra_create('%s', '%s', %s, %s) AS f;"

// Compile-time interface check.
var _ compileplugin.Hooks = Hooks{}

// Hooks implements plugin/compile.Hooks for CAGRA.
type Hooks struct{}

// HandleCreateIndex is lifted from Scope.handleVectorCagraIndex
// (pkg/sql/compile/ddl_index_algo.go:732).
//
// CREATE INDEX uses the always-async path (forceSync=false): the
// cagra_create build is stashed as InitSQL and runs inside the CDC
// pipeline's first iteration.
func (h Hooks) HandleCreateIndex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) error {
	return h.handleCreate(ctx, indexDefs, false)
}

// HandleReindex runs the same code path as create, but honors
// forceSync. The idxcron background reindex executor passes
// forceSync=true so the build happens synchronously inside the txn
// before the CDC task picks up forward changes. Mirrors IVF-FLAT.
func (h Hooks) HandleReindex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef, forceSync bool) error {
	return h.handleCreate(ctx, indexDefs, forceSync)
}

// handleCreate is the shared body for HandleCreateIndex and
// HandleReindex. forceSync controls whether cagra_create runs inside
// the current txn (true — background reindex) or is deferred to the
// CDC pipeline via InitSQL (false — the always-async default path).
func (Hooks) handleCreate(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef, forceSync bool) error {
	if ok, err := ctx.IsExperimentalEnabled(cagraruntime.CagraIndexFlag); err != nil {
		return err
	} else if !ok {
		return moerr.NewInternalErrorNoCtx("experimental_cagra_index is not enabled")
	}

	if len(indexDefs) != 2 {
		return moerr.NewInternalErrorNoCtx("invalid cagra index table definition")
	}
	if len(indexDefs[catalog.Cagra_TblType_Metadata].Parts) != 1 {
		return moerr.NewInternalErrorNoCtx("invalid hnsw index part must be 1.")
	}

	if info := ctx.IndexInfo(); info != nil {
		for _, table := range info.GetIndexTables() {
			if err := ctx.BuildIndexTable(table); err != nil {
				return err
			}
		}
	}

	// Skip index data population for CCPR tables when this is a CCPR task
	// transaction. The index data will be synced via CCPR data
	// synchronization instead.
	originalTableDef := ctx.OriginalTableDef()
	if ctx.IsCCPRTaskTransaction() && ctx.IsTableFromPublication(originalTableDef) {
		return nil
	}

	key := indexDefs[catalog.Cagra_TblType_Storage].IndexTableName
	cache.Cache.Remove(key)

	sqls, err := genDeleteSQL(indexDefs, ctx.QryDatabase())
	if err != nil {
		return err
	}
	for _, sql := range sqls {
		if err = ctx.RunSql(sql); err != nil {
			return err
		}
	}

	buildSqls, err := genBuildSQL(ctx, indexDefs)
	if err != nil {
		return err
	}
	sinkerType := ctx.SinkerTypeFromAlgo(catalog.MoIndexCagraAlgo.ToString())
	indexName := indexDefs[catalog.Cagra_TblType_Metadata].IndexName

	if forceSync {
		// Background reindex: build cagra_create synchronously inside
		// the current txn so the new tag=0 model lands before
		// subsequent steps observe the index. Then re-register the
		// CDC task with startFromNow=true (CDC catches only forward
		// changes; the build we just ran already produced tag=0).
		for _, sql := range buildSqls {
			if err = ctx.RunSql(sql); err != nil {
				return err
			}
		}
		if err = ctx.DropIndexCdcTask(originalTableDef, ctx.QryDatabase(),
			originalTableDef.Name, indexName); err != nil {
			return err
		}
		return ctx.CreateIndexCdcTask(ctx.QryDatabase(), originalTableDef.Name, originalTableDef.TblId,
			indexName, sinkerType, true, "", originalTableDef)
	}

	// Always-async path (CREATE INDEX, foreground reindex): defer the
	// cagra_create build to the CDC pipeline's ProcessInitSQL step.
	// cuvs storage needs both tag=0 (model blob) and tag=1 (CDC
	// events); CagraSync only writes tag=1, so we stash the build SQL
	// as InitSQL — unlike HNSW which passes "" because HnswSync
	// derives both layers from the event stream alone.
	if err = ctx.DropIndexCdcTask(originalTableDef, ctx.QryDatabase(),
		originalTableDef.Name, indexName); err != nil {
		return err
	}
	return ctx.CreateIndexCdcTask(ctx.QryDatabase(), originalTableDef.Name, originalTableDef.TblId,
		indexName, sinkerType, false, strings.Join(buildSqls, ";"), originalTableDef)
}

// ValidateReindexParams is a no-op for CAGRA (matches ddl.go:960
// fall-through).
func (Hooks) ValidateReindexParams(old map[string]string, _ compileplugin.ReindexParamUpdate) (map[string]string, error) {
	return old, nil
}

// HandleDropIndex is a no-op: generic hidden-table cleanup is sufficient.
func (Hooks) HandleDropIndex(_ compileplugin.CompileContext, _ map[string]*plan.IndexDef) error {
	return nil
}

// IdxcronMetadata: CAGRA has no idxcron action wired today
// (SyncDescriptor().IdxcronAction==""). Returns (nil, nil) until the executor
// learns Action_Cagra_Reindex.
func (Hooks) IdxcronMetadata(_ compileplugin.CompileContext) ([]byte, error) {
	return nil, nil
}

// genDeleteSQL is lifted from pkg/sql/compile/util.go:666.
func genDeleteSQL(indexDefs map[string]*plan.IndexDef, qryDatabase string) ([]string, error) {
	meta, ok := indexDefs[catalog.Cagra_TblType_Metadata]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("cagra_meta index definition not found")
	}
	idx, ok := indexDefs[catalog.Cagra_TblType_Storage]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("cagra_index index definition not found")
	}
	return []string{
		fmt.Sprintf("DELETE FROM `%s`.`%s`", qryDatabase, meta.IndexTableName),
		fmt.Sprintf("DELETE FROM `%s`.`%s`", qryDatabase, idx.IndexTableName),
	}, nil
}

// genBuildSQL is lifted from pkg/sql/compile/util.go:688.
func genBuildSQL(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) ([]string, error) {
	originalTableDef := ctx.OriginalTableDef()
	qryDatabase := ctx.QryDatabase()
	const srcAlias = "src"
	pkColName := srcAlias + "." + originalTableDef.Pkey.PkeyColName

	meta, ok := indexDefs[catalog.Cagra_TblType_Metadata]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("cagra_meta index definition not found")
	}
	idx, ok := indexDefs[catalog.Cagra_TblType_Storage]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("cagra_index index definition not found")
	}

	cfg := vectorindex.IndexTableConfig{
		MetadataTable: meta.IndexTableName,
		IndexTable:    idx.IndexTableName,
		DbName:        qryDatabase,
		SrcTable:      originalTableDef.Name,
		PKey:          pkColName,
		KeyPart:       idx.Parts[0],
	}

	threads, err := ctx.ResolveVariable("cagra_threads_build", true, false)
	if err != nil {
		return nil, err
	}
	cfg.ThreadsBuild = threads.(int64)

	idxcap, err := ctx.ResolveVariable("cagra_max_index_capacity", true, false)
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

	sql := fmt.Sprintf(insertIntoCagraIndexTableFormat,
		qryDatabase, originalTableDef.Name,
		srcAlias,
		params,
		string(cfgbytes),
		pkColName,
		part)
	return []string{sql}, nil
}

// filterColumnsFromParams is lifted from pkg/sql/compile/util.go:640.
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

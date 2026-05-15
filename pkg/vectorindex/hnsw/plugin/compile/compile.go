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

// Package compile implements the HNSW plugin's compile-layer (DDL) hooks.
//
// HNSW is the most feature-rich vector index on the compile side:
//   - Gated by the `experimental_hnsw_index` flag.
//   - Skips initial population for CCPR (publication-sourced) tables.
//   - Branches on sync vs async — async indexes don't run an immediate
//     populate, only register a CDC task.
//   - Registers an ISCP CDC task that maintains the hidden tables from
//     the source table's CDC stream.
//
// All of those features go through methods on CompileContext (see
// pkg/vectorindex/plugin/compile/hooks.go) so this package doesn't have to
// import pkg/sql/compile.
//
// Lifted from:
//   - pkg/sql/compile/ddl_index_algo.go:627 (handleVectorHnswIndex)
//   - pkg/sql/compile/util.go:554,576      (gen{Delete,Build}HnswIndex)
package compile

import (
	"encoding/json"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	compileplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/compile"
)

// HnswIndexFlag is the experimental-feature flag gating HNSW DDL. Must
// match the constant in pkg/sql/compile/ddl_index_algo.go.
const HnswIndexFlag = "experimental_hnsw_index"

// insertIntoHnswIndexTableFormat is the SQL template used to populate the
// HNSW index storage table. Lifted from pkg/sql/compile/util.go:118.
const insertIntoHnswIndexTableFormat = "SELECT f.* from `%s`.`%s` AS %s CROSS APPLY hnsw_create('%s', '%s', %s, %s) AS f;"

var _ compileplugin.Hooks = Hooks{}

type Hooks struct{}

// HandleCreateIndex is lifted from Scope.handleVectorHnswIndex
// (pkg/sql/compile/ddl_index_algo.go:627).
func (Hooks) HandleCreateIndex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) error {
	if ok, err := ctx.IsExperimentalEnabled(HnswIndexFlag); err != nil {
		return err
	} else if !ok {
		return moerr.NewInternalErrorNoCtx("experimental_hnsw_index is not enabled")
	}

	if len(indexDefs) != 2 {
		return moerr.NewInternalErrorNoCtx("invalid hnsw index table definition")
	}
	if len(indexDefs[catalog.Hnsw_TblType_Metadata].Parts) != 1 {
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

	key := indexDefs[catalog.Hnsw_TblType_Storage].IndexTableName
	cache.Cache.Remove(key)

	// delete old data first
	sqls, err := genDeleteSQL(indexDefs, ctx.QryDatabase())
	if err != nil {
		return err
	}
	for _, sql := range sqls {
		if err = ctx.RunSql(sql); err != nil {
			return err
		}
	}

	async, err := catalog.IsIndexAsync(indexDefs[catalog.Hnsw_TblType_Metadata].IndexAlgoParams)
	if err != nil {
		return err
	}

	sinkerType := ctx.SinkerTypeFromAlgo(catalog.MoIndexHnswAlgo.ToString())
	indexName := indexDefs[catalog.Hnsw_TblType_Metadata].IndexName

	if !async {
		// Build the index immediately, then register a CDC task that
		// only consumes changes from now forward.
		sqls, err := genBuildSQL(ctx, indexDefs)
		if err != nil {
			return err
		}
		for _, sql := range sqls {
			if err = ctx.RunSql(sql); err != nil {
				return err
			}
		}
		return ctx.CreateIndexCdcTask(ctx.QryDatabase(), originalTableDef.Name, originalTableDef.TblId,
			indexName, sinkerType, true, "", originalTableDef)
	}

	// async: drop any existing CDC task, register a new one consuming the
	// full log from the table's creation timestamp.
	if err := ctx.DropIndexCdcTask(originalTableDef, ctx.QryDatabase(), originalTableDef.Name, indexName); err != nil {
		return err
	}
	return ctx.CreateIndexCdcTask(ctx.QryDatabase(), originalTableDef.Name, originalTableDef.TblId,
		indexName, sinkerType, false, "", originalTableDef)
}

// HandleReindex: same code path as create.
func (h Hooks) HandleReindex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef, _ bool) error {
	return h.HandleCreateIndex(ctx, indexDefs)
}

// ValidateReindexParams: HNSW has no online parameter updates.
func (Hooks) ValidateReindexParams(old map[string]string, _ compileplugin.ReindexParamUpdate) (map[string]string, error) {
	return old, nil
}

// HandleDropIndex is a no-op: the generic CDC unregister path in
// pkg/sql/compile/ddl.go already calls DropIndexCdcTask during DROP INDEX
// (ddl.go:2511). This hook is the seam for any algorithm-specific cleanup
// not covered there.
func (Hooks) HandleDropIndex(_ compileplugin.CompileContext, _ map[string]*plan.IndexDef) error {
	return nil
}

// genDeleteSQL is lifted from pkg/sql/compile/util.go:554.
func genDeleteSQL(indexDefs map[string]*plan.IndexDef, qryDatabase string) ([]string, error) {
	meta, ok := indexDefs[catalog.Hnsw_TblType_Metadata]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("hnsw_meta index definition not found")
	}
	idx, ok := indexDefs[catalog.Hnsw_TblType_Storage]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("hnsw_index index definition not found")
	}
	return []string{
		fmt.Sprintf("DELETE FROM `%s`.`%s`", qryDatabase, meta.IndexTableName),
		fmt.Sprintf("DELETE FROM `%s`.`%s`", qryDatabase, idx.IndexTableName),
	}, nil
}

// genBuildSQL is lifted from pkg/sql/compile/util.go:576.
func genBuildSQL(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) ([]string, error) {
	originalTableDef := ctx.OriginalTableDef()
	qryDatabase := ctx.QryDatabase()
	const srcAlias = "src"
	pkColName := srcAlias + "." + originalTableDef.Pkey.PkeyColName

	meta, ok := indexDefs[catalog.Hnsw_TblType_Metadata]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("hnsw_meta index definition not found")
	}
	idx, ok := indexDefs[catalog.Hnsw_TblType_Storage]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("hnsw_index index definition not found")
	}

	cfg := vectorindex.IndexTableConfig{
		MetadataTable: meta.IndexTableName,
		IndexTable:    idx.IndexTableName,
		DbName:        qryDatabase,
		SrcTable:      originalTableDef.Name,
		PKey:          pkColName,
		KeyPart:       idx.Parts[0],
	}

	threads, err := ctx.ResolveVariable("hnsw_threads_build", true, false)
	if err != nil {
		return nil, err
	}
	cfg.ThreadsBuild = threads.(int64)

	idxcap, err := ctx.ResolveVariable("hnsw_max_index_capacity", true, false)
	if err != nil {
		return nil, err
	}
	cfg.IndexCapacity = idxcap.(int64)

	cfgbytes, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	params := idx.IndexAlgoParams
	part := srcAlias + "." + idx.Parts[0]

	sql := fmt.Sprintf(insertIntoHnswIndexTableFormat,
		qryDatabase, originalTableDef.Name,
		srcAlias,
		params,
		string(cfgbytes),
		pkColName,
		part)
	return []string{sql}, nil
}

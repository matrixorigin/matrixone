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
// pkg/indexplugin/compile/hooks.go) so this package doesn't have to
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
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	hnswruntime "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/runtime"
)

// insertIntoHnswIndexTableFormat is the SQL template used to populate the
// HNSW index storage table. Lifted from pkg/sql/compile/util.go:118.
// The %s placeholders are pre-quoted/escaped via pkg/common/sqlquote: the
// source table and alias are quoted identifiers, params + config are escaped
// string literals, and the pk / part are quoted column references.
const insertIntoHnswIndexTableFormat = "SELECT f.* from %s AS %s CROSS APPLY hnsw_create(%s, %s, %s, %s) AS f;"

var _ compileplugin.Hooks = Hooks{}

type Hooks struct{}

// HandleCreateIndex is lifted from Scope.handleVectorHnswIndex
// (pkg/sql/compile/ddl_index_algo.go:627).
func (h Hooks) HandleCreateIndex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) error {
	// Create never overrides the index's async param: the build is synchronous
	// only if the index itself is sync (decided inside via !async).
	return h.handleCreate(ctx, indexDefs, false)
}

// handleCreate is the shared body of HandleCreateIndex and HandleReindex.
// forceSync=true routes to the inline (!async) build branch regardless of the
// index's async param — used by ALTER REINDEX … FORCE_SYNC (e.g. restore's
// RestoreTable) to rebuild an always-async HNSW index synchronously.
func (Hooks) handleCreate(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef, forceSync bool) error {
	logutil.Infof("[plugin] hnsw handleCreate: isFrontend=%v forceSync=%v defs=%d", ctx.IsFrontend(), forceSync, len(indexDefs))
	// Frontend-only: re-entry from background (idxcron ALTER REINDEX,
	// ProcessInitSQL) must not re-check the flag, since (a) it may
	// have been toggled off since the original CREATE INDEX, and (b)
	// the background context's resolver may not surface the user's
	// value.
	if ctx.IsFrontend() {
		if ok, err := ctx.IsExperimentalEnabled(hnswruntime.HnswIndexFlag); err != nil {
			return err
		} else if !ok {
			return moerr.NewInternalErrorNoCtx("experimental_hnsw_index is not enabled")
		}
	}

	if len(indexDefs) != 2 {
		return moerr.NewInternalErrorNoCtx("invalid hnsw index table definition")
	}
	metaDef, ok := indexDefs[catalog.Hnsw_TblType_Metadata]
	if !ok {
		return moerr.NewInternalErrorNoCtx("hnsw_meta index definition not found")
	}
	storageDef, ok := indexDefs[catalog.Hnsw_TblType_Storage]
	if !ok {
		return moerr.NewInternalErrorNoCtx("hnsw_index index definition not found")
	}
	if len(metaDef.Parts) != 1 {
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

	cache.Cache.Remove(storageDef.IndexTableName)

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

	async, err := catalog.IsIndexAsync(metaDef.IndexAlgoParams)
	if err != nil {
		return err
	}

	sinkerType := ctx.SinkerTypeFromAlgo(catalog.MoIndexHnswAlgo.ToString())
	indexName := metaDef.IndexName

	if !async || forceSync {
		// Build the index immediately, then register a CDC task that
		// only consumes changes from now forward. Drop any prior CDC
		// task first — on REINDEX re-entry the previous task would
		// otherwise survive at its old watermark and replay historical
		// events on top of the freshly built state. forceSync (ALTER
		// REINDEX … FORCE_SYNC) takes this branch even for an async index.
		sqls, err := genBuildSQL(ctx, indexDefs)
		if err != nil {
			return err
		}
		for _, sql := range sqls {
			if err = ctx.RunSql(sql); err != nil {
				return err
			}
		}
		if err := ctx.DropIndexCdcTask(originalTableDef, ctx.QryDatabase(), originalTableDef.Name, indexName); err != nil {
			return err
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

// HandleReindex: same code path as create, but honors forceSync so an
// ALTER REINDEX … FORCE_SYNC (e.g. restore's RestoreTable) rebuilds an
// always-async HNSW index synchronously instead of deferring to CDC.
func (h Hooks) HandleReindex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef, forceSync bool) error {
	return h.handleCreate(ctx, indexDefs, forceSync)
}

// RestoreInitSQL — see CAGRA. Rebuilds the HNSW index post-commit during restore.
func (Hooks) RestoreInitSQL(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) (bool, string, error) {
	metaDef, ok := indexDefs[catalog.Hnsw_TblType_Metadata]
	if !ok {
		return false, "", moerr.NewInternalErrorNoCtx("hnsw_meta index definition not found")
	}
	return true, fmt.Sprintf("ALTER TABLE `%s`.`%s` ALTER REINDEX `%s` hnsw FORCE_SYNC",
		ctx.QryDatabase(), ctx.OriginalTableDef().Name, metaDef.IndexName), nil
}

func (Hooks) ValidateReindexParams(old map[string]string, alter compileplugin.ReindexParamUpdate) (map[string]string, error) {
	return compileplugin.MergeReindexParams(old, alter, "hnsw",
		catalog.HnswM,
		catalog.HnswEfConstruction,
		catalog.HnswEfSearch,
		catalog.IndexAlgoParamMaxIndexCapacity,
	)
}

// HandleDropIndex is a no-op: the generic CDC unregister path in
// pkg/sql/compile/ddl.go already calls DropIndexCdcTask during DROP INDEX
// (ddl.go:2511). This hook is the seam for any algorithm-specific cleanup
// not covered there.
func (Hooks) HandleDropIndex(_ compileplugin.CompileContext, defs map[string]*plan.IndexDef) error {
	logutil.Infof("[plugin] hnsw HandleDropIndex: defs=%d", len(defs))
	return nil
}

// IdxcronMetadata: HNSW has no idxcron action (SyncDescriptor().IdxcronAction=="").
// This method is never called for HNSW.
func (Hooks) IdxcronMetadata(_ compileplugin.CompileContext) ([]byte, error) {
	return nil, nil
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
		fmt.Sprintf("DELETE FROM %s", sqlquote.QualifiedIdent(qryDatabase, meta.IndexTableName)),
		fmt.Sprintf("DELETE FROM %s", sqlquote.QualifiedIdent(qryDatabase, idx.IndexTableName)),
	}, nil
}

// genBuildSQL is lifted from pkg/sql/compile/util.go:576.
func genBuildSQL(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) ([]string, error) {
	originalTableDef := ctx.OriginalTableDef()
	qryDatabase := ctx.QryDatabase()
	const srcAlias = "src"
	pkCol := originalTableDef.Pkey.PkeyColName

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
		PKey:          srcAlias + "." + pkCol,
		KeyPart:       idx.Parts[0],
	}

	threads, err := ctx.ResolveVariable("hnsw_threads_build", true, false)
	if err != nil {
		return nil, err
	}
	cfg.ThreadsBuild = threads.(int64)

	// max_index_capacity now rides algo_params as a flat key (written at
	// CREATE INDEX); hnsw_create / sync read it from there.
	cfgbytes, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	params := idx.IndexAlgoParams

	// Quoted column references for the hnsw_create CROSS APPLY args: `src`.`pk`
	// and `src`.`vec`. Quoting via sqlquote keeps identifiers containing
	// backticks or reserved words valid.
	pkColExpr := sqlquote.QualifiedIdent(srcAlias, pkCol)
	part := sqlquote.QualifiedIdent(srcAlias, idx.Parts[0])

	sql := fmt.Sprintf(insertIntoHnswIndexTableFormat,
		sqlquote.QualifiedIdent(qryDatabase, originalTableDef.Name),
		sqlquote.Ident(srcAlias),
		sqlquote.String(params),
		sqlquote.String(string(cfgbytes)),
		pkColExpr,
		part)
	return []string{sql}, nil
}

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
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	cagraruntime "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin/runtime"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
)

// insertIntoCagraIndexTableFormat is the SQL template used to populate the
// CAGRA index storage table. Lifted from pkg/sql/compile/util.go:122.
// The %s placeholders are pre-quoted/escaped via pkg/common/sqlquote: the
// source table and alias are quoted identifiers, params + config are escaped
// string literals, and the pk / part are quoted column references.
const insertIntoCagraIndexTableFormat = "SELECT f.* from %s AS %s CROSS APPLY cagra_create(%s, %s, %s, %s) AS f;"

// actionCagraReindex mirrors idxcron.Action_*. Inlined to avoid an
// import cycle through pkg/vectorindex/idxcron. Stays in lock-step with
// pkg/vectorindex/cagra/plugin/runtime/runtime.go:35.
const actionCagraReindex = "cagra_reindex"

// Compile-time interface check.
var _ compileplugin.Hooks = Hooks{}

// Hooks implements plugin/compile.Hooks for CAGRA.
type Hooks struct{}

const threadsBuildVar = "cagra_threads_build"

// HandleCreateIndex is lifted from Scope.handleVectorCagraIndex
// (pkg/sql/compile/ddl_index_algo.go:732).
//
// The sync-vs-async branch is driven by the index's `async`
// IndexAlgoParam (catalog.IndexParamAsync). Default (key missing or
// "false"): forceSync=true — cagra_create runs inline in the user's
// CREATE INDEX txn before the CDC task is registered. Explicit
// async="true": forceSync=false — the first CDC iteration builds the model.
func (h Hooks) HandleCreateIndex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) error {
	metaDef, ok := indexDefs[catalog.Cagra_TblType_Metadata]
	if !ok || metaDef == nil {
		return h.handleCreate(ctx, indexDefs, true, false)
	}
	async, err := catalog.IndexParamAsync(metaDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	return h.handleCreate(ctx, indexDefs, !async, false)
}

// HandleReindex uses the shared build path. An inline rebuild publishes the
// replacement model before handing off forward changes to CDC.
func (h Hooks) HandleReindex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef, forceSync bool, _ bool) error {
	return h.handleCreate(ctx, indexDefs, forceSync, true)
}

func (Hooks) RequiresSyncReindex(table *plan.TableDef, index *plan.IndexDef, oldParams, newParams map[string]string) bool {
	return cuvscdc.ReindexChangesStorageType(table, index, oldParams[catalog.Quantization], newParams[catalog.Quantization])
}

// RestoreInitSQL is bypassed for full-source initializers.
func (Hooks) RestoreInitSQL(compileplugin.CompileContext, map[string]*plan.IndexDef) (bool, string, error) {
	return false, "", nil
}

// handleCreate runs the build in the current transaction if forceSync is set.
// Otherwise ISCP builds it from a committed snapshot.
func (h Hooks) handleCreate(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef, forceSync, reindex bool) error {
	logutil.Infof("[plugin] cagra handleCreate: isFrontend=%v forceSync=%v defs=%d", ctx.IsFrontend(), forceSync, len(indexDefs))
	// Gate the experimental flag check on frontend context only. The
	// flag was enforced at the original CREATE INDEX time; re-entry
	// from background (idxcron ALTER REINDEX, ProcessInitSQL) must
	// not re-check it, since (a) the flag may have been toggled off
	// since the index was created, and (b) the background context's
	// resolver may not be able to surface the user's original value.
	if ctx.IsFrontend() {
		if ok, err := ctx.IsExperimentalEnabled(cagraruntime.CagraIndexFlag); err != nil {
			return err
		} else if !ok {
			return moerr.NewInternalErrorNoCtx("experimental_cagra_index is not enabled")
		}
	}

	if len(indexDefs) != 2 {
		return moerr.NewInternalErrorNoCtx("invalid cagra index table definition")
	}
	metaDef, ok := indexDefs[catalog.Cagra_TblType_Metadata]
	if !ok {
		return moerr.NewInternalErrorNoCtx("cagra_meta index definition not found")
	}
	storageDef, ok := indexDefs[catalog.Cagra_TblType_Storage]
	if !ok {
		return moerr.NewInternalErrorNoCtx("cagra_index index definition not found")
	}
	if len(metaDef.Parts) != 1 {
		return moerr.NewInternalErrorNoCtx("invalid cagra index part must be 1.")
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

	if forceSync {
		cache.Cache.RemoveCuvs(storageDef.IndexTableName)
		statements, err := h.SourceSnapshotInitSQL(ctx, indexDefs)
		if err != nil {
			return err
		}
		if reindex {
			if err := cuvscdc.ReplaceSnapshot(ctx.QryDatabase(), originalTableDef, storageDef, statements, ctx.RunSqlWithResult); err != nil {
				return err
			}
		} else {
			for _, sql := range statements {
				if err := ctx.RunSql(sql); err != nil {
					return err
				}
			}
		}
	}
	db := ctx.QryDatabase()
	if err := ctx.DropIndexCdcTask(originalTableDef, db, originalTableDef.Name, metaDef.IndexName); err != nil {
		return err
	}
	sinkerType := ctx.SinkerTypeFromAlgo(catalog.MoIndexCagraAlgo.ToString())
	if err := ctx.CreateIndexSnapshotCdcTask(metaDef.IndexName, sinkerType, forceSync, h.SourceSnapshotBuildVars()); err != nil {
		return err
	}
	if err := registerIdxcronUpdate(ctx, metaDef, db, originalTableDef); err != nil {
		return err
	}
	if forceSync && reindex {
		cache.Cache.RemoveCuvsOnCommit(ctx.TxnOperator(), storageDef.IndexTableName)
	}
	return nil
}

func (Hooks) SourceSnapshotBuildVars() []string {
	return []string{threadsBuildVar, cuvscdc.GPUMultiSimulationVar}
}

// SourceSnapshotInitSQL replaces the complete model from a single source snapshot.
func (Hooks) SourceSnapshotInitSQL(ctx compileplugin.SourceSnapshotContext, defs map[string]*plan.IndexDef) ([]string, error) {
	clear, err := genDeleteSQL(defs, ctx.QryDatabase())
	if err != nil {
		return nil, err
	}
	build, err := genBuildSQL(ctx, defs)
	if err != nil {
		return nil, err
	}
	return append(clear, build...), nil
}

// registerIdxcronUpdate writes the cron task's frozen-metadata row into
// mo_index_update via the BuildIdxcronMetadata + RegisterIdxcronUpdate
// pattern shared with IVF-FLAT (see ivfflat/plugin/compile/compile.go:452).
// BuildIdxcronMetadata returns (nil, nil) for background re-entry — the
// existing row is authoritative, so we skip the write.
func registerIdxcronUpdate(
	ctx compileplugin.CompileContext, indexDef *plan.IndexDef,
	qryDatabase string, originalTableDef *plan.TableDef,
) error {
	metadata, err := Hooks{}.IdxcronMetadata(ctx)
	if err != nil {
		return err
	}
	if len(metadata) == 0 {
		logutil.Infof("[plugin] cagra registerIdxcronUpdate: background re-entry, skip")
		return nil
	}
	return ctx.RegisterIdxcronUpdate(
		originalTableDef.TblId,
		qryDatabase,
		originalTableDef.Name,
		indexDef.IndexName,
		actionCagraReindex,
		metadata,
	)
}

func (Hooks) ValidateReindexParams(old map[string]string, alter compileplugin.ReindexParamUpdate) (map[string]string, error) {
	// Merge first, then validate the EFFECTIVE quantization via the per-algo
	// catalog hook (the single home shared with CREATE). The merged map is the
	// index's actual post-reindex config: the value the reindex set, or — when
	// the reindex omitted QUANTIZATION (e.g. the idxcron-issued rebuild) — the
	// value already stored on the index. Validating the merge (not the raw alter
	// delta) means the check is never skipped just because the statement omitted
	// quantization, and quantization and op_type come from one consistent source.
	merged, err := compileplugin.MergeReindexParams(old, alter, "cagra",
		catalog.IndexAlgoParamMaxIndexCapacity,
		catalog.IndexAlgoParamQuantizerTrainLimit,
		catalog.IntermediateGraphDegree,
		catalog.GraphDegree,
		catalog.ITopkSize,
		catalog.Quantization,
	)
	if err != nil {
		return nil, err
	}
	if err := (cagraruntime.CatalogHooks{}).ValidQuantization(
		merged[catalog.Quantization], merged[catalog.IndexAlgoParamOpType]); err != nil {
		return nil, err
	}
	return merged, nil
}

func (Hooks) HandleDropIndex(_ compileplugin.CompileContext, defs map[string]*plan.IndexDef) error {
	logutil.Infof("[plugin] cagra HandleDropIndex: defs=%d", len(defs))
	// Evict the cached search index so its GPU resources are freed NOW, rather
	// than lingering until the 5-min VectorIndexCacheTTL housekeeping reaps it.
	if storageDef, ok := defs[catalog.Cagra_TblType_Storage]; ok {
		cache.Cache.RemoveCuvs(storageDef.IndexTableName)
	}
	return nil
}

func (Hooks) HiddenTableDropPriority(_ string) int {
	return 0
}

// IdxcronMetadata pins CAGRA's build-time params into the cron task's
// metadata blob so the periodic rebuild uses the values the user
// picked at CREATE INDEX (not whatever the system vars happen to be
// when the cron fires hours/days later). Background re-entry is gated
// by BuildIdxcronMetadata's ctx.IsFrontend() check.
func (Hooks) IdxcronMetadata(ctx compileplugin.CompileContext) ([]byte, error) {
	logutil.Infof("[plugin] cagra IdxcronMetadata: isFrontend=%v", ctx.IsFrontend())
	return compileplugin.BuildIdxcronMetadata(ctx, compileplugin.IdxcronVarSpec{
		// Second-level gate after ctx.IsFrontend(): a sub-Compile
		// inheriting a partial frontend resolver returns nil here →
		// defer to background semantics.
		FrontendProbeVar: "cagra_threads_search",
		// max_index_capacity is NOT captured here — it rides the index's
		// algo_params (set via CREATE INDEX), read directly by the idxcron
		// reindex. The experimental flag is NOT captured either: the background
		// reindex (IsFrontend=false) skips the experimental gate.
		Capture: []string{
			"cagra_threads_build",
			"lower_case_table_names",
		},
	})
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
		fmt.Sprintf("DELETE FROM %s WHERE TRUE", sqlquote.QualifiedIdent(qryDatabase, meta.IndexTableName)),
		fmt.Sprintf("DELETE FROM %s WHERE TRUE", sqlquote.QualifiedIdent(qryDatabase, idx.IndexTableName)),
	}, nil
}

// genBuildSQL is lifted from pkg/sql/compile/util.go:688.
func genBuildSQL(ctx compileplugin.SourceSnapshotContext, indexDefs map[string]*plan.IndexDef) ([]string, error) {
	originalTableDef := ctx.OriginalTableDef()
	qryDatabase := ctx.QryDatabase()
	const srcAlias = "src"
	pkCol := originalTableDef.Pkey.PkeyColName

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
		PKey:          srcAlias + "." + pkCol,
		KeyPart:       idx.Parts[0],
	}

	threads, err := ctx.ResolveVariable(threadsBuildVar, true, false)
	if err != nil {
		return nil, err
	}
	cfg.ThreadsBuild = threads.(int64)

	// max_index_capacity now rides algo_params as a flat key (written at
	// CREATE INDEX); cagra_create reads it from there.
	gpusim, err := ctx.ResolveVariable(cuvscdc.GPUMultiSimulationVar, true, false)
	if err != nil {
		return nil, err
	}
	cfg.GpuMultiSimulation = gpusim.(int64)

	cfgbytes, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	params := idx.IndexAlgoParams

	// Quoted column references for the cagra_create CROSS APPLY args: `src`.`pk`
	// and `src`.`vec`[, `src`.`inc`...]. Quoting via sqlquote keeps identifiers
	// containing backticks or reserved words valid.
	pkColExpr := sqlquote.QualifiedIdent(srcAlias, pkCol)
	part := sqlquote.QualifiedIdent(srcAlias, idx.Parts[0]) + filterColumnsFromParams(params, srcAlias)

	sql := fmt.Sprintf(insertIntoCagraIndexTableFormat,
		sqlquote.QualifiedIdent(qryDatabase, originalTableDef.Name),
		sqlquote.Ident(srcAlias),
		sqlquote.String(params),
		sqlquote.String(string(cfgbytes)),
		pkColExpr,
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
		sb.WriteString(sqlquote.QualifiedIdent(srcAlias, name))
	}
	return sb.String()
}

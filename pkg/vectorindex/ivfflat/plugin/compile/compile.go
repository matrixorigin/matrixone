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

// Package compile implements the IVF-FLAT plugin's compile-layer (DDL) hooks.
//
// Lifted from:
//   - pkg/sql/compile/ddl.go:2348              handleVectorIvfFlatIndex
//   - pkg/sql/compile/ddl_index_algo.go:199    handleIndexColCount
//   - pkg/sql/compile/ddl_index_algo.go:221    handleIvfIndexMetaTable
//   - pkg/sql/compile/ddl_index_algo.go:253    handleIvfIndexCentroidsTable
//   - pkg/sql/compile/ddl_index_algo.go:412    handleIvfIndexEntriesTable
//   - pkg/sql/compile/ddl_index_algo.go:507    handleIvfIndexRegisterUpdate
//   - pkg/sql/compile/ddl_index_algo.go:531    logTimestamp
//   - pkg/sql/compile/ddl_index_algo.go:575    handleIvfIndexDeleteOldEntries
//   - pkg/sql/compile/iscp_util.go:267         getIvfflatMetadata
package compile

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/quantizer"
)

// actionIvfflatReindex mirrors idxcron.Action_Ivfflat_Reindex. Inlined
// here so this package doesn't import pkg/vectorindex/idxcron — which
// would create an import cycle in tests: idxcron's executor_test imports
// pkg/testutil/testengine → pkg/sql/plan → this plugin → idxcron.
//
// Stays in lock-step with pkg/vectorindex/idxcron/executor.go:56.
const actionIvfflatReindex = "ivfflat_reindex"

// Compile-time interface check.
var _ compileplugin.Hooks = Hooks{}

// Hooks implements plugin/compile.Hooks for IVF-FLAT.
type Hooks struct{}

// HandleCreateIndex is lifted verbatim from Scope.handleVectorIvfFlatIndex
// (pkg/sql/compile/ddl.go:2348). HandleReindex routes through the same
// body with forceSync threaded.
func (h Hooks) HandleCreateIndex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) error {
	return runCreateOrReindex(ctx, indexDefs, false)
}

// HandleReindex runs the same body as HandleCreateIndex with forceSync
// threaded into centroid building. Matches ddl.go:980-987 dispatch.
func (h Hooks) HandleReindex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef, forceSync bool) error {
	return runCreateOrReindex(ctx, indexDefs, forceSync)
}

// RestoreInitSQL — see CAGRA. Rebuilds the IVF-FLAT index post-commit during
// restore (re-derives entries against the restored centroids).
func (Hooks) RestoreInitSQL(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) (bool, string, error) {
	metaDef, ok := indexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata]
	if !ok {
		return false, "", moerr.NewInternalErrorNoCtx("ivfflat metadata index definition not found")
	}
	return true, fmt.Sprintf("ALTER TABLE `%s`.`%s` ALTER REINDEX `%s` ivfflat FORCE_SYNC",
		ctx.QryDatabase(), ctx.OriginalTableDef().Name, metaDef.IndexName), nil
}

// ValidateReindexParams handles the IVF-FLAT `lists` update at ALTER
// REINDEX time. The legacy switch at ddl.go:928 wrote new lists into
// the AlgoParams map and persisted it via UPDATE mo_catalog.mo_indexes
// inline — that persistence stays at the SQL-layer call site, so this
// hook only performs the map merge.
func (Hooks) ValidateReindexParams(old map[string]string, alter compileplugin.ReindexParamUpdate) (map[string]string, error) {
	if alter.IndexAlgoParamList > 0 {
		out := make(map[string]string, len(old)+1)
		for k, v := range old {
			out[k] = v
		}
		out[catalog.IndexAlgoParamLists] = strconv.FormatInt(alter.IndexAlgoParamList, 10)
		return out, nil
	}
	return old, nil
}

// HandleDropIndex: IVF-FLAT generic hidden-table deletion is performed
// by the SQL layer; CDC tasks and idxcron registrations are torn down
// via DropAllIndexCdcTasks / DropAllIndexUpdateTasks at the same seam
// (pkg/sql/compile/ddl.go DropIndex path). No additional cleanup here.
func (Hooks) HandleDropIndex(_ compileplugin.CompileContext, defs map[string]*plan.IndexDef) error {
	logutil.Infof("[plugin] ivfflat HandleDropIndex: defs=%d", len(defs))
	return nil
}

// ivfflatIdxcronSpec captures every system / session var the cron-
// triggered ALTER REINDEX needs to mirror the user's CREATE INDEX
// configuration: kmeans tuning, capacity, and the experimental flag.
// Background re-entry is gated by BuildIdxcronMetadata's
// ctx.IsFrontend() check.
var ivfflatIdxcronSpec = compileplugin.IdxcronVarSpec{
	FrontendProbeVar: "ivf_threads_search",
	// kmeans_* are NOT captured here — they ride the index's algo_params
	// (set via CREATE INDEX), which the idxcron reindex reads directly. The
	// experimental flag is NOT captured either: the background reindex
	// (IsFrontend=false) skips the experimental gate.
	Capture: []string{
		"ivf_threads_build",
		"lower_case_table_names",
	},
}

// IdxcronMetadata delegates to the shared declarative helper. The
// previous getIvfflatMetadata function (with its bespoke
// resolve+marshal loop) is replaced by this 3-line spec declaration.
func (Hooks) IdxcronMetadata(ctx compileplugin.CompileContext) ([]byte, error) {
	logutil.Infof("[plugin] ivfflat IdxcronMetadata: isFrontend=%v", ctx.IsFrontend())
	return compileplugin.BuildIdxcronMetadata(ctx, ivfflatIdxcronSpec)
}

// runCreateOrReindex is the shared body for HandleCreateIndex /
// HandleReindex. Lifted from Scope.handleVectorIvfFlatIndex.
//
// Note: unlike HNSW/CAGRA/IVF-PQ, IVF-FLAT is NOT gated by its
// experimental flag at DDL time — the legacy handler on main never
// checked one. The `experimental_ivf_index` variable still flows
// through IdxcronMetadata below for downstream consumers.
func runCreateOrReindex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef, forceSync bool) error {
	logutil.Infof("[plugin] ivfflat runCreateOrReindex: isFrontend=%v forceSync=%v defs=%d", ctx.IsFrontend(), forceSync, len(indexDefs))
	// 1. static check
	if len(indexDefs) != 3 {
		return moerr.NewInternalErrorNoCtx("invalid ivf index table definition")
	}
	metaDef, ok := indexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata]
	if !ok {
		return moerr.NewInternalErrorNoCtx("ivfflat metadata index definition not found")
	}
	centroidsDef, ok := indexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids]
	if !ok {
		return moerr.NewInternalErrorNoCtx("ivfflat centroids index definition not found")
	}
	entriesDef, ok := indexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries]
	if !ok {
		return moerr.NewInternalErrorNoCtx("ivfflat entries index definition not found")
	}
	if len(metaDef.Parts) != 1 {
		return moerr.NewInternalErrorNoCtx("invalid ivf index table definition")
	}

	// 2. create hidden tables
	if info := ctx.IndexInfo(); info != nil {
		for _, table := range info.GetIndexTables() {
			if err := ctx.BuildIndexTable(table); err != nil {
				return err
			}
		}
	}

	originalTableDef := ctx.OriginalTableDef()
	qryDatabase := ctx.QryDatabase()

	// Skip index data population for CCPR tables when this is a CCPR
	// task transaction. The index data will be synced via CCPR data
	// synchronization instead.
	if ctx.IsCCPRTaskTransaction() && ctx.IsTableFromPublication(originalTableDef) {
		return nil
	}

	async, err := catalog.IsIndexAsync(metaDef.IndexAlgoParams)
	if err != nil {
		return err
	}

	// remove the cache with version 0
	cache.Cache.Remove(fmt.Sprintf("%s:0", centroidsDef.IndexTableName))

	// 3. count rows in the source table
	totalCnt, err := indexColCount(ctx, metaDef, qryDatabase, originalTableDef)
	if err != nil {
		return err
	}

	// 4.a populate meta table
	if err = ivfIndexMetaTable(ctx, metaDef, qryDatabase); err != nil {
		return err
	}

	// 4.b + 4.c: build the index. Both kmeans (4.b) and entry assignment (4.c)
	// scan the source table, but queries never re-read it (re-rank fetches only a
	// handful of rows), so run the build's reads with SkipMemoryCacheWrites — this
	// one-shot source scan must not evict the index-entry working set the queries
	// actually hit from the fileservice cache. The optional-interface keeps the
	// CompileContext interface (and its plugin mocks) untouched; non-supporting
	// contexts just build directly.
	buildIndex := func() error {
		if err := ivfIndexCentroidsTable(ctx, centroidsDef, qryDatabase, originalTableDef,
			totalCnt, metaDef.IndexTableName, forceSync); err != nil {
			return err
		}
		if !async || forceSync {
			if err := ivfIndexEntriesTable(ctx, entriesDef, qryDatabase, originalTableDef,
				metaDef.IndexTableName, centroidsDef.IndexTableName); err != nil {
				return err
			}
		}
		return nil
	}
	if r, ok := ctx.(interface{ RunWithSourceReadCacheSkip(func() error) error }); ok {
		err = r.RunWithSourceReadCacheSkip(buildIndex)
	} else {
		err = buildIndex()
	}
	if err != nil {
		return err
	}

	// 4.d delete older entries in index table.
	if err = ivfIndexDeleteOldEntries(ctx, metaDef.IndexTableName,
		centroidsDef.IndexTableName, entriesDef.IndexTableName, qryDatabase); err != nil {
		return err
	}

	// 4.e register auto index update (reindex)
	return registerIdxcronUpdate(ctx, metaDef, qryDatabase, originalTableDef)
}

// indexColCount is lifted from Scope.handleIndexColCount
// (pkg/sql/compile/ddl_index_algo.go:199).
func indexColCount(ctx compileplugin.CompileContext, indexDef *plan.IndexDef,
	qryDatabase string, originalTableDef *plan.TableDef) (int64, error) {
	sql := fmt.Sprintf("select count(`%s`) from `%s`.`%s`;",
		indexDef.Parts[0], qryDatabase, originalTableDef.Name)
	rs, err := ctx.RunSqlWithResult(sql)
	if err != nil {
		return 0, err
	}
	defer rs.Close()
	var n int64
	rs.ReadRows(func(_ int, cols []*vector.Vector) bool {
		if len(cols) == 0 {
			return false
		}
		rows := executor.GetFixedRows[int64](cols[0])
		if len(rows) == 0 {
			return false
		}
		n = rows[0]
		return false
	})
	return n, nil
}

// readQuantizeBound reads a scalar DOUBLE metadata value (e.g. quantize_min /
// quantize_max) by key. found=false when the row is absent (e.g. a pre-quantizer
// index), in which case the caller falls back to a raw cast.
func readQuantizeBound(ctx compileplugin.CompileContext, qryDatabase, metaTbl, key string) (val float64, found bool, err error) {
	sql := fmt.Sprintf("SELECT CAST(`%s` AS DOUBLE) FROM `%s`.`%s` WHERE `%s` = '%s'",
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val, qryDatabase, metaTbl,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_key, key)
	rs, err := ctx.RunSqlWithResult(sql)
	if err != nil {
		return 0, false, err
	}
	defer rs.Close()
	rs.ReadRows(func(_ int, cols []*vector.Vector) bool {
		if len(cols) == 0 {
			return false
		}
		rows := executor.GetFixedRows[float64](cols[0])
		if len(rows) == 0 {
			return false
		}
		val = rows[0]
		found = true
		return false
	})
	return val, found, nil
}

// ivfIndexMetaTable is lifted from Scope.handleIvfIndexMetaTable
// (pkg/sql/compile/ddl_index_algo.go:221).
func ivfIndexMetaTable(ctx compileplugin.CompileContext, indexDef *plan.IndexDef, qryDatabase string) error {
	insertSQL := fmt.Sprintf("insert into `%s`.`%s` (`%s`, `%s`) values('version', '0')"+
		"ON DUPLICATE KEY UPDATE `%s` = CAST( (CAST(`%s` AS BIGINT) + 1) AS CHAR);",
		qryDatabase,
		indexDef.IndexTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
	)
	return ctx.RunSql(insertSQL)
}

// ivfIndexCentroidsTable is lifted from Scope.handleIvfIndexCentroidsTable
// (pkg/sql/compile/ddl_index_algo.go:253).
func ivfIndexCentroidsTable(
	ctx compileplugin.CompileContext, indexDef *plan.IndexDef,
	qryDatabase string, originalTableDef *plan.TableDef,
	totalCnt int64, metadataTableName string, forceSync bool,
) error {
	srcAlias := "src"
	pkColName := srcAlias + "." + originalTableDef.Pkey.PkeyColName

	cfg := vectorindex.IndexTableConfig{
		MetadataTable: metadataTableName,
		IndexTable:    indexDef.IndexTableName,
		DbName:        qryDatabase,
		SrcTable:      originalTableDef.Name,
		PKey:          pkColName,
		KeyPart:       indexDef.Parts[0],
		DataSize:      totalCnt,
	}

	listsval, err := sonic.Get([]byte(indexDef.IndexAlgoParams), catalog.IndexAlgoParamLists)
	if err != nil {
		return err
	}
	centroidParamsListsStr, err := listsval.StrictString()
	if err != nil {
		return err
	}
	centroidParamsLists, err := strconv.Atoi(centroidParamsListsStr)
	if err != nil {
		return err
	}

	var sql string
	if totalCnt == 0 || totalCnt < int64(centroidParamsLists) {
		// not enough rows: seed centroids with a single NULL placeholder.
		// Re-running ALTER REINDEX once the table is populated upgrades
		// the centroid quality.
		sql = fmt.Sprintf("INSERT INTO `%s`.`%s` (`%s`, `%s`, `%s`) "+
			"SELECT "+
			"(SELECT CAST(`%s` AS BIGINT) FROM `%s`.`%s` WHERE `%s` = 'version'), "+
			"1, NULL;",
			qryDatabase,
			indexDef.IndexTableName,
			catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
			catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
			catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
			catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
			qryDatabase,
			metadataTableName,
			catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
		)
	} else {
		threads, err := ctx.ResolveVariable("ivf_threads_build", true, false)
		if err != nil {
			return err
		}
		cfg.ThreadsBuild = threads.(int64)

		// kmeans_train_percent / kmeans_max_iteration now ride algo_params as
		// flat keys (written at CREATE INDEX); ivf_create reads them from there.
		cfgbytes, err := json.Marshal(cfg)
		if err != nil {
			return err
		}

		sql = fmt.Sprintf("SELECT * FROM ivf_create('%s', '%s') AS f;",
			indexDef.IndexAlgoParams, string(cfgbytes))
	}

	async, err := catalog.IsIndexAsync(indexDef.IndexAlgoParams)
	if err != nil {
		return err
	}

	sinkerType := ctx.SinkerTypeFromAlgo(catalog.MoIndexIvfFlatAlgo.ToString())
	indexName := indexDef.IndexName

	if async {
		if forceSync {
			// background reindex: build synchronously inside the txn so
			// the new centroids land before subsequent steps; the CDC
			// task is re-registered to consume only changes from now.
			if err = logTimestamp(ctx, qryDatabase, metadataTableName, "clustering_start"); err != nil {
				return err
			}
			if err = ctx.RunSql(sql); err != nil {
				return err
			}
			if err = logTimestamp(ctx, qryDatabase, metadataTableName, "clustering_end"); err != nil {
				return err
			}
			if err = ctx.DropIndexCdcTask(originalTableDef, qryDatabase, originalTableDef.Name, indexName); err != nil {
				return err
			}
			logutil.Infof("Ivfflat index Async = true, forceSync = true")
			return ctx.CreateIndexCdcTask(qryDatabase, originalTableDef.Name,
				originalTableDef.TblId, indexName, sinkerType, true, "", originalTableDef)
		}
		// async, not forced: defer the actual build to the CDC pipeline,
		// which replays from ts=0.
		if err = ctx.DropIndexCdcTask(originalTableDef, qryDatabase, originalTableDef.Name, indexName); err != nil {
			return err
		}
		logutil.Infof("Ivfflat index Async is true")
		return ctx.CreateIndexCdcTask(qryDatabase, originalTableDef.Name,
			originalTableDef.TblId, indexName, sinkerType, false, sql, originalTableDef)
	}

	// synchronous: build now and don't register CDC.
	if err = logTimestamp(ctx, qryDatabase, metadataTableName, "clustering_start"); err != nil {
		return err
	}
	if err = ctx.RunSql(sql); err != nil {
		return err
	}
	return logTimestamp(ctx, qryDatabase, metadataTableName, "clustering_end")
}

// ivfIndexEntriesTable is lifted from Scope.handleIvfIndexEntriesTable
// (pkg/sql/compile/ddl_index_algo.go:412).
func ivfIndexEntriesTable(
	ctx compileplugin.CompileContext, indexDef *plan.IndexDef,
	qryDatabase string, originalTableDef *plan.TableDef,
	metadataTableName, centroidsTableName string,
) error {
	val, err := sonic.Get([]byte(indexDef.IndexAlgoParams), catalog.IndexAlgoParamOpType)
	if err != nil {
		return err
	}
	optype, err := val.StrictString()
	if err != nil {
		return err
	}

	// QUANTIZATION: if set, entries are stored as the quantization type (the entry
	// column was created with that type in schema.go), so the SELECT casts the
	// base vectors to it. The CENTROIDX assignment still uses the f32 base column.
	indexColName := indexDef.Parts[0]
	entrySelectExpr := fmt.Sprintf("`%s`", indexColName)
	if qv, qerr := sonic.Get([]byte(indexDef.IndexAlgoParams), catalog.Quantization); qerr == nil {
		if qstr, serr := qv.String(); serr == nil && qstr != "" {
			if qt, ok := quantizer.ToVectorType(qstr); ok {
				var dim int32
				for _, c := range originalTableDef.Cols {
					if c.Name == indexColName {
						dim = c.Typ.Width
						break
					}
				}
				if qt == types.T_array_int8 || qt == types.T_array_uint8 {
					// cuVS-style asymmetric scalar quantizer: map the trained
					// [min,max] (stored in metadata by ivf_create) onto the full int8
					// range [-128,127] (or uint8 [0,255]) via q(x)=round(x*mul+add).
					// float16 needs no scale.
					qmin, ok1, err := readQuantizeBound(ctx, qryDatabase, metadataTableName, catalog.SystemSI_IVFFLAT_Metadata_QuantizeMin)
					if err != nil {
						return err
					}
					qmax, ok2, err := readQuantizeBound(ctx, qryDatabase, metadataTableName, catalog.SystemSI_IVFFLAT_Metadata_QuantizeMax)
					if err != nil {
						return err
					}
					col := fmt.Sprintf("`%s`", indexColName)
					if ok1 && ok2 && qt == types.T_array_int8 {
						mul, add := quantizer.Int8Params(qmin, qmax)
						entrySelectExpr = quantizer.Int8EntrySQL(col, mul, add, dim)
					} else if ok1 && ok2 {
						mul, add := quantizer.Uint8Params(qmin, qmax)
						entrySelectExpr = quantizer.Uint8EntrySQL(col, mul, add, dim)
					} else {
						entrySelectExpr = quantizer.CastSQL(col, qt, dim)
					}
				} else {
					entrySelectExpr = quantizer.CastSQL(fmt.Sprintf("`%s`", indexColName), qt, dim)
				}
			}
		}
	}

	var originalTblPkColsCommaSeparated, originalTblPkColMaySerial string
	if originalTableDef.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
		for i, part := range originalTableDef.Pkey.Names {
			if i > 0 {
				originalTblPkColsCommaSeparated += ","
			}
			originalTblPkColsCommaSeparated += fmt.Sprintf("`%s`.`%s`", originalTableDef.Name, part)
		}
		originalTblPkColMaySerial = fmt.Sprintf("serial(%s)", originalTblPkColsCommaSeparated)
	} else {
		originalTblPkColsCommaSeparated = fmt.Sprintf("`%s`.`%s`", originalTableDef.Name, originalTableDef.Pkey.PkeyColName)
		originalTblPkColMaySerial = originalTblPkColsCommaSeparated
	}

	insertSQL := fmt.Sprintf("insert into `%s`.`%s` (`%s`, `%s`, `%s`, `%s`) ",
		qryDatabase,
		indexDef.IndexTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
	)

	centroidsTableForCurrentVersionSql := fmt.Sprintf("(select * from "+
		"`%s`.`%s` where `%s` = "+
		"(select CAST(%s as BIGINT) from `%s`.`%s` where `%s` = 'version'))  as `%s`",
		qryDatabase,
		centroidsTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		qryDatabase,
		metadataTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
		centroidsTableName,
	)

	indexColumnName := indexDef.Parts[0]
	centroidsCrossL2JoinTbl := fmt.Sprintf("%s "+
		"SELECT `%s`, `%s`,  %s, %s"+
		" FROM `%s`.`%s` CENTROIDX ('%s') join %s "+
		" using (`%s`, `%s`) ",
		insertSQL,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
		originalTblPkColMaySerial,
		entrySelectExpr, // base column, or cast(base as <quant type>) under QUANTIZATION
		qryDatabase,
		originalTableDef.Name,
		optype,
		centroidsTableForCurrentVersionSql,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
		indexColumnName,
	)

	if err = logTimestamp(ctx, qryDatabase, metadataTableName, "mapping_start"); err != nil {
		return err
	}
	if err = ctx.RunSql(centroidsCrossL2JoinTbl); err != nil {
		return err
	}
	return logTimestamp(ctx, qryDatabase, metadataTableName, "mapping_end")
}

// registerIdxcronUpdate is lifted from Scope.handleIvfIndexRegisterUpdate
// (pkg/sql/compile/ddl_index_algo.go:507). The previous bespoke
// `getIvfflatMetadata(...)` call has been replaced with the shared
// BuildIdxcronMetadata helper — which returns (nil, nil) when the
// frontend probe fails, signalling background re-entry.
func registerIdxcronUpdate(
	ctx compileplugin.CompileContext, indexDef *plan.IndexDef,
	qryDatabase string, originalTableDef *plan.TableDef,
) error {
	metadata, err := compileplugin.BuildIdxcronMetadata(ctx, ivfflatIdxcronSpec)
	if err != nil {
		return err
	}
	if metadata == nil {
		// background invocation (frontend probe failed) — idxcron
		// itself is the caller, skip re-registration.
		logutil.Infof("Background invoke reindex and ignore register index update function call")
		return nil
	}
	return ctx.RegisterIdxcronUpdate(
		originalTableDef.TblId,
		qryDatabase,
		originalTableDef.Name,
		indexDef.IndexName,
		actionIvfflatReindex,
		metadata,
	)
}

// logTimestamp is lifted from Scope.logTimestamp
// (pkg/sql/compile/ddl_index_algo.go:531).
func logTimestamp(ctx compileplugin.CompileContext, qryDatabase, metadataTableName, metric string) error {
	return ctx.RunSql(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s, %s) "+
		" VALUES ('%s', NOW()) "+
		" ON DUPLICATE KEY UPDATE %s = NOW();",
		qryDatabase,
		metadataTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		metric,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
	))
}

// ivfIndexDeleteOldEntries is lifted from Scope.handleIvfIndexDeleteOldEntries
// (pkg/sql/compile/ddl_index_algo.go:575).
func ivfIndexDeleteOldEntries(
	ctx compileplugin.CompileContext,
	metadataTableName, centroidsTableName, entriesTableName, qryDatabase string,
) error {
	pruneCentroids := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `%s` < "+
		"(SELECT CAST(`%s` AS BIGINT) FROM `%s`.`%s` WHERE `%s` = 'version');",
		qryDatabase, centroidsTableName, catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val, qryDatabase, metadataTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
	)
	pruneEntries := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `%s` < "+
		"(SELECT CAST(`%s` AS BIGINT) FROM `%s`.`%s` WHERE `%s` = 'version');",
		qryDatabase, entriesTableName, catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_val, qryDatabase, metadataTableName,
		catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
	)
	if err := logTimestamp(ctx, qryDatabase, metadataTableName, "pruning_start"); err != nil {
		return err
	}
	if err := ctx.RunSql(pruneCentroids); err != nil {
		return err
	}
	if err := ctx.RunSql(pruneEntries); err != nil {
		return err
	}
	return logTimestamp(ctx, qryDatabase, metadataTableName, "pruning_end")
}

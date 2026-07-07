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

// Package compile implements the fulltext plugin's compile-layer (DDL)
// hooks.
//
// Lifted from:
//   - pkg/sql/compile/ddl_index_algo.go:132 (handleFullTextIndexTable)
//   - pkg/sql/compile/util.go:528          (genInsertIndexTableSqlForFullTextIndex)
//   - pkg/sql/compile/util.go:113          (insertIntoFullTextIndexTableFormat)
package compile

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/fulltext/wand"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
)

// insertIntoFullTextIndexTableFormat is the populate-SQL template,
// lifted verbatim from pkg/sql/compile/util.go:113.
const insertIntoFullTextIndexTableFormat = "INSERT INTO `%s`.`%s` SELECT f.* FROM `%s`.`%s` AS %s CROSS APPLY fulltext_index_tokenize('%s', %s, %s) AS f;"

// Compile-time interface check.
var _ compileplugin.Hooks = Hooks{}

// Hooks implements plugin/compile.Hooks for fulltext indexes.
type Hooks struct{}

// HandleCreateIndex is lifted from Scope.handleFullTextIndexTable
// (pkg/sql/compile/ddl_index_algo.go:132). indexDefs is keyed by
// IndexAlgoTableType — fulltext's BuildFullTextIndexDefs sets that to
// the empty string (see plan/schema.go), so the map has exactly one
// entry under the "" key.
func (Hooks) HandleCreateIndex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) error {
	return handleCreate(ctx, indexDefs, false)
}

// handleCreate is the shared body of HandleCreateIndex and HandleReindex. forceSync=true
// (ALTER REINDEX) routes a retrieval index to the synchronous inline build regardless of
// its async param — mirrors HNSW's handleCreate.
func handleCreate(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef, forceSync bool) error {
	// The postings def is keyed by the empty IndexAlgoTableType. A retrieval
	// index additionally carries WAND chunk-store + metadata defs (keyed by
	// their table types) — all hidden tables of the same index.
	indexDef, ok := indexDefs[""]
	if !ok {
		return moerr.NewInternalErrorNoCtx("fulltext postings index definition not found")
	}

	// 1. create all hidden tables (postings + any WAND store/metadata).
	if info := ctx.IndexInfo(); info != nil {
		for _, table := range info.GetIndexTables() {
			if err := ctx.BuildIndexTable(table); err != nil {
				return err
			}
		}
	}

	originalTableDef := ctx.OriginalTableDef()
	qryDatabase := ctx.QryDatabase()

	// 2. CCPR: skip data population when this is a CCPR task transaction
	// on a publication-subscribed table. The index data syncs via CCPR
	// instead.
	if ctx.IsCCPRTaskTransaction() && ctx.IsTableFromPublication(originalTableDef) {
		return nil
	}

	// The `async` PARAM gates only the initial BUILD (sync vs async), NOT DML: a
	// retrieval index's DML is always async (CatalogHooks.AlwaysAsync=true → CDC), but
	// its initial build is synchronous by default — exactly like HNSW. IsIndexAsync
	// reads the plain param (default false); GetIndexParser detects the WAND parser.
	async, err := catalog.IsIndexAsync(indexDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	retrieval := catalog.GetIndexParser(indexDef.IndexAlgoParams) == "retrieval"
	sinkerType := ctx.SinkerTypeFromAlgo(catalog.MOIndexFullTextAlgo.ToString())

	// 3a. retrieval SYNC build (the default, or ALTER REINDEX forceSync). Build the
	// postings + the tag=0 WAND base INLINE in this txn so genWandBuildSQL's
	// fulltext_wand_create reads fulltext_max_index_capacity straight from the live
	// resolver (no session_vars overlay). Then register a CDC task that maintains the
	// index from now on (startFromNow=true). Mirrors HNSW's !async branch; retrieval DML
	// stays async (AlwaysAsync) — only the initial build is synchronous.
	if retrieval && (!async || forceSync) {
		storeDef, ok := indexDefs[catalog.FullTextIndex_TblType_Storage]
		if !ok {
			return moerr.NewInternalErrorNoCtx("fulltext wand storage index definition not found")
		}
		metaDef, ok := indexDefs[catalog.FullTextIndex_TblType_Metadata]
		if !ok {
			return moerr.NewInternalErrorNoCtx("fulltext wand metadata index definition not found")
		}
		// Drop any prior CDC task first — on REINDEX re-entry it would otherwise survive at
		// its old watermark and replay history over the freshly built state.
		if err = ctx.DropIndexCdcTask(originalTableDef, qryDatabase, originalTableDef.Name, indexDef.IndexName); err != nil {
			return err
		}
		// REINDEX idempotency (no-ops on a fresh CREATE): discard the old postings + tag=1
		// tail; the tag=0 bases + metadata are cleared by the create TVF.
		cfg := wand.TableConfig{DbName: qryDatabase, IndexTable: storeDef.IndexTableName, MetadataTable: metaDef.IndexTableName}
		clearSQLs := append([]string{
			fmt.Sprintf("DELETE FROM %s", sqlquote.QualifiedIdent(qryDatabase, indexDef.IndexTableName)),
		}, wand.DeleteTailSqls(cfg)...)
		for _, sql := range clearSQLs {
			if err = ctx.RunSql(sql); err != nil {
				return err
			}
		}
		// Build postings, then split the tag=0 base per max_index_capacity.
		insertSQLs, err := genInsertSQL(originalTableDef, indexDef, qryDatabase)
		if err != nil {
			return err
		}
		buildSQLs, err := genWandBuildSQL(indexDef, storeDef, metaDef, qryDatabase)
		if err != nil {
			return err
		}
		for _, sql := range append(insertSQLs, buildSQLs...) {
			if err = ctx.RunSql(sql); err != nil {
				return err
			}
		}
		if err = ctx.CreateIndexCdcTask(qryDatabase, originalTableDef.Name,
			originalTableDef.TblId, indexDef.IndexName, sinkerType, true, "", originalTableDef); err != nil {
			return err
		}
		return registerIdxcronUpdate(ctx, indexDef, qryDatabase, originalTableDef)
	}

	// 3b. async (a retrieval index created with async=true, or a non-retrieval async
	// index): register a CDC task; data syncs via ISCP. A retrieval index's non-empty
	// InitSQL builds tag=0 from postings at task start (capacity via the session_vars
	// overlay). Only reached on CREATE — REINDEX forces the sync path above.
	if async || retrieval {
		initSQL, err := wandInitSQL(indexDefs, originalTableDef, indexDef, qryDatabase)
		if err != nil {
			return err
		}
		if err = ctx.CreateIndexCdcTask(qryDatabase, originalTableDef.Name,
			originalTableDef.TblId, indexDef.IndexName, sinkerType, false, initSQL, originalTableDef); err != nil {
			return err
		}
		if retrieval {
			return registerIdxcronUpdate(ctx, indexDef, qryDatabase, originalTableDef)
		}
		return nil
	}

	// 3c. sync (non-retrieval, async=false): populate the postings table inline.
	insertSQLs, err := genInsertSQL(originalTableDef, indexDef, qryDatabase)
	if err != nil {
		return err
	}
	for _, sql := range insertSQLs {
		if err = ctx.RunSql(sql); err != nil {
			return err
		}
	}
	return nil
}

// wandInitSQL builds the ISCP InitSQL — a JSON array of statements run at CDC
// task start — that populates a retrieval index's postings and then builds the
// tag=0 WAND store from them (genInsertSQL -> postings, genWandBuildSQL ->
// tag=0). Returns "" for a non-retrieval index (which has no WAND store).
func wandInitSQL(indexDefs map[string]*plan.IndexDef, originalTableDef *plan.TableDef, indexDef *plan.IndexDef, qryDatabase string) (string, error) {
	storeDef, ok := indexDefs[catalog.FullTextIndex_TblType_Storage]
	if !ok {
		return "", nil
	}
	metaDef, ok := indexDefs[catalog.FullTextIndex_TblType_Metadata]
	if !ok {
		return "", moerr.NewInternalErrorNoCtx("fulltext wand metadata index definition not found")
	}
	insertSQLs, err := genInsertSQL(originalTableDef, indexDef, qryDatabase)
	if err != nil {
		return "", err
	}
	buildSQLs, err := genWandBuildSQL(indexDef, storeDef, metaDef, qryDatabase)
	if err != nil {
		return "", err
	}
	js, err := json.Marshal(append(insertSQLs, buildSQLs...))
	if err != nil {
		return "", err
	}
	return string(js), nil
}

// genWandBuildSQL builds the WAND chunk store from a retrieval index's postings
// hidden table: SELECT over the postings table CROSS APPLY fulltext_wand_create,
// which reads (word, doc_id) rows, sums tf, serializes the index, and INSERTs
// the chunk + metadata rows in its end(). Mirrors HNSW genBuildSQL.
func genWandBuildSQL(postingsDef, storeDef, metaDef *plan.IndexDef, qryDatabase string) ([]string, error) {
	const srcAlias = "p"
	cfg := wand.TableConfig{
		DbName:        qryDatabase,
		IndexTable:    storeDef.IndexTableName,
		MetadataTable: metaDef.IndexTableName,
	}
	cfgbytes, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf("SELECT f.* FROM %s AS %s CROSS APPLY fulltext_wand_create(%s, %s, %s, %s) AS f",
		sqlquote.QualifiedIdent(qryDatabase, postingsDef.IndexTableName),
		sqlquote.Ident(srcAlias),
		sqlquote.String(postingsDef.IndexAlgoParams),
		sqlquote.String(string(cfgbytes)),
		sqlquote.QualifiedIdent(srcAlias, catalog.FullTextIndex_TabCol_Word),
		sqlquote.QualifiedIdent(srcAlias, catalog.FullTextIndex_TabCol_Id))
	return []string{sql}, nil
}

// HandleReindex rebuilds a retrieval index synchronously: it reuses handleCreate with
// forceSync=true, which drops the CDC task, clears the old postings + tag=0 + tag=1, and
// rebuilds the tag=0 base inline (max_index_capacity from the live resolver), then
// re-registers CDC from now. Only the retrieval (WAND) parser has a rebuildable store; a
// postings/ngram index has nothing to reindex.
func (Hooks) HandleReindex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef, _ bool) error {
	indexDef, ok := indexDefs[""]
	if !ok {
		return moerr.NewInternalErrorNoCtx("fulltext postings index definition not found")
	}
	if catalog.GetIndexParser(indexDef.IndexAlgoParams) != "retrieval" {
		return moerr.NewNotSupportedNoCtx("ALTER ... REINDEX is only supported for retrieval fulltext indexes")
	}
	return handleCreate(ctx, indexDefs, true)
}

// RestoreInitSQL — a retrieval (WAND) index carries a compact tag=0 model that
// CreateTable's sync build SEEDS and the block-level clone then APPENDS onto,
// doubling the base. Mirror HNSW: rebuild it post-commit. RestoreTable registers
// the CDC with this ALTER … REINDEX … FORCE_SYNC as its InitSQL (startFromNow=true),
// so the CDC's first iteration reindexes in its own txn — clearing postings +
// tag=0/1 and rebuilding the base from the committed cloned rows, discarding the
// seed+clone duplicate.
//
// A postings/ngram index has no compact model to rebuild: the clone copies its one
// inverted-index hidden table verbatim and the CDC catch-up handles increments. It
// still needs a NON-empty InitSQL so RestoreTable keeps startFromNow=true (watermark
// = post-clone TS, not the snapshot TS) — otherwise the CDC replays the already-cloned
// rows and doubles the postings. Hand it a no-op "SELECT 1".
func (Hooks) RestoreInitSQL(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) (bool, string, error) {
	indexDef, ok := indexDefs[""]
	if !ok {
		return false, "", moerr.NewInternalErrorNoCtx("fulltext postings index definition not found")
	}
	if catalog.GetIndexParser(indexDef.IndexAlgoParams) != "retrieval" {
		return true, "SELECT 1", nil
	}
	return true, fmt.Sprintf("ALTER TABLE `%s`.`%s` ALTER REINDEX `%s` FULLTEXT FORCE_SYNC",
		ctx.QryDatabase(), ctx.OriginalTableDef().Name, indexDef.IndexName), nil
}

// ValidateReindexParams — no-op; fulltext has no reindex-time params.
func (Hooks) ValidateReindexParams(old map[string]string, _ compileplugin.ReindexParamUpdate) (map[string]string, error) {
	return old, nil
}

// HandleDropIndex — a retrieval (WAND) index caches a WandSearch keyed by its
// ft_index storage table (loaded once, held for the idle TTL); evict it here so
// the C-backed postings buffers aren't leaked until the TTL after DROP. A
// postings/ngram fulltext index has no WAND store (no Storage def) and needs no
// cleanup beyond the generic hidden-table deletion the SQL layer performs.
func (Hooks) HandleDropIndex(_ compileplugin.CompileContext, defs map[string]*plan.IndexDef) error {
	if storeDef, ok := defs[catalog.FullTextIndex_TblType_Storage]; ok {
		logutil.Debugf("[wand] HandleDropIndex: evicting search cache for %s", storeDef.IndexTableName)
		veccache.Cache.Remove(storeDef.IndexTableName)
	}
	return nil
}

// actionFulltextReindex mirrors runtime.ActionFulltextReindex / idxcron.Action_* —
// inlined here (as ivfpq/cagra do) to avoid importing the idxcron executor or the
// plugin's runtime package from the compile layer.
const actionFulltextReindex = "fulltext_reindex"

// IdxcronMetadata captures the build-time session vars the background idxcron
// reindex needs — `fulltext_max_index_capacity`, so the cron-triggered
// `ALTER … REINDEX … FULLTEXT FORCE_SYNC` rebuilds with the same capacity the
// index was created with (the executor applies this blob as the reindex's resolver
// overlay). Returns (nil, nil) on background re-entry / partial context, so the
// existing task row persists. Only ever registered for a retrieval index.
func (Hooks) IdxcronMetadata(ctx compileplugin.CompileContext) ([]byte, error) {
	return compileplugin.BuildIdxcronMetadata(ctx, compileplugin.IdxcronVarSpec{
		FrontendProbeVar: "fulltext_max_index_capacity",
		Capture:          []string{"fulltext_max_index_capacity"},
	})
}

// registerIdxcronUpdate schedules the retrieval index's tag=1-tail compaction with
// idxcron. Mirrors ivfpq's helper: skip the write on background re-entry (nil
// metadata) so the frontend CREATE owns registration. Called only for a retrieval
// index (postings/ngram has no rebuildable WAND store).
func registerIdxcronUpdate(ctx compileplugin.CompileContext, indexDef *plan.IndexDef,
	qryDatabase string, originalTableDef *plan.TableDef) error {
	metadata, err := Hooks{}.IdxcronMetadata(ctx)
	if err != nil {
		return err
	}
	if len(metadata) == 0 {
		return nil // background re-entry / partial context — existing row persists
	}
	return ctx.RegisterIdxcronUpdate(originalTableDef.TblId, qryDatabase,
		originalTableDef.Name, indexDef.IndexName, actionFulltextReindex, metadata)
}

// genInsertSQL is lifted from pkg/sql/compile/util.go:528
// (genInsertIndexTableSqlForFullTextIndex).
func genInsertSQL(originalTableDef *plan.TableDef, indexDef *plan.IndexDef, qryDatabase string) ([]string, error) {
	const srcAlias = "src"
	pkColName := srcAlias + "." + originalTableDef.Pkey.PkeyColName
	tblname := indexDef.IndexTableName

	parts := make([]string, 0, len(indexDef.Parts))
	for _, p := range indexDef.Parts {
		parts = append(parts, srcAlias+"."+p)
	}
	concat := strings.Join(parts, ",")

	sql := fmt.Sprintf(insertIntoFullTextIndexTableFormat,
		qryDatabase, tblname,
		qryDatabase, originalTableDef.Name,
		srcAlias,
		indexDef.IndexAlgoParams,
		pkColName,
		concat)

	return []string{sql}, nil
}

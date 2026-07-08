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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/fulltext/wand"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
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

	// Resolve the base-build capacity from the index's persisted algo_params (the immutable
	// max_index_capacity flat param) so the create build splits at the SAME value every later
	// compaction reads — carried to the build TVF via cfg.Capacity (not the live session var).
	var buildCapacity int64
	if retrieval {
		if buildCapacity, err = resolveWandCapacity(indexDef.IndexAlgoParams, ctx.ResolveVariable); err != nil {
			return err
		}
	}

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
			emptyPostingsSQL(indexDef, qryDatabase), // WHERE TRUE — don't truncate the hidden table
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
		buildSQLs, err := genWandBuildSQL(indexDef, storeDef, metaDef, qryDatabase, buildCapacity)
		if err != nil {
			return err
		}
		// Empty the postings table once the WAND base is built from it: for a retrieval
		// index the postings are only the build input, never read again (see emptyPostingsSQL).
		buildSQLs = append(buildSQLs, emptyPostingsSQL(indexDef, qryDatabase))
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
	// overlay). CREATE-only: a REINDEX (forceSync) always takes a sync (re)build — 3a for
	// retrieval, 3c for non-retrieval — never re-registers CDC here.
	if !forceSync && (async || retrieval) {
		initSQL, err := wandInitSQL(indexDefs, originalTableDef, indexDef, qryDatabase, buildCapacity)
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

	// 3c. sync (non-retrieval, async=false): (re)populate the postings table inline.
	// Clear first so this is an idempotent rebuild — a no-op DELETE on a fresh CREATE
	// (empty table), a full re-tokenize on ALTER … REINDEX (forceSync). The ngram
	// postings are maintained synchronously by the DML rewrite (no CDC), so clearing +
	// re-inserting from the current source rows is a complete rebuild. WHERE TRUE keeps
	// the hidden table object (a bare DELETE would truncate/recreate it).
	if err = ctx.RunSql(emptyPostingsSQL(indexDef, qryDatabase)); err != nil {
		return err
	}
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
func wandInitSQL(indexDefs map[string]*plan.IndexDef, originalTableDef *plan.TableDef, indexDef *plan.IndexDef, qryDatabase string, capacity int64) (string, error) {
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
	buildSQLs, err := genWandBuildSQL(indexDef, storeDef, metaDef, qryDatabase, capacity)
	if err != nil {
		return "", err
	}
	// Empty the postings after the async build too (see emptyPostingsSQL) — the InitSQL runs
	// genInsertSQL -> postings -> genWandBuildSQL -> tag=0, then discards the postings.
	buildSQLs = append(buildSQLs, emptyPostingsSQL(indexDef, qryDatabase))
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
func genWandBuildSQL(postingsDef, storeDef, metaDef *plan.IndexDef, qryDatabase string, capacity int64) ([]string, error) {
	const srcAlias = "p"
	cfg := wand.TableConfig{
		DbName:        qryDatabase,
		IndexTable:    storeDef.IndexTableName,
		MetadataTable: metaDef.IndexTableName,
		Capacity:      capacity,
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

// HandleReindex handles ALTER … REINDEX for a fulltext index in one of two modes.
// Default (merge=false): a full rebuild-from-source — reuse handleCreate with
// forceSync=true. For any parser this re-tokenizes the current source rows: a
// non-retrieval (ngram/postings) index clears + repopulates its one inverted-index
// table; a retrieval (WAND) index additionally drops the CDC task, clears tag=0/tag=1,
// rebuilds the tag=0 base inline (max_index_capacity from the live resolver), and
// re-registers CDC from now.
// MERGE (merge=true): incremental compaction — fold the tag=1 CdcTail into the tag=0
// base and tiered-merge small subs over the already-built segments (no re-tokenize,
// bounded memory, O(tail) write) via the standalone fulltext_wand_compact TVF; the CDC
// task and the corpus are untouched. MERGE is WAND-specific — a non-retrieval index has
// no compactable segment store, so it is rejected (use a plain REBUILD instead).
func (Hooks) HandleReindex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef, _ bool, merge bool) error {
	indexDef, ok := indexDefs[""]
	if !ok {
		return moerr.NewInternalErrorNoCtx("fulltext postings index definition not found")
	}
	if merge {
		if catalog.GetIndexParser(indexDef.IndexAlgoParams) != "retrieval" {
			return moerr.NewNotSupportedNoCtx("ALTER ... REINDEX ... MERGE is only supported for retrieval fulltext indexes")
		}
		return handleMergeCompact(ctx, indexDefs)
	}
	// Plain REBUILD works for every parser (retrieval and ngram) — handleCreate's
	// forceSync path re-tokenizes from source idempotently.
	return handleCreate(ctx, indexDefs, true)
}

// handleMergeCompact runs the incremental fold + tiered merge over the retrieval index's
// already-built segments via the standalone fulltext_wand_compact table function, in the
// ALTER's transaction. It requires the alias (AS f) — the TVF is a FUNCTION_SCAN with no
// driving table. The TVF evicts the search cache for the store on completion.
func handleMergeCompact(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) error {
	postingsDef, ok := indexDefs[""]
	if !ok {
		return moerr.NewInternalErrorNoCtx("fulltext postings index definition not found")
	}
	storeDef, ok := indexDefs[catalog.FullTextIndex_TblType_Storage]
	if !ok {
		return moerr.NewInternalErrorNoCtx("fulltext retrieval storage index definition not found")
	}
	metaDef, ok := indexDefs[catalog.FullTextIndex_TblType_Metadata]
	if !ok {
		return moerr.NewInternalErrorNoCtx("fulltext retrieval metadata index definition not found")
	}
	// Resolve capacity from the index's PERSISTED algo_params (the immutable
	// max_index_capacity flat param, pinned at CREATE) and pass it to the TVF, so a manual
	// MERGE never depends on the triggering session's fulltext_max_index_capacity — which
	// could differ from the build value and make the tiered merge mis-judge full base subs.
	capacity, err := resolveWandCapacity(postingsDef.IndexAlgoParams, ctx.ResolveVariable)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("SELECT * FROM fulltext_wand_compact(%s, %s, %s, %s) AS f",
		sqlquote.String(ctx.QryDatabase()),
		sqlquote.String(storeDef.IndexTableName),
		sqlquote.String(metaDef.IndexTableName),
		sqlquote.String(strconv.FormatInt(capacity, 10)))
	return ctx.RunSql(sql)
}

// resolveWandCapacity returns the effective max_index_capacity for a retrieval index:
// the immutable flat param pinned in algo_params at CREATE (MAX_INDEX_CAPACITY option >
// fulltext_max_index_capacity session var > 1M default). Every op — build split, fold
// split, tiered-merge fullness — resolves it the same way so they always agree, no matter
// which session triggers the op.
func resolveWandCapacity(algoParams string, resolve func(string, bool, bool) (any, error)) (int64, error) {
	flat, err := catalog.IndexParamsStringToMap(algoParams)
	if err != nil {
		return 0, err
	}
	return indexplugin.AlgoParamInt(flat[catalog.IndexAlgoParamMaxIndexCapacity],
		resolve, "fulltext_max_index_capacity", 1000000)
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

// ValidateReindexParams accepts a new max_index_capacity on ALTER … REINDEX and merges it
// into algo_params, so the rebuild repartitions the tag=0 base at the new value and every
// later op (fold split, tiered-merge fullness) reads it — the one way to change an index's
// otherwise-immutable capacity after CREATE. Any other REINDEX option is rejected (fulltext
// honors no others). Omitted ⇒ the stored value is kept (e.g. the idxcron-issued rebuild,
// which carries no options).
func (Hooks) ValidateReindexParams(old map[string]string, alter compileplugin.ReindexParamUpdate) (map[string]string, error) {
	return compileplugin.MergeReindexParams(old, alter, "fulltext",
		catalog.IndexAlgoParamMaxIndexCapacity,
	)
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

// emptyPostingsSQL empties the postings hidden table (all rows) while keeping the table object.
// Used to clear before a rebuild AND — for a retrieval index — to discard the postings after
// the WAND base is built from them (they are only the build input; search uses the WAND store,
// CDC builds tail frames directly, and a rebuild/clone re-derives them from source, so keeping
// them is dead weight).
//
// The `WHERE TRUE` is deliberate: a bare `DELETE FROM t` takes MO's truncate fast-path, which
// DROPS + RECREATES the table object — wrong for a catalog-tracked hidden index table (it would
// re-mint the object under the same name). WHERE TRUE forces a row-level delete that empties the
// rows but keeps the table (and its schema) for the next build's genInsertSQL.
func emptyPostingsSQL(indexDef *plan.IndexDef, qryDatabase string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE TRUE", sqlquote.QualifiedIdent(qryDatabase, indexDef.IndexTableName))
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

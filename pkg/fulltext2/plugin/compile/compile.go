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

// Package compile holds the fulltext2 index's compile-layer (DDL) hooks.
package compile

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	fulltext2runtime "github.com/matrixorigin/matrixone/pkg/fulltext2/plugin/runtime"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
)

// parserFromParams extracts the "parser" field from an index's IndexAlgoParams
// JSON (empty → default parser).
func parserFromParams(params string) string {
	if len(params) == 0 {
		return ""
	}
	var p struct {
		Parser string `json:"parser"`
	}
	if err := json.Unmarshal([]byte(params), &p); err != nil {
		return ""
	}
	return p.Parser
}

var _ compileplugin.Hooks = Hooks{}

type Hooks struct{}

// HandleCreateIndex creates the storage + metadata hidden tables and builds the
// tag=0 base segment from the current source rows (Go-side: SELECT pk,col →
// tokenize → Segment → persist). Step-4 first cut: whole-table read + one base
// segment; CDC-async incremental maintenance follows.
func (Hooks) HandleCreateIndex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) error {
	// Gate CREATE FULLTEXT2 INDEX behind experimental_fulltext2_index. Frontend-only:
	// re-checking here (in addition to the framework gate in pkg/sql/compile/util.go
	// via ExperimentalFlag()) catches a flag toggled off since the original CREATE;
	// the background CDC/reindex context may not surface the user's session value, so
	// skip the check there. Mirrors bm25's HandleCreateIndex gate.
	if ctx.IsFrontend() {
		if ok, err := ctx.IsExperimentalEnabled(fulltext2runtime.Fulltext2IndexFlag); err != nil {
			return err
		} else if !ok {
			return moerr.NewInternalErrorNoCtx("experimental_fulltext2_index is not enabled")
		}
	}

	storeDef, ok := indexDefs[catalog.FullText2Index_TblType_Storage]
	if !ok {
		return moerr.NewInternalErrorNoCtx("fulltext2 storage index definition not found")
	}
	metaDef, ok := indexDefs[catalog.FullText2Index_TblType_Metadata]
	if !ok {
		return moerr.NewInternalErrorNoCtx("fulltext2 metadata index definition not found")
	}

	// 1. create the hidden tables.
	if info := ctx.IndexInfo(); info != nil {
		for _, table := range info.GetIndexTables() {
			if err := ctx.BuildIndexTable(table); err != nil {
				return err
			}
		}
	}

	origTable := ctx.OriginalTableDef()
	db := ctx.QryDatabase()
	// CCPR: index data syncs via CCPR on a publication-subscribed table.
	if ctx.IsCCPRTaskTransaction() && ctx.IsTableFromPublication(origTable) {
		return nil
	}

	// Fresh CREATE has no prior tail; build the base + register CDC.
	return buildAndRegisterCDC(ctx, storeDef, metaDef, origTable, db, false)
}

// buildAndRegisterCDC (re)builds the tag=0 base from source and registers the
// ISCP CDC task from now (startFromNow=true, no InitSQL): the inline build covers
// the current rows, so subsequent INSERT/UPDATE/DELETE flow into the tag=1 tail
// via RunFulltext2. The CDC task is dropped+recreated so a REINDEX re-entry does
// not replay history over the fresh base. On REBUILD (clearTail) the prior tag=1
// tail is discarded first — the fresh base already reflects every committed row.
func buildAndRegisterCDC(ctx compileplugin.CompileContext, storeDef, metaDef *plan.IndexDef, origTable *plan.TableDef, db string, clearTail bool) error {
	if clearTail {
		cfg := fulltext2.TableConfig{DbName: db, IndexTable: storeDef.IndexTableName, MetadataTable: metaDef.IndexTableName}
		// REBUILD: clear the tail AND the prior tag=0 base(s) here. The create TVF also
		// clears the bases, but it SKIPS that when the rebuild sees zero source rows
		// (empty/all-deleted table) — so without this a REBUILD over an emptied table
		// would keep serving the stale old base. Evict the local cache too, since the
		// create TVF's post-build eviction is likewise skipped on the zero-doc path.
		clearSqls := append(fulltext2.DeleteTailSqls(cfg), fulltext2.DeleteAllBasesSqls(cfg)...)
		for _, s := range clearSqls {
			if err := ctx.RunSql(s); err != nil {
				return err
			}
		}
		cache.Cache.Remove(storeDef.IndexTableName)
	}
	// buildFromSource clears the prior tag=0 bases (idempotent) and rebuilds them.
	if err := buildFromSource(ctx, storeDef, metaDef, origTable, db); err != nil {
		return err
	}
	sinkerType := ctx.SinkerTypeFromAlgo(catalog.MoIndexFullText2Algo.ToString())
	if err := ctx.DropIndexCdcTask(origTable, db, origTable.Name, storeDef.IndexName); err != nil {
		return err
	}
	if err := ctx.CreateIndexCdcTask(db, origTable.Name, origTable.TblId, storeDef.IndexName, sinkerType, true, "", origTable); err != nil {
		return err
	}
	return registerFulltext2Idxcron(ctx, storeDef, db, origTable)
}

// DefaultMaxIndexCapacity caps each tag=0 sub-index's doc count when the
// WITH max_index_capacity option is omitted.
const DefaultMaxIndexCapacity = int64(1000000)

// buildFromSource builds the index straight from the SOURCE table in one
// statement: SELECT f.* FROM src CROSS APPLY fulltext2_create(params, cfg, pk,
// cols…). The create TVF tokenizes each row in execution (datalink → plain text,
// json → values, ngram/gojieba parser), Add's the tokens to a builder, and at
// end() splits into capacity-bounded tag=0 bases and persists the chunk rows. No
// segment assembly happens in this (compile) layer.
func buildFromSource(ctx compileplugin.CompileContext, storeDef, metaDef *plan.IndexDef, origTable *plan.TableDef, db string) error {
	if len(storeDef.Parts) == 0 {
		return moerr.NewInternalErrorNoCtx("fulltext2 index has no indexed column")
	}
	capacity, err := resolveFulltext2Capacity(storeDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	sql, err := genFulltext2BuildFromSourceSQL(origTable, storeDef, metaDef, db, capacity)
	if err != nil {
		return err
	}
	return ctx.RunSql(sql)
}

// resolveFulltext2Capacity reads max_index_capacity from the index's algo_params,
// defaulting when the WITH option was omitted.
func resolveFulltext2Capacity(algoParams string) (int64, error) {
	if algoParams == "" {
		return DefaultMaxIndexCapacity, nil // no WITH options
	}
	flat, err := catalog.IndexParamsStringToMap(algoParams)
	if err != nil {
		return 0, err
	}
	if v, ok := flat[catalog.IndexAlgoParamMaxIndexCapacity]; ok && v != "" {
		n, perr := strconv.ParseInt(v, 10, 64)
		if perr != nil {
			return 0, perr
		}
		if n > 0 {
			return n, nil
		}
	}
	return DefaultMaxIndexCapacity, nil
}

// resolveFulltext2PositionFree reads position_free from the index's algo_params
// (absence ⇒ false, i.e. a positional/phrase-capable index).
func resolveFulltext2PositionFree(algoParams string) (bool, error) {
	if algoParams == "" {
		return false, nil
	}
	flat, err := catalog.IndexParamsStringToMap(algoParams)
	if err != nil {
		return false, err
	}
	return flat[catalog.IndexAlgoParamPositionFree] == "true", nil
}

// genFulltext2BuildFromSourceSQL emits the CROSS APPLY fulltext2_create build SQL.
func genFulltext2BuildFromSourceSQL(origTable *plan.TableDef, storeDef, metaDef *plan.IndexDef, db string, capacity int64) (string, error) {
	const srcAlias = "src"
	positionFree, err := resolveFulltext2PositionFree(storeDef.IndexAlgoParams)
	if err != nil {
		return "", err
	}
	cfg := fulltext2.TableConfig{
		DbName:        db,
		SrcTable:      origTable.Name,
		IndexTable:    storeDef.IndexTableName,
		MetadataTable: metaDef.IndexTableName,
		PKey:          origTable.Pkey.PkeyColName,
		Parser:        parserFromParams(storeDef.IndexAlgoParams),
		Capacity:      capacity,
		PositionFree:  positionFree,
		FromSource:    true,
	}
	cfgbytes, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}
	cols := make([]string, 0, len(storeDef.Parts))
	for _, p := range storeDef.Parts {
		cols = append(cols, sqlquote.QualifiedIdent(srcAlias, p))
	}
	sql := fmt.Sprintf("SELECT f.* FROM %s AS %s CROSS APPLY fulltext2_create(%s, %s, %s, %s) AS f",
		sqlquote.QualifiedIdent(db, origTable.Name),
		sqlquote.Ident(srcAlias),
		sqlquote.String(storeDef.IndexAlgoParams),
		sqlquote.String(string(cfgbytes)),
		sqlquote.QualifiedIdent(srcAlias, origTable.Pkey.PkeyColName),
		strings.Join(cols, ", "))
	return sql, nil
}

// HandleReindex runs ALTER … ALTER REINDEX: merge=true compacts the tag=1 tail
// into the tag=0 base (dropping dead docs) via fulltext2_compact; merge=false
// (REBUILD) discards the tail and rebuilds the base from source. Mirrors bm25.
func (Hooks) HandleReindex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef, _, merge bool) error {
	storeDef, ok := indexDefs[catalog.FullText2Index_TblType_Storage]
	if !ok {
		return moerr.NewInternalErrorNoCtx("fulltext2 storage index definition not found")
	}
	metaDef, ok := indexDefs[catalog.FullText2Index_TblType_Metadata]
	if !ok {
		return moerr.NewInternalErrorNoCtx("fulltext2 metadata index definition not found")
	}
	if merge {
		return handleMergeCompact(ctx, storeDef, metaDef)
	}
	return buildAndRegisterCDC(ctx, storeDef, metaDef, ctx.OriginalTableDef(), ctx.QryDatabase(), true /* clear tail */)
}

// handleMergeCompact folds the tag=1 CdcTail into the tag=0 base and drops dead
// docs, via the fulltext2_compact TVF (execution-side, one txn) — no re-tokenize.
// Capacity comes from the PERSISTED algo_params (pinned at CREATE) so a manual
// MERGE never depends on the triggering session. Mirrors bm25's handleMergeCompact.
func handleMergeCompact(ctx compileplugin.CompileContext, storeDef, metaDef *plan.IndexDef) error {
	capacity, err := resolveFulltext2Capacity(storeDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	positionFree, err := resolveFulltext2PositionFree(storeDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("SELECT * FROM fulltext2_compact(%s, %s, %s, %s, %s) AS f",
		sqlquote.String(ctx.QryDatabase()),
		sqlquote.String(storeDef.IndexTableName),
		sqlquote.String(metaDef.IndexTableName),
		sqlquote.String(strconv.FormatInt(capacity, 10)),
		sqlquote.String(strconv.FormatBool(positionFree)))
	return ctx.RunSql(sql)
}

// RestoreInitSQL — the clone copies the storage+metadata rows; register CDC from
// the post-clone TS.
func (Hooks) RestoreInitSQL(_ compileplugin.CompileContext, _ map[string]*plan.IndexDef) (bool, string, error) {
	return true, "SELECT 1", nil
}

// ValidateReindexParams — fulltext2 honors position_free on a rebuild (its only
// reindex-time param): POSITION_FREE=TRUE sets it, =FALSE clears it (→ a positional
// rebuild that re-derives positions from source), absence keeps the current setting.
// Other reindex options are ignored. The REBUILD (buildFromSource) then reads the
// merged algo_params. NOTE: changing position_free needs REBUILD (no MERGE) — a MERGE
// cannot re-derive positions when turning position_free OFF.
func (Hooks) ValidateReindexParams(old map[string]string, alter compileplugin.ReindexParamUpdate) (map[string]string, error) {
	pf, ok := alter.Params[catalog.IndexAlgoParamPositionFree]
	if !ok {
		return old, nil
	}
	merged := make(map[string]string, len(old)+1)
	for k, v := range old {
		merged[k] = v
	}
	if pf == "true" {
		// Only gojieba is meaningful position-free (ngram/json trigrams are noise as
		// bag-of-words). Mirrors the CREATE-time check.
		if old["parser"] != "gojieba" {
			return nil, moerr.NewInvalidInputNoCtxf(
				"fulltext2 POSITION_FREE requires the gojieba parser (this index uses %q); ngram/json need positions", old["parser"])
		}
		merged[catalog.IndexAlgoParamPositionFree] = "true"
	} else {
		delete(merged, catalog.IndexAlgoParamPositionFree) // false ⇒ positional (param absent)
	}
	return merged, nil
}

// HandleDropIndex — no algorithm-specific cleanup beyond the generic hidden-table
// deletion the SQL layer performs.
func (Hooks) HandleDropIndex(_ compileplugin.CompileContext, _ map[string]*plan.IndexDef) error {
	return nil
}

// actionFulltext2Reindex is the idxcron action key for fulltext2's scheduled
// compaction; must match the runtime plugin's SyncDescriptor.IdxcronAction.
const actionFulltext2Reindex = "fulltext2_reindex"

// IdxcronMetadata — the scheduled compaction reads max_index_capacity from the
// PERSISTED algo_params, so the metadata blob carries no captured vars; it only
// needs to be non-nil so the idxcron task registers. A background re-entry (not
// frontend) returns nil so the existing task row persists. Mirrors bm25.
func (Hooks) IdxcronMetadata(ctx compileplugin.CompileContext) ([]byte, error) {
	if !ctx.IsFrontend() {
		return nil, nil
	}
	return []byte("{}"), nil
}

// registerFulltext2Idxcron registers the scheduled-compaction task (skipped on a
// background re-entry so the existing task row persists). Mirrors bm25.
func registerFulltext2Idxcron(ctx compileplugin.CompileContext, storeDef *plan.IndexDef, db string, origTable *plan.TableDef) error {
	metadata, err := Hooks{}.IdxcronMetadata(ctx)
	if err != nil {
		return err
	}
	if len(metadata) == 0 {
		return nil
	}
	return ctx.RegisterIdxcronUpdate(origTable.TblId, db, origTable.Name, storeDef.IndexName, actionFulltext2Reindex, metadata)
}

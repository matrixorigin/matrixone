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

	async, err := catalog.IsIndexAsync(indexDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	// A retrieval (WAND) index is ALWAYS async — its serialized store can't be
	// row-patched inside a txn — regardless of the async param (mirrors
	// Hooks.AlwaysAsync(parser=="retrieval")).
	retrieval := catalog.GetIndexParser(indexDef.IndexAlgoParams) == "retrieval"

	// 3a. async (incl. every retrieval index): register a CDC task; data syncs
	// via ISCP. For retrieval, the initial tag=0 build runs from postings at CDC
	// task start via a non-empty InitSQL (CREATE returns before it is populated),
	// so the postings hidden table is still required.
	if async || retrieval {
		logutil.Infof("fulltext index Async is true")
		sinkerType := ctx.SinkerTypeFromAlgo(catalog.MOIndexFullTextAlgo.ToString())
		initSQL, err := wandInitSQL(indexDefs, originalTableDef, indexDef, qryDatabase)
		if err != nil {
			return err
		}
		return ctx.CreateIndexCdcTask(qryDatabase, originalTableDef.Name,
			originalTableDef.TblId, indexDef.IndexName, sinkerType, false, initSQL, originalTableDef)
	}

	// 3b. sync (non-retrieval, async=false): populate the postings table inside
	// the txn via CROSS APPLY fulltext_index_tokenize.
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

// HandleReindex — fulltext does not support ALTER … REINDEX.
func (Hooks) HandleReindex(_ compileplugin.CompileContext, _ map[string]*plan.IndexDef, _ bool) error {
	return moerr.NewNotSupportedNoCtx("ALTER ... REINDEX is not supported for fulltext indexes")
}

// RestoreInitSQL — fulltext has no compact model to rebuild; the clone copies
// the inverted-index hidden table and the CDC catch-up handles incremental
// changes. Register the CDC with startFromNow=true so its watermark is the
// post-clone TS (not the snapshot TS), avoiding a replay of the already-cloned
// rows. A non-empty InitSQL is required for startFromNow to be honored, so hand
// it a no-op "SELECT 1".
func (Hooks) RestoreInitSQL(_ compileplugin.CompileContext, _ map[string]*plan.IndexDef) (bool, string, error) {
	return true, "SELECT 1", nil
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

// IdxcronMetadata — fulltext has no idxcron action
// (SyncDescriptor().IdxcronAction == ""); this is never invoked.
func (Hooks) IdxcronMetadata(_ compileplugin.CompileContext) ([]byte, error) {
	return nil, nil
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

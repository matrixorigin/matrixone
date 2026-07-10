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

// Package compile implements the bm25 plugin's compile-layer (DDL) hooks.
//
// Phase 2 (sync-only): HandleCreateIndex / HandleReindex build the binary
// (WAND) index synchronously from the source rows via a single
//
//	SELECT f.* FROM src CROSS APPLY bm25_create(params, cfg{FromSource}, pk, cols…)
//
// statement — the create TVF tokenizes each row in-Go and splits at
// max_index_capacity, so there is no postings round-trip. CDC (live DML sync),
// idxcron merge-compaction, and restore land in Phase 4.
package compile

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/bm25/wand"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// DefaultMaxIndexCapacity caps each tag=0 sub-index's doc count when the
// WITH max_index_capacity option is omitted.
const DefaultMaxIndexCapacity = int64(1000000)

// Compile-time interface check.
var _ compileplugin.Hooks = Hooks{}

// Hooks implements plugin/compile.Hooks for bm25.
type Hooks struct{}

func (Hooks) HandleCreateIndex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) error {
	return handleCreate(ctx, indexDefs)
}

// HandleReindex — ALTER … REINDEX rebuilds the whole binary index from the
// current source rows (a full re-tokenize). forceSync is implicit: bm25's build
// is always synchronous in Phase 2. MERGE compaction lands in Phase 4.
func (Hooks) HandleReindex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef, _ bool) error {
	return handleCreate(ctx, indexDefs)
}

// handleCreate builds the storage+metadata hidden tables and the tag=0 base
// synchronously from the source rows. Idempotent: it clears any prior tag=1
// tail / tag=0 base first, so it doubles as the REINDEX rebuild body.
func handleCreate(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) error {
	storeDef, ok := indexDefs[catalog.Bm25Index_TblType_Storage]
	if !ok {
		return moerr.NewInternalErrorNoCtx("bm25 storage index definition not found")
	}
	metaDef, ok := indexDefs[catalog.Bm25Index_TblType_Metadata]
	if !ok {
		return moerr.NewInternalErrorNoCtx("bm25 metadata index definition not found")
	}

	// 1. create the hidden tables.
	if info := ctx.IndexInfo(); info != nil {
		for _, table := range info.GetIndexTables() {
			if err := ctx.BuildIndexTable(table); err != nil {
				return err
			}
		}
	}

	originalTableDef := ctx.OriginalTableDef()
	qryDatabase := ctx.QryDatabase()

	// 2. CCPR: skip data population when this is a CCPR task transaction on a
	// publication-subscribed table (the index data syncs via CCPR instead).
	if ctx.IsCCPRTaskTransaction() && ctx.IsTableFromPublication(originalTableDef) {
		return nil
	}

	capacity, err := resolveBm25Capacity(storeDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	sinkerType := ctx.SinkerTypeFromAlgo(catalog.MoIndexBm25Algo.ToString())

	// 3. Drop any prior CDC task first — on REINDEX re-entry it would otherwise
	// survive at its old watermark and replay history over the freshly built state.
	if err = ctx.DropIndexCdcTask(originalTableDef, qryDatabase, originalTableDef.Name, storeDef.IndexName); err != nil {
		return err
	}

	// 4. Clear any prior tag=1 tail (no-op on a fresh CREATE; a real clear on
	// REINDEX), then build the tag=0 base from source in one CROSS APPLY statement.
	cfg := wand.TableConfig{DbName: qryDatabase, IndexTable: storeDef.IndexTableName, MetadataTable: metaDef.IndexTableName}
	for _, sql := range wand.DeleteTailSqls(cfg) {
		if err = ctx.RunSql(sql); err != nil {
			return err
		}
	}
	buildSQLs, err := genBm25BuildFromSourceSQL(originalTableDef, storeDef, metaDef, qryDatabase, capacity)
	if err != nil {
		return err
	}
	for _, sql := range buildSQLs {
		if err = ctx.RunSql(sql); err != nil {
			return err
		}
	}

	// 5. Register the CDC task that maintains the index from now on (startFromNow=
	// true): post-create DML flows into the tag=1 CdcTail via the WAND sinker. The
	// initial build above already covers the pre-create rows.
	return ctx.CreateIndexCdcTask(qryDatabase, originalTableDef.Name,
		originalTableDef.TblId, storeDef.IndexName, sinkerType, true, "", originalTableDef)
}

func (Hooks) RestoreInitSQL(compileplugin.CompileContext, map[string]*plan.IndexDef) (bool, string, error) {
	// Phase 4: rebuild from the restored rows. Phase 2 has no CDC, so restore
	// is a no-op rebuild (the block-clone carries the storage tables as-is).
	return false, "", nil
}

func (Hooks) ValidateReindexParams(old map[string]string, _ compileplugin.ReindexParamUpdate) (map[string]string, error) {
	return old, nil
}

func (Hooks) HandleDropIndex(compileplugin.CompileContext, map[string]*plan.IndexDef) error {
	// CDC-task teardown lands in Phase 4; the generic hidden-table deletion the
	// SQL layer performs is sufficient for a sync-only index.
	return nil
}

func (Hooks) IdxcronMetadata(compileplugin.CompileContext) ([]byte, error) {
	return nil, nil
}

// resolveBm25Capacity reads max_index_capacity from the index's algo_params,
// defaulting when the WITH option was omitted.
func resolveBm25Capacity(algoParams string) (int64, error) {
	flat, err := catalog.IndexParamsStringToMap(algoParams)
	if err != nil {
		return 0, err
	}
	if v, ok := flat[catalog.IndexAlgoParamMaxIndexCapacity]; ok && v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, err
		}
		if n > 0 {
			return n, nil
		}
	}
	return DefaultMaxIndexCapacity, nil
}

// genBm25BuildFromSourceSQL builds the binary index straight from the SOURCE
// table in one statement: SELECT f.* FROM src CROSS APPLY bm25_create(params,
// cfg{FromSource}, pk, cols…). The create TVF tokenizes each row in-Go (jieba)
// and Add's the tokens; cfg carries FromSource=true and max_index_capacity.
func genBm25BuildFromSourceSQL(originalTableDef *plan.TableDef, storeDef, metaDef *plan.IndexDef, qryDatabase string, capacity int64) ([]string, error) {
	const srcAlias = "src"
	cfg := wand.TableConfig{
		DbName:        qryDatabase,
		IndexTable:    storeDef.IndexTableName,
		MetadataTable: metaDef.IndexTableName,
		Capacity:      capacity,
		FromSource:    true,
	}
	cfgbytes, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	cols := make([]string, 0, len(storeDef.Parts))
	for _, p := range storeDef.Parts {
		cols = append(cols, sqlquote.QualifiedIdent(srcAlias, p))
	}
	sql := fmt.Sprintf("SELECT f.* FROM %s AS %s CROSS APPLY bm25_create(%s, %s, %s, %s) AS f",
		sqlquote.QualifiedIdent(qryDatabase, originalTableDef.Name),
		sqlquote.Ident(srcAlias),
		sqlquote.String(storeDef.IndexAlgoParams),
		sqlquote.String(string(cfgbytes)),
		sqlquote.QualifiedIdent(srcAlias, originalTableDef.Pkey.PkeyColName),
		strings.Join(cols, ", "))
	return []string{sql}, nil
}

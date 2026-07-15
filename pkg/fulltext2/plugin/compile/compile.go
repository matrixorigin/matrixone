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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var _ compileplugin.Hooks = Hooks{}

type Hooks struct{}

// HandleCreateIndex creates the storage + metadata hidden tables and builds the
// tag=0 base segment from the current source rows (Go-side: SELECT pk,col →
// tokenize → Segment → persist). Step-4 first cut: whole-table read + one base
// segment; CDC-async incremental maintenance follows.
func (Hooks) HandleCreateIndex(ctx compileplugin.CompileContext, indexDefs map[string]*plan.IndexDef) error {
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
	return buildFromSource(ctx, storeDef, metaDef, origTable, db)
}

// buildFromSource reads (pk, indexed-col) from the source table, tokenizes each
// row into a positional segment, and persists it as the tag=0 base.
func buildFromSource(ctx compileplugin.CompileContext, storeDef, metaDef *plan.IndexDef, origTable *plan.TableDef, db string) error {
	if len(storeDef.Parts) == 0 {
		return moerr.NewInternalErrorNoCtx("fulltext2 index has no indexed column")
	}
	col := storeDef.Parts[0]
	pkName := origTable.Pkey.PkeyColName
	if len(origTable.Pkey.Names) == 1 {
		pkName = origTable.Pkey.Names[0]
	}
	var pkType int32 = -1
	for _, c := range origTable.Cols {
		if c.Name == pkName {
			pkType = c.Typ.Id
			break
		}
	}
	if pkType < 0 {
		return moerr.NewInternalErrorNoCtx("fulltext2: primary key column not found")
	}

	sql := fmt.Sprintf("SELECT `%s`, `%s` FROM `%s`.`%s`", pkName, col, db, origTable.Name)
	res, err := ctx.RunSqlWithResult(sql)
	if err != nil {
		return err
	}
	defer res.Close()

	var docs []fulltext2.Doc
	for _, bat := range res.Batches {
		if bat == nil {
			continue
		}
		for i := 0; i < bat.RowCount(); i++ {
			if bat.Vecs[1].IsNull(uint64(i)) {
				continue // NULL text → nothing to index for this row
			}
			pk := extractPk(bat.Vecs[0], i, pkType)
			if pk == nil {
				return moerr.NewNotSupportedNoCtxf("fulltext2: unsupported primary key type %s", types.T(pkType).String())
			}
			text := append([]byte(nil), []byte(bat.Vecs[1].GetStringAt(i))...)
			docs = append(docs, fulltext2.Doc{Pk: pk, Text: text})
		}
	}
	if len(docs) == 0 {
		return nil // empty table → no tag=0 base (index is CDC-only)
	}

	seg, err := fulltext2.BuildSegmentFromDocs(fulltext2.SubIndexId("base", 0), pkType, docs, tokenizer.NewSimpleTokenizer())
	if err != nil {
		return err
	}
	cfg := fulltext2.TableConfig{
		DbName:        db,
		SrcTable:      origTable.Name,
		IndexTable:    storeDef.IndexTableName,
		MetadataTable: metaDef.IndexTableName,
		PKey:          pkName,
	}
	sqls, cleanup, err := seg.ToInsertSqls(cfg, 0, 0 /* tag=0 base */)
	if err != nil {
		return err
	}
	defer cleanup()
	for _, s := range sqls {
		if err := ctx.RunSql(s); err != nil {
			return err
		}
	}
	return nil
}

// extractPk reads the source pk value into the Go type the segment codec expects.
func extractPk(vec *vector.Vector, i int, pkType int32) any {
	switch types.T(pkType) {
	case types.T_int64:
		return vector.GetFixedAtNoTypeCheck[int64](vec, i)
	case types.T_int32:
		return vector.GetFixedAtNoTypeCheck[int32](vec, i)
	case types.T_uint64:
		return vector.GetFixedAtNoTypeCheck[uint64](vec, i)
	case types.T_uint32:
		return vector.GetFixedAtNoTypeCheck[uint32](vec, i)
	case types.T_varchar, types.T_char, types.T_text, types.T_datalink,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_json:
		return append([]byte(nil), []byte(vec.GetStringAt(i))...)
	default:
		return nil
	}
}

// HandleReindex — rebuild lands with the maintenance step.
func (Hooks) HandleReindex(_ compileplugin.CompileContext, _ map[string]*plan.IndexDef, _, _ bool) error {
	return nil
}

// RestoreInitSQL — the clone copies the storage+metadata rows; register CDC from
// the post-clone TS.
func (Hooks) RestoreInitSQL(_ compileplugin.CompileContext, _ map[string]*plan.IndexDef) (bool, string, error) {
	return true, "SELECT 1", nil
}

// ValidateReindexParams — no reindex-time params.
func (Hooks) ValidateReindexParams(old map[string]string, _ compileplugin.ReindexParamUpdate) (map[string]string, error) {
	return old, nil
}

// HandleDropIndex — no algorithm-specific cleanup beyond the generic hidden-table
// deletion the SQL layer performs.
func (Hooks) HandleDropIndex(_ compileplugin.CompileContext, _ map[string]*plan.IndexDef) error {
	return nil
}

// IdxcronMetadata — no idxcron action yet.
func (Hooks) IdxcronMetadata(_ compileplugin.CompileContext) ([]byte, error) {
	return nil, nil
}

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
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
	if len(indexDefs) != 1 {
		return moerr.NewInternalErrorNoCtx("invalid fulltext index table definition")
	}
	// Single-entry map — grab the sole value regardless of key. The
	// dispatcher keys by catalog.ToLower(IndexAlgoTableType) which is
	// "" for fulltext today, but we don't depend on that here.
	var indexDef *plan.IndexDef
	for _, def := range indexDefs {
		indexDef = def
	}

	// 1. create the hidden table.
	if info := ctx.IndexInfo(); info != nil {
		tables := info.GetIndexTables()
		if len(tables) != 1 {
			return moerr.NewInternalErrorNoCtx("index table count not equal to 1")
		}
		if err := ctx.BuildIndexTable(tables[0]); err != nil {
			return err
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

	// 3a. async: register a CDC task; data syncs via ISCP.
	if async {
		logutil.Infof("fulltext index Async is true")
		sinkerType := ctx.SinkerTypeFromAlgo(catalog.MOIndexFullTextAlgo.ToString())
		return ctx.CreateIndexCdcTask(qryDatabase, originalTableDef.Name,
			originalTableDef.TblId, indexDef.IndexName, sinkerType, false, "", originalTableDef)
	}

	// 3b. sync: populate the index table inside the txn via
	// CROSS APPLY fulltext_index_tokenize.
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

// HandleReindex — fulltext does not support ALTER … REINDEX.
func (Hooks) HandleReindex(_ compileplugin.CompileContext, _ map[string]*plan.IndexDef, _ bool) error {
	return moerr.NewNotSupportedNoCtx("ALTER ... REINDEX is not supported for fulltext indexes")
}

// ValidateReindexParams — no-op; fulltext has no reindex-time params.
func (Hooks) ValidateReindexParams(old map[string]string, _ compileplugin.ReindexParamUpdate) (map[string]string, error) {
	return old, nil
}

// HandleDropIndex — no algorithm-specific cleanup beyond the generic
// hidden-table deletion the SQL layer already performs.
func (Hooks) HandleDropIndex(_ compileplugin.CompileContext, _ map[string]*plan.IndexDef) error {
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

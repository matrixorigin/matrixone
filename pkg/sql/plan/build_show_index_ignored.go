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

package plan

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

const showIndexIgnoredEmpty = "SELECT CAST('' AS VARCHAR) AS `Key_name`, CAST('' AS VARCHAR) AS `Primary_key`, CAST(0 AS INT) AS `Error_code`, CAST('' AS VARCHAR) AS `Reason`, FROM_UNIXTIME(0) AS `Ignored_at` WHERE FALSE"

// ignoredReasonSql returns the expression of the reason of a row: its message, or the message
// of its error code when it has none.
func ignoredReasonSql() string {
	var b strings.Builder
	fmt.Fprintf(&b, "CAST(COALESCE(%s, CASE %s", sqlquote.Ident(catalog.IndexIgnored_TblCol_Reason), sqlquote.Ident(catalog.IndexIgnored_TblCol_Code))
	for _, code := range vectorindex.UnstorableCodes {
		fmt.Fprintf(&b, " WHEN %d THEN %s", code, sqlquote.String(moerr.MessageOf(code)))
	}
	b.WriteString(" ELSE '' END) AS VARCHAR)")
	return b.String()
}

// buildShowIndexIgnored lists, for each index of a table, the rows the index left out
// because it cannot store them: the primary key, the error code, the reason and when the
// change was synced.
func buildShowIndexIgnored(stmt *tree.ShowIndexIgnored, ctx CompilerContext) (*Plan, error) {
	dbName, err := databaseIsValid(stmt.TableName.GetDBName(), ctx, nil)
	if err != nil {
		return nil, err
	}
	tblName := stmt.TableName.GetTableName()
	objRef, tableDef, err := ctx.Resolve(dbName, tblName, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
	}

	defs, err := tableWithPartitions(ctx, dbName, tableDef)
	if err != nil {
		return nil, err
	}

	var selects []string
	reason := ignoredReasonSql()
	ignoredTables := make(map[string]struct{})
	for _, def := range defs {
		for _, idx := range def.Indexes {
			if idx.IndexAlgoTableType != catalog.IndexIgnored_TblType || !idx.TableExist {
				continue
			}
			selects = append(selects, fmt.Sprintf(
				"SELECT CAST(%s AS VARCHAR) AS `Key_name`, CAST(%s AS VARCHAR) AS `Primary_key`, %s AS `Error_code`, %s AS `Reason`, FROM_UNIXTIME(%s DIV 1000000000) AS `Ignored_at` FROM %s",
				sqlquote.String(idx.IndexName),
				sqlquote.Ident(catalog.IndexIgnored_TblCol_Pk),
				sqlquote.Ident(catalog.IndexIgnored_TblCol_Code),
				reason,
				sqlquote.Ident(catalog.IndexIgnored_TblCol_Ts),
				sqlquote.QualifiedIdent(dbName, idx.IndexTableName)))
			ignoredTables[idx.IndexTableName] = struct{}{}
		}
	}
	sql := showIndexIgnoredEmpty
	if len(selects) > 0 {
		sql = strings.Join(selects, " UNION ALL ")
	}
	newStmt, err := getRewriteSQLStmt(ctx, "SELECT * FROM ("+sql+") t ORDER BY `Key_name`, `Ignored_at`, `Primary_key`")
	if err != nil {
		return nil, err
	}
	defer newStmt.Free()
	if stmt.Where != nil {
		newStmt.(*tree.Select).Select.(*tree.SelectClause).Where = stmt.Where
	}
	p, err := getReturnDdlBySelectStmt(ctx, newStmt, plan.DataDefinition_SHOW_INDEX)
	if err != nil {
		return nil, err
	}
	readOnBehalfOf(p, ignoredTables, objRef)
	return p, nil
}

// tableWithPartitions returns tableDef and the definitions of its partitions: a partitioned
// table keeps its indexes on its partitions.
func tableWithPartitions(ctx CompilerContext, dbName string, tableDef *TableDef) ([]*TableDef, error) {
	defs := []*TableDef{tableDef}
	if tableDef.Partition == nil {
		return defs, nil
	}
	ps := ctx.GetProcess().GetPartitionService()
	if !ps.Enabled() {
		return defs, nil
	}
	meta, err := ps.GetPartitionMetadata(ctx.GetContext(), tableDef.GetTblId(), ctx.GetProcess().GetTxnOperator())
	if err != nil {
		return nil, err
	}
	for _, p := range meta.Partitions {
		_, def, err := ctx.Resolve(dbName, p.PartitionTableName, nil)
		if err != nil {
			return nil, err
		}
		if def != nil {
			defs = append(defs, def)
		}
	}
	return defs, nil
}

// readOnBehalfOf makes the scans of the hidden tables of p checked as reads of the table objRef.
func readOnBehalfOf(p *Plan, hidden map[string]struct{}, objRef *ObjectRef) {
	if objRef == nil {
		return
	}
	for _, node := range p.GetQuery().GetNodes() {
		if node.NodeType != plan.Node_TABLE_SCAN || node.ObjRef == nil {
			continue
		}
		if _, ok := hidden[node.ObjRef.ObjName]; ok {
			node.ParentObjRef = DeepCopyObjectRef(objRef)
		}
	}
}

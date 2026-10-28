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
)

const showIndexIgnoredEmpty = "SELECT CAST('' AS VARCHAR) AS `Key_name`, CAST('' AS VARCHAR) AS `Primary_key`, CAST('' AS VARCHAR) AS `Reason`, FROM_UNIXTIME(0) AS `Ignored_at` WHERE FALSE"

// buildShowIndexIgnored lists, for each index of a table, the rows the index left out
// because it cannot store them: the primary key, the reason and when the change was synced.
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

	// A partitioned table keeps its indexes on its partitions.
	defs := []*TableDef{tableDef}
	if tableDef.Partition != nil {
		if ps := ctx.GetProcess().GetPartitionService(); ps.Enabled() {
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
		}
	}

	var selects []string
	ignoredTables := make(map[string]struct{})
	for _, def := range defs {
		for _, idx := range def.Indexes {
			if idx.IndexAlgoTableType != catalog.IndexIgnored_TblType || !idx.TableExist {
				continue
			}
			selects = append(selects, fmt.Sprintf(
				"SELECT CAST(%s AS VARCHAR) AS `Key_name`, CAST(%s AS VARCHAR) AS `Primary_key`, CAST(%s AS VARCHAR) AS `Reason`, FROM_UNIXTIME(%s DIV 1000000000) AS `Ignored_at` FROM %s",
				sqlquote.String(idx.IndexName),
				sqlquote.Ident(catalog.IndexIgnored_TblCol_Pk),
				sqlquote.Ident(catalog.IndexIgnored_TblCol_Reason),
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
	// The ignored tables are read on behalf of the table: privileges are checked on it.
	for _, node := range p.GetQuery().GetNodes() {
		if node.NodeType != plan.Node_TABLE_SCAN || node.ObjRef == nil {
			continue
		}
		if _, ok := ignoredTables[node.ObjRef.ObjName]; ok && objRef != nil {
			node.ParentObjRef = DeepCopyObjectRef(objRef)
		}
	}
	return p, nil
}

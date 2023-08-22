// Copyright 2021 - 2022 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildReplace(stmt *tree.Replace, ctx CompilerContext, isPrepareStmt bool) (p *Plan, err error) {
	tblInfo, err := getDmlTableInfo(ctx, tree.TableExprs{stmt.Table}, nil, nil, "replace")
	if err != nil {
		return nil, err
	}
	if len(tblInfo.tableDefs) != 1 {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "replace does not support multi-table")
	}
	tableDef := tblInfo.tableDefs[0]
	keys := getAllKeys(tableDef)
	deleteCond := ""

	if keys != nil {
		// table has keys
		if len(stmt.Columns) != 0 {
			// replace into table set col1 = val1, col2 = val2, ...
			row := stmt.Rows.Select.(*tree.ValuesClause).Rows[0]
			keyToRow := getKeyToRowMatch(stmt.Columns)
			keepKeys := filterKeys(keys, stmt.Columns)
			disjunction := make([]string, 0, len(keepKeys))
			for _, key := range keepKeys {
				disjunction = append(disjunction, buildConjunction(key, row, keyToRow))
			}
			deleteCond = strings.Join(disjunction, " or ")
		} else {
			// replace into table values (...);
			keyToRow := make(map[string]int, len(tableDef.Cols))
			for i, col := range tableDef.Cols {
				keyToRow[col.Name] = i
			}

			rows := stmt.Rows.Select.(*tree.ValuesClause).Rows
			disjunction := make([]string, 0, len(rows)*len(keys))
			for _, row := range rows {
				for _, key := range keys {
					disjunction = append(disjunction, buildConjunction(key, row, keyToRow))
				}
			}
			deleteCond = strings.Join(disjunction, " or ")
		}
	}

	return &Plan{
		Plan: &plan.Plan_Query{
			Query: &plan.Query{
				StmtType: plan.Query_REPLACE,
				Nodes: []*plan.Node{
					{NodeType: plan.Node_REPLACE, ReplaceCtx: &plan.ReplaceCtx{TableDef: tableDef, DeleteCond: deleteCond}},
				},
			},
		},
	}, nil
}

func isMapSubset(m, sub map[string]struct{}) bool {
	if len(sub) > len(m) {
		return false
	}
	for k := range sub {
		if _, ok := m[k]; !ok {
			return false
		}
	}
	return true
}

func getAllKeys(tableDef *plan.TableDef) []map[string]struct{} {
	n := 0
	for _, index := range tableDef.Indexes {
		if index.Unique {
			n++
		}
	}
	if tableDef.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
		n++
	}
	if n == 0 {
		return nil
	}

	keys := make([]map[string]struct{}, 0, n)
	keys = append(keys, make(map[string]struct{}))
	for _, part := range tableDef.Pkey.Names {
		keys[0][part] = struct{}{}
	}
	for _, index := range tableDef.Indexes {
		if index.Unique {
			keys = append(keys, make(map[string]struct{}))
			for _, key := range index.Parts {
				keys[len(keys)-1][key] = struct{}{}
			}
		}
	}
	return keys
}

func getInsertedCol(cols tree.IdentifierList) map[string]struct{} {
	insertedCol := make(map[string]struct{}, len(cols))
	for _, col := range cols {
		insertedCol[string(col)] = struct{}{}
	}
	return insertedCol
}

func filterKeys(keys []map[string]struct{}, cols tree.IdentifierList) []map[string]struct{} {
	keepKeys := keys[:0]
	insertedCol := getInsertedCol(cols)
	for _, key := range keys {
		if isMapSubset(insertedCol, key) {
			keepKeys = append(keepKeys, key)
		}
	}
	for i := len(keepKeys); i < len(keys); i++ {
		keys[i] = nil // or the zero value of T
	}
	return keepKeys
}

func getKeyToRowMatch(columns tree.IdentifierList) map[string]int {
	keyToRow := make(map[string]int, len(columns))
	for i, col := range columns {
		keyToRow[string(col)] = i
	}
	return keyToRow
}

func buildConjunction(key map[string]struct{}, row tree.Exprs, keyToRow map[string]int) string {
	conjunctions := make([]string, 0, len(key))
	for k := range key {
		fmtctx := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
		row[keyToRow[k]].Format(fmtctx)
		conjunctions = append(conjunctions, fmt.Sprintf("%s in (select %s)", k, fmtctx.String()))
	}
	return "(" + strings.Join(conjunctions, " and ") + ")"
}

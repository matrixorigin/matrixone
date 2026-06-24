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
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// AlterColumnHooks is an optional plan-layer hook for algorithm-specific
// metadata maintenance during ALTER TABLE column mutations.
type AlterColumnHooks interface {
	HandleAlterDropColumn(tableDef *planpb.TableDef, indexDef *planpb.IndexDef, colName string) (bool, error)
	HandleAlterRenameColumn(tableDef *planpb.TableDef, oldColName, newColName string) ([]string, error)
}

func IncludedColumnAffected(indexDef *planpb.IndexDef, colName string) bool {
	if indexDef == nil {
		return false
	}
	for _, includeCol := range indexDef.IncludedColumns {
		if catalog.ResolveAlias(includeCol) == colName {
			return true
		}
	}
	return false
}

func RenameIncludedColumnsForAlgo(tableDef *planpb.TableDef, algo, oldColName, newColName string, syncAlgoParams bool) ([]string, error) {
	if tableDef == nil || oldColName == newColName {
		return nil, nil
	}

	type includeUpdate struct {
		cols       []string
		algoParams string
	}
	updatedByIndexName := make(map[string]includeUpdate)
	for _, indexDef := range tableDef.Indexes {
		if indexDef == nil || !strings.EqualFold(indexDef.IndexAlgo, algo) {
			continue
		}
		newIncludedColumns, changed := rewriteIncludedColumnNames(indexDef.IncludedColumns, oldColName, newColName)
		if !changed {
			continue
		}

		indexDef.IncludedColumns = newIncludedColumns
		update := includeUpdate{cols: newIncludedColumns}
		if syncAlgoParams {
			params, err := catalog.IndexParamsStringToMap(indexDef.IndexAlgoParams)
			if err != nil {
				return nil, err
			}
			delete(params, "include_columns")
			params[catalog.IncludedColumns] = strings.Join(newIncludedColumns, ",")
			update.algoParams, err = catalog.IndexParamsMapToJsonString(params)
			if err != nil {
				return nil, err
			}
			indexDef.IndexAlgoParams = update.algoParams
		}
		updatedByIndexName[indexDef.IndexName] = update
	}

	if len(updatedByIndexName) == 0 {
		return nil, nil
	}

	indexNames := make([]string, 0, len(updatedByIndexName))
	for indexName := range updatedByIndexName {
		indexNames = append(indexNames, indexName)
	}
	sort.Strings(indexNames)

	sqls := make([]string, 0, len(indexNames))
	for _, indexName := range indexNames {
		update := updatedByIndexName[indexName]
		encoded, err := catalog.MarshalIncludeColumnsValue(update.cols)
		if err != nil {
			return nil, err
		}
		setClause := fmt.Sprintf("included_columns = '%s'", escapeSQLStringLiteral(encoded))
		if syncAlgoParams {
			setClause += fmt.Sprintf(", algo_params = '%s'", escapeSQLStringLiteral(update.algoParams))
		}
		sqls = append(sqls, fmt.Sprintf(
			"update `mo_catalog`.`mo_indexes` set %s where table_id = %d and name = '%s' ; ",
			setClause, tableDef.TblId, escapeSQLStringLiteral(indexName)))
	}
	return sqls, nil
}

func rewriteIncludedColumnNames(includedColumns []string, oldColName, newColName string) ([]string, bool) {
	if oldColName == newColName || len(includedColumns) == 0 {
		return includedColumns, false
	}

	newIncludedColumns := append([]string(nil), includedColumns...)
	changed := false
	for i, includeCol := range newIncludedColumns {
		if catalog.ResolveAlias(includeCol) == oldColName {
			newIncludedColumns[i] = newColName
			changed = true
		}
	}
	return newIncludedColumns, changed
}

func escapeSQLStringLiteral(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, "'", "''")
}

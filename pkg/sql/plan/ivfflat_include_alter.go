// Copyright 2024 Matrix Origin
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

func indexIncludesColumn(indexDef *planpb.IndexDef, colName string) bool {
	for _, includeCol := range indexDef.IncludedColumns {
		if catalog.ResolveAlias(includeCol) == colName {
			return true
		}
	}
	return false
}

func indexDependsOnColumn(indexDef *planpb.IndexDef, colName string) (bool, error) {
	for _, part := range indexDef.Parts {
		if catalog.ResolveAlias(part) == colName {
			return true, nil
		}
	}

	return indexIncludesColumn(indexDef, colName), nil
}

func collectAffectedIndexNamesForAlter(indexDefs []*planpb.IndexDef, affectedCols []string) ([]string, error) {
	if len(indexDefs) == 0 || len(affectedCols) == 0 {
		return nil, nil
	}

	affectedIndexNames := make(map[string]struct{}, len(indexDefs))
	for _, colName := range affectedCols {
		for _, idxDef := range indexDefs {
			depends, err := indexDependsOnColumn(idxDef, colName)
			if err != nil {
				return nil, err
			}
			if depends {
				affectedIndexNames[idxDef.IndexName] = struct{}{}
			}
		}
	}

	if len(affectedIndexNames) == 0 {
		return nil, nil
	}

	names := make([]string, 0, len(affectedIndexNames))
	for name := range affectedIndexNames {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func rewriteIncludedColumnNames(includedColumns []string, oldColName, newColName string) ([]string, bool) {
	if oldColName == newColName {
		return includedColumns, false
	}
	if len(includedColumns) == 0 {
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

func renameColumnInVectorIndexIncludedColumns(tableDef *planpb.TableDef, oldColName, newColName string) ([]string, error) {
	if oldColName == newColName {
		return nil, nil
	}

	updatedByIndexName := make(map[string][]string)
	for _, indexDef := range tableDef.Indexes {
		newIncludedColumns, changed := rewriteIncludedColumnNames(indexDef.IncludedColumns, oldColName, newColName)
		if !changed {
			continue
		}

		indexDef.IncludedColumns = newIncludedColumns
		updatedByIndexName[indexDef.IndexName] = newIncludedColumns
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
		encoded, err := catalog.MarshalIncludeColumnsValue(updatedByIndexName[indexName])
		if err != nil {
			return nil, err
		}
		sqls = append(sqls, fmt.Sprintf(
			"update `mo_catalog`.`mo_indexes` set included_columns = '%s' where table_id = %d and name = '%s' ; ",
			escapeSQLStringLiteral(encoded),
			tableDef.TblId,
			indexName,
		))
	}
	return sqls, nil
}

func escapeSQLStringLiteral(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, "'", "''")
}

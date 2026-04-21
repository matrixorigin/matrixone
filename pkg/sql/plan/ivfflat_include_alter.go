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

func ivfIndexIncludesColumn(indexDef *planpb.IndexDef, colName string) (bool, error) {
	if !catalog.IsIvfIndexAlgo(indexDef.IndexAlgo) {
		return false, nil
	}

	includeCols, err := getIvfIncludeColumnNamesFromParams(indexDef.IndexAlgoParams)
	if err != nil {
		return false, err
	}

	for _, includeCol := range includeCols {
		if catalog.ResolveAlias(includeCol) == colName {
			return true, nil
		}
	}
	return false, nil
}

func indexDependsOnColumn(indexDef *planpb.IndexDef, colName string) (bool, error) {
	for _, part := range indexDef.Parts {
		if catalog.ResolveAlias(part) == colName {
			return true, nil
		}
	}

	return ivfIndexIncludesColumn(indexDef, colName)
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

func rewriteIvfIncludeColumnNames(indexAlgoParams, oldColName, newColName string) (string, bool, error) {
	if oldColName == newColName {
		return indexAlgoParams, false, nil
	}

	includeCols, err := getIvfIncludeColumnNamesFromParams(indexAlgoParams)
	if err != nil {
		return "", false, err
	}
	if len(includeCols) == 0 {
		return indexAlgoParams, false, nil
	}

	changed := false
	for i, includeCol := range includeCols {
		if catalog.ResolveAlias(includeCol) == oldColName {
			includeCols[i] = newColName
			changed = true
		}
	}
	if !changed {
		return indexAlgoParams, false, nil
	}

	params, err := catalog.IndexParamsStringToMap(indexAlgoParams)
	if err != nil {
		return "", false, err
	}
	encoded, err := catalog.MarshalIncludeColumnsValue(includeCols)
	if err != nil {
		return "", false, err
	}
	params[catalog.IndexAlgoParamIncludeColumns] = encoded

	newParams, err := catalog.IndexParamsMapToJsonString(params)
	if err != nil {
		return "", false, err
	}
	return newParams, true, nil
}

func renameColumnInIvfIndexAlgoParams(tableDef *planpb.TableDef, oldColName, newColName string) ([]string, error) {
	if oldColName == newColName {
		return nil, nil
	}

	updatedByIndexName := make(map[string]string)
	for _, indexDef := range tableDef.Indexes {
		newParams, changed, err := rewriteIvfIncludeColumnNames(indexDef.IndexAlgoParams, oldColName, newColName)
		if err != nil {
			return nil, err
		}
		if !changed {
			continue
		}

		indexDef.IndexAlgoParams = newParams
		updatedByIndexName[indexDef.IndexName] = newParams
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
		sqls = append(sqls, fmt.Sprintf(
			"update `mo_catalog`.`mo_indexes` set algo_params = '%s' where table_id = %d and name = '%s' ; ",
			escapeSQLStringLiteral(updatedByIndexName[indexName]),
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

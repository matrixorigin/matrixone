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
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func indexDependsOnColumn(indexDef *planpb.IndexDef, colName string) (bool, error) {
	if indexDef == nil {
		return false, nil
	}
	for _, part := range indexDef.Parts {
		if catalog.ResolveAlias(part) == colName {
			return true, nil
		}
	}

	return includedColumnAffected(indexDef, colName), nil
}

func includedColumnAffected(indexDef *planpb.IndexDef, colName string) bool {
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

func renameIncludedColumnsForAlgo(tableDef *planpb.TableDef, algo, oldColName, newColName string, syncAlgoParams bool) ([]string, error) {
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
		setClause := fmt.Sprintf("included_columns = '%s'", sqlquote.EscapeString(encoded))
		if syncAlgoParams {
			setClause += fmt.Sprintf(", algo_params = '%s'", sqlquote.EscapeString(update.algoParams))
		}
		sqls = append(sqls, fmt.Sprintf(
			"update `mo_catalog`.`mo_indexes` set %s where table_id = %d and name = '%s' ; ",
			setClause, tableDef.TblId, sqlquote.EscapeString(indexName)))
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

func renameColumnRequiresPluginIndexRebuild(tableDef *planpb.TableDef, oldColName string) (bool, error) {
	if tableDef == nil {
		return false, nil
	}
	for _, indexDef := range tableDef.Indexes {
		if indexDef == nil {
			continue
		}
		p, ok := indexplugin.Get(indexDef.IndexAlgo)
		if !ok {
			continue
		}
		rebuildHook, ok := p.Plan().(planplugin.RenameColumnRebuildHook)
		if !ok {
			continue
		}
		requiresRebuild, err := rebuildHook.RenameColumnRequiresIndexRebuild(tableDef, indexDef, oldColName)
		if err != nil {
			return false, err
		}
		if requiresRebuild {
			return true, nil
		}
	}
	return false, nil
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

func handleAlterRenameColumnWithPluginHooks(tableDef *planpb.TableDef, oldColName, newColName string) ([]string, error) {
	if tableDef == nil || oldColName == newColName {
		return nil, nil
	}

	seenAlgo := make(map[string]struct{})
	var sqls []string
	for _, indexDef := range tableDef.Indexes {
		if indexDef == nil {
			continue
		}
		algo := catalog.ToLower(indexDef.IndexAlgo)
		if _, seen := seenAlgo[algo]; seen {
			continue
		}
		seenAlgo[algo] = struct{}{}

		p, ok := indexplugin.Get(algo)
		if !ok {
			continue
		}
		alterHooks, ok := p.Plan().(planplugin.AlterColumnHooks)
		if !ok {
			continue
		}

		hookSQLs, err := alterHooks.HandleAlterRenameColumn(tableDef, oldColName, newColName)
		if err != nil {
			return nil, err
		}
		sqls = append(sqls, hookSQLs...)
	}
	return sqls, nil
}

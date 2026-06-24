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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
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

	return planplugin.IncludedColumnAffected(indexDef, colName), nil
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

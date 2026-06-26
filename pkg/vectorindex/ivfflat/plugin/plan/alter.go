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
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var _ planplugin.AlterColumnHooks = Hooks{}

func (Hooks) HandleAlterDropColumn(_ *planpb.TableDef, indexDef *planpb.IndexDef, colName string) (bool, error) {
	return planplugin.IncludedColumnAffected(indexDef, colName), nil
}

func (Hooks) HandleAlterRenameColumn(tableDef *planpb.TableDef, oldColName, newColName string) ([]string, error) {
	entriesTables := ivfflatEntriesTablesWithIncludedColumn(tableDef, oldColName)

	sqls, err := planplugin.RenameIncludedColumnsForAlgo(tableDef, catalog.MoIndexIvfFlatAlgo.ToString(), oldColName, newColName, false)
	if err != nil {
		return nil, err
	}

	oldPhysicalCol := catalog.SystemSI_IVFFLAT_IncludeColPrefix + oldColName
	newPhysicalCol := catalog.SystemSI_IVFFLAT_IncludeColPrefix + newColName
	for _, entriesTable := range entriesTables {
		sqls = append(sqls, fmt.Sprintf(
			"alter table %s rename column %s to %s ; ",
			sqlquote.QualifiedIdent(tableDef.GetDbName(), entriesTable),
			sqlquote.Ident(oldPhysicalCol),
			sqlquote.Ident(newPhysicalCol),
		))
	}
	return sqls, nil
}

func ivfflatEntriesTablesWithIncludedColumn(tableDef *planpb.TableDef, colName string) []string {
	if tableDef == nil {
		return nil
	}

	entriesByIndexName := make(map[string]string)
	for _, indexDef := range tableDef.Indexes {
		if indexDef == nil ||
			!strings.EqualFold(indexDef.IndexAlgo, catalog.MoIndexIvfFlatAlgo.ToString()) ||
			catalog.ToLower(indexDef.IndexAlgoTableType) != catalog.SystemSI_IVFFLAT_TblType_Entries ||
			!planplugin.IncludedColumnAffected(indexDef, colName) ||
			indexDef.IndexTableName == "" {
			continue
		}
		entriesByIndexName[indexDef.IndexName] = indexDef.IndexTableName
	}

	if len(entriesByIndexName) == 0 {
		return nil
	}

	indexNames := make([]string, 0, len(entriesByIndexName))
	for indexName := range entriesByIndexName {
		indexNames = append(indexNames, indexName)
	}
	sort.Strings(indexNames)

	entriesTables := make([]string, 0, len(indexNames))
	for _, indexName := range indexNames {
		entriesTables = append(entriesTables, entriesByIndexName[indexName])
	}
	return entriesTables
}

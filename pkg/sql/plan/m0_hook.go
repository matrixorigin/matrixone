// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type m0PlanHooks struct {
	selectAdd           func(CompilerContext, []tree.AlterTableOption, *TableDef) (bool, error)
	preserveClone       func(*TableDef, *Plan, string, string, bool) (bool, error)
	validateAdd         func(*ColDef) error
	disableCountNotNull bool
}

var activeM0PlanHooks m0PlanHooks

func m0SelectAddColumn(
	ctx CompilerContext,
	options []tree.AlterTableOption,
	tableDef *plan.TableDef,
) (bool, error) {
	if activeM0PlanHooks.selectAdd == nil {
		return false, nil
	}
	return activeM0PlanHooks.selectAdd(ctx, options, tableDef)
}

func m0PreserveCloneLayout(
	source *TableDef,
	createPlan *Plan,
	database, table string,
	temporary bool,
) (bool, error) {
	if activeM0PlanHooks.preserveClone == nil {
		return false, nil
	}
	return activeM0PlanHooks.preserveClone(source, createPlan, database, table, temporary)
}

func m0ValidateBoundAddColumn(col *ColDef) error {
	if activeM0PlanHooks.validateAdd == nil {
		return moerr.NewInternalErrorNoCtxf("M0: metadata-only ADD COLUMN is not enabled")
	}
	return activeM0PlanHooks.validateAdd(col)
}

func M0AppendDomain(table *plan.TableDef) bool {
	if activeM0PlanHooks.selectAdd == nil || table == nil || table.ClusterBy != nil {
		return false
	}
	hasHiddenPK := false
	for _, col := range table.Cols {
		if col != nil && col.Hidden && col.Name == catalog.CPrimaryKeyColName {
			hasHiddenPK = true
			break
		}
	}
	if !hasHiddenPK {
		return false
	}
	if table.Pkey != nil {
		pk := table.Pkey.CompPkeyCol
		if table.Pkey.PkeyColName != catalog.CPrimaryKeyColName ||
			pk != nil && (!pk.Hidden || !pk.Primary || pk.Name != catalog.CPrimaryKeyColName) {
			return false
		}
	}
	return true
}

func M0ResolveColumnPosition(table *plan.TableDef, name string, fallback int) int {
	if activeM0PlanHooks.selectAdd == nil || table == nil || table.Name2ColIndex == nil {
		return fallback
	}
	if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
		name = name[dot+1:]
	}
	if pos, ok := table.Name2ColIndex[strings.ToLower(name)]; ok {
		return int(pos)
	}
	return fallback
}

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

// orderODKUUpdatePhases leaves user assignments in AST order. A change is
// decided before implicit ON UPDATE and its generated dependencies execute.
// Generated columns retain schema dependency order within both partitions.
func orderODKUUpdatePhases(table *plan.TableDef, names []string, exprs []*plan.Expr, explicit, automatic int, autoColumns map[string]bool) ([]string, []*plan.Expr, int, []int32) {
	autoNames := make([]string, 0, len(autoColumns))
	for name := range autoColumns {
		autoNames = append(autoNames, name)
	}
	autoDependent := odkuAffectedColumns(table, autoNames)
	orderedNames := make([]string, 0, len(names))
	orderedExprs := make([]*plan.Expr, 0, len(exprs))
	appendAt := func(i int) {
		orderedNames = append(orderedNames, names[i])
		orderedExprs = append(orderedExprs, exprs[i])
	}
	for i := 0; i < explicit; i++ {
		appendAt(i)
	}
	independent := func(i int) bool {
		col := table.Cols[table.Name2ColIndex[names[i]]]
		return col.GeneratedCol != nil && !autoDependent[col.Name]
	}
	for i := explicit + automatic; i < len(names); i++ {
		if independent(i) {
			appendAt(i)
		}
	}
	changeCount := len(orderedNames)
	for i := explicit; i < explicit+automatic; i++ {
		appendAt(i)
	}
	for i := explicit + automatic; i < len(names); i++ {
		if !independent(i) {
			appendAt(i)
		}
	}
	written := make(map[string]bool, len(names))
	for _, name := range names {
		written[name] = true
	}
	var columns []int32
	for i, col := range table.Cols {
		if !col.Hidden && written[col.Name] && !autoDependent[col.Name] {
			columns = append(columns, int32(i))
		}
	}
	return orderedNames, orderedExprs, changeCount, columns
}

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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// determineGroupByHashKeys records a minimal physical equality key for each
// ordinary aggregate. GroupBy remains unchanged because it defines the logical
// output; GroupByHashKey is only an executor hint proven from table metadata.
func (builder *QueryBuilder) determineGroupByHashKeys(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	for _, childID := range node.Children {
		builder.determineGroupByHashKeys(childID)
	}
	builder.determineGroupByHashKey(node)
}

// determineGroupByHashKey proves the physical equality key for one aggregate.
// Keep this separate from the tree walk because optimizer rewrites can create a
// new aggregate after the initial annotation pass.
func (builder *QueryBuilder) determineGroupByHashKey(node *pbplan.Node) {
	node.GroupByHashKey = nil
	if node.NodeType != pbplan.Node_AGG || len(node.GroupBy) < 2 || hasInactiveGroupingColumn(node.GroupingFlag) {
		return
	}

	// Direct base-table column references are deliberately required here. This
	// keeps the proof local and makes derived expressions remain hash keys.
	groupedColumns := make(map[int32]map[int32]struct{})
	for _, expr := range node.GroupBy {
		if col := expr.GetCol(); col != nil {
			columns := groupedColumns[col.RelPos]
			if columns == nil {
				columns = make(map[int32]struct{})
				groupedColumns[col.RelPos] = columns
			}
			columns[col.ColPos] = struct{}{}
		}
	}

	determinedTables := make(map[int32]map[int32]struct{})
	for tag, grouped := range groupedColumns {
		tableDef := builder.tag2Table[tag]
		pkColumns, ok := primaryKeyColumnPositions(tableDef)
		if !ok {
			continue
		}
		allGrouped := true
		for _, colPos := range pkColumns {
			if _, exists := grouped[colPos]; !exists {
				allGrouped = false
				break
			}
		}
		if allGrouped {
			pkSet := make(map[int32]struct{}, len(pkColumns))
			for _, colPos := range pkColumns {
				pkSet[colPos] = struct{}{}
			}
			determinedTables[tag] = pkSet
		}
	}

	if len(determinedTables) == 0 {
		return
	}

	hashKey := make([]int32, 0, len(node.GroupBy))
	for i, expr := range node.GroupBy {
		col := expr.GetCol()
		if col == nil {
			hashKey = append(hashKey, int32(i))
			continue
		}
		tableDef := builder.tag2Table[col.RelPos]
		pkSet, determined := determinedTables[col.RelPos]
		_, isPrimaryKey := pkSet[col.ColPos]
		if !determined || col.ColPos < 0 || int(col.ColPos) >= len(tableDef.Cols) || isPrimaryKey {
			hashKey = append(hashKey, int32(i))
		}
	}

	// Empty means the legacy "all columns" behavior on the wire. A valid PK
	// proof always leaves at least one key, but keep this guard explicit.
	if len(hashKey) > 0 && len(hashKey) < len(node.GroupBy) {
		node.GroupByHashKey = hashKey
	}
}

func hasInactiveGroupingColumn(flags []bool) bool {
	for _, flag := range flags {
		if !flag {
			return true
		}
	}
	return false
}

func primaryKeyColumnPositions(tableDef *pbplan.TableDef) ([]int32, bool) {
	if tableDef == nil || tableDef.Pkey == nil || tableDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		return nil, false
	}

	pkNames := tableDef.Pkey.Names
	if len(pkNames) == 0 {
		// A hidden composite key does not reveal its user-visible components.
		if tableDef.Pkey.PkeyColName == "" || tableDef.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
			return nil, false
		}
		pkNames = []string{tableDef.Pkey.PkeyColName}
	}

	positions := make([]int32, 0, len(pkNames))
	for _, name := range pkNames {
		pos, ok := tableColumnPosition(tableDef, name)
		if !ok {
			return nil, false
		}
		positions = append(positions, pos)
	}
	return positions, len(positions) > 0
}

func tableColumnPosition(tableDef *pbplan.TableDef, name string) (int32, bool) {
	if pos, ok := tableDef.Name2ColIndex[name]; ok && pos >= 0 && int(pos) < len(tableDef.Cols) {
		return pos, true
	}
	for pos, col := range tableDef.Cols {
		if strings.EqualFold(col.Name, name) {
			return int32(pos), true
		}
	}
	return 0, false
}

func isPhysicalGroupByKey(node *pbplan.Node, groupByPos int) bool {
	if len(node.GroupByHashKey) == 0 {
		return true
	}
	for _, pos := range node.GroupByHashKey {
		if int(pos) == groupByPos {
			return true
		}
	}
	return false
}

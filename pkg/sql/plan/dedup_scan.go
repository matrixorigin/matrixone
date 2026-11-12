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

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

// markDedupTargetScan marks the scan of the table a DEDUP join checks keys against as
// narrow: it reads the key columns of a few rows, so it prefetches those columns and
// never downloads whole objects.
func (builder *QueryBuilder) markDedupTargetScan(nodeID int32) {
	if node := builder.qry.Nodes[nodeID]; node.NodeType == plan.Node_TABLE_SCAN {
		node.NarrowScan = true
	}
}

// readsSubsetOfColumns reports whether a scan of a table with columns all reads only some
// of its visible columns, so it reads column ranges instead of whole objects. Hidden
// columns, as the row id or a composite primary key, do not count: a scan of every visible
// column reads whole objects.
func readsSubsetOfColumns(all, read []*plan.ColDef) bool {
	visible := func(cols []*plan.ColDef) int {
		n := 0
		for _, col := range cols {
			if !col.Hidden {
				n++
			}
		}
		return n
	}
	return visible(read) < visible(all)
}

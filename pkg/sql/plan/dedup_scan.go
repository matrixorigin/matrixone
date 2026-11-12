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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// markDedupTargetScan marks the scan of the table a DEDUP join checks keys against as
// narrow: it reads the key columns of a few rows, so it prefetches those columns and
// never downloads whole objects.
func (builder *QueryBuilder) markDedupTargetScan(nodeID int32) {
	if node := builder.qry.Nodes[nodeID]; node.NodeType == plan.Node_TABLE_SCAN {
		node.NarrowScan = true
	}
}

// scanReadsColumnRanges reports whether the scan of node, which reads the columns read of
// the columns all, should read column ranges instead of whole objects: when it leaves
// unread a payload column, such as a vector, blob or text column, that holds at least half
// of the table's bytes by its stats. Without the column sizes it reads whole objects.
func (builder *QueryBuilder) scanReadsColumnRanges(node *plan.Node, all, read []*plan.ColDef) bool {
	if !readsSubsetOfColumns(all, read) || builder.compCtx == nil {
		return false
	}
	w := builder.getStatsInfoByTableID(node.TableDef.GetTblId())
	if w == nil || w.GetStats() == nil {
		return false
	}
	stats := w.GetStats()
	total, ok := completeStatsSizeMap(stats, &plan.TableDef{Cols: all})
	if !ok || total == 0 {
		return false
	}
	readNames := make(map[string]struct{}, len(read))
	for _, col := range read {
		readNames[col.Name] = struct{}{}
	}
	for _, col := range all {
		if _, isRead := readNames[col.Name]; isRead || col.Hidden || !isPayloadType(types.T(col.Typ.Id)) {
			continue
		}
		if 2*stats.SizeMap[col.Name] >= total {
			return true
		}
	}
	return false
}

// isPayloadType reports whether a column of type t holds large values: vectors, blobs,
// text or JSON documents.
func isPayloadType(t types.T) bool {
	switch t {
	case types.T_blob, types.T_text, types.T_json:
		return true
	}
	return t.IsArray()
}

// readsSubsetOfColumns reports whether a scan of a table with columns all reads only some
// of its visible columns. Hidden columns, as the row id or a composite primary key, do not
// count: a scan of every visible column reads the whole object.
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

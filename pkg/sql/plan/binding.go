// Copyright 2022 Matrix Origin
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

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// initSourceColumnProvenance gives physical scans and sequential UPDATE row
// images the same immutable column metadata. Values may change; defaults do not.
func (b *Binding) initSourceColumnProvenance(columns []*plan.ColDef, snapshots []SourceColumnMetadata) {
	b.outputColumnProvenance = make([]OutputColumnProvenance, len(columns))
	for i, col := range columns {
		var metadata SourceColumnMetadata
		if snapshots == nil {
			metadata = snapshotSourceColumnMetadata(col)
		} else {
			metadata = snapshots[i]
		}
		b.outputColumnProvenance[i] = OutputColumnProvenance{
			State: ProvenanceSingleSource, CTASDefaultPolicy: CTASDefaultInheritSource,
			Source: &SourceColumn{RelPos: b.tag, ColPos: int32(i), TableID: b.tableID,
				Metadata: metadata},
		}
	}
}

func (b *Binding) FindColumn(col string) int32 {
	if id, ok := b.colIdByName[col]; ok {
		return id
	}

	return NotFound
}

func NewBinding(tag, nodeID int32, db, table string, tableID uint64, cols []string, colIsHidden []bool, types []*plan.Type, isClusterTable bool, defaultVals []string) *Binding {
	binding := &Binding{
		tag:            tag,
		nodeId:         nodeID,
		db:             db,
		table:          table,
		tableID:        tableID,
		cols:           cols,
		originCols:     cols,
		colIsHidden:    colIsHidden,
		types:          types,
		isClusterTable: isClusterTable,
		defaults:       defaultVals,
	}

	binding.colIdByName = make(map[string]int32)
	for i, col := range cols {
		if _, ok := binding.colIdByName[col]; ok {
			binding.colIdByName[col] = AmbiguousName
		} else {
			binding.colIdByName[col] = int32(i)
		}
	}

	return binding
}

// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"

var _ Policy = (*Basic)(nil)

type Basic struct {
	id            uint64
	tableName     string
	objectMinRows int
	objHeap       *heapBuilder[*catalog.SegmentEntry]
}

func NewBasicPolicy() *Basic {
	return &Basic{
		objHeap: &heapBuilder[*catalog.SegmentEntry]{
			items: make(itemSet[*catalog.SegmentEntry], 2),
			cap:   2,
		},
	}
}

// impl Policy for Basic
func (o *Basic) OnObject(obj *catalog.SegmentEntry) {
	rowsLeftOnSeg := obj.Stat.Rows - obj.Stat.Dels
	// it has too few rows, merge it
	iscandidate := func() bool {
		if rowsLeftOnSeg < o.objectMinRows {
			return true
		}
		if rowsLeftOnSeg < obj.Stat.Rows/2 {
			return true
		}
		return false
	}

	if iscandidate() {
		o.objHeap.push(&mItem[*catalog.SegmentEntry]{
			row:   rowsLeftOnSeg,
			entry: obj,
		})
	}
}

func (o *Basic) Revise(cpu, mem float64) []*catalog.SegmentEntry {
	return o.objHeap.finish()
}

func (o *Basic) ResetForTable(id uint64, schema *catalog.Schema) {
	o.id = id
	o.tableName = schema.Name
	o.objectMinRows = determineObjectMinRows(int(schema.SegmentMaxBlocks), int(schema.BlockMaxRows))
	o.objHeap.reset()
}

func determineObjectMinRows(segMaxBlks, blkMaxRows int) int {
	// the max rows of a full object
	objectFullRows := segMaxBlks * blkMaxRows
	// we want every object has at least 5 blks rows
	objectMinRows := constMergeMinBlks * blkMaxRows
	if objectFullRows < objectMinRows { // for small config in unit test
		return objectFullRows
	}
	return objectMinRows
}

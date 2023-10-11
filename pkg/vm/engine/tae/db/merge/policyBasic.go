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

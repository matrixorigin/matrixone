package jobs

import (
	"bytes"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

var MergeBlocksTaskFactory = func(metas []*catalog.BlockEntry) tasks.TxnTaskFactory {
	return func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return NewMergeBlocksTask(ctx, txn, metas)
	}
}

type mergeBlocksTask struct {
	*tasks.BaseTask
	txn       txnif.AsyncTxn
	metas     []*catalog.BlockEntry
	compacted []handle.Block
	created   []handle.Block
	newSeg    handle.Segment
}

func NewMergeBlocksTask(ctx *tasks.Context, txn txnif.AsyncTxn, metas []*catalog.BlockEntry) (task *mergeBlocksTask, err error) {
	task = &mergeBlocksTask{
		txn:       txn,
		metas:     metas,
		created:   make([]handle.Block, 0),
		compacted: make([]handle.Block, 0),
	}
	dbName := metas[0].GetSegment().GetTable().GetDB().GetName()
	database, err := txn.GetDatabase(dbName)
	if err != nil {
		return
	}
	relName := metas[0].GetSchema().Name
	rel, err := database.GetRelationByName(relName)
	if err != nil {
		return
	}
	for _, meta := range metas {
		seg, err := rel.GetSegment(meta.GetSegment().GetID())
		if err != nil {
			return nil, err
		}
		blk, err := seg.GetBlock(meta.GetID())
		if err != nil {
			return nil, err
		}
		task.compacted = append(task.compacted, blk)
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.MergeBlocksTask, ctx)
	return
}

func (task *mergeBlocksTask) mergeColumn(vecs []*vector.Vector, sortedIdx *[]uint32, isPrimary bool, fromLayout, toLayout []uint32) (column []*vector.Vector, mapping []uint32) {
	if isPrimary {
		column, mapping = mergesort.MergeSortedColumn(vecs, sortedIdx, fromLayout, toLayout)
	} else {
		column = mergesort.ShuffleColumn(vecs, *sortedIdx, fromLayout, toLayout)
	}
	return
}

func (task *mergeBlocksTask) Execute() (err error) {
	// 1. Get total rows of all blocks to be compacted: 10000
	// 2. Decide created blocks layout: []int{3000,3000,3000,1000}
	// 3. Merge sort blocks and split it into created blocks
	// 4. Record all mappings: []int
	// 5. PrepareMergeBlock(mappings)
	schema := task.metas[0].GetSchema()
	var deletes *roaring.Bitmap
	var compressed bytes.Buffer
	var decompressed bytes.Buffer
	var vec *vector.Vector
	vecs := make([]*vector.Vector, 0)
	rows := make([]uint32, len(task.compacted))
	length := 0
	for i, block := range task.compacted {
		if vec, deletes, err = block.GetColumnDataById(int(schema.PrimaryKey), &compressed, &decompressed); err != nil {
			return
		}
		vec = compute.ApplyDeleteToVector(vec, deletes)
		vecs = append(vecs, vec)
		rows[i] = uint32(gvec.Length(vec))
		length += vector.Length(vec)
		seg := block.GetSegment()
		created, err := seg.CreateNonAppendableBlock()
		if err != nil {
			return err
		}
		task.created = append(task.created, created)
	}
	to := make([]uint32, 0)
	maxrow := task.compacted[0].GetMeta().(*catalog.BlockEntry).GetSchema().BlockMaxRows
	totalRows := length
	for totalRows > 0 {
		if totalRows > int(maxrow) {
			to = append(to, maxrow)
			totalRows -= int(maxrow)
		} else {
			to = append(to, uint32(totalRows))
			break
		}
	}

	node := common.GPool.Alloc(uint64(length * 4))
	buf := node.Buf[:length]
	defer common.GPool.Free(node)
	sortedIdx := *(*[]uint32)(unsafe.Pointer(&buf))
	vecs, mapping := task.mergeColumn(vecs, &sortedIdx, true, rows, to)
	logutil.Infof("mapping is %v", mapping)
	for i, vec := range vecs {
		created := task.created[i]
		bf := created.GetMeta().(*catalog.BlockEntry).GetBlockData().GetBlockFile()
		if bf.WriteColumnVec(task.txn.GetStartTS(), int(schema.PrimaryKey), vec); err != nil {
			return
		}
	}

	for i := 0; i < len(schema.ColDefs); i++ {
		if i == int(schema.PrimaryKey) {
			continue
		}
		vecs = vecs[:0]
		compressed.Reset()
		decompressed.Reset()
		for _, block := range task.compacted {
			if vec, deletes, err = block.GetColumnDataById(i, &compressed, &decompressed); err != nil {
				return
			}
			vec = compute.ApplyDeleteToVector(vec, deletes)
			vecs = append(vecs, vec)
		}
		vecs, _ = task.mergeColumn(vecs, &sortedIdx, false, rows, to)
		for pos, vec := range vecs {
			created := task.created[pos]
			bf := created.GetMeta().(*catalog.BlockEntry).GetBlockData().GetBlockFile()
			if bf.WriteColumnVec(task.txn.GetStartTS(), i, vec); err != nil {
				return
			}
		}
	}
	for i, created := range task.created {
		bf := created.GetMeta().(*catalog.BlockEntry).GetBlockData().GetBlockFile()
		bf.WriteTS(task.txn.GetStartTS())
		bf.WriteRows(rows[i])
	}
	for _, compacted := range task.compacted {
		seg := compacted.GetSegment()
		if err = seg.SoftDeleteBlock(compacted.Fingerprint().BlockID); err != nil {
			return
		}
	}

	return
}

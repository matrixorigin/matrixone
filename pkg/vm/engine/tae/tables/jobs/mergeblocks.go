package jobs

import (
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/txnentries"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

var MergeBlocksTaskFactory = func(metas []*catalog.BlockEntry, segMeta *catalog.SegmentEntry, scheduler tasks.TaskScheduler) tasks.TxnTaskFactory {
	return func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return NewMergeBlocksTask(ctx, txn, metas, segMeta, scheduler)
	}
}

type mergeBlocksTask struct {
	*tasks.BaseTask
	txn       txnif.AsyncTxn
	segMeta   *catalog.SegmentEntry
	metas     []*catalog.BlockEntry
	compacted []handle.Block
	created   []handle.Block
	rel       handle.Relation
	newSeg    handle.Segment
	scheduler tasks.TaskScheduler
	scopes    []common.ID
}

func NewMergeBlocksTask(ctx *tasks.Context, txn txnif.AsyncTxn, metas []*catalog.BlockEntry, segMeta *catalog.SegmentEntry, scheduler tasks.TaskScheduler) (task *mergeBlocksTask, err error) {
	task = &mergeBlocksTask{
		txn:       txn,
		metas:     metas,
		created:   make([]handle.Block, 0),
		compacted: make([]handle.Block, 0),
		scheduler: scheduler,
		segMeta:   segMeta,
	}
	dbName := metas[0].GetSegment().GetTable().GetDB().GetName()
	database, err := txn.GetDatabase(dbName)
	if err != nil {
		return
	}
	relName := metas[0].GetSchema().Name
	task.rel, err = database.GetRelationByName(relName)
	if err != nil {
		return
	}
	for _, meta := range metas {
		seg, err := task.rel.GetSegment(meta.GetSegment().GetID())
		if err != nil {
			return nil, err
		}
		blk, err := seg.GetBlock(meta.GetID())
		if err != nil {
			return nil, err
		}
		task.compacted = append(task.compacted, blk)
		task.scopes = append(task.scopes, *meta.AsCommonID())
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.DataCompactionTask, ctx)
	return
}

func (task *mergeBlocksTask) Scopes() []common.ID { return task.scopes }

func (task *mergeBlocksTask) mergeColumn(vecs []*vector.Vector, sortedIdx *[]uint32, isPrimary bool, fromLayout, toLayout []uint32) (column []*vector.Vector, mapping []uint32) {
	if isPrimary {
		column, mapping = mergesort.MergeSortedColumn(vecs, sortedIdx, fromLayout, toLayout)
	} else {
		column = mergesort.ShuffleColumn(vecs, *sortedIdx, fromLayout, toLayout)
	}
	return
}

func (task *mergeBlocksTask) Execute() (err error) {
	var targetSeg handle.Segment
	if task.segMeta == nil {
		if targetSeg, err = task.rel.CreateNonAppendableSegment(); err != nil {
			return err
		}
		task.segMeta = targetSeg.GetMeta().(*catalog.SegmentEntry)
	} else {
		if targetSeg, err = task.rel.GetSegment(task.segMeta.GetID()); err != nil {
			return
		}
	}

	schema := task.metas[0].GetSchema()
	var deletes *roaring.Bitmap
	var vec *vector.Vector
	vecs := make([]*vector.Vector, 0)
	rows := make([]uint32, len(task.compacted))
	length := 0
	var fromAddr []uint32
	var toAddr []uint32
	ids := make([]*common.ID, 0, len(task.compacted))
	for i, block := range task.compacted {
		if vec, deletes, err = block.GetColumnDataById(int(schema.PrimaryKey), nil, nil); err != nil {
			return
		}
		vec = compute.ApplyDeleteToVector(vec, deletes)
		vecs = append(vecs, vec)
		rows[i] = uint32(gvec.Length(vec))
		fromAddr = append(fromAddr, uint32(length))
		length += vector.Length(vec)
		ids = append(ids, block.Fingerprint())
	}
	to := make([]uint32, 0)
	maxrow := schema.BlockMaxRows
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
	// logutil.Infof("mapping is %v", mapping)
	// logutil.Infof("sortedIdx is %v", sortedIdx)
	ts := task.txn.GetStartTS()
	var flushTask tasks.Task
	length = 0
	var blk handle.Block
	for _, vec := range vecs {
		toAddr = append(toAddr, uint32(length))
		length += gvec.Length(vec)
		blk, err = targetSeg.CreateNonAppendableBlock()
		if err != nil {
			return err
		}
		task.created = append(task.created, blk)
		meta := blk.GetMeta().(*catalog.BlockEntry)
		closure := meta.GetBlockData().FlushColumnDataClosure(ts, int(schema.PrimaryKey), vec, false)
		flushTask, err = task.scheduler.ScheduleScopedFn(tasks.WaitableCtx, tasks.IOTask, meta.AsCommonID(), closure)
		if err != nil {
			return
		}
		if err = flushTask.WaitDone(); err != nil {
			return
		}
		// bf := blk.GetMeta().(*catalog.BlockEntry).GetBlockData().GetBlockFile()
		// if bf.WriteColumnVec(task.txn.GetStartTS(), int(schema.PrimaryKey), vec); err != nil {
		// 	return
		// }
	}

	for i := 0; i < len(schema.ColDefs); i++ {
		if i == int(schema.PrimaryKey) {
			continue
		}
		vecs = vecs[:0]
		for _, block := range task.compacted {
			if vec, deletes, err = block.GetColumnDataById(i, nil, nil); err != nil {
				return
			}
			vec = compute.ApplyDeleteToVector(vec, deletes)
			vecs = append(vecs, vec)
		}
		vecs, _ = task.mergeColumn(vecs, &sortedIdx, false, rows, to)
		for pos, vec := range vecs {
			created := task.created[pos]
			closure := created.GetMeta().(*catalog.BlockEntry).GetBlockData().FlushColumnDataClosure(ts, i, vec, false)
			flushTask, err = task.scheduler.ScheduleScopedFn(tasks.WaitableCtx, tasks.IOTask, created.Fingerprint(), closure)
			if err != nil {
				return
			}
			if err = flushTask.WaitDone(); err != nil {
				return
			}
		}
	}
	for i, created := range task.created {
		closure := created.GetMeta().(*catalog.BlockEntry).GetBlockData().SyncBlockDataClosure(ts, rows[i])
		flushTask, err = task.scheduler.ScheduleScopedFn(tasks.WaitableCtx, tasks.IOTask, created.Fingerprint(), closure)
		if err != nil {
			return
		}
		if err = flushTask.WaitDone(); err != nil {
			return
		}
	}
	for _, compacted := range task.compacted {
		seg := compacted.GetSegment()
		if err = seg.SoftDeleteBlock(compacted.Fingerprint().BlockID); err != nil {
			return
		}
	}

	txnEntry := txnentries.NewMergeBlocksEntry(task.txn, task.compacted, task.created, mapping, fromAddr, toAddr, task.scheduler)
	if err = task.txn.LogTxnEntry(task.segMeta.GetTable().GetID(), txnEntry, ids); err != nil {
		return
	}

	return
}

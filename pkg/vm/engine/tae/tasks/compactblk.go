package tasks

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type compactBlockTask struct {
	txn      txnif.AsyncTxn
	block    handle.Block
	newBlock handle.Block
	meta     *catalog.BlockEntry
}

func NewCompactBlockTask(txn txnif.AsyncTxn, block handle.Block) *compactBlockTask {
	meta := block.GetMeta().(*catalog.BlockEntry)
	return &compactBlockTask{
		txn:   txn,
		block: block,
		meta:  meta,
	}
}

func (task *compactBlockTask) PrepareData() (bat *batch.Batch, err error) {
	attrs := task.meta.GetSchema().Attrs()
	bat = batch.New(true, attrs)

	for i, colDef := range task.meta.GetSchema().ColDefs {
		// var comp bytes.Buffer
		// var decomp bytes.Buffer
		vec, mask, err := task.block.GetVectorCopy(colDef.Name, nil, nil)
		if err != nil {
			return bat, err
		}
		vec = compute.ApplyDeleteToVector(vec, mask)
		bat.Vecs[i] = vec
	}
	if err = mergesort.SortBlockColumns(bat.Vecs, int(task.meta.GetSchema().PrimaryKey)); err != nil {
		return
	}
	return
}

func (task *compactBlockTask) GetNewBlock() handle.Block { return task.newBlock }

func (task *compactBlockTask) OnExecute() (err error) {
	data, err := task.PrepareData()
	if err != nil {
		return
	}
	seg := task.block.GetSegment()
	rel := seg.GetRelation()
	newBlk, err := seg.CreateNonAppendableBlock()
	if err != nil {
		return err
	}
	if err = seg.SoftDeleteBlock(task.block.Fingerprint().BlockID); err != nil {
		return err
	}
	newBlkData := newBlk.GetMeta().(*catalog.BlockEntry).GetBlockData()
	blockFile := newBlkData.GetBlockFile()
	if err = blockFile.WriteBatch(data, task.txn.GetStartTS()); err != nil {
		return
	}
	if err = rel.PrepareCompactBlock(task.block.Fingerprint(), newBlk.Fingerprint()); err != nil {
		return
	}
	task.newBlock = newBlk
	return
}

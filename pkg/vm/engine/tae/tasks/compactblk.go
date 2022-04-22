package tasks

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type compactBlockTask struct {
	txn   txnif.AsyncTxn
	block handle.Block
}

func NewCompactBlockTask(txn txnif.AsyncTxn, block handle.Block) *compactBlockTask {
	return &compactBlockTask{
		txn:   txn,
		block: block,
	}
}

func (task *compactBlockTask) PrepareData() (bat *batch.Batch, err error) {
	meta := task.block.GetMeta().(*catalog.BlockEntry)
	attrs := meta.GetSchema().Attrs()
	bat = batch.New(true, attrs)

	for i, colDef := range meta.GetSchema().ColDefs {
		var comp bytes.Buffer
		var decomp bytes.Buffer
		vec, mask, err := task.block.GetVectorCopy(colDef.Name, &comp, &decomp)
		if err != nil {
			return bat, err
		}
		vec = compute.ApplyDeleteToVector(vec, mask)
		bat.Vecs[i] = vec
	}
	if err = mergesort.SortBlockColumns(bat.Vecs, int(meta.GetSchema().PrimaryKey)); err != nil {
		return
	}
	return
}

func (task *compactBlockTask) OnExecute() (err error) {
	return
	// bat, err := task.PrepareData()
	// if err != nil {
	// 	return
	// }
	// return
}

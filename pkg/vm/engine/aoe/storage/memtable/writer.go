package memtable

import (
	"matrixone/pkg/container/vector"
	"matrixone/pkg/logutil"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
)

type MemtableWriter struct {
	Opts     *e.Options
	Dirname  string
	Memtable imem.IMemTable
}

func (sw *MemtableWriter) Flush() (err error) {
	mt := sw.Memtable.(*MemTable)
	bat := mt.Block.GetFullBatch()
	defer bat.Close()
	var vecs []*vector.Vector
	for idx, _ := range bat.GetAttrs() {
		node := bat.GetVectorByAttr(idx)
		vecs = append(vecs, node.CopyToVector())
	}
	bw := dataio.NewBlockWriter(vecs, mt.Meta, sw.Dirname)
	bw.SetPreExecutor(func() {
		logutil.Infof(" %s | Memtable | Flushing", bw.GetFileName())
	})
	bw.SetPostExecutor(func() {
		logutil.Infof(" %s | Memtable | Flushed", bw.GetFileName())
	})
	return bw.Execute()
}

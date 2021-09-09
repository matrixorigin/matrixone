package memtable

import (
	"matrixone/pkg/container/vector"
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
)

type memTableWriter struct {
	memTable *memTable
}

func (mw *memTableWriter) Flush() (err error) {
	bat := mw.memTable.iblk.GetFullBatch()
	defer bat.Close()
	var vecs []*vector.Vector
	for idx, _ := range bat.GetAttrs() {
		node := bat.GetVectorByAttr(idx)
		vecs = append(vecs, node.CopyToVector())
	}
	bw := dataio.NewBlockWriter(vecs, mw.memTable.meta, mw.memTable.meta.Segment.Table.Conf.Dir)
	bw.SetPreExecutor(func() {
		logutil.Infof(" %s | memTable | Flushing", bw.GetFileName())
	})
	bw.SetPostExecutor(func() {
		logutil.Infof(" %s | memTable | Flushed", bw.GetFileName())
	})
	return bw.Execute()
}

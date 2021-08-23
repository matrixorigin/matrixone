package memtable

import (
	"context"
	"matrixone/pkg/container/vector"
	logutil2 "matrixone/pkg/logutil"
	e "matrixone/pkg/vm/engine/aoe/storage"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
)

const (
	MEMTABLE_WRITER = "MW"
)

func init() {
	dio.WRITER_FACTORY.RegisterBuilder(MEMTABLE_WRITER, &MemtableWriterBuilder{})
}

type MemtableWriterBuilder struct {
}

func (b *MemtableWriterBuilder) Build(wf ioif.IWriterFactory, ctx context.Context) ioif.Writer {
	mt := ctx.Value("memtable").(imem.IMemTable)
	if mt == nil {
		panic("logic error")
	}
	w := &MemtableWriter{
		Opts:     wf.GetOpts(),
		Dirname:  wf.GetDir(),
		Memtable: mt,
	}
	return w
}

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
		logutil2.Debugf(" %s | Memtable | Flushing", bw.GetFileName())
	})
	bw.SetPostExecutor(func() {
		logutil2.Debugf(" %s | Memtable | Flushed", bw.GetFileName())
	})
	return bw.Execute()
}

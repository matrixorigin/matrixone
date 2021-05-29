package memtable

import (
	"context"
	"fmt"
	e "matrixone/pkg/vm/engine/aoe/storage"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	// "os"
	// "io"
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

func MakeMemtableFileName(id *layout.ID) string {
	return fmt.Sprintf("%d_%d_%d_%d", id.TableID, id.SegmentID, id.BlockID, id.PartID)
}

func (sw *MemtableWriter) Flush() (err error) {
	return nil
}

// func (sw *MemtableWriter) Write(obj interface{}) (err error) {
// 	data := obj.()

// 	// log.Infof("PreCommit CheckPoint: %s", fname)
// 	fname := e.MakeFilename(sw.Dirname, e.FTSpillMemory, MakeSpillFileName(&sw.ID), false)
// 	w, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE, 0666)
// 	if err != nil {
// 		return err
// 	}
// 	_, err = w.Write(data)
// 	if err != nil {
// 		return err
// 	}
// 	return err
// }

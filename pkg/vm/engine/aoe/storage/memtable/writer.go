package memtable

import (
	"context"
	"encoding/binary"
	log "github.com/sirupsen/logrus"
	"matrixone/pkg/container/types"
	e "matrixone/pkg/vm/engine/aoe/storage"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	"os"
	"path/filepath"
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
	id := sw.Memtable.GetID()
	fname := e.MakeFilename(sw.Dirname, e.FTBlock, id.ToBlockFileName(), false)
	log.Infof("Flushing memtable: %s", fname)
	dir := filepath.Dir(fname)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
	}
	if err != nil {
		return err
	}
	w, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer w.Close()

	mt := sw.Memtable.(*MemTable)

	zmIndexes := []index.Index{}

	// TODO: Here mock zonemap index for test
	mtTypes := mt.Meta.Segment.Schema.Types()
	for idx, ctype := range mtTypes {
		if ctype.Oid == types.T_int32 {
			minv := int32(1) + int32(idx)*100
			maxv := int32(99) + int32(idx)*100
			zm := index.NewZoneMap(ctype, minv, maxv, int16(idx))
			zmIndexes = append(zmIndexes, zm)
		}
	}

	ibuf, err := index.DefaultRWHelper.WriteIndexes(zmIndexes)
	if err != nil {
		return err
	}
	_, err = w.Write(ibuf)
	if err != nil {
		return err
	}

	buf := make([]byte, 2+len(mtTypes)*8*2)
	binary.BigEndian.PutUint16(buf, uint16(len(mt.Meta.Segment.Schema.ColDefs)))
	for idx, t := range mtTypes {
		colSize := mt.Meta.Count * uint64(t.Size)
		binary.BigEndian.PutUint64(buf[2+idx*16:], id.BlockID)
		binary.BigEndian.PutUint64(buf[2+idx*16+8:], colSize)
	}
	_, err = w.Write(buf)
	if err != nil {
		return err
	}
	handle := mt.Block.GetBlockHandle()
	defer handle.Close()
	for idx := 0; idx < handle.Cols(); idx++ {
		_, err = handle.GetPageNode(idx, 0).DataNode.WriteTo(w)
		if err != nil {
			return err
		}
	}
	log.Infof("%s Flushed", fname)
	return err
}

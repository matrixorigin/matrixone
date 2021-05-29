package memtable

import (
	"context"
	"encoding/binary"
	e "matrixone/pkg/vm/engine/aoe/storage"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	"os"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"
	// "matrixone/pkg/vm/engine/aoe/storage/layout"
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

	buf := make([]byte, 2+len(mt.Types)*8)
	binary.BigEndian.PutUint16(buf, uint16(len(mt.Columns)))
	for idx, t := range mt.Types {
		colSize := mt.Meta.Count * uint64(t.Size)
		binary.BigEndian.PutUint64(buf[2+idx*8:], colSize)
	}
	_, err = w.Write(buf)
	if err != nil {
		return err
	}
	for _, colBlk := range mt.Columns {
		cursor := col.ScanCursor{}
		err := colBlk.InitScanCursor(&cursor)
		if err != nil {
			return err
		}
		for {
			err = cursor.Init()
			if err == nil {
				break
			}
			time.Sleep(time.Duration(1) * time.Millisecond)
		}
		defer cursor.Close()
		if err != nil {
			return err
		}
		_, err = w.Write(cursor.Current.GetBuf())
		if err != nil {
			return err
		}
	}
	log.Infof("%s Flushed", fname)
	return err
}

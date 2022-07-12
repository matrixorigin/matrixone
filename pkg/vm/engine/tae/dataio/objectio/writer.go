package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/tfs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl/adaptors"
	"os"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type Writer struct {
	fs    tfs.FS
	block blockFile
}

func NewWriter(fs tfs.FS) *Writer {
	return &Writer{
		fs: fs,
	}
}

func (w *Writer) WriteABlkColumn(
	version uint64,
	id *common.ID,
	column containers.Vector) (info common.FileInfo, err error) {
	name := EncodeColBlkNameWithVersion(id, version, w.fs)
	f, err := w.fs.OpenFile(name, os.O_CREATE)
	if err != nil {
		return
	}
	defer f.Close()
	info = f.Stat()
	writerBuffer := adaptors.NewBuffer(nil)
	defer writerBuffer.Close()
	if _, err = column.WriteTo(writerBuffer); err != nil {
		return
	}
	if _, err = f.Write(writerBuffer.Bytes()); err != nil {
		return
	}
	return
}

func (w *Writer) WriteABlkColumns(
	version uint64,
	id *common.ID,
	columns *containers.Batch) (infos []common.FileInfo, err error) {
	var info common.FileInfo
	for colIdx := range columns.Attrs {
		colId := *id
		colId.Idx = uint16(colIdx)
		if info, err = w.WriteABlkColumn(version, &colId, columns.Vecs[colIdx]); err != nil {
			return
		}
		infos = append(infos, info)
	}
	return
}

func (w *Writer) WriteBlockColumn(
	version uint64,
	id *common.ID,
	column *vector.Vector) (err error) {
	name := EncodeColBlkNameWithVersion(id, version, w.fs)
	f, err := w.fs.OpenFile(name, os.O_CREATE)
	if err != nil {
		return
	}
	defer f.Close()
	buf, err := column.Show()
	if err != nil {
		return
	}
	_, err = f.Write(buf)
	return
}

func (w *Writer) WriteUpdates(
	version uint64,
	id *common.ID,
	data []byte) (err error) {
	name := EncodeUpdateNameWithVersion(id, version, w.fs)
	f, err := w.fs.OpenFile(name, os.O_CREATE)
	if err != nil {
		return
	}
	defer f.Close()
	_, err = f.Write(data)
	return
}

func (w *Writer) WriteDeletes(
	version uint64,
	id *common.ID,
	data []byte) (err error) {
	name := EncodeDeleteNameWithVersion(id, version, w.fs)
	f, err := w.fs.OpenFile(name, os.O_CREATE)
	if err != nil {
		return
	}
	defer f.Close()
	_, err = f.Write(data)
	return
}

func (w *Writer) WriteIndexMeta(
	id *common.ID,
	data []byte) (err error) {
	name := EncodeMetaIndexName(id, w.fs)
	f, err := w.fs.OpenFile(name, os.O_CREATE)
	if err != nil {
		return
	}
	defer f.Close()
	_, err = f.Write(data)
	return
}

func (w *Writer) WriteIndex(
	id *common.ID,
	idx int,
	data []byte) (err error) {
	name := EncodeIndexName(id, idx, w.fs)
	f, err := w.fs.OpenFile(name, os.O_CREATE)
	if err != nil {
		return
	}
	defer f.Close()
	_, err = f.Write(data)
	return
}

func (w *Writer) WriteData(
	version uint64,
	id *common.ID,
	data []byte) (err error) {
	name := EncodeColBlkNameWithVersion(id, version, w.fs)
	f, err := w.fs.OpenFile(name, os.O_CREATE)
	if err != nil {
		return
	}
	defer f.Close()
	_, err = f.Write(data)
	return
}

func (w *Writer) WriteZonemapIndexFromSource(
	version uint64,
	cols []int,
	indexT catalog.IndexT,
	id *common.ID,
	source *vector.Vector) (err error) {
	// TODO
	return
}

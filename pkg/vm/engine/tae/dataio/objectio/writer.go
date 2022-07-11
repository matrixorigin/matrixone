package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/tfs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl/adaptors"
	"io/fs"
	"os"

	movec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type Writer struct {
	fs tfs.FS
}

func NewWriter(fs tfs.FS) *Writer {
	return &Writer{
		fs: fs,
	}
}

func (w *Writer) WriteABlkColumn(
	version uint64,
	id *common.ID,
	column containers.Vector) (info fs.FileInfo, err error) {
	name := EncodeColBlkNameWithVersion(id, version, w.fs)
	f, err := w.fs.OpenFile(name, os.O_CREATE)
	if err != nil {
		return
	}
	defer f.Close()
	if info, err = f.Stat(); err != nil {
		return
	}
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
	columns *containers.Batch) (infos []fs.FileInfo, err error) {
	var info fs.FileInfo
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
	column *movec.Vector,
) (err error) {
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
	view *model.BlockView) (err error) {
	// TODO
	return
}

func (w *Writer) WriteDeletes(
	version uint64,
	id *common.ID,
	view *model.BlockView) (err error) {
	// TODO
	return
}

func (w *Writer) WriteZonemapIndexFromSource(
	version uint64,
	cols []int,
	indexT catalog.IndexT,
	id *common.ID,
	source *movec.Vector) (err error) {
	// TODO
	return
}

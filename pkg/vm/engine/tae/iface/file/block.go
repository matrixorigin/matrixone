package file

import (
	"io"

	"github.com/RoaringBitmap/roaring"
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
)

type Block interface {
	Base
	Sync() error
	// IsAppendable() bool
	WriteTS(ts uint64) error
	ReadTS() (uint64, error)
	WriteRows(rows uint32) error
	ReadRows() uint32

	// OpenDeletesFile() common.IRWFile
	WriteDeletes(buf []byte) error
	ReadDeletes(buf []byte) error

	OpenColumn(colIdx int) (ColumnBlock, error)
	// WriteColumn(colIdx int, ts uint64, data []byte, updates []byte) (common.IVFile, error)

	// TODO: Remove later
	LoadIBatch(colTypes []types.Type, maxRow uint32) (bat batch.IBatch, err error)
	WriteIBatch(bat batch.IBatch, ts uint64, masks map[uint16]*roaring.Bitmap, vals map[uint16]map[uint32]interface{}, deletes *roaring.Bitmap) error
	WriteBatch(bat *gbat.Batch, ts uint64) error
	LoadBatch(attrs []string, colTypes []types.Type) (bat *gbat.Batch, err error)
}

type ColumnBlock interface {
	io.Closer
	WriteTS(ts uint64) error
	WriteData(buf []byte) error
	WriteIndex(idx int, buf []byte) error
	WriteUpdates(buf []byte) error

	ReadTS() uint64
	ReadData(buf []byte) error
	ReadIndex(idx int, buf []byte) error
	ReadUpdates(buf []byte) error

	GetDataFileStat() common.FileInfo

	OpenUpdateFile() (common.IRWFile, error)
	OpenIndexFile(idx int) (common.IRWFile, error)
	OpenDataFile() (common.IRWFile, error)
}

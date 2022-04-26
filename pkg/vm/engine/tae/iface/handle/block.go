package handle

import (
	"bytes"
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type BlockIt interface {
	Iterator
	GetBlock() Block
}

type FilterOp int16

const (
	FilterEq FilterOp = iota
	FilterBatchEq
	FilterBtw
)

type Filter struct {
	Op  FilterOp
	Col *vector.Vector
	Val interface{}
}

type BlockReader interface {
	io.Closer
	ID() uint64
	String() string
	GetByFilter(filter Filter) (uint32, error)
	GetVectorCopy(string, *bytes.Buffer, *bytes.Buffer) (*vector.Vector, *roaring.Bitmap, error)
	GetVectorCopyById(int, *bytes.Buffer, *bytes.Buffer) (*vector.Vector, *roaring.Bitmap, error)
	GetMeta() interface{}
	Fingerprint() *common.ID
	Rows() int
	BatchDedup(col *vector.Vector) error

	IsAppendableBlock() bool
	// IsAppendable() bool

	PrepareCompact() error

	GetSegment() Segment

	GetTotalChanges() int
}

type BlockWriter interface {
	io.Closer
	String() string
	Append(data *batch.Batch, offset uint32) (uint32, error)
	Update(row uint32, col uint16, v interface{}) error
	RangeDelete(start, end uint32) error

	PushDeleteOp(filter Filter) error
	PushUpdateOp(filter Filter, attr string, val interface{}) error
}

type Block interface {
	BlockReader
	BlockWriter
}

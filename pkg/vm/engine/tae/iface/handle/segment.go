package handle

import (
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type SegmentIt interface {
	Iterator
	GetSegment() Segment
}

type SegmentReader interface {
	io.Closer
	GetID() uint64
	MakeBlockIt() BlockIt
	MakeReader() Reader
	GetByFilter(filter Filter, offsetOnly bool) (map[uint64]*batch.Batch, error)
	String() string
	GetMeta() interface{}

	BatchDedup(col *vector.Vector) error
}

type SegmentWriter interface {
	io.Closer
	String() string
	Append(data *batch.Batch, offset uint32) (uint32, error)
	Update(blk uint64, row uint32, col uint16, v interface{}) error
	RangeDelete(blk uint64, start, end uint32) error

	PushDeleteOp(filter Filter) error
	PushUpdateOp(filter Filter, attr string, val interface{}) error

	CreateBlock() (Block, error)
}

type Segment interface {
	SegmentReader
	SegmentWriter
}

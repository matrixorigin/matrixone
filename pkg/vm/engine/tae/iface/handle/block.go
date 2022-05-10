package handle

import (
	"bytes"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
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

func NewEQFilter(v interface{}) *Filter {
	return &Filter{
		Op:  FilterEq,
		Val: v,
	}
}

type BlockReader interface {
	io.Closer
	ID() uint64
	String() string
	GetByFilter(filter Filter) (uint32, error)
	GetColumnDataByName(string, *bytes.Buffer, *bytes.Buffer) (*model.ColumnView, error)
	GetColumnDataById(int, *bytes.Buffer, *bytes.Buffer) (*model.ColumnView, error)
	GetMeta() interface{}
	Fingerprint() *common.ID
	Rows() int
	BatchDedup(col *vector.Vector) error

	IsAppendableBlock() bool

	GetSegment() Segment

	GetTotalChanges() int
}

type BlockWriter interface {
	io.Closer
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

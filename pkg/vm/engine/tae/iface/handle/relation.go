package handle

import (
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type Reader interface {
	Next(ctx interface{}, attrs []string) (*batch.Batch, error)
}

type Relation interface {
	io.Closer
	ID() uint64
	Rows() int64
	Size(attr string) int64
	String() string
	SimplePPString(common.PPLevel) string
	GetCardinality(attr string) int64
	Schema() interface{}
	MakeSegmentIt() SegmentIt
	MakeReader() Reader
	MakeBlockIt() BlockIt

	RangeDelete(id *common.ID, start, end uint32) error
	Update(id *common.ID, row uint32, col uint16, v interface{}) error
	GetByFilter(filter *Filter) (id *common.ID, offset uint32, err error)
	GetValue(id *common.ID, row uint32, col uint16) (interface{}, error)

	BatchDedup(col *vector.Vector) error
	Append(data *batch.Batch) error

	GetMeta() interface{}
	CreateSegment() (Segment, error)
	GetSegment(id uint64) (Segment, error)
}

type RelationIt interface {
	Iterator
	GetRelation() Relation
}

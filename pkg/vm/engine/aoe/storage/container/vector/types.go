package vector

import (
	"github.com/cockroachdb/errors"
	"io"
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/types"
	ro "matrixone/pkg/container/vector"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/container"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"sync"
)

var (
	VecWriteRoErr       = errors.New("write on readonly vector")
	VecInvalidOffsetErr = errors.New("invalid error")
)

type IVectorWriter interface {
	io.Closer
	SetValue(int, interface{})
	Append(int, interface{}) error
	AppendVector(*ro.Vector, int) (int, error)
}

type IVector interface {
	IsReadonly() bool
	dbi.IVectorReader
	IVectorWriter
	GetLatestView() IVector
	PlacementNew(t types.Type, capacity uint64)
}

type IVectorNode interface {
	buf.IMemoryNode
	IVector
}

type BaseVector struct {
	sync.RWMutex
	Type     types.Type
	StatMask container.Mask
	VMask    *nulls.Nulls
}

type StdVector struct {
	BaseVector
	Data         []byte
	FreeFunc     buf.MemoryFreeFunc
	NodeCapacity uint64
	AllocSize    uint64
}

type StrVector struct {
	BaseVector
	Data         *types.Bytes
	FreeFunc     buf.MemoryFreeFunc
	NodeCapacity uint64
	AllocSize    uint64
}

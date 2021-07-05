package vector

import (
	"github.com/cockroachdb/errors"
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/types"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/container"
	"sync"
)

var (
	VecWriteRoErr       = errors.New("write on readonly vector")
	VecInvalidOffsetErr = errors.New("invalid error")
)

type StdVector struct {
	sync.RWMutex
	Type         types.Type
	StatMask     container.Mask
	Data         []byte
	VMask        *nulls.Nulls
	FreeFunc     buf.MemoryFreeFunc
	NodeCapacity uint64
	AllocSize    uint64
}

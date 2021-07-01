package vector

import (
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/types"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"sync"

	// "sync"

	"github.com/cockroachdb/errors"
)

type Mask = uint64

const (
	ReadonlyMask Mask = 0x01000000
	HasNullMask  Mask = 0x02000000
	PosMask      Mask = 0x00FFFFFF
)

var (
	VecWriteRoErr       = errors.New("write on readonly vector")
	VecInvalidOffsetErr = errors.New("invalid error")
)

type StdVector struct {
	Type         types.Type
	StatMask     Mask
	Data         []byte
	MaskMtx      sync.RWMutex
	VMaskMtx     sync.RWMutex
	VMask        *nulls.Nulls
	FreeFunc     buf.MemoryFreeFunc
	NodeCapacity uint64
}

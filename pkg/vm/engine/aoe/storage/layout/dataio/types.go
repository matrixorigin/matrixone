package dataio

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
)

type Pointer struct {
	Offset int64
	Len    uint64
}

type Key struct {
	Col uint64
	ID  common.ID
	// BlockID uint64
	// PartID  uint32
}

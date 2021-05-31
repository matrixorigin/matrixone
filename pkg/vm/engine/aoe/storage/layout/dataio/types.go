package dataio

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout"
)

type Pointer struct {
	Offset int64
	Len    uint64
}

type Key struct {
	Col uint64
	ID  layout.ID
	// BlockID uint64
	// PartID  uint32
}

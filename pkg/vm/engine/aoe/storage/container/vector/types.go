package vector

import (
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/types"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
)

type StdVector struct {
	Type         types.Type
	Data         []byte
	VMask        *nulls.Nulls
	FreeFunc     buf.MemoryFreeFunc
	NodeCapacity uint64
}

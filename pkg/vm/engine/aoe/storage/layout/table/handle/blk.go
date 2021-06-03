package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
)

type BlockHandle struct {
	ID   common.ID
	Cols []col.IColumnBlock
}

func (bh *BlockHandle) Fetch() *chunk.Chunk {
	// TODO
	return nil
}

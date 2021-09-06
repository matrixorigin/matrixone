package base

import (
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type BlockFlusher = func(base.INode, batch.IBatch, *metadata.Block, *dataio.TransientBlockFile) error

type IMutableBlock interface {
	GetData() batch.IBatch
	GetFile() *dataio.TransientBlockFile
}

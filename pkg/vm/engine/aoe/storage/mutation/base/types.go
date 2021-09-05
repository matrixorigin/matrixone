package base

import (
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
)

type IMutableBlock interface {
	GetData() batch.IBatch
	GetFile() *dataio.TransientBlockFile
}

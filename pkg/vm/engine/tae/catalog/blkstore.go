package catalog

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"

type BlockStore struct {
	colFiles []common.IVFile
}

package gc

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"

type TableEntry struct {
	tid    uint64
	blocks []common.ID
	drop   bool
}

type ObjectEntry struct {
	common.RefHelper
	name  string
	table TableEntry
}

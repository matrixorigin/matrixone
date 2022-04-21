package txnimpl

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
)

type appendCtx struct {
	driver data.BlockAppender
	node   InsertNode
	start  uint32
	count  uint32
}

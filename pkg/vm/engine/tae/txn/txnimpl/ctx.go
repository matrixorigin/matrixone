package txnimpl

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"

type blockCompactionCtx struct {
	from, to *common.ID
}

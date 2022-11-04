package logtail

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
)

func TestTxnTable1(t *testing.T) {
	idAlloc := common.NewTxnIDAllocator()
	tsAlloc := types.GlobalTsAlloctor
	table := NewTxnTable(10, tsAlloc)
	for i := 0; i < 22; i++ {
		txn := new(txnbase.Txn)
		txn.TxnCtx = txnbase.NewTxnCtx(idAlloc.Alloc(), tsAlloc.Alloc(), nil)
		assert.NoError(t, table.AddTxn(txn))
	}
	t.Log(table.BlockCount())
	t.Log(table.String())
}

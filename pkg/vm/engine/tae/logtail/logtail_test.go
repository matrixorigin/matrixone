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
	timestamps := make([]types.TS, 0)
	fn1 := func(block *txnBlock) bool {
		timestamps = append(timestamps, block.bornTS)
		return true
	}
	table.Scan(fn1)
	assert.Equal(t, 3, len(timestamps))

	ckp := timestamps[0].Prev()
	cnt := table.TruncateByTimeStamp(ckp)
	assert.Equal(t, 0, cnt)

	ckp = timestamps[0].Next()
	cnt = table.TruncateByTimeStamp(ckp)
	assert.Equal(t, 0, cnt)

	ckp = timestamps[1].Prev()
	cnt = table.TruncateByTimeStamp(ckp)
	assert.Equal(t, 0, cnt)

	t.Log(table.String())
	ckp = timestamps[1].Next()
	cnt = table.TruncateByTimeStamp(ckp)
	assert.Equal(t, 1, cnt)
}

package tables

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "TAETABLES"
)

func TestInsertInfo(t *testing.T) {
	ts := common.NextGlobalSeqNum()
	capacity := uint32(10000)
	info := newInsertInfo(nil, ts, capacity)
	cnt := int(capacity) - 1
	now := time.Now()
	txns := make([]txnif.TxnReader, 0)
	for i := 0; i < cnt; i++ {
		txn := newMockTxn()
		txn.TxnCtx.CommitTS = common.NextGlobalSeqNum()
		txn.TxnCtx.State = txnif.TxnStateCommitted
		info.RecordTxnLocked(uint32(i), txn, nil)
		txns = append(txns, txn)
	}
	t.Logf("Record takes %s", time.Since(now))
	{
		txn := newMockTxn()
		txn.TxnCtx.CommitTS = common.NextGlobalSeqNum()
		txn.TxnCtx.State = txnif.TxnStateCommitted
		info.RecordTxnLocked(uint32(cnt), txn, nil)
		txns = append(txns, txn)
	}
	now = time.Now()

	t.Logf("Record takes %s", time.Since(now))
	// tsCol, _ := info.ts.CopyToVector()
	// t.Log(tsCol.String())
	now = time.Now()
	for _, txn := range txns {
		info.ApplyCommitLocked(txn)
	}

	t.Logf("Commit takes %s", time.Since(now))
	now = time.Now()
	offset := info.GetVisibleOffsetLocked(txns[0].GetStartTS())
	t.Logf("GetVisibleOffset takes %s", time.Since(now))
	assert.Equal(t, -1, offset)
	offset = info.GetVisibleOffsetLocked(txns[len(txns)-1].GetCommitTS())
	assert.Equal(t, int(capacity-1), offset)
}

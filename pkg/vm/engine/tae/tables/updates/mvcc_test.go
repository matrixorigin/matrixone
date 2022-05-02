package updates

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMutationControllerAppend(t *testing.T) {
	mc := NewMVCCHandle(nil)

	nodeCnt := 10000
	rowsPerNode := uint32(5)
	ts := uint64(2)
	queries := make([]uint64, 0)
	queries = append(queries, ts-1)
	for i := 0; i < nodeCnt; i++ {
		txn := mockTxn()
		txn.CommitTS = ts
		node := mc.AddAppendNodeLocked(txn, rowsPerNode*(uint32(i)+1))
		node.ApplyCommit(nil)
		queries = append(queries, ts+1)
		ts += 2
	}

	st := time.Now()
	for i, qts := range queries {
		row, ok := mc.GetMaxVisibleRowLocked(qts)
		if i == 0 {
			assert.False(t, ok)
		} else {
			assert.True(t, ok)
			assert.Equal(t, uint32(i)*rowsPerNode, row)
		}
	}
	t.Logf("%s -- %d ops", time.Since(st), len(queries))
}

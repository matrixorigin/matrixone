// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtail

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
)

func TestTxnTable1(t *testing.T) {
	txnCnt := 22
	blockSize := 10
	idAlloc := common.NewTxnIDAllocator()
	tsAlloc := types.GlobalTsAlloctor
	table := NewTxnTable(blockSize, tsAlloc)
	for i := 0; i < txnCnt; i++ {
		txn := new(txnbase.Txn)
		txn.TxnCtx = txnbase.NewTxnCtx(idAlloc.Alloc(), tsAlloc.Alloc(), nil)
		txn.PrepareTS = tsAlloc.Alloc()
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

	cnt := 0

	op := func(row RowT) (goNext bool) {
		cnt++
		return true
	}

	table.ForeachRowInBetween(
		timestamps[0].Prev(),
		types.MaxTs(),
		nil,
		op,
	)
	assert.Equal(t, txnCnt, cnt)

	cnt = 0
	table.ForeachRowInBetween(
		timestamps[1],
		types.MaxTs(),
		nil,
		op,
	)
	assert.Equal(t, txnCnt-blockSize, cnt)

	cnt = 0
	table.ForeachRowInBetween(
		timestamps[2],
		types.MaxTs(),
		nil,
		op,
	)
	assert.Equal(t, txnCnt-2*blockSize, cnt)

	ckp := timestamps[0].Prev()
	cnt = table.TruncateByTimeStamp(ckp)
	assert.Equal(t, 0, cnt)

	// these two are in first block, do not delete
	ckp = timestamps[0].Next()
	cnt = table.TruncateByTimeStamp(ckp)
	assert.Equal(t, 0, cnt)

	ckp = timestamps[1].Prev()
	cnt = table.TruncateByTimeStamp(ckp)
	assert.Equal(t, 0, cnt)

	// do not delete if truncate all
	assert.Equal(t, 0, table.TruncateByTimeStamp(types.MaxTs()))
	assert.Equal(t, 0, table.TruncateByTimeStamp(types.MaxTs()))

	ckp = timestamps[1].Next()
	cnt = table.TruncateByTimeStamp(ckp)
	assert.Equal(t, 1, cnt)

	ckp = timestamps[2]
	cnt = table.TruncateByTimeStamp(ckp)
	// 2 blocks left and skip deleting only one block
	assert.Equal(t, 0, cnt)
}

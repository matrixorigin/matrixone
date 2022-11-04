package logtail

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type txnRow struct {
	txnif.AsyncTxn
}

func (row *txnRow) Length() int             { return 1 }
func (row *txnRow) Window(_, _ int) *txnRow { return nil }

type txnBlock struct {
	sync.RWMutex
	bornTS types.TS
	rows   []*txnRow
}

func (blk *txnBlock) Length() int {
	blk.RLock()
	defer blk.RUnlock()
	return len(blk.rows)
}

func (blk *txnBlock) IsAppendable() bool {
	return blk != nil
}

func (blk *txnBlock) Append(row *txnRow) (err error) {
	blk.Lock()
	defer blk.Unlock()
	blk.rows = append(blk.rows, row)
	return
}

func (blk *txnBlock) Close() {
	blk.Lock()
	defer blk.Unlock()
	blk.bornTS = types.TS{}
	blk.rows = make([]*txnRow, 0)
}

func (blk *txnBlock) String() string {
	length := blk.Length()
	var buf bytes.Buffer
	_, _ = buf.WriteString(
		fmt.Sprintf("TXNBLK-[%s][Len=%d]", blk.bornTS.ToString(), length))
	return buf.String()
}

type TxnTable struct {
	*model.AOT[*txnBlock, *txnRow]
}

func blockCompareFn(a, b *txnBlock) bool {
	return a.bornTS.Less(b.bornTS)
}

func timeBasedTruncateFactory(ts types.TS) func(b *txnBlock) bool {
	return func(b *txnBlock) bool {
		return b.bornTS.GreaterEq(ts)
	}
}

func NewTxnTable(blockSize int, clock *types.TsAlloctor) *TxnTable {
	factory := func() *txnBlock {
		return &txnBlock{
			bornTS: clock.Alloc(),
			rows:   make([]*txnRow, 0),
		}
	}
	return &TxnTable{
		AOT: model.NewAOT[
			*txnBlock,
			*txnRow](
			blockSize,
			factory,
			blockCompareFn,
		),
	}
}

func (table *TxnTable) AddTxn(txn txnif.AsyncTxn) (err error) {
	row := &txnRow{
		AsyncTxn: txn,
	}
	err = table.Append(row)
	return
}

func (table *TxnTable) TruncateByTimeStamp(ts types.TS) (cnt int) {
	filter := timeBasedTruncateFactory(ts)
	return table.Truncate(filter)
}

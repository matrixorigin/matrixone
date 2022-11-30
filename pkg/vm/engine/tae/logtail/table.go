package logtail

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type RowT = *txnRow
type BlockT = *txnBlock

// type TableReader struct {
// 	from, to types.TS
// 	it       btree.GenericIter[*txnBlock]
// }

// func (r *TableReader) GetDirty() (tree *common.Tree, count int) {
// 	tree = common.NewTree()
// 	op := func(txn txnif.AsyncTxn) (moveOn bool) {
// 		if memo := txn.GetMemo(); memo.HasAnyTableDataChanges() {
// 			tree.Merge(memo.GetDirty())
// 		}
// 		count++
// 		return true
// 	}
// 	r.readTxnInBetween(r.from, r.to, op)
// }

// func (r *TableReader) foreach(
// 	tableView *TxnTable,
// 	from, to types.TS,
// 	op func(row RowT) (goNext bool),
// ) {
// 	pivot := &txnBlock{bornTS: from}

// }

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

func (blk *txnBlock) ForeachRowInBetween(
	from, to types.TS,
	op func(row RowT) (goNext bool),
) (outOfRange bool) {
	blk.RLock()
	rows := blk.rows[:len(blk.rows)]
	blk.RUnlock()
	for _, row := range rows {
		ts := row.GetPrepareTS()
		if ts.IsEmpty() || ts.Greater(to) {
			outOfRange = true
			return
		}
		if ts.Less(from) {
			logutil.Infof("ts %s, from %s", ts.ToString(), from.ToString())
			continue
		}

		if !op(row) {
			outOfRange = true
			return false
		}
	}
	return
}

func (blk *txnBlock) String() string {
	length := blk.Length()
	var buf bytes.Buffer
	_, _ = buf.WriteString(
		fmt.Sprintf("TXNBLK-[%s][Len=%d]", blk.bornTS.ToString(), length))
	return buf.String()
}

type TxnTable struct {
	*model.AOT[BlockT, RowT]
}

func blockCompareFn(a, b BlockT) bool {
	return a.bornTS.Less(b.bornTS)
}

func timeBasedTruncateFactory(ts types.TS) func(b BlockT) bool {
	return func(b BlockT) bool {
		return b.bornTS.GreaterEq(ts)
	}
}

func NewTxnTable(blockSize int, clock *types.TsAlloctor) *TxnTable {
	factory := func(row RowT) BlockT {
		return &txnBlock{
			bornTS: row.GetStartTS(),
			rows:   make([]*txnRow, 0, blockSize),
		}
	}
	return &TxnTable{
		AOT: model.NewAOT[
			BlockT,
			RowT](
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

func (table *TxnTable) ForeachRowInBetween(
	from, to types.TS,
	op func(row RowT) (goNext bool),
) {
	snapshot := table.BlocksSnapshot()
	pivot := &txnBlock{bornTS: from}
	snapshot.Descend(pivot, func(blk BlockT) bool {
		pivot.bornTS = blk.bornTS
		return false
	})
	snapshot.Ascend(pivot, func(blk BlockT) bool {
		if blk.bornTS.Greater(to) {
			return false
		}
		outOfRange := blk.ForeachRowInBetween(
			from,
			to,
			op,
		)

		return !outOfRange
	})
}

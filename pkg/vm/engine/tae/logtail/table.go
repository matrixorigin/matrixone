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
	"bytes"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type RowT = *txnRow
type BlockT = *txnBlock

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
			continue
		}

		if !op(row) {
			outOfRange = true
			return
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
		ts := row.GetPrepareTS()
		if ts == txnif.UncommitTS {
			ts = clock.Alloc()
		}
		return &txnBlock{
			bornTS: ts,
			rows:   make([]*txnRow, 0, blockSize),
		}
	}
	return &TxnTable{
		AOT: model.NewAOT(
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
	snapshot := table.Snapshot()
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

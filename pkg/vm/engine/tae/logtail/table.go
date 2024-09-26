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
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"go.uber.org/zap"
)

type RowT = *txnRow
type BlockT = *txnBlock

type smallTxn struct {
	memo      *txnif.TxnMemo
	startTS   types.TS
	prepareTS types.TS
	state     txnif.TxnState
	lsn       uint64
}

func (txn *smallTxn) GetMemo() *txnif.TxnMemo {
	return txn.memo
}

func (txn *smallTxn) GetTxnState(_ bool) txnif.TxnState {
	return txn.state
}

func (txn *smallTxn) GetStartTS() types.TS {
	return txn.startTS
}

func (txn *smallTxn) GetPrepareTS() types.TS {
	return txn.prepareTS
}

func (txn *smallTxn) GetCommitTS() types.TS {
	return txn.prepareTS
}

func (txn *smallTxn) GetLSN() uint64 {
	return txn.lsn
}

type summary struct {
	hasCatalogChanges bool
	tids              map[uint64]struct{}
	// TODO
	// maxLsn
}

type txnRow struct {
	source atomic.Pointer[txnbase.Txn]
	packed atomic.Pointer[smallTxn]
}

func (row *txnRow) GetLSN() uint64 {
	if txn := row.source.Load(); txn != nil {
		return txn.GetLSN()
	}
	txn := row.packed.Load()
	return txn.GetLSN()
}

func (row *txnRow) GetMemo() *txnif.TxnMemo {
	if txn := row.source.Load(); txn != nil {
		return txn.GetMemo()
	}
	txn := row.packed.Load()
	return txn.GetMemo()
}

func (row *txnRow) GetTxnState(waitIfcommitting bool) txnif.TxnState {
	if txn := row.source.Load(); txn != nil {
		return txn.GetTxnState(waitIfcommitting)
	}
	txn := row.packed.Load()
	return txn.GetTxnState(waitIfcommitting)
}

func (row *txnRow) GetStartTS() types.TS {
	if txn := row.source.Load(); txn != nil {
		return txn.GetStartTS()
	}
	txn := row.packed.Load()
	return txn.GetStartTS()
}

func (row *txnRow) GetPrepareTS() types.TS {
	if txn := row.source.Load(); txn != nil {
		return txn.GetPrepareTS()
	}
	txn := row.packed.Load()
	return txn.GetPrepareTS()
}

func (row *txnRow) GetCommitTS() types.TS {
	if txn := row.source.Load(); txn != nil {
		return txn.GetCommitTS()
	}
	txn := row.packed.Load()
	return txn.GetCommitTS()
}

func (row *txnRow) Length() int             { return 1 }
func (row *txnRow) Window(_, _ int) *txnRow { return nil }

func (row *txnRow) IsCompacted() bool {
	return row.packed.Load() != nil
}

func (row *txnRow) TryCompact() (compacted, changed bool) {
	if txn := row.source.Load(); txn == nil {
		return true, false
	} else {
		state := txn.GetTxnState(false)
		if state == txnif.TxnStateCommitted || state == txnif.TxnStateRollbacked {
			packed := &smallTxn{
				memo:      txn.GetMemo(),
				startTS:   txn.GetStartTS(),
				prepareTS: txn.GetPrepareTS(),
				state:     state,
				lsn:       txn.GetLSN(),
			}
			row.packed.Store(packed)
			row.source.Store(nil)
			return true, true
		}
	}
	return false, false
}

type txnBlock struct {
	sync.RWMutex
	bornTS  types.TS
	rows    []*txnRow
	summary atomic.Pointer[summary]
}

func (blk *txnBlock) Length() int {
	blk.RLock()
	defer blk.RUnlock()
	return len(blk.rows)
}

func (blk *txnBlock) TryCompact() (all bool, changed bool) {
	blk.RLock()
	defer blk.RUnlock()
	if len(blk.rows) < cap(blk.rows) {
		return false, false
	}
	if blk.rows[len(blk.rows)-1].IsCompacted() {
		return true, false
	}
	all = true
	for _, row := range blk.rows {
		if compacted, _ := row.TryCompact(); !compacted {
			all = false
			break
		}
	}
	if all {
		changed = true
	}
	return
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

func (blk *txnBlock) trySumary() {
	summary := new(summary)
	summary.tids = make(map[uint64]struct{}, 8)
	for _, row := range blk.rows {
		memo := row.GetMemo()
		if !summary.hasCatalogChanges && memo.HasCatalogChanges() {
			summary.hasCatalogChanges = true
		}
		for k := range memo.Tree.Tables {
			summary.tids[k] = struct{}{}
		}
	}
	blk.summary.CompareAndSwap(nil, summary)
}

func (blk *txnBlock) ForeachRowInBetween(
	from, to types.TS,
	rowOp func(row RowT) (goNext bool),
) (outOfRange bool, readRows int) {
	var rows []*txnRow
	if blk.summary.Load() == nil {
		blk.RLock()
		rows = blk.rows[:len(blk.rows)]
		capacity := cap(blk.rows)
		blk.RUnlock()
		if capacity == len(rows) && blk.summary.Load() == nil {
			blk.trySumary()
		}
	} else {
		rows = blk.rows
	}
	for _, row := range rows {
		readRows += 1
		ts := row.GetPrepareTS()
		if ts.IsEmpty() || ts.GT(&to) {
			outOfRange = true
			return
		}
		if ts.LT(&from) {
			continue
		}

		if !rowOp(row) {
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

func (blk *txnBlock) Less(b BlockT) bool {
	return blk.bornTS.LT(&b.bornTS)
}

func timeBasedTruncateFactory(ts types.TS) func(b BlockT) bool {
	return func(b BlockT) bool {
		return b.bornTS.GE(&ts)
	}
}

func NewTxnTable(blockSize int, nowClock func() types.TS) *TxnTable {
	factory := func(row RowT) BlockT {
		ts := row.GetPrepareTS()
		if ts == txnif.UncommitTS {
			ts = nowClock()
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
			(*txnBlock).Less,
		),
	}
}

func (table *TxnTable) TryCompact(from types.TS, rt *dbutils.Runtime) (to types.TS) {
	snapshot := table.Snapshot()
	snapshot.Ascend(
		&txnBlock{bornTS: from},
		func(blk BlockT) bool {
			allCompacted, changed := blk.TryCompact()
			if changed {
				rt.Logtail.CompactStats.Add(1)
			}
			to = blk.bornTS
			return allCompacted
		})
	return
}

func (table *TxnTable) AddTxn(txn txnif.AsyncTxn) (err error) {
	row := &txnRow{}
	row.source.Store(txn.GetBase().(*txnbase.Txn))
	err = table.Append(row)
	return
}

func (table *TxnTable) TruncateByTimeStamp(ts types.TS) (cnt int) {
	filter := timeBasedTruncateFactory(ts)
	return table.Truncate(filter)
}

func (table *TxnTable) ForeachRowInBetween(
	from, to types.TS,
	skipBlkOp func(blk BlockT) bool,
	rowOp func(row RowT) (goNext bool),
) (readRows int) {
	snapshot := table.Snapshot()
	pivot := &txnBlock{bornTS: from}
	outOfLeft := true
	snapshot.Descend(pivot, func(blk BlockT) bool {
		pivot.bornTS = blk.bornTS
		outOfLeft = false
		return false
	})

	// from is smaller than the very first block and it is not special like 0-0, 0-1, 1-0
	ts := types.BuildTS(1, 1)
	if outOfLeft && from.GT(&ts) {
		minTs := types.TS{}
		snapshot.Ascend(&txnBlock{}, func(blk *txnBlock) bool {
			minTs = blk.bornTS
			return false
		})
		logutil.Info("[logtail] fetch with too small ts", zap.String("ts", from.ToString()), zap.String("minTs", minTs.ToString()))
	}
	snapshot.Ascend(pivot, func(blk BlockT) bool {
		if blk.bornTS.GT(&to) {
			return false
		}

		if skipBlkOp != nil && skipBlkOp(blk) {
			prepareTS := blk.rows[len(blk.rows)-1].GetPrepareTS()
			return prepareTS.LE(&to)
		}
		outOfRange, cnt := blk.ForeachRowInBetween(
			from,
			to,
			rowOp,
		)
		readRows += cnt

		return !outOfRange
	})
	return
}

// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tables

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type insertInfo struct {
	rwlocker   *sync.RWMutex
	offsets    vector.IVector
	ts         vector.IVector
	offsetTxns map[uint32]txnif.TxnReader
	txnMap     map[uint64]uint32
	maxTs      uint64
	minTs      uint64
	maxOffset  uint32
}

func newInsertInfo(rwlocker *sync.RWMutex, maxTs uint64, capacity uint32) *insertInfo {
	if rwlocker == nil {
		rwlocker = new(sync.RWMutex)
	}
	return &insertInfo{
		rwlocker: rwlocker,
		offsets: vector.NewVector(types.Type{
			Oid:   types.T(types.T_uint32),
			Size:  4,
			Width: 32},
			uint64(capacity)),
		ts: vector.NewVector(
			types.Type{
				Oid:   types.T(types.T_uint64),
				Size:  8,
				Width: 64},
			uint64(capacity)),
		offsetTxns: make(map[uint32]txnif.TxnReader),
		txnMap:     make(map[uint64]uint32),
		maxTs:      maxTs,
		minTs:      maxTs,
	}
}

func (info *insertInfo) RecordTxnLocked(offset uint32, txn txnif.TxnReader) {
	pos := uint32(info.offsets.Length())
	info.offsets.Append(1, []uint32{offset})
	info.ts.Append(1, []uint64{txn.GetCommitTS()})
	info.txnMap[txn.GetID()] = pos
	info.offsetTxns[pos] = txn
	info.maxTs = txn.GetCommitTS()
	info.maxOffset = offset
}

func (info *insertInfo) ApplyCommitLocked(txn txnif.TxnReader) error {
	pos := info.txnMap[txn.GetID()]
	delete(info.txnMap, txn.GetID())
	delete(info.offsetTxns, pos)
	return nil
}

func (info *insertInfo) GetVisibleOffsetLocked(ts uint64) int {
	if ts >= info.maxTs {
		return int(info.maxOffset)
	}
	offset := -1
	pos := -1
	l := 0
	h := info.ts.Length() - 1
	for {
		if l > h {
			break
		}
		m := (l + h) / 2
		v, _ := info.ts.GetValue(m)
		vv := v.(uint64)
		if vv < ts {
			l = m + 1
			pos = m
		} else if vv > ts {
			h = m - 1
		} else {
			pos = m
			break
		}
	}
	if pos >= 0 {
		tsv, _ := info.offsets.GetValue(pos)
		offset = int(tsv.(uint32))
	}
	return offset
}

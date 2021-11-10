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

package metadata

import (
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type PPLevel uint8

const (
	PPL0 PPLevel = iota
	PPL1
	PPL2
)

type BaseEntry struct {
	sync.RWMutex
	Id         uint64      `json:"id"`
	CommitInfo *CommitInfo `json:"commit"`
}

func (e *BaseEntry) GetShardId() uint64 {
	return e.GetCommit().GetShardId()
}

func (e *BaseEntry) LatestLogIndexLocked() *LogIndex {
	return e.CommitInfo.LogIndex
}

func (e *BaseEntry) LatestLogIndex() *LogIndex {
	e.RLock()
	defer e.RUnlock()
	return e.LatestLogIndexLocked()
}

func (e *BaseEntry) FirstCommitLocked() *CommitInfo {
	var ret *CommitInfo
	fn := func(info *CommitInfo) bool {
		ret = info
		return true
	}
	e.ForEachCommitLocked(fn)
	return ret
}

func (e *BaseEntry) GetCommit() *CommitInfo {
	e.RLock()
	defer e.RUnlock()
	return e.CommitInfo
}

func (e *BaseEntry) IsFullLocked() bool {
	return e.CommitInfo.Op == OpUpgradeFull
}

func (e *BaseEntry) IsCloseLocked() bool {
	return e.CommitInfo.Op == OpUpgradeClose
}

func (e *BaseEntry) IsSortedLocked() bool {
	return e.CommitInfo.Op == OpUpgradeSorted
}

func (e *BaseEntry) checkStale1(info *CommitInfo) error {
	if e.CommitInfo == nil || e.CommitInfo.LogIndex == nil {
		return nil
	}
	switch info.Op {
	case OpHardDelete:
		return nil
	case OpReplaced:
		return nil
	case OpUpgradeFull:
		return nil
	}
	comp := e.CommitInfo.LogIndex.Compare(info.LogIndex)
	if comp > 0 {
		return CommitStaleErr
	} else if comp == 0 && !e.CommitInfo.SameTran(info) {
		logutil.Error(e.PString(PPL1))
		logutil.Error(info.PString(PPL1))
		return CommitStaleErr
	}
	return nil
}

func (e *BaseEntry) onCommit(info *CommitInfo) error {
	err := e.checkStale1(info)
	if err != nil {
		return err
	}
	info.SetNext(e.CommitInfo)
	e.CommitInfo = info
	return nil
}

func (e *BaseEntry) PString(level PPLevel) string {
	return e.CommitInfo.PString(level)
}

func (e *BaseEntry) GetAppliedIndex() (uint64, bool) {
	curr := e.CommitInfo
	id, ok := curr.GetAppliedIndex()
	if ok {
		return id, ok
	}
	next := curr.GetNext()
	for next != nil {
		id, ok = next.(*CommitInfo).GetAppliedIndex()
		if ok {
			return id, ok
		}
		next = next.GetNext()
	}
	return id, ok
}

func (e *BaseEntry) HasCommittedLocked() bool {
	return e.CommitInfo.HasCommitted()
}

func (e *BaseEntry) HasCommitted() bool {
	e.RLock()
	defer e.RUnlock()
	return e.HasCommittedLocked()
}

func (e *BaseEntry) CanUseTxn(tranId uint64) bool {
	e.RLock()
	defer e.RUnlock()
	return e.CanUseTxnLocked(tranId)
}

func (e *BaseEntry) CanUseTxnLocked(tranId uint64) bool {
	return e.CommitInfo.CanUseTxn(tranId)
}

func (e *BaseEntry) onCommitted(id uint64) *BaseEntry {
	if e.CommitInfo.CommitId > id {
		return nil
	}
	return &BaseEntry{
		Id:         e.Id,
		CommitInfo: e.CommitInfo,
	}
}

func (e *BaseEntry) UseCommitted(filter *commitFilter) *BaseEntry {
	e.RLock()
	defer e.RUnlock()
	return e.UseCommittedLocked(filter)
}

func (e *BaseEntry) UseCommittedLocked(filter *commitFilter) *BaseEntry {
	var curr common.ISSLLNode
	curr = e.CommitInfo
	for curr != nil {
		info := curr.(*CommitInfo)
		if filter.Eval(info) && !filter.EvalStop(info) {
			cInfo := info.Clone()
			return &BaseEntry{
				Id:         e.Id,
				CommitInfo: cInfo,
			}
		} else if filter.EvalStop(info) {
			return nil
		}
		curr = curr.GetNext()
	}
	return nil
}

func (e *BaseEntry) ForEachCommitLocked(fn func(*CommitInfo) bool) {
	var curr common.ISSLLNode
	curr = e.CommitInfo
	for curr != nil {
		info := curr.(*CommitInfo)
		if ok := fn(info); !ok {
			break
		}
		curr = curr.GetNext()
	}
	return
}

func (e *BaseEntry) FindCommitByIndexLocked(index *LogIndex) *CommitInfo {
	var found *CommitInfo
	fn := func(info *CommitInfo) bool {
		comp := info.LogIndex.Compare(index)
		if comp == 0 {
			found = info
			return false
		}
		if comp < 0 && info.IsDeleted() {
			return false
		}
		return true
	}
	e.ForEachCommitLocked(fn)
	return found
}

func (e *BaseEntry) IsDeletedInTxnLocked(txn *TxnCtx) bool {
	if txn == nil {
		return e.IsDeletedLocked()
	}
	if e.CanUseTxnLocked(txn.tranId) {
		return e.IsDeletedLocked()
	}

	var isDeleted bool
	fn := func(info *CommitInfo) bool {
		if info.CanUseTxn(txn.tranId) {
			isDeleted = info.IsDeleted()
			return false
		}
		return true
	}
	e.ForEachCommitLocked(fn)
	return isDeleted
}

// Guarded by e.Lock()
func (e *BaseEntry) IsSoftDeletedLocked() bool {
	return e.CommitInfo.IsSoftDeleted()
}

func (e *BaseEntry) IsDeletedLocked() bool {
	return e.CommitInfo.IsDeleted()
}

func (e *BaseEntry) IsReplacedLocked() bool {
	return e.CommitInfo.IsReplaced()
}

func (e *BaseEntry) IsDeleted() bool {
	e.RLock()
	defer e.RUnlock()
	return e.IsDeletedLocked()
}

func (e *BaseEntry) IsSoftDeleted() bool {
	e.RLock()
	defer e.RUnlock()
	return e.CommitInfo.IsSoftDeleted()
}

func (e *BaseEntry) IsHardDeletedLocked() bool {
	return e.CommitInfo.IsHardDeleted()
}

func (e *BaseEntry) IsHardDeleted() bool {
	e.RLock()
	defer e.RUnlock()
	return e.CommitInfo.IsHardDeleted()
}

func (e *BaseEntry) CommitLocked(id uint64) {
	if IsTransientCommitId(id) {
		panic(fmt.Sprintf("Cannot commit transient id %d", id))
	}
	if e.HasCommittedLocked() {
		panic(fmt.Sprintf("Cannot commit committed entry: %s", e.PString(PPL0)))
	}
	e.CommitInfo.CommitId = id
}

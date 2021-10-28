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
	Id         uint64
	CommitInfo *CommitInfo
}

func (e *BaseEntry) GetShardId() uint64 {
	return e.GetCommit().GetShardId()
}

func (e *BaseEntry) GetFirstCommit() *CommitInfo {
	prev := e.CommitInfo
	curr := prev.GetNext()
	for curr != nil {
		prev = curr.(*CommitInfo)
		curr = curr.GetNext()
	}
	return prev
}

func (e *BaseEntry) GetCommit() *CommitInfo {
	e.RLock()
	defer e.RUnlock()
	return e.CommitInfo
}

// Should be guarded
func (e *BaseEntry) IsFull() bool {
	return e.CommitInfo.Op == OpUpgradeFull
}

// Should be guarded
func (e *BaseEntry) IsClose() bool {
	return e.CommitInfo.Op == OpUpgradeClose
}

// Should be guarded
func (e *BaseEntry) IsSorted() bool {
	return e.CommitInfo.Op == OpUpgradeSorted
}

func (e *BaseEntry) onNewCommit(info *CommitInfo) {
	info.SetNext(e.CommitInfo)
	e.CommitInfo = info
}

func (e *BaseEntry) PString(level PPLevel) string {
	s := fmt.Sprintf("Id=%d,%s", e.Id, e.CommitInfo.PString(level))
	return s
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

// Guarded by entry mutex
func (e *BaseEntry) HasCommittedLocked() bool {
	return !IsTransientCommitId(e.CommitInfo.CommitId)
}

func (e *BaseEntry) HasCommitted() bool {
	e.RLock()
	defer e.RUnlock()
	return !IsTransientCommitId(e.CommitInfo.CommitId)
}

func (e *BaseEntry) CanUse(tranId uint64) bool {
	e.RLock()
	defer e.RUnlock()
	if e.HasCommittedLocked() && e.CommitInfo.TranId > tranId {
		return true
	}
	return tranId == e.CommitInfo.TranId
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
	var curr common.ISSLLNode
	curr = e.CommitInfo
	for curr != nil {
		info := curr.(*CommitInfo)
		if filter.Eval(info) {
			cInfo := *info
			return &BaseEntry{
				Id:         e.Id,
				CommitInfo: &cInfo,
			}
		}
		curr = curr.GetNext()
	}
	return nil
}

// Guarded by e.Lock()
func (e *BaseEntry) IsSoftDeletedLocked() bool {
	return e.CommitInfo.IsSoftDeleted()
}

func (e *BaseEntry) IsDeletedLocked() bool {
	return e.IsSoftDeletedLocked() || e.IsHardDeletedLocked()
}

func (e *BaseEntry) IsDeleted() bool {
	e.RLock()
	defer e.RUnlock()
	return e.IsSoftDeletedLocked() || e.IsHardDeletedLocked()
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
		panic(fmt.Sprintf("Cannot commit committed entry"))
	}
	e.CommitInfo.CommitId = id
}
